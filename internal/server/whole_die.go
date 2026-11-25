package server

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Project-HAMi/ascend-device-plugin/internal"
	"github.com/Project-HAMi/ascend-device-plugin/internal/manager"
	plugin "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const wholeDieResource = "huawei.com/Ascend910"

type mixedDeviceManager interface {
	MixedDevices() *internal.MixedDevices
	WholeDevices() []*manager.Device
}

type wholeDieServer struct {
	plugin.UnimplementedDevicePluginServer
	mgr mixedDeviceManager
}

func wholeDieID(phyID int32) string {
	return "Ascend910-" + strconv.FormatInt(int64(phyID), 10)
}

func (s *wholeDieServer) devices() []*plugin.Device {
	var devices []*plugin.Device
	for _, die := range s.mgr.WholeDevices() {
		health := plugin.Unhealthy
		if die.Health {
			health = plugin.Healthy
		}
		devices = append(devices, &plugin.Device{ID: wholeDieID(die.PhyID), Health: health})
	}
	slices.SortFunc(devices, func(a, b *plugin.Device) int { return strings.Compare(a.ID, b.ID) })
	return devices
}

func (s *wholeDieServer) GetDevicePluginOptions(context.Context, *plugin.Empty) (*plugin.DevicePluginOptions, error) {
	return &plugin.DevicePluginOptions{GetPreferredAllocationAvailable: true}, nil
}

func (s *wholeDieServer) ListAndWatch(_ *plugin.Empty, stream plugin.DevicePlugin_ListAndWatchServer) error {
	poll := time.NewTicker(time.Second)
	defer poll.Stop()
	var previous map[string]string
	for {
		devices := s.devices()
		current := make(map[string]string, len(devices))
		for _, d := range devices {
			current[d.ID] = d.Health
		}
		if previous == nil || !maps.Equal(previous, current) {
			if err := stream.Send(&plugin.ListAndWatchResponse{Devices: devices}); err != nil {
				return err
			}
			previous = current
		}
		select {
		case <-poll.C:
		case <-stream.Context().Done():
			return nil
		}
	}
}

// selectGroup is also used by Allocate because kubelet may fall back to
// selecting devices itself after calling GetPreferredAllocation.
func (s *wholeDieServer) selectGroup(available, required []string, count int) ([]string, error) {
	if count <= 0 || len(required) > count {
		return nil, fmt.Errorf("invalid whole-die allocation size %d", count)
	}
	healthy := make(map[string]bool)
	for _, d := range s.devices() {
		healthy[d.ID] = d.Health == plugin.Healthy
	}
	for _, group := range s.mgr.MixedDevices().WholeDieGroups {
		eligible := make(map[string]bool)
		for _, phy := range group {
			id := wholeDieID(phy)
			if healthy[id] && slices.Contains(available, id) {
				eligible[id] = true
			}
		}
		if len(eligible) < count {
			continue
		}
		ids := make([]string, 0, count)
		for _, id := range required {
			if !eligible[id] {
				break
			}
			ids = append(ids, id)
			delete(eligible, id)
		}
		if len(ids) != len(required) {
			continue
		}
		for _, phy := range group {
			id := wholeDieID(phy)
			if len(ids) == count {
				break
			}
			if eligible[id] {
				ids = append(ids, id)
			}
		}
		return ids, nil
	}
	return nil, fmt.Errorf("no single HCCS group can satisfy the whole-die allocation")
}

func (s *wholeDieServer) GetPreferredAllocation(_ context.Context, req *plugin.PreferredAllocationRequest) (*plugin.PreferredAllocationResponse, error) {
	resp := &plugin.PreferredAllocationResponse{}
	for _, ctr := range req.GetContainerRequests() {
		ids, err := s.selectGroup(ctr.GetAvailableDeviceIDs(), ctr.GetMustIncludeDeviceIDs(), int(ctr.GetAllocationSize()))
		if err != nil {
			return nil, err
		}
		resp.ContainerResponses = append(resp.ContainerResponses, &plugin.ContainerPreferredAllocationResponse{DeviceIDs: ids})
	}
	return resp, nil
}

func (s *wholeDieServer) Allocate(_ context.Context, req *plugin.AllocateRequest) (*plugin.AllocateResponse, error) {
	resp := &plugin.AllocateResponse{}
	for _, ctr := range req.GetContainerRequests() {
		requested := ctr.GetDevicesIds()
		ids, err := s.selectGroup(requested, requested, len(requested))
		if err != nil {
			return nil, err
		}
		physical := make([]int, 0, len(ids))
		for _, id := range ids {
			phy, err := strconv.Atoi(strings.TrimPrefix(id, "Ascend910-"))
			if err != nil {
				return nil, err
			}
			physical = append(physical, phy)
		}
		slices.Sort(physical)
		visible := make([]string, 0, len(ids))
		for _, phy := range physical {
			visible = append(visible, strconv.Itoa(phy))
		}
		// MindCluster's useAscendDocker=true physical allocation delegates
		// /dev/davinci* and the management devices to Ascend Docker Runtime.
		resp.ContainerResponses = append(resp.ContainerResponses, &plugin.ContainerAllocateResponse{Envs: map[string]string{
			"ASCEND_VISIBLE_DEVICES": strings.Join(visible, ","),
			"ASCEND_RUNTIME_OPTIONS": "",
		}})
	}
	return resp, nil
}
