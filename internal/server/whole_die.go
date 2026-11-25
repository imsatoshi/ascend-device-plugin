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
	CheckWholeDie(*manager.Device) error
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
	healthy := make(map[string]*manager.Device)
	config := s.mgr.MixedDevices()
	ids := make(map[int32]string)
	for _, die := range s.mgr.WholeDevices() {
		if die.Health {
			id := wholeDieID(die.PhyID)
			healthy[id] = die
			ids[die.PartitionID(config)] = id
		}
	}
	var best []string
	bestFree := len(available) + 1
	bestSize := 0
	for _, group := range config.WholeDieGroups {
		var eligible []string
		for _, partitionID := range group {
			id := ids[partitionID]
			if healthy[id] != nil && slices.Contains(available, id) {
				eligible = append(eligible, id)
			}
		}
		if len(eligible) < count || len(eligible) > bestFree ||
			(len(eligible) == bestFree && len(group) >= bestSize) {
			continue
		}
		remaining := slices.Clone(eligible)
		for _, id := range required {
			i := slices.Index(remaining, id)
			if i < 0 {
				break
			}
			remaining = slices.Delete(remaining, i, i+1)
		}
		if len(remaining) != len(eligible)-len(required) {
			continue
		}
		best = fillFromModules(required, remaining, count, healthy)
		bestFree, bestSize = len(eligible), len(group)
	}
	if best != nil {
		return best, nil
	}
	return nil, fmt.Errorf("no single HCCS group can satisfy the whole-die allocation")
}

// Finish partially allocated modules, then use full pairs and spare dies.
// Split a healthy pair only when the requested count requires it.
func fillFromModules(required, available []string, count int, devices map[string]*manager.Device) []string {
	selected := slices.Clone(required)
	take := func(ids ...string) {
		if len(selected)+len(ids) <= count {
			selected = append(selected, ids...)
		}
	}
	requiredModules := make(map[int32]bool)
	for _, id := range required {
		requiredModules[devices[id].CardID] = true
	}
	modules := make(map[int32][]string)
	var order []int32
	for _, id := range available {
		card := devices[id].CardID
		if requiredModules[card] {
			take(id)
			continue
		}
		if len(modules[card]) == 0 {
			order = append(order, card)
		}
		modules[card] = append(modules[card], id)
	}
	var spare, deferred []string
	for _, card := range order {
		ids := modules[card]
		switch {
		case len(ids) == 1:
			spare = append(spare, ids...)
		case len(selected)+len(ids) <= count:
			take(ids...)
		default:
			deferred = append(deferred, ids...)
		}
	}
	for _, id := range append(spare, deferred...) {
		take(id)
	}
	return selected
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
	devices := make(map[string]*manager.Device)
	for _, dev := range s.mgr.WholeDevices() {
		devices[wholeDieID(dev.PhyID)] = dev
	}
	for _, ctr := range req.GetContainerRequests() {
		requested := ctr.GetDevicesIds()
		ids, err := s.selectGroup(requested, requested, len(requested))
		if err != nil {
			return nil, err
		}
		physical := make([]int, 0, len(ids))
		for _, id := range ids {
			dev := devices[id]
			if dev == nil || !dev.Health {
				return nil, fmt.Errorf("whole die %s is unavailable", id)
			}
			if err := s.mgr.CheckWholeDie(dev); err != nil {
				return nil, err
			}
			physical = append(physical, int(dev.PhyID))
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
