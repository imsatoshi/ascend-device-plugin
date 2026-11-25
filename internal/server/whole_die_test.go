package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/Project-HAMi/ascend-device-plugin/internal"
	"github.com/Project-HAMi/ascend-device-plugin/internal/manager"
	plugin "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type mixedManagerFixture struct {
	*FakeManager
	config internal.MixedDevices
	whole  []*manager.Device
}

func (m *mixedManagerFixture) MixedDevices() *internal.MixedDevices { return &m.config }
func (m *mixedManagerFixture) WholeDevices() []*manager.Device      { return m.whole }

func TestWholeDieAllocation(t *testing.T) {
	mgr := &mixedManagerFixture{
		config: internal.MixedDevices{WholeDieGroups: [][]int32{{0, 1}, {2, 3}}, SharedDies: []int32{4}},
		FakeManager: &FakeManager{VDeviceCountFunc: func() int { return 4 },
			GetDevicesFunc: func() []*manager.Device { return []*manager.Device{{UUID: "shared", PhyID: 4, Health: true}} }},
	}
	for id := int32(0); id < 4; id++ {
		mgr.whole = append(mgr.whole, &manager.Device{PhyID: id, Health: true})
	}
	server := &wholeDieServer{mgr: mgr}
	shared := &PluginServer{mgr: mgr}
	if got := server.devices(); len(got) != 4 || got[0].ID != "Ascend910-0" {
		t.Fatalf("whole publication: %v", got)
	}
	if got := shared.apiDevices(); len(got) != 4 || got[0].ID != "shared-0" {
		t.Fatalf("shared publication: %v", got)
	}
	preferred, err := server.GetPreferredAllocation(context.Background(), &plugin.PreferredAllocationRequest{
		ContainerRequests: []*plugin.ContainerPreferredAllocationRequest{{
			AvailableDeviceIDs:   []string{"Ascend910-3", "Ascend910-2", "Ascend910-0", "Ascend910-1", "shared-0"},
			MustIncludeDeviceIDs: []string{"Ascend910-3"},
			AllocationSize:       2,
		}},
	})
	if err != nil || !reflect.DeepEqual(preferred.ContainerResponses[0].DeviceIDs, []string{"Ascend910-3", "Ascend910-2"}) {
		t.Fatalf("wrong HCCS locality: %v %v", preferred, err)
	}
	allocate := func(ids ...string) (*plugin.AllocateResponse, error) {
		return server.Allocate(context.Background(), &plugin.AllocateRequest{
			ContainerRequests: []*plugin.ContainerAllocateRequest{{DevicesIds: ids}},
		})
	}
	response, err := allocate("Ascend910-1", "Ascend910-0")
	if err != nil {
		t.Fatal(err)
	}
	// MindCluster with useAscendDocker=true, presetVDevice=true and physical IDs.
	want := &plugin.ContainerAllocateResponse{Envs: map[string]string{
		"ASCEND_VISIBLE_DEVICES": "0,1", "ASCEND_RUNTIME_OPTIONS": "",
	}}
	if !reflect.DeepEqual(response.ContainerResponses[0], want) {
		t.Fatalf("expected the vendor runtime contract, got %v", response)
	}
	for _, ids := range [][]string{{"shared-0"}, {"Ascend910-0", "Ascend910-3"}, {"Ascend910-1", "Ascend910-1"}} {
		if _, err := allocate(ids...); err == nil {
			t.Fatalf("accepted invalid request %v", ids)
		}
	}
}
