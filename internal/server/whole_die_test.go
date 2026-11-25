package server

import (
	"context"
	"errors"
	"github.com/Project-HAMi/HAMi/pkg/device"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"net"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Project-HAMi/ascend-device-plugin/internal"
	"github.com/Project-HAMi/ascend-device-plugin/internal/manager"
	plugin "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type mixedManagerFixture struct {
	*FakeManager
	config        internal.MixedDevices
	whole         []*manager.Device
	wholeCheckErr error
}

func (m *mixedManagerFixture) MixedDevices() *internal.MixedDevices { return &m.config }
func (m *mixedManagerFixture) WholeDevices() []*manager.Device      { return m.whole }

func (m *mixedManagerFixture) CheckWholeDie(*manager.Device) error { return m.wholeCheckErr }

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
	mgr.wholeCheckErr = errors.New("residual vNPU")
	if _, err := allocate("Ascend910-0"); !errors.Is(err, mgr.wholeCheckErr) {
		t.Fatalf("whole allocation accepted a die with a residual vNPU: %v", err)
	}
	mgr.wholeCheckErr = nil
	for _, ids := range [][]string{{"shared-0"}, {"Ascend910-0", "Ascend910-3"}, {"Ascend910-1", "Ascend910-1"}} {
		if _, err := allocate(ids...); err == nil {
			t.Fatalf("accepted invalid request %v", ids)
		}
	}
}

func TestWholeDieBestFitAndModulePairs(t *testing.T) {
	mgr := &mixedManagerFixture{config: internal.MixedDevices{
		WholeDieGroups: [][]int32{{0, 1, 2, 3, 4, 5, 6, 7}, {8, 9, 10, 11}},
	}}
	var available []string
	for i := int32(0); i < 12; i++ {
		card := i / 2
		if i >= 8 {
			card = 4 + i%2
		}
		mgr.whole = append(mgr.whole, &manager.Device{PhyID: i, CardID: card, Health: true})
		available = append(available, wholeDieID(i))
	}
	s := &wholeDieServer{mgr: mgr}
	for _, tc := range []struct {
		name                      string
		available, required, want []string
		size                      int
	}{
		{"best fit", available, nil, []string{available[8], available[10], available[9], available[11]}, 4},
		{"module pair", available, nil, []string{available[8], available[10]}, 2},
		{"honor required module", available, []string{available[9]}, []string{available[9], available[11]}, 2},
		{"consume singleton before splitting a pair", []string{available[8], available[9], available[10]}, nil, []string{available[9]}, 1},
		{"no cross-group fallback", []string{available[0], available[1], available[8], available[9]}, nil, nil, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.selectGroup(tc.available, tc.required, tc.size)
			if !reflect.DeepEqual(got, tc.want) || (err == nil) != (tc.want != nil) {
				t.Fatalf("selected %v (%v), want %v", got, err, tc.want)
			}
		})
	}
}

func TestMixedSharedAllocationGuard(t *testing.T) {
	dies := map[string]*manager.Device{"shared": {Health: true, PhyID: 4}, "unhealthy": {PhyID: 5}}
	ps := &PluginServer{mgr: &mixedManagerFixture{FakeManager: &FakeManager{
		GetDeviceByUUIDFunc: func(id string) *manager.Device { return dies[id] },
	}}}
	for _, tc := range []struct {
		name, mode, template string
		ids                  []string
		wantError            bool
	}{
		{"valid", VNPUModeTemplate, "vir05_1c_16g", []string{"shared"}, false},
		{"unhealthy", "", "vir05_1c_16g", []string{"unhealthy"}, true},
		{"two devices", "", "vir05_1c_16g", []string{"shared", "shared"}, true},
		{"wrong template", "", "vir06_1c_16g", []string{"shared"}, true},
		{"wrong mode", VNPUModeHamiCore, "vir05_1c_16g", []string{"shared"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{}
			pod.Annotations = map[string]string{VNPUModeAnnotation: tc.mode}
			var devices device.ContainerDevices
			info := map[string]RuntimeInfo{}
			for _, id := range tc.ids {
				devices = append(devices, device.ContainerDevice{UUID: id})
				info[id] = RuntimeInfo{Temp: tc.template}
			}
			resp, err := ps.buildContainerAllocateResponse(pod, "ctr", devices, info)
			if tc.wantError {
				if err == nil || err.Error() != "mixed shared pool requires one healthy die and the vir05_1c_16g template" {
					t.Fatalf("expected mixed shared isolation error, got %v", err)
				}
			} else if err != nil || resp.Envs["ASCEND_VISIBLE_DEVICES"] != "4" {
				t.Fatalf("expected shared die 4, got %v (%v)", resp, err)
			}
		})
	}
}

type recordingKubelet struct {
	plugin.UnimplementedRegistrationServer
	mu       sync.Mutex
	requests []*plugin.RegisterRequest
}

func (k *recordingKubelet) Register(_ context.Context, req *plugin.RegisterRequest) (*plugin.Empty, error) {
	k.mu.Lock()
	k.requests = append(k.requests, req)
	k.mu.Unlock()
	return &plugin.Empty{}, nil
}

func TestMixedWholeEndpointStart(t *testing.T) {
	dir := t.TempDir()
	// Unix socket paths on macOS are limited to 104 bytes. Use a short
	// filename and run both registrations through the fake kubelet socket.
	kubeletSocket := filepath.Join(dir, "k.sock")
	listener, err := net.Listen("unix", kubeletSocket)
	if err != nil {
		t.Fatal(err)
	}
	kubelet := grpc.NewServer()
	recorder := &recordingKubelet{}
	plugin.RegisterRegistrationServer(kubelet, recorder)
	go func() { _ = kubelet.Serve(listener) }()
	t.Cleanup(kubelet.Stop)
	ps := &PluginServer{socket: filepath.Join(dir, "share.sock"), mgr: &mixedManagerFixture{
		FakeManager: &FakeManager{ResourceNameFunc: func() string { return "huawei.com/Ascend910C" }},
		whole:       []*manager.Device{{PhyID: 0, Health: true}, {PhyID: 1}},
	}}
	ps.dialFunc = func(socket string, timeout time.Duration) (*grpc.ClientConn, error) {
		if socket == plugin.KubeletSocket {
			socket = kubeletSocket
		}
		return (&PluginServer{}).dial(socket, timeout)
	}
	t.Cleanup(func() { _ = ps.Stop() })
	if err := ps.Start(); err != nil {
		t.Fatal(err)
	}
	recorder.mu.Lock()
	requests := append([]*plugin.RegisterRequest(nil), recorder.requests...)
	recorder.mu.Unlock()
	if len(requests) != 2 || requests[0].ResourceName != "huawei.com/Ascend910C" ||
		requests[1].ResourceName != "huawei.com/Ascend910" ||
		requests[1].Endpoint != "hami-ascend-whole-die.sock" ||
		!requests[1].GetOptions().GetGetPreferredAllocationAvailable() {
		t.Fatalf("wrong kubelet registration: %v", requests)
	}
	conn, err := ps.dial(ps.wholeDieSocket(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := plugin.NewDevicePluginClient(conn).ListAndWatch(t.Context(), &plugin.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	update, err := stream.Recv()
	if err != nil || len(update.GetDevices()) != 2 || update.Devices[0].Health != plugin.Healthy || update.Devices[1].Health != plugin.Unhealthy {
		t.Fatalf("whole socket did not publish health for both dies: %v (%v)", update, err)
	}
}
