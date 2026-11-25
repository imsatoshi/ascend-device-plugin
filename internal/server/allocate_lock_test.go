package server

import (
	"context"
	"strings"
	"testing"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/util"
	"github.com/Project-HAMi/HAMi/pkg/util/client"
	"github.com/Project-HAMi/ascend-device-plugin/internal/manager"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

func TestAllocateRejectsStaleRequestWithoutMutatingOwner(t *testing.T) {
	for _, scenario := range []string{"missing lock", "different device", "successful", "wrong UID", "running"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			t.Cleanup(setupInRequestDevices(testCommonWord))
			ps := &PluginServer{nodeName: "test-node", commonWord: testCommonWord, mgr: &FakeManager{},
				toAllocDeviceAnno: device.InRequestDevices[testCommonWord]}
			allocation := device.EncodePodSingleDevice(device.PodSingleDevice{{cd("assigned-card", testCommonWord, 16384, 4)}})
			node, pod, cleanup := setupAllocateEnv(ps.nodeName, "current", "default", 1, map[string]string{
				ps.toAllocDeviceAnno: allocation, util.DeviceBindPhase: util.DeviceBindAllocating,
				util.BindTimeAnnotations: "1", util.AssignedNodeAnnotations: ps.nodeName,
			})
			t.Cleanup(cleanup)
			id := "assigned-card-0"
			switch scenario {
			case "missing lock":
				delete(node.Annotations, NodeLockAscend)
			case "different device":
				id = "old-card-0"
			case "successful":
				pod.Annotations[util.DeviceBindPhase] = util.DeviceBindSuccess
			case "wrong UID":
				node.Annotations[volcanoLockOwner] = "deleted-uid"
			case "running":
				pod.Status.Phase = v1.PodRunning
			}
			kube := client.GetClient()
			nodes := kube.CoreV1().Nodes()
			pods := kube.CoreV1().Pods(pod.Namespace)
			if _, err := nodes.Update(ctx, node, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := pods.Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			_, err := ps.Allocate(ctx, &v1beta1.AllocateRequest{ContainerRequests: []*v1beta1.ContainerAllocateRequest{{DevicesIds: []string{id}}}})
			if err == nil {
				t.Fatal("accepted an unassociated Allocate request")
			}
			if scenario == "different device" && !strings.Contains(err.Error(), "device IDs do not match") {
				t.Fatal(err)
			}
			got, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if got.Annotations[util.DeviceBindPhase] != pod.Annotations[util.DeviceBindPhase] || got.Annotations[ps.toAllocDeviceAnno] != allocation {
				t.Fatalf("stale request modified owner: %v", got.Annotations)
			}
			gotNode, err := nodes.Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil || gotNode.Annotations[NodeLockAscend] != node.Annotations[NodeLockAscend] {
				t.Fatalf("owner lock changed: %v, %v", gotNode, err)
			}
		})
	}
}

func TestPreferredAllocationThenAllocateTwoContainers(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(setupInRequestDevices(testCommonWord))
	ps := &PluginServer{nodeName: "test-node", commonWord: testCommonWord,
		allocAnno: "huawei.com/Ascend910", toAllocDeviceAnno: device.InRequestDevices[testCommonWord],
		mgr: &FakeManager{GetDeviceByUUIDFunc: func(uuid string) *manager.Device {
			return &manager.Device{UUID: uuid, PhyID: 3}
		}},
	}
	allocation := device.EncodePodSingleDevice(device.PodSingleDevice{
		{cd("first-card", testCommonWord, 16384, 4)}, {cd("second-card", testCommonWord, 16384, 4)},
	})
	node, pod, cleanup := setupAllocateEnv(ps.nodeName, "multi", "default", 2, map[string]string{
		ps.toAllocDeviceAnno: allocation, util.DeviceBindPhase: util.DeviceBindAllocating,
		ps.allocAnno: `[{"UUID":"first-card","temp":"vir05_1c_16g"},{"UUID":"second-card","temp":"vir05_1c_16g"}]`,
	})
	t.Cleanup(cleanup)
	kube := client.GetClient()
	options, err := ps.GetDevicePluginOptions(ctx, &v1beta1.Empty{})
	if err != nil || !options.GetPreferredAllocationAvailable {
		t.Fatalf("preferred option: %v, %v", options, err)
	}
	for i, expected := range []string{"first-card-2", "second-card-1"} {
		preferred, err := ps.GetPreferredAllocation(ctx, &v1beta1.PreferredAllocationRequest{
			ContainerRequests: []*v1beta1.ContainerPreferredAllocationRequest{{AllocationSize: 1,
				AvailableDeviceIDs: []string{"old-card-0", "second-card-1", "first-card-2"}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		ids := preferred.ContainerResponses[0].DeviceIDs
		if len(ids) != 1 || ids[0] != expected {
			t.Fatalf("preferred %v, want %q", ids, expected)
		}
		if _, err := ps.Allocate(ctx, &v1beta1.AllocateRequest{ContainerRequests: []*v1beta1.ContainerAllocateRequest{{DevicesIds: ids}}}); err != nil {
			t.Fatal(err)
		}
		current, err := kube.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		currentNode, err := kube.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		wantPhase, wantLock := util.DeviceBindAllocating, node.Annotations[NodeLockAscend]
		if i == 1 {
			wantPhase, wantLock = util.DeviceBindSuccess, ""
		}
		if current.Annotations[util.DeviceBindPhase] != wantPhase || currentNode.Annotations[NodeLockAscend] != wantLock {
			t.Fatalf("container %d published phase %q and lock %q, want %q and %q", i, current.Annotations[util.DeviceBindPhase], currentNode.Annotations[NodeLockAscend], wantPhase, wantLock)
		}
	}
}
