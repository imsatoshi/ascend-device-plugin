package server

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/util"
	"github.com/Project-HAMi/HAMi/pkg/util/client"
	"github.com/Project-HAMi/HAMi/pkg/util/nodelock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

const (
	volcanoLockOwner   = "volcano.sh/ascend-bind-lock-owner"
	volcanoLockAttempt = "volcano.sh/ascend-bind-lock-attempt"
	volcanoPodAttempt  = "volcano.sh/ascend-bind-attempt"
)

type allocationLock struct {
	value   string
	owner   string
	attempt string
}

func readAllocationLock(node *v1.Node) allocationLock {
	return allocationLock{node.Annotations[NodeLockAscend], node.Annotations[volcanoLockOwner], node.Annotations[volcanoLockAttempt]}
}

func (ps *PluginServer) lockedPendingPod(ctx context.Context) (*v1.Pod, allocationLock, error) {
	kube := client.GetClient()
	node, err := kube.CoreV1().Nodes().Get(ctx, ps.nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, allocationLock{}, err
	}
	lock := readAllocationLock(node)
	_, ns, name, err := nodelock.ParseNodeLock(lock.value)
	if err != nil || ns == "" || name == "" {
		return nil, lock, fmt.Errorf("no named Ascend allocation lock on node %s", ps.nodeName)
	}
	pod, err := kube.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, lock, err
	}
	if pod.Spec.NodeName != ps.nodeName || pod.DeletionTimestamp != nil ||
		pod.Status.Phase != v1.PodPending || pod.Annotations[util.DeviceBindPhase] != util.DeviceBindAllocating ||
		(lock.owner != "" && lock.owner != string(pod.UID)) ||
		(lock.attempt != "" && pod.Annotations[volcanoPodAttempt] != lock.attempt) {
		return nil, lock, fmt.Errorf("lock owner %s/%s is not a Pending Pod with an active Ascend allocation", ns, name)
	}
	return pod, lock, nil
}

func slotDeviceUUID(id string) string {
	dash := strings.LastIndexByte(id, '-')
	if dash <= 0 {
		return ""
	}
	slot, err := strconv.Atoi(id[dash+1:])
	if err != nil || slot < 0 || id[dash+1:] != strconv.Itoa(slot) {
		return ""
	}
	return id[:dash]
}

func requestedDevicesMatch(want device.ContainerDevices, ids []string) bool {
	if len(want) != len(ids) {
		return false
	}
	counts := make(map[string]int, len(want))
	for _, dev := range want {
		counts[dev.UUID]++
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		uuid := slotDeviceUUID(id)
		if uuid == "" || seen[id] || counts[uuid] == 0 {
			return false
		}
		seen[id] = true
		counts[uuid]--
	}
	return true
}

func preferredSlots(want device.ContainerDevices, available, required []string) ([]string, error) {
	ids := slices.Clone(required)
	pool := slices.Clone(available)
	slices.Sort(pool)
	used := make(map[string]bool, len(ids))
	counts := make(map[string]int, len(want))
	for _, dev := range want {
		counts[dev.UUID]++
	}
	for _, id := range ids {
		uuid := slotDeviceUUID(id)
		if used[id] || counts[uuid] == 0 || !slices.Contains(pool, id) {
			return nil, fmt.Errorf("required device %q does not match the locked Pod", id)
		}
		used[id] = true
		counts[uuid]--
	}
	for _, id := range pool {
		uuid := slotDeviceUUID(id)
		if !used[id] && counts[uuid] > 0 {
			used[id] = true
			counts[uuid]--
			ids = append(ids, id)
		}
	}
	if len(ids) != len(want) {
		return nil, fmt.Errorf("kubelet has insufficient slots on the devices assigned to the locked Pod")
	}
	return ids, nil
}

func (ps *PluginServer) patchAllocation(ctx context.Context, pod *v1.Pod, lock allocationLock, annotations map[string]string) error {
	kube := client.GetClient()
	node, err := kube.CoreV1().Nodes().Get(ctx, ps.nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if readAllocationLock(node) != lock {
		return fmt.Errorf("Ascend allocation lock changed for %s/%s", pod.Namespace, pod.Name)
	}
	payload, err := json.Marshal(map[string]any{"metadata": map[string]any{
		"uid": pod.UID, "resourceVersion": pod.ResourceVersion, "annotations": annotations,
	}})
	if err != nil {
		return err
	}
	_, err = kube.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.MergePatchType, payload, metav1.PatchOptions{})
	return err
}

func (ps *PluginServer) releaseAllocation(ctx context.Context, lock allocationLock) error {
	nodes := client.GetClient().CoreV1().Nodes()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := nodes.Get(ctx, ps.nodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if readAllocationLock(node) != lock {
			return nil
		}
		payload, err := json.Marshal(map[string]any{"metadata": map[string]any{
			"resourceVersion": node.ResourceVersion, "annotations": map[string]any{
				NodeLockAscend: nil, volcanoLockOwner: nil, volcanoLockAttempt: nil,
			},
		}})
		if err != nil {
			return err
		}
		_, err = nodes.Patch(ctx, node.Name, types.MergePatchType, payload, metav1.PatchOptions{})
		return err
	})
}
