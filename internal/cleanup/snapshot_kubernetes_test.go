/*
 * Copyright 2026 The HAMi Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package cleanup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/Project-HAMi/ascend-device-plugin/internal"
	"github.com/Project-HAMi/ascend-device-plugin/internal/manager"
)

func TestKubernetesPodSnapshotMapsLivePodToCard(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "serve", UID: "pod-1"},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
			Containers: []corev1.Container{{
				Name: "worker",
				Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
					corev1.ResourceName("huawei.com/Ascend910B4"): resource.MustParse("1"),
				}},
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "worker",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	pod.Annotations = map[string]string{
		"huawei.com/Ascend910B4": `[{"UUID":"uuid-7","Temp":"vir05_1c_16g"}]`,
	}
	clientset := fake.NewSimpleClientset(pod)
	snapshot, err := NewKubernetesPodSnapshot(clientset, "node-1", fakeResolver{device: &manager.Device{UUID: "uuid-7", CardID: 7}}, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(snapshot.Stop)
	checkpoint := filepath.Join(t.TempDir(), "kubelet_internal_checkpoint")
	if err := os.WriteFile(checkpoint, []byte(`{"Data":{"PodDeviceEntries":[],"RegisteredDevices":{"huawei.com/Ascend910B4":["uuid-7"]}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot.checkpointPath = checkpoint
	waitForInformer(t, snapshot.podListerSynced)
	result, err := snapshot.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	claim, ok := result.ClaimsByCard[7]
	if !ok || claim.State != ClaimProtected {
		t.Fatalf("ClaimsByCard[7] = %+v, want protected claim", claim)
	}
}

func TestKubernetesPodSnapshotFailsClosedForUnknownCheckpointPod(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	snapshot, err := NewKubernetesPodSnapshot(clientset, "node-1", fakeResolver{device: &manager.Device{UUID: "uuid-7", CardID: 7}}, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(snapshot.Stop)
	checkpoint := filepath.Join(t.TempDir(), "kubelet_internal_checkpoint")
	if err := os.WriteFile(checkpoint, []byte(`{"Data":{"PodDeviceEntries":[{"PodUID":"gone","ResourceName":"huawei.com/Ascend910B4","DeviceIDs":{"0":["uuid-7-0"]}}],"RegisteredDevices":{"huawei.com/Ascend910B4":["uuid-7-0"]}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot.checkpointPath = checkpoint
	waitForInformer(t, snapshot.podListerSynced)
	result, err := snapshot.Snapshot(context.Background())
	if err != nil || !result.Ready || result.ClaimsByCard[7].State != ClaimProtected {
		t.Fatalf("Snapshot() = result=%+v err=%v, want mapped stale checkpoint to protect card 7", result, err)
	}
}

func TestKubernetesPodSnapshotAgesOutStaleCheckpointUID(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	snapshot, err := NewKubernetesPodSnapshot(clientset, "node-1", fakeResolver{device: &manager.Device{UUID: "uuid-7", CardID: 7}}, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(snapshot.Stop)
	checkpoint := filepath.Join(t.TempDir(), "kubelet_internal_checkpoint")
	if err := os.WriteFile(checkpoint, []byte(`{"Data":{"PodDeviceEntries":[{"PodUID":"gone","ResourceName":"huawei.com/Ascend910B4","DeviceIDs":{"0":["uuid-7-0"]}}],"RegisteredDevices":{"huawei.com/Ascend910B4":["uuid-7-0"]}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot.checkpointPath = checkpoint
	snapshot.staleGrace = time.Second
	snapshot.staleConfirmations = 2
	current := time.Unix(0, 0)
	snapshot.clock = func() time.Time { return current }
	waitForInformer(t, snapshot.podListerSynced)
	if result, err := snapshot.Snapshot(context.Background()); err != nil || !result.Ready || result.ClaimsByCard[7].State != ClaimProtected {
		t.Fatalf("first Snapshot() = result=%+v err=%v, want temporary card protection", result, err)
	}
	current = current.Add(time.Second)
	result, err := snapshot.Snapshot(context.Background())
	if err != nil || !result.Ready {
		t.Fatalf("second Snapshot() = result=%+v err=%v, want stale UID aged out", result, err)
	}
	if _, protected := result.ClaimsByCard[7]; protected {
		t.Fatalf("second Snapshot() retained stale checkpoint protection: %+v", result.ClaimsByCard[7])
	}
}

func TestKubernetesPodSnapshotIgnoresNonAscendCheckpointEntries(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	snapshot, err := NewKubernetesPodSnapshot(clientset, "node-1", fakeResolver{}, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(snapshot.Stop)
	checkpoint := filepath.Join(t.TempDir(), "kubelet_internal_checkpoint")
	if err := os.WriteFile(checkpoint, []byte(`{"Data":{"PodDeviceEntries":[{"PodUID":"net-pod","ResourceName":"intel.com/sriov","DeviceIDs":{"0":["vf-0"]}}],"RegisteredDevices":{"huawei.com/Ascend910B4":["uuid-7-0"]}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot.checkpointPath = checkpoint
	waitForInformer(t, snapshot.podListerSynced)
	result, err := snapshot.Snapshot(context.Background())
	if err != nil || !result.Ready || len(result.UnknownPodUIDs) != 0 {
		t.Fatalf("Snapshot() = result=%+v err=%v, want non-Ascend checkpoint entry ignored", result, err)
	}
}

func TestPodHasAscendResourceSupportsMultipleCardFamilies(t *testing.T) {
	var snapshot KubernetesPodSnapshot
	for _, resourceName := range []string{
		"huawei.com/Ascend310P",
		"huawei.com/Ascend310P3",
		"huawei.com/Ascend910",
		"huawei.com/Ascend910A",
		"huawei.com/Ascend910B3",
		"huawei.com/Ascend910B4-1",
		"huawei.com/Ascend910C",
	} {
		pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				corev1.ResourceName(resourceName): resource.MustParse("1"),
			}},
		}}}}
		if !snapshot.podHasAscendResource(pod) {
			t.Fatalf("podHasAscendResource(%q) = false, want true", resourceName)
		}
	}
}

func TestPodDeviceUUIDsAcceptsEmptyPostAllocationPlaceholder(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			"huawei.com/Ascend910B4":                    `[{"UUID":"uuid-7"}]`,
			"hami.io/Ascend910B4-1-devices-to-allocate": ";",
		}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				corev1.ResourceName("huawei.com/Ascend910B4"): resource.MustParse("1"),
			}},
		}}},
	}
	var snapshot KubernetesPodSnapshot
	hasResource, uuids, err := snapshot.podDeviceUUIDs(pod)
	if err != nil || !hasResource || len(uuids) != 1 || uuids[0] != "uuid-7" {
		t.Fatalf("podDeviceUUIDs() = resource=%v uuids=%v err=%v, want valid runtime UUID", hasResource, uuids, err)
	}
}

type fakeResolver struct {
	device *manager.Device
	mixed  *internal.MixedDevices
}

func (r fakeResolver) MixedDevices() *internal.MixedDevices { return r.mixed }
func (r fakeResolver) ResourceName() string                 { return "huawei.com/Ascend910B3" }

func (r fakeResolver) GetDeviceByUUID(uuid string) *manager.Device {
	if r.device != nil && r.device.UUID == uuid {
		return r.device
	}
	return nil
}

func waitForInformer(t *testing.T, synced func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !synced() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !synced() {
		t.Fatal("pod informer did not sync")
	}
}

func TestMixedSnapshotSharedClaims(t *testing.T) {
	const shared = "huawei.com/Ascend910B3"
	const wholeEntry = `{"PodUID":"pod-1","ResourceName":"huawei.com/Ascend910","DeviceIDs":{"0":["Ascend910-0","Ascend910-1","Ascend910-2","Ascend910-3"]}}`
	const sharedEntry = `{"PodUID":"pod-1","ResourceName":"huawei.com/Ascend910B3","DeviceIDs":{"0":["uuid-7-2"]}}`
	const badEntry = `{"PodUID":"pod-1","ResourceName":"huawei.com/Ascend910B3","DeviceIDs":{"0":["unknown-2"]}}`
	one := resource.MustParse("1")
	request := corev1.ResourceRequirements{Requests: corev1.ResourceList{shared: one}}
	limit := corev1.ResourceRequirements{Limits: corev1.ResourceList{shared: one}}
	for _, tc := range []struct {
		name          string
		resources     corev1.ResourceRequirements
		init          bool
		annotations   map[string]string
		extraEntries  string
		stale         bool
		wantProtected bool
	}{
		{name: "shared_init_request_missing_annotation", init: true, resources: request},
		{name: "shared_request_missing_annotation", resources: request},
		{name: "shared_init_limit_missing_annotation", init: true, resources: limit},
		{name: "shared_limit_missing_annotation", resources: limit},
		{name: "consumed_shared_annotation_without_runtime_uuid", annotations: map[string]string{"hami.io/Ascend910B3-devices-to-allocate": ";"}},
		{name: "malformed_annotation_only", annotations: map[string]string{"hami.io/Ascend910B3-devices-to-allocate": "broken"}},
		{name: "unresolved_shared_uuid", resources: limit, annotations: map[string]string{shared: `[{"UUID":"unknown"}]`}},
		{name: "partial_runtime_annotation", resources: limit, annotations: map[string]string{shared: `[{"UUID":"uuid-7"},{}]`}},
		{name: "shared_checkpoint_without_pod_mapping", extraEntries: "," + sharedEntry},
		{name: "unmapped_checkpoint_alongside_mapped_entry", resources: limit, annotations: map[string]string{shared: `[{"UUID":"uuid-7"}]`}, extraEntries: "," + badEntry + "," + sharedEntry},
		{name: "stale_combined_checkpoint", stale: true, extraEntries: "," + sharedEntry, wantProtected: true},
		{name: "stale_partly_unmappable_checkpoint", stale: true, extraEntries: "," + badEntry + "," + sharedEntry},
		{name: "mapped_combined_pod", init: true, resources: request, annotations: map[string]string{shared: `[{"UUID":"uuid-7"}]`}, extraEntries: "," + sharedEntry, wantProtected: true},
		{name: "shared_annotation_only", annotations: map[string]string{"hami.io/Ascend910B3-devices-to-allocate": "uuid-7,Ascend910B3,16384,5"}, wantProtected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "combined", UID: "pod-1", Annotations: tc.annotations},
				Spec: corev1.PodSpec{NodeName: "node-1", Containers: []corev1.Container{{
					Name: "whole", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
						"huawei.com/Ascend910": resource.MustParse("4"),
					}},
				}}},
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			}
			ctr := corev1.Container{Name: "shared", Resources: tc.resources}
			if tc.init {
				pod.Spec.InitContainers = []corev1.Container{ctr}
			} else {
				pod.Spec.Containers = append(pod.Spec.Containers, ctr)
			}
			hw := testHardware()
			resolver := fakeResolver{device: hw.devices[0], mixed: &internal.MixedDevices{}}
			client := fake.NewSimpleClientset(pod)
			if tc.stale {
				client = fake.NewSimpleClientset()
			}
			snapshot, err := NewKubernetesPodSnapshot(client, "node-1", resolver, "")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(snapshot.Stop)
			snapshot.readFile = func(string) ([]byte, error) {
				return []byte(fmt.Sprintf(`{"Data":{"PodDeviceEntries":[%s%s],"RegisteredDevices":{%q:["uuid-7-2"]}}}`, wholeEntry, tc.extraEntries, shared)), nil
			}
			waitForInformer(t, snapshot.podListerSynced)
			r := NewReconciler(hw, snapshot, ReconcilerOptions{})
			result, err := r.Reconcile(context.Background())
			if tc.wantProtected {
				if err != nil || result.CardsProtected != 1 || result.VNPUsInspected != 0 {
					t.Fatalf("expected protection for all siblings on the claimed card: result=%+v err=%v", result, err)
				}
			} else if err == nil || !result.UnknownSnapshot || result.CardsInspected != 0 {
				t.Fatalf("expected unmappable claim to stop cleanup: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestMixedSnapshotRequiresSharedRegistration(t *testing.T) {
	for _, tc := range []struct {
		name, registrations string
		mixed               *internal.MixedDevices
		wantReady           bool
	}{
		{name: "whole_only", mixed: &internal.MixedDevices{}, registrations: `"huawei.com/Ascend910":["Ascend910-0"]`},
		{name: "other_ascend", mixed: &internal.MixedDevices{}, registrations: `"huawei.com/Ascend910C":["uuid-0"]`},
		{name: "empty_shared", mixed: &internal.MixedDevices{}, registrations: `"huawei.com/Ascend910B3":[],"huawei.com/Ascend910":["Ascend910-0"]`},
		{name: "shared", mixed: &internal.MixedDevices{}, registrations: `"huawei.com/Ascend910B3":["uuid-7-0"]`, wantReady: true},
		{name: "pure", registrations: `"huawei.com/Ascend910":["Ascend910-0"]`, wantReady: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := NewKubernetesPodSnapshot(fake.NewSimpleClientset(), "node-1", fakeResolver{mixed: tc.mixed}, "")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(snapshot.Stop)
			snapshot.readFile = func(string) ([]byte, error) {
				return []byte(fmt.Sprintf(`{"Data":{"PodDeviceEntries":[],"RegisteredDevices":{%s}}}`, tc.registrations)), nil
			}
			waitForInformer(t, snapshot.podListerSynced)
			result, err := snapshot.Snapshot(context.Background())
			if result.Ready != tc.wantReady || (err == nil) != tc.wantReady {
				t.Fatalf("Snapshot() ready=%v err=%v, want ready=%v", result.Ready, err, tc.wantReady)
			}
		})
	}
}
