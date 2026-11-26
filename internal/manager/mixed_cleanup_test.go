/*
 * Copyright 2026 The HAMi Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package manager_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/Project-HAMi/ascend-device-plugin/internal/cleanup"
	"github.com/Project-HAMi/ascend-device-plugin/internal/manager"
)

// Record cleanup scans separately from the whole endpoint's health checks
// in UpdateDevice. All hardware operations go through the manager.
type recordingManager struct {
	*manager.AscendManager
	scanned   []int32
	destroyed [][2]int32
}

func (m *recordingManager) ListVirtualDevices(id int32) ([]manager.VirtualDevice, error) {
	m.scanned = append(m.scanned, id)
	return m.AscendManager.ListVirtualDevices(id)
}

func (m *recordingManager) DestroyVirtualDevice(id int32, vdev uint32) error {
	m.destroyed = append(m.destroyed, [2]int32{id, int32(vdev)})
	return m.AscendManager.DestroyVirtualDevice(id, vdev)
}

func TestMixedWholePodIdleVNPUCleanup(t *testing.T) {
	for _, tc := range []struct {
		name, wholeAlias string
		pod, checkpoint  bool
	}{
		{name: "no_whole_allocation"},
		{name: "whole_pod_and_checkpoint", pod: true, checkpoint: true},
		{name: "stale_whole_checkpoint", checkpoint: true},
		{name: "configured_whole_alias", wholeAlias: "huawei.com/AscendDedicated", pod: true, checkpoint: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := manager.NewMixedCleanupTestManager(t, tc.wholeAlias)
			whole := mgr.MixedDevices().WholeResource()
			client := fake.NewSimpleClientset()
			if tc.pod {
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "whole", Namespace: "default", UID: "whole-uid"},
					Spec: corev1.PodSpec{NodeName: "node-1", Containers: []corev1.Container{{
						Name: "worker", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
							corev1.ResourceName(whole): resource.MustParse("2"),
						}},
					}}},
					Status: corev1.PodStatus{Phase: corev1.PodRunning},
				}
				client = fake.NewSimpleClientset(pod)
			}
			prefix := strings.TrimPrefix(whole, "huawei.com/")
			ids := fmt.Sprintf(`["%s-0","%s-1"]`, prefix, prefix)
			entries := "[]"
			if tc.checkpoint {
				entries = fmt.Sprintf(`[{"PodUID":"whole-uid","ResourceName":%q,"DeviceIDs":{"0":%s}}]`, whole, ids)
			}
			data := fmt.Sprintf(`{"Data":{"PodDeviceEntries":%s,"RegisteredDevices":{%q:%s,%q:["die-7-0","die-5-0"]}}}`, entries, whole, ids, mgr.ResourceName())
			path := filepath.Join(t.TempDir(), "kubelet_internal_checkpoint")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			snapshot, err := cleanup.NewKubernetesPodSnapshot(client, "node-1", mgr, path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(snapshot.Stop)
			ctx := context.Background()
			deadline := time.Now().Add(5 * time.Second)
			for {
				_, err := snapshot.Snapshot(ctx)
				if err == nil {
					break
				}
				if err.Error() != "pod informer cache is not synced" || time.Now().After(deadline) {
					t.Fatal(err)
				}
				time.Sleep(10 * time.Millisecond)
			}

			now := time.Unix(100, 0)
			hw := &recordingManager{AscendManager: mgr}
			r := cleanup.NewReconciler(hw, snapshot, cleanup.ReconcilerOptions{
				GracePeriod: time.Minute, Confirmations: 2, Clock: func() time.Time { return now },
			})
			if _, err := r.Reconcile(ctx); err != nil || len(hw.destroyed) != 0 {
				t.Fatalf("first reconciliation: destroyed=%v err=%v", hw.destroyed, err)
			}
			now = now.Add(time.Minute)
			result, err := r.Reconcile(ctx)
			if err != nil || result.VNPUsDestroyed != 1 || result.VNPUsSkipped != 1 ||
				!slices.Equal(hw.destroyed, [][2]int32{{7, 100}}) {
				t.Fatalf("result=%+v destroyed=%v err=%v, want only idle vdev 100 on logic ID 7", result, hw.destroyed, err)
			}
			if !slices.Equal(hw.scanned, []int32{7, 5, 7, 5}) {
				t.Fatalf("cleanup scanned %v, want only the two shared logic IDs on each pass", hw.scanned)
			}
		})
	}
}
