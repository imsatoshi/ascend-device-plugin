/*
 * Copyright 2026 The HAMi Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package cleanup

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Project-HAMi/ascend-device-plugin/internal"
	"github.com/Project-HAMi/ascend-device-plugin/internal/manager"
)

type fakeHardware struct {
	mixed            *internal.MixedDevices
	devices          []*manager.Device
	virtual          map[int32][]manager.VirtualDevice
	updateErr        error
	listErr          map[int32]error
	destroyErr       map[int32]error
	destroyErrByVDev map[int32]error
	destroyed        []manager.VirtualDevice
}

func (f *fakeHardware) MixedDevices() *internal.MixedDevices { return f.mixed }
func (f *fakeHardware) UpdateDevice() error                  { return f.updateErr }
func (f *fakeHardware) GetDevices() []*manager.Device        { return f.devices }
func (f *fakeHardware) ListVirtualDevices(logicID int32) ([]manager.VirtualDevice, error) {
	if err := f.listErr[logicID]; err != nil {
		return nil, err
	}
	return append([]manager.VirtualDevice(nil), f.virtual[logicID]...), nil
}
func (f *fakeHardware) DestroyVirtualDevice(logicID int32, vdevID uint32) error {
	if err := f.destroyErr[logicID]; err != nil {
		return err
	}
	if err := f.destroyErrByVDev[int32(vdevID)]; err != nil {
		return err
	}
	remaining := f.virtual[logicID][:0]
	for _, vdev := range f.virtual[logicID] {
		if vdev.VDevID == int32(vdevID) {
			f.destroyed = append(f.destroyed, vdev)
			continue
		}
		remaining = append(remaining, vdev)
	}
	f.virtual[logicID] = remaining
	return nil
}

type fakeSnapshot struct {
	snapshot AllocationSnapshot
	err      error
}

type sequenceSnapshot struct {
	snapshots []AllocationSnapshot
	calls     int
}

func (s *sequenceSnapshot) Snapshot(context.Context) (AllocationSnapshot, error) {
	index := s.calls
	if index >= len(s.snapshots) {
		index = len(s.snapshots) - 1
	}
	s.calls++
	return s.snapshots[index], nil
}

func (f fakeSnapshot) Snapshot(context.Context) (AllocationSnapshot, error) {
	return f.snapshot, f.err
}

func TestReconcilerNeverDestroysWhenSnapshotIsUnknown(t *testing.T) {
	hardware := testHardware()
	r := NewReconciler(hardware, fakeSnapshot{err: errors.New("api unavailable")}, ReconcilerOptions{
		GracePeriod:   time.Nanosecond,
		Confirmations: 1,
		Clock:         func() time.Time { return time.Unix(0, 0) },
	})
	result, err := r.Reconcile(context.Background())
	if err == nil || !result.UnknownSnapshot {
		t.Fatalf("Reconcile() = result=%+v err=%v, want unknown snapshot error", result, err)
	}
	if len(hardware.destroyed) != 0 {
		t.Fatalf("destroyed %d vNPUs for an unknown snapshot", len(hardware.destroyed))
	}
}

func TestReconcilerResetAllClearsVnpuObservations(t *testing.T) {
	r := NewReconciler(testHardware(), fakeSnapshot{snapshot: AllocationSnapshot{Ready: true}}, ReconcilerOptions{})
	r.seenVNPUs[virtualDeviceKey{logicID: 3, vdevID: 11}] = time.Unix(0, 0)
	var result ReconcileResult
	r.resetAll(&result)
	if len(r.seenVNPUs) != 0 {
		t.Fatalf("seenVNPUs after resetAll() = %v, want empty", r.seenVNPUs)
	}
}

func TestReconcilerRechecksOwnershipBeforeDestroy(t *testing.T) {
	hardware := testHardware()
	snapshot := &sequenceSnapshot{snapshots: []AllocationSnapshot{
		{Ready: true},
		{Ready: true},
		{Ready: true, ClaimsByCard: map[int32]CardClaim{7: {CardID: 7, State: ClaimProtected}}},
	}}
	now := time.Unix(0, 0)
	r := NewReconciler(hardware, snapshot, ReconcilerOptions{
		GracePeriod:   time.Nanosecond,
		Confirmations: 1,
		Clock:         func() time.Time { return now },
	})
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(hardware.destroyed) != 0 {
		t.Fatalf("destroyed=%v after ownership appeared between snapshots", hardware.destroyed)
	}
}

func TestReconcilerRequiresFreshGraceForNewVnpu(t *testing.T) {
	hardware := testHardware()
	now := time.Unix(0, 0)
	r := NewReconciler(hardware, fakeSnapshot{snapshot: AllocationSnapshot{Ready: true}}, ReconcilerOptions{
		GracePeriod:   10 * time.Minute,
		Confirmations: 1,
		Clock:         func() time.Time { return now },
	})
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(10 * time.Minute)
	hardware.virtual[3] = append(hardware.virtual[3], manager.VirtualDevice{LogicID: 3, CardID: 7, VDevID: 13, IsContainerUsed: 0})
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(hardware.destroyed) != 1 || hardware.destroyed[0].VDevID != 11 {
		t.Fatalf("destroyed=%v, want only pre-existing vdev 11", hardware.destroyed)
	}
}

func TestReconcilerProtectsCardWithLivePod(t *testing.T) {
	hardware := testHardware()
	r := NewReconciler(hardware, fakeSnapshot{snapshot: AllocationSnapshot{
		Ready: true,
		ClaimsByCard: map[int32]CardClaim{
			7: {CardID: 7, State: ClaimProtected},
		},
	}}, ReconcilerOptions{GracePeriod: time.Nanosecond, Confirmations: 1})
	result, err := r.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if result.CardsProtected != 1 || len(hardware.destroyed) != 0 {
		t.Fatalf("result=%+v destroyed=%v, want protected card and no destroy", result, hardware.destroyed)
	}
}

func TestReconcilerReclaimsOnlyIdleVNPUsAfterConfirmation(t *testing.T) {
	hardware := testHardware()
	now := time.Unix(100, 0)
	r := NewReconciler(hardware, fakeSnapshot{snapshot: AllocationSnapshot{Ready: true}}, ReconcilerOptions{
		GracePeriod:   10 * time.Minute,
		Confirmations: 3,
		Clock:         func() time.Time { return now },
	})
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(context.Background()); err != nil {
			t.Fatalf("confirmation %d: Reconcile() error = %v", i, err)
		}
		if len(hardware.destroyed) != 0 {
			t.Fatalf("destroyed before grace/confirmation: %v", hardware.destroyed)
		}
		now = now.Add(10 * time.Minute)
	}
	result, err := r.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("final Reconcile() error = %v", err)
	}
	if len(hardware.destroyed) != 1 || hardware.destroyed[0].VDevID != 11 {
		t.Fatalf("destroyed=%v, want only idle vdev 11", hardware.destroyed)
	}
	if result.VNPUsSkipped != 1 || result.VNPUsDestroyed != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestReconcilerEmptySiblingConfirmationByLayout(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		mixed                     *internal.MixedDevices
		wantResets, wantDestroyed int
	}{
		{name: "mixed", mixed: &internal.MixedDevices{}, wantDestroyed: 1},
		{name: "pure", wantResets: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hw := testHardware() // logic ID 3 has one idle and one used vNPU
			hw.mixed = tc.mixed
			hw.devices = append(hw.devices, &manager.Device{LogicID: 5, CardID: 7})
			now := time.Unix(100, 0)
			r := NewReconciler(hw, fakeSnapshot{snapshot: AllocationSnapshot{Ready: true}}, ReconcilerOptions{
				GracePeriod: time.Minute, Confirmations: 2, Clock: func() time.Time { return now },
			})
			first, err := r.Reconcile(context.Background())
			if err != nil || first.ConfirmationResets != tc.wantResets || len(hw.destroyed) != 0 {
				t.Fatalf("before grace: result=%+v destroyed=%v err=%v", first, hw.destroyed, err)
			}
			now = now.Add(time.Minute)
			second, err := r.Reconcile(context.Background())
			if err != nil || second.ConfirmationResets != tc.wantResets || len(hw.destroyed) != tc.wantDestroyed {
				t.Fatalf("after grace: result=%+v destroyed=%v err=%v", second, hw.destroyed, err)
			}
			if tc.wantDestroyed == 1 && (hw.destroyed[0].LogicID != 3 || hw.destroyed[0].VDevID != 11) {
				t.Fatalf("destroyed=%v, want idle vdev 11 on logic ID 3", hw.destroyed)
			}
		})
	}
}

func TestReconcilerMixedCardClaimResetsBothDiesBeforeDestroy(t *testing.T) {
	hw := testHardware()
	hw.mixed = &internal.MixedDevices{}
	hw.devices = append(hw.devices, &manager.Device{LogicID: 5, CardID: 7})
	snapshot := &sequenceSnapshot{snapshots: []AllocationSnapshot{
		{Ready: true},
		{Ready: true},
		{Ready: true, ClaimsByCard: map[int32]CardClaim{7: {CardID: 7, State: ClaimProtected}}},
		{Ready: true},
	}}
	now := time.Unix(100, 0)
	r := NewReconciler(hw, snapshot, ReconcilerOptions{
		GracePeriod: time.Minute, Confirmations: 2, Clock: func() time.Time { return now },
	})
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Only die 3 has enough history to trigger the second snapshot.
	hw.virtual[5] = []manager.VirtualDevice{{LogicID: 5, CardID: 7, VDevID: 13}}
	now = now.Add(time.Minute)
	result, err := r.Reconcile(context.Background())
	if err != nil || result.ConfirmationResets != 2 || len(hw.destroyed) != 0 {
		t.Fatalf("late card claim: result=%+v destroyed=%v err=%v", result, hw.destroyed, err)
	}
	now = now.Add(time.Minute)
	if _, err := r.Reconcile(context.Background()); err != nil || len(hw.destroyed) != 0 {
		t.Fatalf("first observation after claim cleared: destroyed=%v err=%v", hw.destroyed, err)
	}
}

func TestReconcilerUsesLatestCandidateFailureTime(t *testing.T) {
	hardware := testHardware()
	now := time.Unix(0, 0)
	snapshot := &mutableSnapshot{snapshot: AllocationSnapshot{Ready: true}}
	r := NewReconciler(hardware, snapshot, ReconcilerOptions{
		GracePeriod:   10 * time.Minute,
		Confirmations: 1,
		Clock:         func() time.Time { return now },
	})
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = time.Unix(20*60, 0)
	snapshot.snapshot.ClaimsByCard = map[int32]CardClaim{
		7: {CardID: 7, State: ClaimCandidate, CandidateSince: time.Unix(15*60, 0)},
	}
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(hardware.destroyed) != 0 {
		t.Fatalf("destroyed candidate before its failure grace period: %v", hardware.destroyed)
	}
	now = time.Unix(25*60, 0)
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(hardware.destroyed) != 1 {
		t.Fatalf("destroyed=%v, want one vNPU after candidate grace period", hardware.destroyed)
	}
}

func TestReconcilerUnknownFailureTimestampUsesStableFirstSeen(t *testing.T) {
	hardware := testHardware()
	snapshot := fakeSnapshot{snapshot: AllocationSnapshot{
		Ready: true,
		ClaimsByCard: map[int32]CardClaim{
			7: {CardID: 7, State: ClaimCandidate},
		},
	}}
	now := time.Unix(0, 0)
	r := NewReconciler(hardware, snapshot, ReconcilerOptions{
		GracePeriod:   10 * time.Minute,
		Confirmations: 3,
		Clock:         func() time.Time { return now },
	})
	for cycle := 0; cycle < 50 && len(hardware.destroyed) == 0; cycle++ {
		if _, err := r.Reconcile(context.Background()); err != nil {
			t.Fatalf("cycle %d: Reconcile() error = %v", cycle, err)
		}
		now = now.Add(time.Minute)
	}
	if len(hardware.destroyed) != 1 {
		t.Fatalf("destroyed=%v after 50 minutes, want one vNPU; unknown failure timestamp must use tracker firstSeen", hardware.destroyed)
	}
}

func TestReconcilerDoesNotLetStaleCandidateSinceBypassGrace(t *testing.T) {
	hardware := testHardware()
	snapshot := fakeSnapshot{snapshot: AllocationSnapshot{
		Ready: true,
		ClaimsByCard: map[int32]CardClaim{
			7: {CardID: 7, State: ClaimCandidate, CandidateSince: time.Unix(-2*60*60, 0)},
		},
	}}
	now := time.Unix(0, 0)
	r := NewReconciler(hardware, snapshot, ReconcilerOptions{
		GracePeriod:   10 * time.Minute,
		Confirmations: 1,
		Clock:         func() time.Time { return now },
	})
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(5 * time.Minute)
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(hardware.destroyed) != 0 {
		t.Fatalf("destroyed stale candidate before current grace period: %v", hardware.destroyed)
	}
	now = now.Add(5 * time.Minute)
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(hardware.destroyed) != 1 {
		t.Fatalf("destroyed=%v, want one vNPU after current grace period", hardware.destroyed)
	}
}

func TestReconcilerRetriesRemainingVNPUsWithoutResettingGrace(t *testing.T) {
	hardware := testHardware()
	hardware.virtual[3] = []manager.VirtualDevice{
		{LogicID: 3, CardID: 7, VDevID: 11, IsContainerUsed: 0},
		{LogicID: 3, CardID: 7, VDevID: 13, IsContainerUsed: 0},
	}
	hardware.destroyErrByVDev = map[int32]error{13: errors.New("temporary destroy failure")}
	now := time.Unix(0, 0)
	r := NewReconciler(hardware, fakeSnapshot{snapshot: AllocationSnapshot{Ready: true}}, ReconcilerOptions{
		GracePeriod:   time.Nanosecond,
		Confirmations: 1,
		Clock:         func() time.Time { return now },
	})
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if _, err := r.Reconcile(context.Background()); err == nil {
		t.Fatal("expected partial destroy error")
	}
	if len(hardware.destroyed) != 1 || hardware.destroyed[0].VDevID != 11 {
		t.Fatalf("destroyed=%v, want only vdev 11", hardware.destroyed)
	}
	delete(hardware.destroyErrByVDev, 13)
	now = now.Add(time.Second)
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(hardware.destroyed) != 2 || hardware.destroyed[1].VDevID != 13 {
		t.Fatalf("destroyed=%v, want immediate retry of remaining vdev 13", hardware.destroyed)
	}
}

func TestReconcilerResetsConfirmationAfterProtectedClaim(t *testing.T) {
	hardware := testHardware()
	now := time.Unix(0, 0)
	snapshot := &mutableSnapshot{snapshot: AllocationSnapshot{Ready: true}}
	r := NewReconciler(hardware, snapshot, ReconcilerOptions{
		GracePeriod:   time.Nanosecond,
		Confirmations: 2,
		Clock:         func() time.Time { return now },
	})
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot.snapshot.ClaimsByCard = map[int32]CardClaim{7: {CardID: 7, State: ClaimProtected}}
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot.snapshot.ClaimsByCard = nil
	now = now.Add(time.Minute)
	if _, err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(hardware.destroyed) != 0 {
		t.Fatalf("destroyed after only one post-protection confirmation: %v", hardware.destroyed)
	}
}

func TestClassifyPodUsesContainerState(t *testing.T) {
	tests := []struct {
		name  string
		pod   *corev1.Pod
		state podClaimState
	}{
		{
			name: "run container error is candidate",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				Phase:             corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "RunContainerError"}}}},
			}},
			state: claimCandidate,
		},
		{
			name: "container creating is protected",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				Phase:             corev1.PodPending,
				ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}},
			}},
			state: claimProtected,
		},
		{
			name:  "running phase without status is unknown",
			pod:   &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}},
			state: claimUnknown,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state, _, _ := classifyPod(tc.pod, false)
			if state != tc.state {
				t.Fatalf("classifyPod() = %q, want %q", state, tc.state)
			}
		})
	}
}

func TestClassifyPodReturnsFailureTimestamp(t *testing.T) {
	finished := time.Unix(123, 0)
	pod := &corev1.Pod{Status: corev1.PodStatus{
		Phase: corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{{
			State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "RunContainerError"}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(finished)}},
		}},
	}}
	state, _, failingSince := classifyPod(pod, false)
	if state != claimCandidate || !failingSince.Equal(finished) {
		t.Fatalf("classifyPod() = state=%q failingSince=%v, want candidate at %v", state, failingSince, finished)
	}
}

func TestClassifyPodCreateFailureWithoutTerminationTimestampReturnsZero(t *testing.T) {
	pod := &corev1.Pod{Status: corev1.PodStatus{
		Phase: corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "RunContainerError"}},
		}},
	}}
	state, _, failingSince := classifyPod(pod, false)
	if state != claimCandidate || !failingSince.IsZero() {
		t.Fatalf("classifyPod() = state=%q failingSince=%v, want candidate with zero timestamp", state, failingSince)
	}
}

func TestClassifyCheckpointedCreateFailureAsCandidate(t *testing.T) {
	pod := &corev1.Pod{Status: corev1.PodStatus{
		Phase: corev1.PodPending,
		ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "RunContainerError"}},
		}},
	}}
	state, reason, _ := classifyPod(pod, true)
	if state != claimCandidate {
		t.Fatalf("classifyPod() = state=%q reason=%q, want checkpointed failure candidate", state, reason)
	}
}

type mutableSnapshot struct{ snapshot AllocationSnapshot }

func (m *mutableSnapshot) Snapshot(context.Context) (AllocationSnapshot, error) {
	return m.snapshot, nil
}

func testHardware() *fakeHardware {
	return &fakeHardware{
		devices: []*manager.Device{{LogicID: 3, CardID: 7, UUID: "uuid-7"}},
		virtual: map[int32][]manager.VirtualDevice{
			3: {
				{LogicID: 3, CardID: 7, VDevID: 11, TemplateName: "vir05_1c_16g", IsContainerUsed: 0},
				{LogicID: 3, CardID: 7, VDevID: 12, TemplateName: "vir05_1c_16g", IsContainerUsed: 1},
			},
		},
		listErr:          make(map[int32]error),
		destroyErr:       make(map[int32]error),
		destroyErrByVDev: make(map[int32]error),
	}
}
