/*
 * Copyright 2026 The HAMi Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cleanup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type KubernetesPodSnapshot struct {
	nodeName           string
	clientset          kubernetes.Interface
	resolver           DeviceResolver
	wholeResource      string
	sharedResource     string
	podLister          corelisters.PodLister
	podListerSynced    cache.InformerSynced
	stopCh             chan struct{}
	checkpointPath     string
	readFile           func(string) ([]byte, error)
	clock              Clock
	mu                 sync.Mutex
	staleCheckpoint    map[string]checkpointStaleState
	staleGrace         time.Duration
	staleConfirmations int
}

const (
	defaultStaleCheckpointGrace         = 10 * time.Minute
	defaultStaleCheckpointConfirmations = 3
)

type checkpointStaleState struct {
	firstAbsent time.Time
	count       int
}

type checkpointAllocation struct {
	CardIDs  []int32
	Unmapped bool
}

func NewKubernetesPodSnapshot(clientset kubernetes.Interface, nodeName string, resolver DeviceResolver, checkpointPath string) (*KubernetesPodSnapshot, error) {
	if clientset == nil {
		return nil, fmt.Errorf("kubernetes client is unavailable")
	}
	if resolver == nil {
		return nil, fmt.Errorf("device resolver is unavailable")
	}
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return nil, fmt.Errorf("node name is empty")
	}
	if checkpointPath == "" {
		checkpointPath = DefaultCheckpointPath
	}
	stopCh := make(chan struct{})
	factory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		0,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = "spec.nodeName=" + nodeName
		}),
	)
	pods := factory.Core().V1().Pods()
	snapshot := &KubernetesPodSnapshot{
		nodeName:           nodeName,
		clientset:          clientset,
		resolver:           resolver,
		podLister:          pods.Lister(),
		podListerSynced:    pods.Informer().HasSynced,
		stopCh:             stopCh,
		checkpointPath:     checkpointPath,
		readFile:           os.ReadFile,
		clock:              time.Now,
		staleCheckpoint:    make(map[string]checkpointStaleState),
		staleGrace:         defaultStaleCheckpointGrace,
		staleConfirmations: defaultStaleCheckpointConfirmations,
	}
	if mixed := resolver.MixedDevices(); mixed != nil {
		// The validated whole endpoint owns a disjoint hardware partition.
		snapshot.wholeResource = mixed.WholeResource()
		snapshot.sharedResource = resolver.ResourceName()
	}
	factory.Start(stopCh)
	return snapshot, nil
}

// Stop is intentionally separate from Snapshot. The device-plugin keeps this
// informer alive across kubelet socket restarts, so PluginServer.Stop must not
// close it during a normal restart.
func (s *KubernetesPodSnapshot) Stop() {
	select {
	case <-s.stopCh:
		return
	default:
		close(s.stopCh)
	}
}

func (s *KubernetesPodSnapshot) Snapshot(ctx context.Context) (AllocationSnapshot, error) {
	result := AllocationSnapshot{
		NodeName:     s.nodeName,
		ClaimsByCard: make(map[int32]CardClaim),
		CollectedAt:  s.clock(),
	}
	if !s.podListerSynced() {
		return result, fmt.Errorf("pod informer cache is not synced")
	}
	select {
	case <-ctx.Done():
		return result, ctx.Err()
	default:
	}

	listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	podList, err := s.clientset.CoreV1().Pods("").List(listCtx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + s.nodeName})
	if err != nil {
		return result, fmt.Errorf("list node pods from Kubernetes API: %w", err)
	}
	checkpointAllocations, err := s.readCheckpointAllocations()
	if err != nil {
		return result, err
	}
	podUIDs := make(map[string]struct{}, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName != s.nodeName {
			continue
		}
		podUIDs[string(pod.UID)] = struct{}{}
	}
	staleCards, staleUnknown := s.observeStaleCheckpointUIDs(checkpointAllocations, podUIDs, result.CollectedAt)
	result.UnknownPodUIDs = append(result.UnknownPodUIDs, staleUnknown...)
	for cardID := range staleCards {
		claim := result.ClaimsByCard[cardID]
		claim.CardID = cardID
		claim.State = mergeClaimState(claim.State, claimProtected)
		claim.Evidence = append(claim.Evidence, ClaimEvidence{
			State:  ClaimProtected,
			Reason: "stale Ascend checkpoint allocation is within the protection window",
			Source: "kubelet-checkpoint",
		})
		result.ClaimsByCard[cardID] = claim
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName != s.nodeName {
			continue
		}
		uid := string(pod.UID)
		allocation, checkpointed := checkpointAllocations[uid]
		hasResource, uuids, parseErr := s.podDeviceUUIDs(pod)
		if s.wholeResource != "" {
			// A shared checkpoint entry or malformed allocation annotation is
			// still ownership evidence on a Pod that requests whole dies.
			hasResource = hasResource || checkpointed || parseErr != nil || len(uuids) > 0
		}
		if uid == "" {
			if hasResource {
				result.UnknownPodUIDs = append(result.UnknownPodUIDs, pod.Namespace+"/"+pod.Name)
			}
			continue
		}
		if !hasResource && len(uuids) == 0 {
			continue
		}
		if parseErr != nil || len(uuids) == 0 || allocation.Unmapped {
			result.UnknownPodUIDs = append(result.UnknownPodUIDs, uid)
			continue
		}

		state, reason, failingSince := classifyPod(pod, checkpointed)
		if state == claimTerminal {
			continue
		}
		// A zero FailingSince is intentional when the runtime failure has no
		// termination timestamp (notably OCI create failures). The persistent
		// confirmation tracker records the first candidate observation once;
		// do not replace that stable start with this snapshot's CollectedAt.
		for _, uuid := range uuids {
			device := s.resolver.GetDeviceByUUID(uuid)
			if device == nil {
				result.UnknownPodUIDs = append(result.UnknownPodUIDs, uid)
				continue
			}
			claim := result.ClaimsByCard[device.CardID]
			claim.CardID = device.CardID
			claim.State = mergeClaimState(claim.State, state)
			if state == claimCandidate && (claim.CandidateSince.IsZero() || failingSince.After(claim.CandidateSince)) {
				claim.CandidateSince = failingSince
			}
			claim.PodUIDs = appendUnique(claim.PodUIDs, uid)
			claim.Evidence = append(claim.Evidence, ClaimEvidence{
				PodUID:       uid,
				Namespace:    pod.Namespace,
				PodName:      pod.Name,
				State:        toClaimState(state),
				Reason:       reason,
				DeviceUUID:   uuid,
				Source:       "kubernetes-pod",
				FailingSince: failingSince,
			})
			result.ClaimsByCard[device.CardID] = claim
		}
		if s.wholeResource != "" {
			// Kubelet slots identify a parent card. If the checkpoint also
			// names a card outside this Pod's annotations, keep it protected.
			for _, id := range allocation.CardIDs {
				claim := result.ClaimsByCard[id]
				if containsString(claim.PodUIDs, uid) {
					continue
				}
				claim.CardID = id
				claim.State = ClaimProtected
				claim.PodUIDs = append(claim.PodUIDs, uid)
				claim.Evidence = append(claim.Evidence, ClaimEvidence{
					PodUID: uid, State: ClaimProtected, Source: "kubelet-checkpoint",
					Reason: "shared checkpoint card is absent from Pod annotations",
				})
				result.ClaimsByCard[id] = claim
			}
		}
	}
	if len(result.UnknownPodUIDs) > 0 {
		result.Ready = false
		return result, fmt.Errorf("cannot map %d Pod/device records to an Ascend card", len(result.UnknownPodUIDs))
	}
	result.Ready = true
	return result, nil
}

// observeStaleCheckpointUIDs protects against transient API/checkpoint races
// while allowing a checkpoint entry that remains absent from several fresh
// API snapshots to age out. Without this bounded state, a kubelet checkpoint
// entry left behind by a failed container creation can block cleanup forever.
func (s *KubernetesPodSnapshot) observeStaleCheckpointUIDs(checkpointAllocations map[string]checkpointAllocation, podUIDs map[string]struct{}, now time.Time) (map[int32]struct{}, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for uid := range s.staleCheckpoint {
		if _, present := checkpointAllocations[uid]; !present {
			delete(s.staleCheckpoint, uid)
		}
	}
	protectedCards := make(map[int32]struct{})
	var unknown []string
	for uid, allocation := range checkpointAllocations {
		if _, present := podUIDs[uid]; present {
			delete(s.staleCheckpoint, uid)
			continue
		}
		state := s.staleCheckpoint[uid]
		if state.firstAbsent.IsZero() {
			state.firstAbsent = now
		}
		state.count++
		s.staleCheckpoint[uid] = state
		if state.count < s.staleConfirmations || now.Sub(state.firstAbsent) < s.staleGrace {
			if len(allocation.CardIDs) == 0 || allocation.Unmapped {
				unknown = append(unknown, uid)
				continue
			}
			for _, cardID := range allocation.CardIDs {
				protectedCards[cardID] = struct{}{}
			}
		}
	}
	return protectedCards, unknown
}

type podClaimState string

const (
	claimCandidate podClaimState = "candidate"
	claimProtected podClaimState = "protected"
	claimUnknown   podClaimState = "unknown"
	claimTerminal  podClaimState = "terminal"
)

func classifyPod(pod *corev1.Pod, checkpointed bool) (podClaimState, string, time.Time) {
	if pod.DeletionTimestamp != nil && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
		return claimProtected, "Pod is being deleted", time.Time{}
	}
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return claimTerminal, "Pod is terminal; checkpoint state cannot make it runnable again", time.Time{}
	}

	statuses := append(append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...)
	hasRunning := false
	hasProtectedWaiting := false
	hasFailedWaiting := false
	hasFailedTerminated := false
	failureTimeUnknown := false
	var latestFailure time.Time
	for _, status := range statuses {
		if status.State.Running != nil {
			hasRunning = true
		}
		if status.State.Waiting != nil {
			switch status.State.Waiting.Reason {
			case "ContainerCreating", "PodInitializing", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError":
				hasProtectedWaiting = true
			case "RunContainerError", "CreateContainerError", "StartError", "CrashLoopBackOff":
				hasFailedWaiting = true
			default:
				if status.State.Waiting.Reason != "" {
					return claimUnknown, "unrecognized container waiting reason: " + status.State.Waiting.Reason, time.Time{}
				}
			}
			if isFailedWaitingReason(status.State.Waiting.Reason) {
				failureSince := containerFailureSince(status)
				if failureSince.IsZero() {
					failureTimeUnknown = true
				} else if failureSince.After(latestFailure) {
					latestFailure = failureSince
				}
			}
		}
		if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
			hasFailedTerminated = true
			failureSince := containerFailureSince(status)
			if failureSince.IsZero() {
				failureTimeUnknown = true
			} else if failureSince.After(latestFailure) {
				latestFailure = failureSince
			}
		}
	}
	if hasRunning {
		if checkpointed {
			return claimProtected, "a running container is retained in kubelet checkpoint", time.Time{}
		}
		return claimProtected, "a container is running", time.Time{}
	}
	if hasProtectedWaiting || (pod.Status.Phase == corev1.PodPending && !hasFailedWaiting && !hasFailedTerminated) {
		if checkpointed {
			return claimProtected, "Pod is in a startup window and retained in kubelet checkpoint", time.Time{}
		}
		return claimProtected, "Pod is in a startup window", time.Time{}
	}
	if hasFailedWaiting || hasFailedTerminated {
		if failureTimeUnknown {
			if checkpointed {
				return claimCandidate, "checkpoint-retained Pod is in a failure/retry state with an unknown failure timestamp", time.Time{}
			}
			return claimCandidate, "all observed containers are in a failure/retry state with an unknown failure timestamp", time.Time{}
		}
		if checkpointed {
			return claimCandidate, "checkpoint-retained Pod is in a failure/retry state", latestFailure
		}
		return claimCandidate, "all observed containers are in a failure/retry state", latestFailure
	}
	if checkpointed {
		return claimProtected, "Pod has incomplete state and is retained in kubelet checkpoint", time.Time{}
	}
	if pod.Status.Phase == corev1.PodRunning {
		return claimUnknown, "Running Pod has no recognizable container state", time.Time{}
	}
	return claimUnknown, "Pod container state is incomplete", time.Time{}
}

func isFailedWaitingReason(reason string) bool {
	switch reason {
	case "RunContainerError", "CreateContainerError", "StartError", "CrashLoopBackOff":
		return true
	default:
		return false
	}
}

func containerFailureSince(status corev1.ContainerStatus) time.Time {
	if status.LastTerminationState.Terminated != nil && !status.LastTerminationState.Terminated.FinishedAt.IsZero() {
		return status.LastTerminationState.Terminated.FinishedAt.Time
	}
	if status.State.Terminated != nil && !status.State.Terminated.FinishedAt.IsZero() {
		return status.State.Terminated.FinishedAt.Time
	}
	return time.Time{}
}

func toClaimState(state podClaimState) ClaimState {
	switch state {
	case claimCandidate:
		return ClaimCandidate
	case claimProtected:
		return ClaimProtected
	default:
		return ClaimUnknown
	}
}

func mergeClaimState(old ClaimState, next podClaimState) ClaimState {
	if old == "" {
		return toClaimState(next)
	}
	if old == ClaimProtected || next == claimProtected {
		return ClaimProtected
	}
	if old == ClaimUnknown || next == claimUnknown {
		return ClaimUnknown
	}
	return ClaimCandidate
}

func appendUnique(values []string, value string) []string {
	for _, current := range values {
		if current == value {
			return values
		}
	}
	return append(values, value)
}

func (s *KubernetesPodSnapshot) podDeviceUUIDs(pod *corev1.Pod) (bool, []string, error) {
	hasResource := s.podHasAscendResource(pod)
	sharedToAllocate := "hami.io/" + strings.TrimPrefix(s.sharedResource, "huawei.com/") + "-devices-to-allocate"
	var uuids []string
	var parseErr error
	for key, value := range pod.Annotations {
		if !s.tracksResource(key) && !strings.Contains(key, "-devices-to-allocate") {
			continue
		}
		if s.isAuxiliaryResource(key) {
			continue
		}
		if s.sharedResource != "" && (key == s.sharedResource || key == sharedToAllocate) {
			hasResource = true
		}
		if strings.TrimSpace(value) == "" {
			continue
		}
		parsed, err := parseDeviceAnnotation(value, s.wholeResource != "")
		if err != nil {
			parseErr = err
			continue
		}
		uuids = append(uuids, parsed...)
	}
	return hasResource, uniqueStrings(uuids), parseErr
}

// Keep other Ascend claims fail-closed. Only the configured whole endpoint
// is known to be disjoint from the shared manager's devices.
func (s *KubernetesPodSnapshot) tracksResource(name string) bool {
	return name != s.wholeResource && (name == s.sharedResource || strings.HasPrefix(name, "huawei.com/Ascend"))
}

func (s *KubernetesPodSnapshot) isAuxiliaryResource(name string) bool {
	return name != s.sharedResource && (strings.HasSuffix(name, "-memory") || strings.HasSuffix(name, "-core"))
}

func (s *KubernetesPodSnapshot) podHasAscendResource(pod *corev1.Pod) bool {
	for _, container := range append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...) {
		for _, resources := range []corev1.ResourceList{container.Resources.Limits, container.Resources.Requests} {
			for resource := range resources {
				name := string(resource)
				if s.tracksResource(name) && !s.isAuxiliaryResource(name) {
					return true
				}
			}
		}
	}
	return false
}

func parseDeviceAnnotation(value string, requireUUID bool) ([]string, error) {
	var runtimeInfos []struct {
		UUID string `json:"UUID"`
	}
	if err := json.Unmarshal([]byte(value), &runtimeInfos); err == nil {
		var uuids []string
		for _, info := range runtimeInfos {
			if requireUUID && strings.TrimSpace(info.UUID) == "" {
				return nil, fmt.Errorf("device annotation contains an empty UUID")
			}
			if info.UUID != "" {
				uuids = append(uuids, info.UUID)
			}
		}
		return uuids, nil
	}

	// HAMi's devices-to-allocate annotation is encoded as
	// uuid,type,memory,core[:uuid,type,memory,core];...
	var uuids []string
	sawNonEmptySegment := false
	for _, container := range strings.Split(value, ";") {
		for _, device := range strings.Split(container, ":") {
			device = strings.TrimSpace(device)
			if device == "" {
				continue
			}
			sawNonEmptySegment = true
			fields := strings.Split(device, ",")
			if len(fields) < 4 {
				return nil, fmt.Errorf("invalid device annotation segment %q", device)
			}
			uuids = append(uuids, fields[0])
		}
	}
	if !sawNonEmptySegment {
		return nil, nil
	}
	if len(uuids) == 0 {
		return nil, fmt.Errorf("device annotation contains no UUID")
	}
	return uuids, nil
}

func uniqueStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !containsString(result, value) {
			result = append(result, value)
		}
	}
	return result
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (s *KubernetesPodSnapshot) readCheckpointAllocations() (map[string]checkpointAllocation, error) {
	contents, err := s.readFile(s.checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("read kubelet allocation checkpoint: %w", err)
	}
	var checkpoint struct {
		Data *struct {
			PodDeviceEntries  json.RawMessage
			RegisteredDevices map[string][]string
		}
	}
	if err := json.Unmarshal(contents, &checkpoint); err != nil {
		return nil, fmt.Errorf("decode kubelet allocation checkpoint: %w", err)
	}
	if checkpoint.Data == nil || checkpoint.Data.RegisteredDevices == nil || len(checkpoint.Data.PodDeviceEntries) == 0 {
		return nil, fmt.Errorf("kubelet allocation checkpoint is uninitialized or unsupported")
	}
	if err := s.checkRegisteredDevices(checkpoint.Data.RegisteredDevices); err != nil {
		return nil, err
	}
	var entries []struct {
		PodUID       string
		ResourceName string
		DeviceIDs    map[string][]string
	}
	if err := json.Unmarshal(checkpoint.Data.PodDeviceEntries, &entries); err != nil {
		return nil, fmt.Errorf("decode kubelet Pod allocations: %w", err)
	}
	allocations := make(map[string]checkpointAllocation, len(entries))
	for _, entry := range entries {
		if !s.tracksResource(entry.ResourceName) {
			continue
		}
		if entry.PodUID == "" {
			return nil, fmt.Errorf("kubelet allocation checkpoint contains an empty Pod UID")
		}
		allocation := allocations[entry.PodUID]
		cards := s.checkpointCardIDs(entry.DeviceIDs)
		if s.wholeResource != "" && len(cards) == 0 {
			allocation.Unmapped = true
		}
		allocation.CardIDs = appendUniqueInt32(allocation.CardIDs, cards...)
		allocations[entry.PodUID] = allocation
	}
	return allocations, nil
}

func (s *KubernetesPodSnapshot) checkRegisteredDevices(registered map[string][]string) error {
	if s.wholeResource != "" {
		if len(registered[s.sharedResource]) == 0 {
			return fmt.Errorf("kubelet allocation checkpoint has no registered devices for %s", s.sharedResource)
		}
		return nil
	}
	for name, ids := range registered {
		if strings.HasPrefix(name, "huawei.com/Ascend") && len(ids) > 0 {
			return nil
		}
	}
	return fmt.Errorf("kubelet allocation checkpoint has no registered Ascend devices")
}

func (s *KubernetesPodSnapshot) checkpointCardIDs(deviceIDs map[string][]string) []int32 {
	seen := make(map[int32]struct{})
	for _, ids := range deviceIDs {
		for _, id := range ids {
			device := s.resolver.GetDeviceByUUID(id)
			if device == nil {
				idx := strings.LastIndexByte(id, '-')
				if idx <= 0 {
					return nil
				}
				if _, err := strconv.Atoi(id[idx+1:]); err != nil {
					return nil
				}
				device = s.resolver.GetDeviceByUUID(id[:idx])
			}
			if device == nil {
				return nil
			}
			seen[device.CardID] = struct{}{}
		}
	}
	cardIDs := make([]int32, 0, len(seen))
	for cardID := range seen {
		cardIDs = append(cardIDs, cardID)
	}
	return cardIDs
}

func appendUniqueInt32(values []int32, additions ...int32) []int32 {
	for _, addition := range additions {
		found := false
		for _, value := range values {
			if value == addition {
				found = true
				break
			}
		}
		if !found {
			values = append(values, addition)
		}
	}
	return values
}

var _ PodAllocationSnapshot = (*KubernetesPodSnapshot)(nil)
