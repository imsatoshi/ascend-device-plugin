/*
 * Copyright 2024 The HAMi Authors.
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

package server

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	// "github.com/Project-HAMi/HAMi/pkg/device/ascend"
	"github.com/Project-HAMi/HAMi/pkg/device"
	"github.com/Project-HAMi/HAMi/pkg/util"
	"github.com/Project-HAMi/ascend-device-plugin/internal/cleanup"
	"github.com/Project-HAMi/ascend-device-plugin/internal/manager"
)

const (
	// RegisterAnnos = "hami.io/node-register-ascend"
	// PodAllocAnno = "huawei.com/AscendDevices"
	NodeLockAscend             = "hami.io/mutex.lock"
	Ascend910Prefix            = "Ascend910"
	Ascend910CType             = "Ascend910C"
	VNPUModeAnnotation         = "huawei.com/vnpu-mode"
	VNPUModeTemplate           = "template"
	VNPUModeHamiCore           = "hami-core"
	VNPUModeENPU               = "enpu"
	VNPUNodeSelectorAnnotation = "hami-vnpu-core"
	VNPUNodeENPUAnnotation     = "hami.io/enpu"
)

var (
	reportTimeOffset = flag.Int64("report_time_offset", 1, "report time offset")
)

type PluginServer struct {
	v1beta1.UnimplementedDevicePluginServer

	commonWord                    string
	nodeName                      string
	registerAnno                  string
	handshakeAnno                 string
	allocAnno                     string
	toAllocDeviceAnno             string
	grpcServer                    *grpc.Server
	mgr                           manager.Manager
	socket                        string
	stopCh                        chan any
	healthCh                      chan int32
	checkIdleVNPUInterval         int
	enablePeriodicIdleVNPUCleanup bool
	idleVNPUReconciler            cleanup.IdleVNPUReconciler
	wg                            sync.WaitGroup
	allocationMu                  sync.Mutex

	// lastPublishedDevices fingerprints the device set most recently handed to
	// kubelet through ListAndWatch, so watchAndRegister only pushes an update
	// when the set actually changed.
	lastPublishedDevices string

	// pendingDeviceShare lists the chips whose device-share could not be
	// enabled because the driver reported a workload still running on them.
	// Written by Start and then by watchAndRegister on every tick, and read by
	// ListAndWatch and registerHAMi, which leave these chips unregistered, so
	// every access goes through pendingMu.
	pendingDeviceShare []shareTarget
	pendingMu          sync.RWMutex

	// test hooks — injected by tests to avoid real socket/kubelet dependencies
	dialFunc                 func(unixSocketPath string, timeout time.Duration) (*grpc.ClientConn, error)
	registerKubeletFunc      func() error
	prepareHostResourcesFunc func() error
}

type RuntimeInfo struct {
	UUID   string `json:"UUID,omitempty"`
	Temp   string `json:"temp,omitempty"`
	Memory *int64 `json:"memory,omitempty"`
	Core   *int32 `json:"core,omitempty"`
}

// enpuManager provides optional ENPU capability detection.
type enpuManager interface {
	IsEnpu() bool
}

type fullCardOnlyManager interface {
	IsFullCardOnly() bool
}

func managerIsFullCardOnly(mgr manager.Manager) bool {
	m, ok := mgr.(fullCardOnlyManager)
	return ok && m.IsFullCardOnly()
}

type enpuPolicyManager interface {
	EnpuPolicy() string
}

func managerUsesENPU(mgr manager.Manager) bool {
	m, ok := mgr.(enpuManager)
	return ok && m.IsEnpu()
}

func managerENPUPolicy(mgr manager.Manager) string {
	m, ok := mgr.(enpuPolicyManager)
	if !ok {
		return ""
	}
	return m.EnpuPolicy()
}

func podUsesENPU(pod *v1.Pod) bool {
	if pod == nil || pod.Annotations == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(pod.Annotations[VNPUModeAnnotation])) {
	case VNPUModeENPU, "ubs-virt", "vcann-rt":
		return true
	default:
		return false
	}
}

func NewPluginServer(mgr manager.Manager, nodeName string, checkIdleVNPUInterval int, enablePeriodicIdleVNPUCleanup bool) (*PluginServer, error) {
	if enablePeriodicIdleVNPUCleanup && checkIdleVNPUInterval <= 0 {
		return nil, fmt.Errorf("check_idle_vnpu_interval must be greater than zero when periodic idle vNPU cleanup is enabled")
	}
	commonWord := mgr.CommonWord()
	server := &PluginServer{
		commonWord:                    commonWord,
		nodeName:                      nodeName,
		registerAnno:                  fmt.Sprintf("hami.io/node-register-%s", commonWord),
		handshakeAnno:                 fmt.Sprintf("hami.io/node-handshake-%s", commonWord),
		allocAnno:                     fmt.Sprintf("huawei.com/%s", commonWord),
		toAllocDeviceAnno:             fmt.Sprintf("hami.io/%s-devices-to-allocate", commonWord),
		mgr:                           mgr,
		socket:                        path.Join(v1beta1.DevicePluginPath, fmt.Sprintf("%s.sock", commonWord)),
		stopCh:                        make(chan any),
		healthCh:                      make(chan int32),
		checkIdleVNPUInterval:         checkIdleVNPUInterval,
		enablePeriodicIdleVNPUCleanup: enablePeriodicIdleVNPUCleanup,
	}
	// enable calling hami methods
	device.InRequestDevices[commonWord] = server.toAllocDeviceAnno
	return server, nil
}

// SetIdleVNPUReconciler wires the platform-side safety policy used by both
// startup cleanup and the periodic cleanup loop. It is intentionally separate
// from NewPluginServer to keep existing embedders and unit tests source
// compatible while making production wiring explicit in cmd/main.go.
func (ps *PluginServer) SetIdleVNPUReconciler(reconciler cleanup.IdleVNPUReconciler) {
	ps.idleVNPUReconciler = reconciler
}

func (ps *PluginServer) ResetIdleVNPUCleanupState() {
	if resettable, ok := ps.idleVNPUReconciler.(interface{ Reset() }); ok {
		resettable.Reset()
	}
}

// prepareHostResources wraps the package-level prepareHostResources() to
// allow test injection via the prepareHostResourcesFunc hook.
func (ps *PluginServer) prepareHostResources() error {
	if ps.prepareHostResourcesFunc != nil {
		return ps.prepareHostResourcesFunc()
	}
	return prepareHostResources()
}

func (ps *PluginServer) Start() error {
	if ps.mgr.IsHamiVnpuCore() {
		if err := ps.prepareHostResources(); err != nil {
			klog.Errorf("Failed to prepare hami-vnpu-core host resources: %v", err)
			return err
		}
	}
	if managerUsesENPU(ps.mgr) {
		if err := prepareENPUHostResources(); err != nil {
			klog.Errorf("Failed to prepare ENPU host resources: %v", err)
			return err
		}
	}

	ps.stopCh = make(chan any)
	ps.grpcServer = grpc.NewServer()

	err := ps.mgr.UpdateDevice()
	if err != nil {
		return err
	}
	if err := ps.enableNodeDeviceShare(); err != nil {
		return err
	}
	// The device set below is what the first ListAndWatch response publishes.
	// Taken after device-share so that chips withheld for a running workload
	// are already excluded.
	ps.lastPublishedDevices = ps.deviceFingerprint()
	err = ps.serve()
	if err != nil {
		return err
	}
	err = ps.registerKubelet()
	if err != nil {
		return err
	}
	// sync.WaitGroup.Go handles the Add(1)/Done() pairing internally, so the
	// goroutine bodies no longer need their own defer ps.wg.Done().
	if ps.enablePeriodicIdleVNPUCleanup {
		ps.wg.Go(ps.startPeriodicCheckIdleVNPUs)
	}
	ps.wg.Go(ps.watchAndRegister)
	return nil
}

func (ps *PluginServer) startPeriodicCheckIdleVNPUs() {
	ticker := time.NewTicker(time.Duration(ps.checkIdleVNPUInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			klog.Info("Running scheduled idle vNPU cleanup")
			if err := ps.CleanupIdleVNPUs(); err != nil {
				klog.Errorf("Failed to cleanup idle vNPUs: %v", err)
			}
		case <-ps.stopCh:
			klog.Info("Stopping cleanup goroutine")
			return
		}
	}
}

func (ps *PluginServer) Stop() error {
	if ps.stopCh != nil {
		select {
		case <-ps.stopCh:
			// already closed; no-op
		default:
			close(ps.stopCh)
		}
	}
	if ps.grpcServer != nil {
		ps.grpcServer.Stop()
	}
	ps.wg.Wait()
	_ = os.Remove(ps.socket)
	return nil
}

func (ps *PluginServer) StopCh() <-chan any {
	return ps.stopCh
}

func (ps *PluginServer) CleanupIdleVNPUs() error {
	if ps.idleVNPUReconciler == nil {
		return fmt.Errorf("idle vNPU reconciler is not configured")
	}
	result, err := ps.idleVNPUReconciler.Reconcile(context.Background())
	klog.Infof("idle vNPU reconcile: cards=%d protected=%d reclaimable=%d inspected=%d destroyed=%d skipped=%d ignored=%d unknown=%v destroyErrors=%d resets=%d", result.CardsInspected, result.CardsProtected, result.CardsReclaimable, result.VNPUsInspected, result.VNPUsDestroyed, result.VNPUsSkipped, result.VNPUsIgnored, result.UnknownSnapshot, result.DestroyErrors, result.ConfirmationResets)
	return err
}

func (ps *PluginServer) serve() error {
	_ = os.Remove(ps.socket)
	sock, err := net.Listen("unix", ps.socket)
	if err != nil {
		return err
	}
	v1beta1.RegisterDevicePluginServer(ps.grpcServer, ps)
	resourceName := ps.mgr.ResourceName()
	ps.wg.Go(func() {
		lastCrashTime := time.Now()
		restartCount := 0
		for {
			select {
			case <-ps.stopCh:
				return
			default:
			}
			klog.Infof("Starting GRPC server for '%s'", resourceName)
			err := ps.grpcServer.Serve(sock)
			if err == nil {
				break
			}

			klog.Infof("GRPC server for '%s' crashed with error: %v", resourceName, err)

			// restart if it has not been too often
			// i.e. if server has crashed more than 5 times and it didn't last more than one hour each time
			if restartCount > 5 {
				// quit
				klog.Fatalf("GRPC server for '%s' has repeatedly crashed recently. Quitting", resourceName)
			}
			timeSinceLastCrash := time.Since(lastCrashTime).Seconds()
			lastCrashTime = time.Now()
			if timeSinceLastCrash > 3600 {
				// it has been one hour since the last crash.. reset the count
				// to reflect on the frequency
				restartCount = 1
			} else {
				restartCount++
			}
		}
	})

	// Wait for server to start by launching a blocking connexion
	conn, err := ps.dial(ps.socket, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to dial device plugin socket: %w", err)
	}
	_ = conn.Close()

	return nil
}

func (ps *PluginServer) apiDevices() []*v1beta1.Device {
	devs := ps.mgr.GetDevices()
	devices := make([]*v1beta1.Device, 0, len(devs))
	vCount := ps.mgr.VDeviceCount()
	pending := ps.pendingShareChips()
	for _, dev := range devs {
		if pending[chipKey{Card: dev.CardID, Chip: dev.DeviceID}] {
			continue
		}
		health := v1beta1.Unhealthy
		if dev.Health {
			health = v1beta1.Healthy
		}
		for i := range vCount {
			device := v1beta1.Device{
				ID:     fmt.Sprintf("%s-%d", dev.UUID, i),
				Health: health,
			}
			devices = append(devices, &device)
		}
	}
	klog.V(5).Infof("api devices: %v", devices)
	return devices
}

func (ps *PluginServer) GetDevicePluginOptions(context.Context, *v1beta1.Empty) (*v1beta1.DevicePluginOptions, error) {
	return &v1beta1.DevicePluginOptions{GetPreferredAllocationAvailable: true}, nil
}

func (ps *PluginServer) ListAndWatch(e *v1beta1.Empty, s v1beta1.DevicePlugin_ListAndWatchServer) error {
	_ = s.Send(&v1beta1.ListAndWatchResponse{Devices: ps.apiDevices()})
	for {
		select {
		case <-ps.stopCh:
			return nil
		case <-ps.healthCh:
			_ = s.Send(&v1beta1.ListAndWatchResponse{Devices: ps.apiDevices()})
		}
	}
}

func (ps *PluginServer) GetPreferredAllocation(ctx context.Context, requests *v1beta1.PreferredAllocationRequest) (*v1beta1.PreferredAllocationResponse, error) {
	pod, _, err := ps.lockedPendingPod(ctx)
	if err != nil {
		return nil, fmt.Errorf("get pending pod: %w", err)
	}
	allocations, err := ps.decodeDeviceAnnotations(pod)
	if err != nil {
		return nil, err
	}
	response := &v1beta1.PreferredAllocationResponse{}
	for _, request := range requests.GetContainerRequests() {
		devices, _, err := ps.popNextContainerDevices(pod, allocations)
		if err != nil {
			return nil, err
		}
		if int(request.GetAllocationSize()) != len(devices) {
			return nil, fmt.Errorf("preferred allocation size does not match the locked Pod")
		}
		ids, err := preferredSlots(devices, request.GetAvailableDeviceIDs(), request.GetMustIncludeDeviceIDs())
		if err != nil {
			return nil, err
		}
		response.ContainerResponses = append(response.ContainerResponses, &v1beta1.ContainerPreferredAllocationResponse{DeviceIDs: ids})
	}
	return response, nil
}

func (ps *PluginServer) Allocate(ctx context.Context, reqs *v1beta1.AllocateRequest) (*v1beta1.AllocateResponse, error) {
	ps.allocationMu.Lock()
	defer ps.allocationMu.Unlock()

	pod, lock, err := ps.lockedPendingPod(ctx)
	if err != nil {
		return nil, fmt.Errorf("get pending pod: %w", err)
	}
	podDevices, err := ps.decodeDeviceAnnotations(pod)
	if err != nil {
		return nil, fmt.Errorf("decode device annotations: %w", err)
	}

	requests := reqs.GetContainerRequests()
	if len(requests) == 0 {
		return nil, fmt.Errorf("empty Ascend Allocate request")
	}
	// Validate the whole request before any phase update or runtime side effect.
	selections := make([]device.ContainerDevices, len(requests))
	containers := make([]string, len(requests))
	for i, req := range requests {
		selections[i], containers[i], err = ps.popNextContainerDevices(pod, podDevices)
		if err != nil {
			return nil, fmt.Errorf("get next container devices: %w", err)
		}
		if len(selections[i]) != len(req.GetDevicesIds()) {
			return nil, fmt.Errorf("device number not matched: annotation has %d, request has %d", len(selections[i]), len(req.GetDevicesIds()))
		}
		if !requestedDevicesMatch(selections[i], req.GetDevicesIds()) {
			return nil, fmt.Errorf("requested device IDs do not match the locked Pod %s/%s", pod.Namespace, pod.Name)
		}
	}

	response, err := ps.buildAllocateResponse(pod, selections, containers)
	if err != nil {
		// The Pod revision prevents a failed write from replacing a concurrent success.
		if patchErr := ps.patchAllocation(ctx, pod, lock, map[string]string{util.DeviceBindPhase: util.DeviceBindFailed}); patchErr == nil {
			if releaseErr := ps.releaseAllocation(ctx, lock); releaseErr != nil {
				klog.ErrorS(releaseErr, "Failed to release Ascend allocation lock", "pod", klog.KObj(pod))
			}
		} else {
			klog.ErrorS(patchErr, "Failed to publish Ascend allocation failure", "pod", klog.KObj(pod))
		}
		return nil, err
	}

	remaining := device.EncodePodSingleDevice(podDevices)
	annotations := map[string]string{ps.toAllocDeviceAnno: remaining}
	completed := true
	for key, value := range pod.Annotations {
		if key == ps.toAllocDeviceAnno {
			value = remaining
		}
		if strings.HasPrefix(key, "hami.io/Ascend") && strings.HasSuffix(key, "-devices-to-allocate") &&
			strings.Trim(value, ";") != "" {
			completed = false
			break
		}
	}
	if completed {
		annotations[util.DeviceBindPhase] = util.DeviceBindSuccess
	}
	if err := ps.patchAllocation(ctx, pod, lock, annotations); err != nil {
		return nil, fmt.Errorf("publish Ascend allocation: %w", err)
	}
	if completed {
		if err := ps.releaseAllocation(ctx, lock); err != nil {
			klog.ErrorS(err, "Failed to release Ascend allocation lock", "pod", klog.KObj(pod))
		}
	}
	return response, nil
}

func (ps *PluginServer) buildAllocateResponse(pod *v1.Pod, selections []device.ContainerDevices, containers []string) (*v1beta1.AllocateResponse, error) {
	runtimeInfo, err := ps.buildRuntimeInfoLookup(pod)
	if err != nil {
		return nil, fmt.Errorf("build runtimeInfo lookup: %w", err)
	}
	response := &v1beta1.AllocateResponse{}
	for i, devices := range selections {
		container, err := ps.buildContainerAllocateResponse(pod, containers[i], devices, runtimeInfo)
		if err != nil {
			return nil, fmt.Errorf("build container allocate response: %w", err)
		}
		response.ContainerResponses = append(response.ContainerResponses, container)
	}
	return response, nil
}

func (ps *PluginServer) PreStartContainer(context.Context, *v1beta1.PreStartContainerRequest) (*v1beta1.PreStartContainerResponse, error) {
	return &v1beta1.PreStartContainerResponse{}, nil
}
