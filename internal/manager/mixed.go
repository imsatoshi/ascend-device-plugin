package manager

import (
	"fmt"

	"github.com/Project-HAMi/ascend-device-plugin/internal"
)

// partitionDies uses physical die IDs. DCMI CardID identifies a dual-die
// module on A3 and may not straddle an allocation group under UNION_POLICY.
func partitionDies(config *internal.MixedDevices, devices []*Device) (whole, shared []*Device, err error) {
	if len(config.WholeDieGroups) == 0 || len(config.SharedDies) == 0 {
		return nil, nil, fmt.Errorf("mixed allocation requires both whole and shared dies")
	}
	groups := make(map[int32]int)
	for index, ids := range append(append([][]int32(nil), config.WholeDieGroups...), config.SharedDies) {
		if len(ids) == 0 {
			return nil, nil, fmt.Errorf("empty die group")
		}
		for _, id := range ids {
			if _, exists := groups[id]; exists || id < 0 {
				return nil, nil, fmt.Errorf("invalid or duplicate physical die %d", id)
			}
			groups[id] = index
		}
	}
	boards := make(map[int32]int)
	physical := make(map[int32]bool)
	uuids := make(map[string]bool)
	for _, dev := range devices {
		group, ok := groups[dev.PhyID]
		if !ok || physical[dev.PhyID] || dev.UUID == "" || uuids[dev.UUID] {
			return nil, nil, fmt.Errorf("unexpected or duplicate die %d (%s)", dev.PhyID, dev.UUID)
		}
		physical[dev.PhyID], uuids[dev.UUID] = true, true
		if prior, exists := boards[dev.CardID]; exists && prior != group {
			return nil, nil, fmt.Errorf("DCMI card %d spans die groups", dev.CardID)
		}
		boards[dev.CardID] = group
		if group == len(config.WholeDieGroups) {
			shared = append(shared, dev)
		} else {
			whole = append(whole, dev)
		}
	}
	if len(physical) != len(groups) {
		return nil, nil, fmt.Errorf("declared %d dies, discovered %d", len(groups), len(physical))
	}
	return whole, shared, nil
}

func (am *AscendManager) MixedDevices() *internal.MixedDevices {
	if am.nodeConfig == nil {
		return nil
	}
	return am.nodeConfig.MixedDevices
}

func (am *AscendManager) WholeDevices() []*Device {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.wholeDevs
}

func (am *AscendManager) validateMixedMode() error {
	if am.MixedDevices() == nil {
		return nil
	}
	mode, err := am.vnpuMode()
	if err != nil {
		return err
	}
	if am.CommonWord() != "Ascend910C" || am.ResourceName() != "huawei.com/Ascend910C" ||
		mode != internal.VNPUModeTemplate || am.VDeviceCount() != 4 || !am.nodeConfig.FilterDevices.IsEmpty() {
		return fmt.Errorf("mixed devices require Ascend910C four-slot template mode and an explicit partition without filterDevices")
	}
	return nil
}

// PartialDeviceRefreshError means the partition was validated and published,
// with the affected dies marked unavailable.
type PartialDeviceRefreshError struct{ error }

func (err *PartialDeviceRefreshError) Unwrap() error { return err.error }

func (am *AscendManager) mixedDevicesByLogicID() map[int32]*Device {
	am.mu.RLock()
	defer am.mu.RUnlock()
	known := make(map[int32]*Device, len(am.devs)+len(am.wholeDevs))
	for _, devices := range [][]*Device{am.wholeDevs, am.devs} {
		for _, dev := range devices {
			known[dev.LogicID] = dev
		}
	}
	return known
}

func (am *AscendManager) markMixedUnhealthy() {
	am.mu.Lock()
	defer am.mu.Unlock()
	unhealthy := func(devices []*Device) []*Device {
		result := make([]*Device, 0, len(devices))
		for _, device := range devices {
			copy := *device
			copy.Health = false
			result = append(result, &copy)
		}
		return result
	}
	am.devs = unhealthy(am.devs)
	am.wholeDevs = unhealthy(am.wholeDevs)
}

// CheckWholeDie verifies that the die has no residual vNPU from a previous
// mode. Driver errors withhold the die until the next successful discovery.
func (am *AscendManager) CheckWholeDie(device *Device) error {
	physical, err := am.mgr.GetPhysicIDFromLogicID(device.LogicID)
	if err != nil || physical != device.PhyID {
		return fmt.Errorf("cannot verify physical identity of die %d", device.PhyID)
	}
	infos, err := am.mgr.GetVirtualDeviceInfo(device.LogicID)
	if err != nil {
		return fmt.Errorf("cannot verify vNPU state of die %d: %w", device.PhyID, err)
	}
	if infos.TotalResource.VDevNum != 0 || len(infos.VDevInfo) != 0 {
		return fmt.Errorf("whole die %d still has vNPUs", device.PhyID)
	}
	return nil
}
