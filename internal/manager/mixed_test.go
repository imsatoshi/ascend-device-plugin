package manager

import (
	"ascend-common/devmanager"
	"ascend-common/devmanager/common"
	"ascend-common/devmanager/dcmi"
	"errors"
	"fmt"
	"testing"

	"github.com/Project-HAMi/ascend-device-plugin/internal"
)

func TestMixedDiePartition(t *testing.T) {
	config := &internal.MixedDevices{WholeDieGroups: [][]int32{{0, 1}}, SharedDies: []int32{2, 3}}
	dies := []*Device{
		{PhyID: 3, LogicID: 7, CardID: 4, UUID: "d"},
		{PhyID: 0, LogicID: 2, CardID: 6, UUID: "a"},
		{PhyID: 2, LogicID: 5, CardID: 4, UUID: "c"},
		{PhyID: 1, LogicID: 1, CardID: 6, UUID: "b"},
	}
	whole, shared, err := partitionDies(config, dies)
	if err != nil || len(whole) != 2 || len(shared) != 2 || whole[0].UUID != "a" || shared[0].UUID != "d" || shared[1].UUID != "c" {
		t.Fatalf("wrong partition: %v %v %v", whole, shared, err)
	}
	overlap := &internal.MixedDevices{WholeDieGroups: [][]int32{{0, 1}}, SharedDies: []int32{1, 2, 3}}
	if _, _, err := partitionDies(overlap, dies); err == nil {
		t.Fatal("overlapping die ranges accepted")
	}
	if _, _, err := partitionDies(config, dies[:3]); err == nil {
		t.Fatal("missing die accepted")
	}
	splitBoard := &internal.MixedDevices{WholeDieGroups: [][]int32{{0, 2}}, SharedDies: []int32{1, 3}}
	if _, _, err := partitionDies(splitBoard, dies); err == nil {
		t.Fatal("a dual-die module spans groups")
	}
}

type mixedTestDriver struct {
	*devmanager.DeviceManagerMock
	fault     string
	destroyed []int32
}

func (d *mixedTestDriver) GetDeviceList() (int32, []int32, error) {
	return 4, []int32{7, 2, 5, 1}, nil
}

func (d *mixedTestDriver) GetPhysicIDFromLogicID(id int32) (int32, error) {
	if id == 7 && d.fault == "identity" {
		return 0, errors.New("DCMI identity query failed")
	}
	return map[int32]int32{7: 3, 2: 0, 5: 2, 1: 1}[id], nil
}

func (d *mixedTestDriver) GetCardIDDeviceID(id int32) (int32, int32, error) {
	phy := map[int32]int32{7: 3, 2: 0, 5: 2, 1: 1}[id]
	return phy / 2, phy % 2, nil
}

func (d *mixedTestDriver) GetDieID(id int32, _ dcmi.DieType) (string, error) {
	return fmt.Sprintf("die-%d", id), nil
}

func (d *mixedTestDriver) GetDeviceHealth(id int32) (uint32, error) {
	if id == 7 && d.fault == "health" {
		return 0, errors.New("DCMI health query failed")
	}
	return 0, nil
}

func (d *mixedTestDriver) GetVirtualDeviceInfo(id int32) (common.VirtualDevInfo, error) {
	if id == 2 && d.fault == "virtual" {
		return common.VirtualDevInfo{}, errors.New("DCMI vNPU query failed")
	}
	return common.VirtualDevInfo{}, nil
}

func (d *mixedTestDriver) DestroyVirtualDevice(id int32, _ uint32) error {
	d.destroyed = append(d.destroyed, id)
	return nil
}

func newMixedTestManager(driver *mixedTestDriver) *AscendManager {
	return &AscendManager{mgr: driver, nodeConfig: &internal.NodeConfig{
		MixedDevices: &internal.MixedDevices{WholeDieGroups: [][]int32{{0, 1}}, SharedDies: []int32{2, 3}},
	}}
}

func TestUpdateDevicePublishesOnlyItsGroup(t *testing.T) {
	am := newMixedTestManager(&mixedTestDriver{})
	if err := am.UpdateDevice(); err != nil {
		t.Fatal(err)
	}
	whole, shared := am.WholeDevices(), am.GetDevices()
	if len(whole) != 2 || whole[0].PhyID != 0 || whole[1].PhyID != 1 ||
		len(shared) != 2 || shared[0].PhyID != 3 || shared[1].PhyID != 2 {
		t.Fatalf("wrong group publication: whole=%v shared=%v", whole, shared)
	}
	if am.GetDeviceByUUID("die-2") != nil || am.GetDeviceByUUID("die-7") == nil {
		t.Fatal("the shared endpoint UUID lookup must be isolated from whole dies")
	}
}

func TestMixedDeviceRefreshIsolatesDCMIFault(t *testing.T) {
	driver := &mixedTestDriver{}
	am := newMixedTestManager(driver)
	check := func(unhealthy int32) {
		t.Helper()
		devices := append(append([]*Device(nil), am.WholeDevices()...), am.GetDevices()...)
		if len(devices) != 4 {
			t.Fatalf("got %d devices, want 4", len(devices))
		}
		for _, dev := range devices {
			if dev.Health == (dev.LogicID == unhealthy) {
				t.Errorf("die %d health=%v, failed die=%d", dev.LogicID, dev.Health, unhealthy)
			}
		}
	}
	if err := am.UpdateDevice(); err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"health", "identity", "virtual"} {
		t.Run(fault, func(t *testing.T) {
			driver.fault = fault
			if err := am.UpdateDevice(); err == nil {
				t.Error("driver failure must request a fast retry")
			}
			failed := int32(7)
			if fault == "virtual" {
				failed = 2
			}
			check(failed)
			driver.fault = ""
			if err := am.UpdateDevice(); err != nil {
				t.Fatal(err)
			}
			check(-1)
		})
	}
}

func TestMixedCleanupGuard(t *testing.T) {
	driver := &mixedTestDriver{fault: "identity"}
	am := newMixedTestManager(driver)
	for _, id := range []int32{2, 7} {
		if !am.shouldIgnoreDeviceForCleanup(id, 0) || am.DestroyVirtualDevice(id, 100) == nil {
			t.Errorf("cleanup accepted whole or unresolved logic ID %d", id)
		}
	}
	if am.shouldIgnoreDeviceForCleanup(5, 1) || am.DestroyVirtualDevice(5, 100) != nil ||
		len(driver.destroyed) != 1 || driver.destroyed[0] != 5 {
		t.Fatalf("only shared die can be cleaned: %v", driver.destroyed)
	}
}
