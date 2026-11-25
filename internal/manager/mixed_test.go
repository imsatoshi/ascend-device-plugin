package manager

import (
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
