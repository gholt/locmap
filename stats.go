package valuelocmap

import (
	"fmt"
	"sync"
	"unsafe"

	"gopkg.in/gholt/brimtext.v1"
)

type Stats struct {
	// ActiveCount is the number of locations whose timestamp & inactiveMask ==
	// 0 as given when Stats() was called.
	ActiveCount uint64
	// ActiveBytes is the number of bytes represented by locations whose
	// timestamp & inactiveMask == 0 as given when Stats() was called.
	ActiveBytes uint64

	inactiveMask      uint64
	statsDebug        bool
	workers           uint32
	roots             uint32
	usedRoots         uint32
	entryPageSize     uint64
	entryLockPageSize uint64
	splitLevel        uint32
	nodes             uint64
	depthCounts       []uint64
	allocedEntries    uint64
	allocedInOverflow uint64
	usedEntries       uint64
	usedInOverflow    uint64
	inactive          uint64
}

func (s *Stats) String() string {
	report := [][]string{
		{"ActiveCount", fmt.Sprintf("%d", s.ActiveCount)},
		{"ActiveBytes", fmt.Sprintf("%d", s.ActiveBytes)},
	}
	if s.statsDebug {
		depthCounts := fmt.Sprintf("%d", s.depthCounts[0])
		for i := 1; i < len(s.depthCounts); i++ {
			depthCounts += fmt.Sprintf(" %d", s.depthCounts[i])
		}
		report = append(report, [][]string{
			{"activePercentage", fmt.Sprintf("%.1f%%", 100*float64(s.ActiveCount)/float64(s.usedEntries))},
			{"inactiveMask", fmt.Sprintf("%016x", s.inactiveMask)},
			{"workers", fmt.Sprintf("%d", s.workers)},
			{"roots", fmt.Sprintf("%d (%d bytes)", s.roots, uint64(s.roots)*uint64(unsafe.Sizeof(valueLocMapNode{})))},
			{"usedRoots", fmt.Sprintf("%d", s.usedRoots)},
			{"entryPageSize", fmt.Sprintf("%d (%d bytes)", s.entryPageSize, uint64(s.entryPageSize)*uint64(unsafe.Sizeof(valueLocMapEntry{})))},
			{"entryLockPageSize", fmt.Sprintf("%d (%d bytes)", s.entryLockPageSize, uint64(s.entryLockPageSize)*uint64(unsafe.Sizeof(sync.RWMutex{})))},
			{"splitLevel", fmt.Sprintf("%d +-10%%", s.splitLevel)},
			{"nodes", fmt.Sprintf("%d", s.nodes)},
			{"depth", fmt.Sprintf("%d", len(s.depthCounts))},
			{"depthCounts", depthCounts},
			{"allocedEntries", fmt.Sprintf("%d", s.allocedEntries)},
			{"allocedInOverflow", fmt.Sprintf("%d %.1f%%", s.allocedInOverflow, 100*float64(s.allocedInOverflow)/float64(s.allocedEntries))},
			{"usedEntries", fmt.Sprintf("%d %.1f%%", s.usedEntries, 100*float64(s.usedEntries)/float64(s.allocedEntries))},
			{"usedInOverflow", fmt.Sprintf("%d %.1f%%", s.usedInOverflow, 100*float64(s.usedInOverflow)/float64(s.usedEntries))},
			{"inactive", fmt.Sprintf("%d %.1f%%", s.inactive, 100*float64(s.inactive)/float64(s.usedEntries))},
		}...)
	}
	return brimtext.Align(report, nil)
}
