// Will be run if environment long_test=true
// Since this has concurrency tests, you probably want to run with something
// like:
// $ long_test=true go test -cpu=1,3,7
// The above command uses about 2.5G of memory and takes a bit over 3 minutes
// to run on my MacBook Pro Retina 15".

package locmap

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/gholt/brimio"
)

func TestGroupExerciseSplitMergeLong(t *testing.T) {
	if !RUN_LONG {
		t.Skip("skipping unless env long_test=true")
	}
	// count is the number of keys per keyset. Each key in a keyset will be
	// added and removed sequentially but the keysets will be executing
	// concurrently.
	count := 2000
	keysetCount := 100
	// Roots is set low to get deeper quicker and cause more contention.
	// PageSize is set low to cause more page creation and deletion.
	// SplitMultiplier is set low to get splits to happen quicker.
	locmap := NewGroupLocMap(&GroupLocMapConfig{Roots: 8, PageSize: 512, SplitMultiplier: 1}).(*groupLocMap)
	// Override the mergeLevel to make it happen more often.
	for i := 0; i < len(locmap.roots); i++ {
		locmap.roots[i].mergeLevel = locmap.roots[i].splitLevel - 2
	}
	if locmap.roots[0].mergeLevel < 10 {
		t.Fatal(locmap.roots[0].mergeLevel)
	}
	keyspaces := make([][]byte, keysetCount)
	for i := 0; i < keysetCount; i++ {
		keyspaces[i] = make([]byte, count*16)
		brimio.NewSeededScrambled(int64(i)).Read(keyspaces[i])
		// since scrambled doesn't guarantee uniqueness, we do that in the
		// middle of each key.
		for j := uint32(0); j < uint32(count); j++ {
			binary.BigEndian.PutUint32(keyspaces[i][j*16+4:], j)
		}
	}
	kt := func(ka uint64, kb uint64, ts uint64, b uint32, o uint32, l uint32) {

		for nA := uint64(0); nA < 5; nA++ {
			for nB := uint64(0); nB < 5; nB++ {
				locmap.Set(ka, kb, nA, nB, ts, b, o, l, false)
			}
		}

		if ts&2 != 0 { // test calls discard with 2 as a mask quite often
			return
		}

		for nA := uint64(0); nA < 5; nA++ {
			for nB := uint64(0); nB < 5; nB++ {
				ts2, b2, o2, l2 := locmap.Get(ka, kb, nA, nB)

				if (b != 0 && ts2 != ts) || (b == 0 && ts2 != 0) {
					panic(fmt.Sprintf("%x %x %d %d %d %d ! %d", ka, kb, ts, b, o, l, ts2))
				}
				if b2 != b {
					panic(fmt.Sprintf("%x %x %d %d %d %d ! %d", ka, kb, ts, b, o, l, b2))
				}
				if o2 != o {
					panic(fmt.Sprintf("%x %x %d %d %d %d ! %d", ka, kb, ts, b, o, l, o2))
				}
				if l2 != l {
					panic(fmt.Sprintf("%x %x %d %d %d %d ! %d", ka, kb, ts, b, o, l, l2))
				}

			}
		}
		items := locmap.GetGroup(ka, kb)
		if len(items) != 25 {
			panic(fmt.Sprintf("%x %x %d %d %d %d ! %d", ka, kb, ts, b, o, l, len(items)))
		}
		for nA := uint64(0); nA < 5; nA++ {
			for nB := uint64(0); nB < 5; nB++ {
				found := false
				for _, item := range items {
					if item.NameKeyA == nA && item.NameKeyB == nB {
						found = true
						break
					}
				}
				if !found {
					panic(fmt.Sprintf("%x %x %d %d %d %d ! %x %x", ka, kb, ts, b, o, l, nA, nB))
				}
			}
		}
		for _, item := range items {
			ts2 := item.Timestamp
			b2 := item.BlockID
			o2 := item.Offset
			l2 := item.Length
			if (b != 0 && ts2 != ts) || (b == 0 && ts2 != 0) {
				panic(fmt.Sprintf("%x %x %d %d %d %d ! %d", ka, kb, ts, b, o, l, ts2))
			}
			if b2 != b {
				panic(fmt.Sprintf("%x %x %d %d %d %d ! %d", ka, kb, ts, b, o, l, b2))
			}
			if o2 != o {
				panic(fmt.Sprintf("%x %x %d %d %d %d ! %d", ka, kb, ts, b, o, l, o2))
			}
			if l2 != l {
				panic(fmt.Sprintf("%x %x %d %d %d %d ! %d", ka, kb, ts, b, o, l, l2))
			}
		}

	}
	halfBytes := count / 2 * 16
	wg := sync.WaitGroup{}
	wg.Add(keysetCount)
	for i := 0; i < keysetCount; i++ {
		go func(j int) {
			for k := halfBytes - 16; k >= 0; k -= 16 {
				kt(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 1, 1, 2, 3)
			}
			for k := halfBytes - 16; k >= 0; k -= 16 {
				kt(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 2, 3, 4, 5)
			}
			for k := halfBytes - 16; k >= 0; k -= 16 {
				kt(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 3, 0, 0, 0)
			}
			for k := len(keyspaces[j]) - 16; k >= halfBytes; k -= 16 {
				kt(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 1, 1, 2, 3)
			}
			if j%100 == 0 {
				locmap.Discard(0, math.MaxUint64, 2)
			}
			if j%100 == 33 {
				uselessCounter := 0
				stopped, more := locmap.ScanCallback(0, math.MaxUint64, 0, 0, math.MaxUint64, math.MaxUint64, func(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestamp uint64, length uint32) bool {
					uselessCounter++
					return true
				})
				if more {
					panic(fmt.Sprintf("%x", stopped))
				}
			}
			if j%100 == 66 {
				locmap.SetInactiveMask(1)
				locmap.Stats(false)
			}
			for k := len(keyspaces[j]) - 16; k >= halfBytes; k -= 16 {
				kt(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 2, 3, 4, 5)
			}
			for k := len(keyspaces[j]) - 16; k >= halfBytes; k -= 16 {
				kt(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 3, 0, 0, 0)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	locmap.SetInactiveMask(0)
	stats := locmap.Stats(false)
	if stats.ActiveCount != 0 {
		t.Fatal(stats.ActiveCount)
	}
	if stats.ActiveBytes != 0 {
		t.Fatal(stats.ActiveBytes)
	}
}
