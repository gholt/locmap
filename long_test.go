// Will be run if environment long_test=true
// Since this has concurrency tests, you probably want to run with something
// like:
// $ long_test=true go test -cpu=1,3,7
// You'll need a good amount of RAM too. The above uses about 3-4G of memory
// and takes about 2-3 minutes to run on my MacBook Pro Retina 15".

package valuelocmap

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"

	"gopkg.in/gholt/brimutil.v1"
)

var RUN_LONG bool = false

func init() {
	if os.Getenv("long_test") == "true" {
		RUN_LONG = true
	}
}

func TestExerciseSplitMergeLong(t *testing.T) {
	if !RUN_LONG {
		t.Skip("skipping unless env long_test=true")
	}
	// count is the number of keys per keyset. Each key in a keyset will be
	// added and removed sequentially but the keysets will be executing
	// concurrently.
	count := 20000
	keysetCount := 1000
	// Roots is set low to get deeper quicker and cause more contention.
	// PageSize is set low to cause more page creation and deletion.
	// SplitMultiplier is set low to get splits to happen quicker.
	vlm := New(&Config{Roots: 8, PageSize: 512, SplitMultiplier: 1}).(*valueLocMap)
	// Override the mergeLevel to make it happen more often.
	for i := 0; i < len(vlm.roots); i++ {
		vlm.roots[i].mergeLevel = vlm.roots[i].splitLevel - 2
	}
	if vlm.roots[0].mergeLevel < 10 {
		t.Fatal(vlm.roots[0].mergeLevel)
	}
	keyspaces := make([][]byte, keysetCount)
	for i := 0; i < keysetCount; i++ {
		keyspaces[i] = make([]byte, count*16)
		brimutil.NewSeededScrambled(int64(i)).Read(keyspaces[i])
		// since scrambled doesn't guarantee uniqueness, we do that in the
		// middle of each key.
		for j := uint32(0); j < uint32(count); j++ {
			binary.BigEndian.PutUint32(keyspaces[i][j*16+4:], j)
		}
	}
	kt := func(ka uint64, kb uint64, ts uint64, b uint32, o uint32, l uint32) {
		vlm.Set(ka, kb, ts, b, o, l, false)
		if ts&2 != 0 { // test calls discard with 2 as a mask quite often
			return
		}
		ts2, b2, o2, l2 := vlm.Get(ka, kb)
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
				vlm.Discard(0, math.MaxUint64, 2)
			}
			if j%100 == 33 {
				uselessCounter := 0
				stopped, more := vlm.ScanCallback(0, math.MaxUint64, 0, 0, math.MaxUint64, math.MaxUint64, func(keyA uint64, keyB uint64, timestamp uint64, length uint32) {
					uselessCounter++
				})
				if more {
					panic(fmt.Sprintf("%x", stopped))
				}
			}
			if j%100 == 66 {
				vlm.GatherStats(1, false)
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
	endingCount, length, _ := vlm.GatherStats(uint64(0), false)
	if endingCount != 0 {
		t.Fatal(endingCount)
	}
	if length != 0 {
		t.Fatal(length)
	}
}
