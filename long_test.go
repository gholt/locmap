// Will be run if environment long_test=true
// Since this has concurrency tests, you probably want to run with something
// like:
// $ long_test=true go test -cpu=1,3,7
// You'll need a good amount of RAM too. The above uses about 3G of memory and
// takes about 3.5 minutes to run on my MacBook Pro Retina 15".

package valuelocmap

import (
	"encoding/binary"
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
	count := 100000
	keysetCount := 1000
	// OptRoots is set low to get deeper quicker and cause more contention.
	// OptPageSize is set low to cause more page creation and deletion.
	// OptSplitMultiplier is set low to get splits to happen quicker.
	vlm := New(OptRoots(8), OptPageSize(512), OptSplitMultiplier(1)).(*valueLocMap)
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
		brimutil.NewSeededScrambled(int64(1)).Read(keyspaces[i])
	}
	halfBytes := count / 2 * 16
	wg := sync.WaitGroup{}
	for i := 0; i < keysetCount; i++ {
		wg.Add(1)
		go func(j int) {
			for k := halfBytes - 16; k >= 0; k -= 16 {
				vlm.Set(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 1, 1, 2, 3, false)
			}
			for k := halfBytes - 16; k >= 0; k -= 16 {
				vlm.Set(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 2, 3, 4, 5, false)
			}
			for k := halfBytes - 16; k >= 0; k -= 16 {
				vlm.Set(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 3, 0, 0, 0, false)
			}
			for k := len(keyspaces[j]) - 16; k >= halfBytes; k -= 16 {
				vlm.Set(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 1, 1, 2, 3, false)
			}
			for k := len(keyspaces[j]) - 16; k >= halfBytes; k -= 16 {
				vlm.Set(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 2, 3, 4, 5, false)
			}
			for k := len(keyspaces[j]) - 16; k >= halfBytes; k -= 16 {
				vlm.Set(binary.BigEndian.Uint64(keyspaces[j][k:]), binary.BigEndian.Uint64(keyspaces[j][k+8:]), 3, 0, 0, 0, false)
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
