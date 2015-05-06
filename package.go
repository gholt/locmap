// Package valuelocmap provides a concurrency-safe data structure that maps
// keys to value locations. A key is 128 bits and is specified using two
// uint64s (keyA, keyB). A value location depends on the valuelocmap
// implementation chosen, but has at least a blockID and offset. Each mapping
// is assigned a timestamp and the greatest timestamp wins.
//
// The timestamp usually has some number of the lowest bits in use for state
// information such as active and inactive entries. For example, the lowest bit
// might be used as 0 = active, 1 = deletion marker so that deletion events are
// retained for some time period before being completely removed with Discard.
// Exactly how many bits are used and what they're used for is outside the
// scope of the mapping itself.
//
// This implementation essentially uses a tree structure of slices of key to
// location assignments. When a slice fills up, an additional slice is created
// and half the data is moved to the new slice and the tree structure grows. If
// a slice empties, it is merged with its pair in the tree structure and the
// tree shrinks. The tree is balanced by high bits of the key, and locations
// are distributed in the slices by the low bits.
package valuelocmap

//go:generate got valuelocmap.got valuelocmap.go
//go:generate got valuelocmap_test.got valuelocmap_test.go
//go:generate got valuelocmap.got valuelocmapl.go l
//go:generate got valuelocmap_test.got valuelocmapl_test.go l
