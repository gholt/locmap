package valuelocmap

// TODO: Implement and test all the things for real.

// GroupLocMap is an interface for tracking the mappings from keys to the
// locations of their values.
type GroupLocMap interface {
	// Get returns timestamp, blockID, offset, length for keyA, keyB, nameKeyA, nameKeyB.
	Get(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64) (timestamp uint64, blockID uint32, offset uint32, length uint32)
	// GetGroup returns all items that match keyA, keyB.
	GetGroup(keyA uint64, keyB uint64) []*GroupLocMapItem
	// Set stores timestamp, blockID, offset, length for keyA, keyB and returns
	// the previous timestamp stored. If a newer item is already stored for
	// keyA, keyB, that newer item is kept. If an item with the same timestamp
	// is already stored, it is usually kept unless evenIfSameTimestamp is set
	// true, in which case the passed in data is kept (useful to update a
	// location that moved from memory to disk, for example). Setting an item
	// to blockID == 0 removes it from the mapping if the timestamp stored is
	// less than (or equal to if evenIfSameTimestamp) the timestamp passed in.
	Set(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestamp uint64, blockID uint32, offset uint32, length uint32, evenIfSameTimestamp bool) (previousTimestamp uint64)
	// Discard removes any items in the start:stop (inclusive) range whose
	// timestamp & mask != 0.
	Discard(start uint64, stop uint64, mask uint64)
	// ScanCallback calls the callback for each item within the start:stop
	// range (inclusive) whose timestamp & mask != 0 || mask == 0, timestamp &
	// notMask == 0, and timestamp <= cutoff, up to max times; it will return
	// the keyA value the scan stopped and more will be true if there are
	// possibly more items but max was reached.
	//
	// Note that callbacks may have been made with keys that were greater than
	// or equal to where the scan indicated it had stopped (reached the max).
	// This is because the callbacks are made as items are encountered while
	// walking the structure and the structure is not 100% in key order. The
	// items are grouped and the groups are in key order, but the items in each
	// group are not. The max, if reached, will likely be reached in the middle
	// of a group. This means that items may be duplicated in a subsequent scan
	// that begins where a previous scan indicated it stopped.
	//
	// In practice with the valuestore use case this hasn't been an issue yet.
	// Discard passes don't duplicate because the same keys won't match the
	// modified mask from the previous pass. Outgoing pull replication passes
	// just end up with some additional keys placed in the bloom filter
	// resulting in slightly higher false positive rates (corrected for with
	// subsequent iterations). Outgoing push replication passes end up sending
	// duplicate information, wasting a bit of bandwidth.
	//
	// Additionally, the callback itself may abort the scan early by returning
	// false, in which case the (stopped, more) return values are not
	// particularly useful.
	ScanCallback(start uint64, stop uint64, mask uint64, notMask uint64, cutoff uint64, max uint64, callback func(keyA uint64, keyB uint64, nameKeyA uint64, nameKeyB uint64, timestamp uint64, length uint32) bool) (stopped uint64, more bool)
	// SetInactiveMask defines the mask to use with a timestamp to determine if
	// a location is inactive (deleted, locally removed, etc.) and is used by
	// Stats to determine what to count for its ActiveCount and ActiveBytes.
	SetInactiveMask(mask uint64)
	// Stats returns a Stats instance giving information about the GroupLocMap.
	//
	// Note that this walks the entire data structure and is relatively
	// expensive; debug = true will make it even more expensive.
	//
	// The various values reported when debug=true are left undocumented
	// because they are subject to change based on implementation. They are
	// only provided when Stats.String() is called.
	Stats(debug bool) *Stats
}

type GroupLocMapItem struct {
	NameKeyA  uint64
	NameKeyB  uint64
	Timestamp uint64
	BlockID   uint32
	Offset    uint32
	Length    uint32
}

func (tlm *groupLocMap) GetGroup(keyA uint64, keyB uint64) []*GroupLocMapItem {
	return nil
}
