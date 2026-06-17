package immut

import (
	"cmp"
	"hash/maphash"
	"math/bits"
)

const (
	// champBranchingFactorBits is the number of bits needed to represent a branching
	// factor of 64. It is 6 because we need 6 bits to represent 64 items.
	champBranchingFactorBits = 6
	// champBranchingFactorBitsMask is a mask to extract branch bits for CHAMP with branching
	// factor of 64. This is 6 bits because [branchingFactor64Bits] is 6.
	champBranchingFactorBitsMask uint64 = 0b111111
	// champMaxDepth is the max depth of the CHAMP.
	// This is 11 because CHAMP uses 64 bit hash, and 6 bits are used per level.
	// The 11th level uses only 4 bits since the previous 10 levels use a total of 60 bits,
	// leaving only 4 for the 11th level.
	champMaxDepth = 11
)

type champEntry[K comparable, V any] struct {
	Key   K
	Value V
}

type champNode[K cmp.Ordered, V any] struct {
	entryMap uint64
	childMap uint64
	entries  []champEntry[K, V]
	children []*champNode[K, V]
}

func (node *champNode[K, V]) get(key K, hash uint64, depth int) (V, bool) {
	if depth == champMaxDepth {
		// Max depth, must linear search to handle full hash collisions.
		// Iterate all entries and find matching key if there is any.
		for _, entry := range node.entries {
			if entry.Key == key {
				return entry.Value, true
			}
		}

		var zero V
		return zero, false
	}

	bitmapPos := champBitmapPos(hash, depth)

	// Check for entry.
	if node.entryMap&bitmapPos != 0 {
		// Get index of entry.
		index := champIndex(node.entryMap, bitmapPos)

		// Check if key matches to ensure no hash collision.
		if entry := node.entries[index]; entry.Key == key {
			return entry.Value, true
		}

		// Key did not match (hash collision).
		var zero V
		return zero, false
	}

	// Check for child.
	if node.childMap&bitmapPos != 0 {
		// Get index of child.
		index := champIndex(node.childMap, bitmapPos)
		// Recurse on child.
		return node.children[index].get(key, hash, depth+1)
	}

	// Key did not match (no entry or children).
	var zero V
	return zero, false
}

func (node *champNode[K, V]) insert(key K, hash uint64, value V, depth int, hashSeed maphash.Seed) *champNode[K, V] {
	if depth == champMaxDepth {
		// Max depth, do not use hashing here to avoid hash collision.
		// Check for existing entry to overwrite.
		for i := range node.entries {
			if node.entries[i].Key == key {
				result := &champNode[K, V]{
					entryMap: node.entryMap,
					childMap: node.childMap,
					children: node.children,
					entries:  make([]champEntry[K, V], len(node.entries)),
				}
				copy(result.entries, node.entries)
				result.entries[i].Value = value
				return result
			}
		}

		// Add new entry.
		entryIndex := len(node.entries)
		for i := range node.entries {
			if node.entries[i].Key > key {
				entryIndex = i
				break
			}
		}

		newEntries := make([]champEntry[K, V], len(node.entries)+1)
		copy(newEntries[:entryIndex], node.entries[:entryIndex])
		newEntries[entryIndex] = champEntry[K, V]{Key: key, Value: value}
		copy(newEntries[entryIndex+1:], node.entries[entryIndex:])

		return &champNode[K, V]{
			entryMap: node.entryMap,
			childMap: node.childMap,
			children: node.children,
			entries:  newEntries,
		}
	}

	bitmapPos := champBitmapPos(hash, depth)

	// Check for entry.
	if node.entryMap&bitmapPos != 0 {
		// Entry exists.
		// Get index of entry.
		entryIndex := champIndex(node.entryMap, bitmapPos)

		// Check if key matches to decide whether to overwrite or handle
		// hash collision and propagate to child.
		existingEntry := node.entries[entryIndex]
		if existingEntry.Key == key {
			// Overwrite entry (not a collision).
			result := &champNode[K, V]{
				entryMap: node.entryMap,
				childMap: node.childMap,
				children: node.children,
				entries:  make([]champEntry[K, V], len(node.entries)),
			}
			copy(result.entries, node.entries)
			result.entries[entryIndex].Value = value
			return result
		}

		// Split existing entry into a child branch:
		// 1. The entry is removed from entries, so new length is len - 1
		newEntries := make([]champEntry[K, V], len(node.entries)-1)
		copy(newEntries[:entryIndex], node.entries[:entryIndex])
		copy(newEntries[entryIndex:], node.entries[entryIndex+1:])

		// 2. A new child node is added, so new children length is len + 1
		child := &champNode[K, V]{}
		child = child.insert(existingEntry.Key, maphash.Comparable(hashSeed, existingEntry.Key), existingEntry.Value, depth+1, hashSeed)
		child = child.insert(key, hash, value, depth+1, hashSeed)

		newChildMap := node.childMap | bitmapPos
		childIndex := champIndex(newChildMap, bitmapPos)

		newChildren := make([]*champNode[K, V], len(node.children)+1)
		copy(newChildren[:childIndex], node.children[:childIndex])
		newChildren[childIndex] = child
		copy(newChildren[childIndex+1:], node.children[childIndex:])

		return &champNode[K, V]{
			entryMap: node.entryMap & ^bitmapPos,
			childMap: newChildMap,
			entries:  newEntries,
			children: newChildren,
		}
	}

	// Check for child.
	// Either get existing one, or create a new one.
	if node.childMap&bitmapPos != 0 {
		// Child exists.
		childIndex := champIndex(node.childMap, bitmapPos)
		child := node.children[childIndex].insert(key, hash, value, depth+1, hashSeed)

		// Mutating an existing child path: structural sharing for entries,
		// and we only allocate a new children slice of the exact same size to update the pointer.
		newChildren := make([]*champNode[K, V], len(node.children))
		copy(newChildren, node.children)
		newChildren[childIndex] = child

		return &champNode[K, V]{
			entryMap: node.entryMap,
			childMap: node.childMap,
			entries:  node.entries,
			children: newChildren,
		}
	}

	// Child does not exist.
	// Create entry.
	// Slot is completely empty: entries length increases by 1, children is shared.
	entryIndex := champIndex(node.entryMap, bitmapPos)
	newEntryMap := node.entryMap | bitmapPos

	newEntries := make([]champEntry[K, V], len(node.entries)+1)
	copy(newEntries[:entryIndex], node.entries[:entryIndex])
	newEntries[entryIndex] = champEntry[K, V]{Key: key, Value: value}
	copy(newEntries[entryIndex+1:], node.entries[entryIndex:])

	return &champNode[K, V]{
		entryMap: newEntryMap,
		childMap: node.childMap,
		entries:  newEntries,
		children: node.children,
	}
}

func (node *champNode[K, V]) delete(key K, hash uint64, depth int) *champNode[K, V] {
	if depth == champMaxDepth {
		for i := range node.entries {
			if node.entries[i].Key == key {
				// Exact size allocation for removing an element
				if len(node.entries) == 1 {
					return nil // Terminal node is now completely empty
				}
				newEntries := make([]champEntry[K, V], len(node.entries)-1)
				copy(newEntries[:i], node.entries[:i])
				copy(newEntries[i:], node.entries[i+1:])

				return &champNode[K, V]{
					entryMap: node.entryMap,
					childMap: node.childMap,
					entries:  newEntries,
					children: node.children,
				}
			}
		}

		return nil
	}

	bitmapPos := champBitmapPos(hash, depth)

	if node.entryMap&bitmapPos != 0 {
		entryIndex := champIndex(node.entryMap, bitmapPos)
		if node.entries[entryIndex].Key != key {
			return nil // Hash collision but different key: item doesn't exist
		}

		// Exact size allocation: remove 1 entry from this node
		newEntries := make([]champEntry[K, V], len(node.entries)-1)
		copy(newEntries[:entryIndex], node.entries[:entryIndex])
		copy(newEntries[entryIndex:], node.entries[entryIndex+1:])

		return &champNode[K, V]{
			entryMap: node.entryMap & ^bitmapPos,
			childMap: node.childMap,
			entries:  newEntries,
			children: node.children,
		}
	}

	if node.childMap&bitmapPos != 0 {
		childIndex := champIndex(node.childMap, bitmapPos)

		child := node.children[childIndex].delete(key, hash, depth+1)
		if child == nil {
			return nil
		}

		if len(child.entries) == 1 && len(child.children) == 0 {
			// Child only has one item remaining, can be collapsed.
			// Child can never reach 0 entries, because it gets created at 2 and collapsed at 1.
			// Check the entries length, not the entryMap 1 count, to covers full hash collisions.
			entry := child.entries[0]

			// Remove the child pointer
			newChildren := make([]*champNode[K, V], len(node.children)-1)
			copy(newChildren[:childIndex], node.children[:childIndex])
			copy(newChildren[childIndex:], node.children[childIndex+1:])

			// Add the collapsed leaf entry
			newEntryMap := node.entryMap | bitmapPos
			entryIndex := champIndex(newEntryMap, bitmapPos)

			newEntries := make([]champEntry[K, V], len(node.entries)+1)
			copy(newEntries[:entryIndex], node.entries[:entryIndex])
			newEntries[entryIndex] = entry
			copy(newEntries[entryIndex+1:], node.entries[entryIndex:])

			return &champNode[K, V]{
				entryMap: newEntryMap,
				childMap: node.childMap & ^bitmapPos,
				entries:  newEntries,
				children: newChildren,
			}
		}

		// Standard child update: Structural sharing for entries array,
		// allocate an exact matching slice size only for the updated child pointer array.
		newChildren := make([]*champNode[K, V], len(node.children))
		copy(newChildren, node.children)
		newChildren[childIndex] = child

		return &champNode[K, V]{
			entryMap: node.entryMap,
			childMap: node.childMap,
			entries:  node.entries, // Structurally shared!
			children: newChildren,
		}
	}

	// Nothing to delete
	return nil
}

func (node *champNode[K, V]) traverse(yield func(K, V) bool) bool {
	// If we are at a terminal collision node, ignore bitmaps and yield linearly
	if node.entryMap == 0 && node.childMap == 0 && len(node.entries) > 0 {
		for _, entry := range node.entries {
			if !yield(entry.Key, entry.Value) {
				return false
			}
		}
		return true
	}

	var entryIdx, childIdx int

	for i := range 64 {
		bit := uint64(1) << i
		if node.entryMap&bit != 0 {
			if !yield(node.entries[entryIdx].Key, node.entries[entryIdx].Value) {
				return false
			}
			entryIdx++
		}

		if node.childMap&bit != 0 {
			if !node.children[childIdx].traverse(yield) {
				return false
			}
			childIdx++
		}
	}

	return true
}

func (node *champNode[K, V]) traverseKeys(yield func(K) bool) bool {
	// If we are at a terminal collision node, ignore bitmaps and yield linearly
	if node.entryMap == 0 && node.childMap == 0 && len(node.entries) > 0 {
		for _, entry := range node.entries {
			if !yield(entry.Key) {
				return false
			}
		}
		return true
	}

	var entryIdx, childIdx int

	for i := range 64 {
		bit := uint64(1) << i
		if node.entryMap&bit != 0 {
			if !yield(node.entries[entryIdx].Key) {
				return false
			}
			entryIdx++
		}

		if node.childMap&bit != 0 {
			if !node.children[childIdx].traverseKeys(yield) {
				return false
			}
			childIdx++
		}
	}

	return true
}

func (node *champNode[K, V]) traverseValues(yield func(V) bool) bool {
	// If we are at a terminal collision node, ignore bitmaps and yield linearly
	if node.entryMap == 0 && node.childMap == 0 && len(node.entries) > 0 {
		for _, entry := range node.entries {
			if !yield(entry.Value) {
				return false
			}
		}
		return true
	}

	var entryIdx, childIdx int

	for i := range 64 {
		bit := uint64(1) << i
		if node.entryMap&bit != 0 {
			if !yield(node.entries[entryIdx].Value) {
				return false
			}
			entryIdx++
		}

		if node.childMap&bit != 0 {
			if !node.children[childIdx].traverseValues(yield) {
				return false
			}
			childIdx++
		}
	}

	return true
}

// champBitmapPos gets the position of the hash's item within
// a bitmap representing a node's items at the given depth.
func champBitmapPos(hash uint64, depth int) uint64 {
	// There are branchingFactor64Bits bits per level.
	// At depth 1, we want the first branchingFactor64Bits.
	// At depth 2, we want the next branchingFactor64Bits.
	// And so on. So multiply branchingFactor64Bits by depth.
	rShiftForDepth := depth * champBranchingFactorBits

	// Extract the bits for the given depth.
	// Shift right to get thsi depth's bits all the way at the right.
	// Apply mask to clear any bits to the left of them.
	bitsForDepth := (hash >> rShiftForDepth) & champBranchingFactorBitsMask

	// Get the position within a 64-bit bitmap for the number that these bits represent.
	return uint64(1) << bitsForDepth
}

// champIndex gets an index for the item in the bitmap at the given position.
func champIndex(bitmap, bitmapPos uint64) int {
	// bitmapPos is a single set bit, so subtracting 1 yields a mask of all 1 bits.
	// For example, if bitmapPos is 0b1000, bitmapPos-1 is 0b0111.
	// This can be used to mask all bits before the given position.
	mask := bitmapPos - 1
	// Extract bits lower than (to the right of) bitmapPos.
	bitsLowerThanPos := bitmap & mask
	// Count the number of lower bits.
	// This provides the index into the slice of items in the node.
	return bits.OnesCount64(bitsLowerThanPos)
}
