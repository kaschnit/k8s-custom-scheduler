package immut

import (
	"cmp"
	"hash/maphash"
	"math/bits"
	"slices"
)

const (
	// champBranchingFactor is the branching factor of the CHAMP.
	// The branching factor is the maximum nodes per level.
	champBranchingFactor = 64
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

// clone creates a clone of the node.
func (node *champNode[K, V]) clone() *champNode[K, V] {
	nodeClone := &champNode[K, V]{
		entryMap: node.entryMap,
		childMap: node.childMap,
	}

	if len(node.entries) > 0 {
		nodeClone.entries = make([]champEntry[K, V], len(node.entries))
		copy(nodeClone.entries, node.entries)
	}
	if len(node.children) > 0 {
		nodeClone.children = make([]*champNode[K, V], len(node.children))
		copy(nodeClone.children, node.children)
	}

	return nodeClone
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
				result := node.clone()
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
		result := node.clone()
		result.entries = slices.Insert(result.entries, entryIndex, champEntry[K, V]{Key: key, Value: value})
		return result
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
			result := node.clone()
			result.entryMap |= bitmapPos
			result.entries[entryIndex].Value = value
			return result
		}

		result := node.clone()

		// Move entry to child (collision).
		// Split existing and new colliding entry to child
		child := &champNode[K, V]{}
		child = child.insert(existingEntry.Key, maphash.Comparable(hashSeed, existingEntry.Key), existingEntry.Value, depth+1, hashSeed)
		child = child.insert(key, hash, value, depth+1, hashSeed)

		// Remove entry since it was moved to child
		result.entryMap &= ^bitmapPos
		result.entries = slices.Delete(result.entries, entryIndex, entryIndex+1)

		// Add child since we created a new child
		result.childMap |= bitmapPos
		childIndex := champIndex(result.childMap, bitmapPos)
		result.children = slices.Insert(result.children, childIndex, child)

		return result
	}

	// Check for child.
	// Either get existing one, or create a new one.
	if node.childMap&bitmapPos != 0 {
		// Child exists.
		childIndex := champIndex(node.childMap, bitmapPos)
		child := node.children[childIndex].insert(key, hash, value, depth+1, hashSeed)
		result := node.clone()
		result.children[childIndex] = child
		return result
	}

	// Child does not exist.
	// Create entry
	result := node.clone()
	result.entryMap |= bitmapPos
	entryIndex := champIndex(result.entryMap, bitmapPos)
	result.entries = slices.Insert(result.entries, entryIndex, champEntry[K, V]{Key: key, Value: value})

	return result
}

func (node *champNode[K, V]) delete(key K, hash uint64, depth int) *champNode[K, V] {
	if depth == champMaxDepth {
		for i := range node.entries {
			if node.entries[i].Key == key {
				result := node.clone()
				result.entries = slices.Delete(result.entries, i, i+1)
				return result
			}
		}

		return nil
	}

	bitmapPos := champBitmapPos(hash, depth)

	if node.entryMap&bitmapPos != 0 {
		result := node.clone()

		// Set entry bit to 0 to indicate not present.
		result.entryMap &= ^bitmapPos
		entryIndex := champIndex(result.entryMap, bitmapPos)
		// Delete entry.
		result.entries = slices.Delete(result.entries, entryIndex, entryIndex+1)

		return result
	}

	if node.childMap&bitmapPos != 0 {
		childIndex := champIndex(node.childMap, bitmapPos)

		child := node.children[childIndex].delete(key, hash, depth+1)
		if child == nil {
			// Nothing was deleted, return current node without cloning.
			return node
		}

		result := node.clone()
		if len(child.entries) == 1 && len(child.children) == 0 {
			// Child only has one item remaining, can be collapsed.
			// Child can never reach 0 entries, because it gets created at 2 and collapsed at 1.
			// Check the entries length, not the entryMap 1 count, to covers full hash collisions.
			entry := child.entries[0]

			// Set child bit to 0 to indicate not present.
			result.childMap &= ^bitmapPos
			// Delete child.
			result.children = slices.Delete(result.children, childIndex, childIndex+1)

			result.entryMap |= bitmapPos
			entryIndex := champIndex(result.entryMap, bitmapPos)
			result.entries = slices.Insert(result.entries, entryIndex, entry)
		} else {
			result.children[childIndex] = child
		}

		return result
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
