package immut

import (
	"cmp"
	"hash/maphash"
	"iter"
)

// Map is an immutable map.
// The map is based on the Compressed Hash-Array Mapped Prefix-tree (CHAMP) data structure.
// This data structure is a variant of Hash-Array Mapped Trie (HAMT) that is more CPU cache-friendly
// at the cost of slightly more memory usage.
// All operations are thread-safe.
type Map[K cmp.Ordered, V any] struct {
	hashSeed maphash.Seed
	size     int
	root     champNode[K, V]
}

// MakeMap creates a [Map].
func MakeMap[K cmp.Ordered, V any]() Map[K, V] {
	return Map[K, V]{
		hashSeed: maphash.MakeSeed(),
	}
}

// Get gets the value associated with key.
// If the key exists, returns the value and true.
// If the key doesn't exist, returns the zero value and false.
func (m Map[K, V]) Get(key K) (V, bool) {
	return m.root.get(key, maphash.Comparable(m.hashSeed, key), 1)
}

// Set associates the value with the key.
// Returns a copy of the map without mutating the original even if an identical
// key/value pair already existed in the map.
func (m Map[K, V]) Set(key K, value V) Map[K, V] {
	newRoot, isNewKey := m.root.insert(key, maphash.Comparable(m.hashSeed, key), value, 1, m.hashSeed)

	result := Map[K, V]{
		hashSeed: m.hashSeed,
		size:     m.size,
		root:     newRoot,
	}

	if isNewKey {
		result.size++
	}

	return result
}

// Delete removes the key and its associated value.
// If the key exists, returns a copy of the map without mutating the original.
// If the key doesn't exist, returns this map without mutating.
func (m Map[K, V]) Delete(key K) Map[K, V] {
	newRoot, deleted := m.root.delete(key, maphash.Comparable(m.hashSeed, key), 1)
	if !deleted {
		return m
	}

	return Map[K, V]{
		hashSeed: m.hashSeed,
		size:     m.size - 1,
		root:     newRoot,
	}
}

// Len returns the number of items in this map.
func (m Map[K, V]) Len() int {
	return m.size
}

// All iterates the key/value pairs.
func (m Map[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		m.root.traverse(yield)
	}
}

// Keys iterates the keys.
func (m Map[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		m.root.traverseKeys(yield)
	}
}

// Values iterates the values.
func (m Map[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		m.root.traverseValues(yield)
	}
}
