package lazy

import (
	"iter"
	"sync"
)

// Map is a lazy map that uses reference counting to determine when writes are needed.
//
// The entire underlying data is shallow-copied when cloned.
// Each underlying value is cloned only when needed.
//
// Map provides an interface that looks like a deep-copyable data structure,
// but only data that needs to be deep-copied is actually deep-copied when it needs to be.
//
// Map can be used with non-pointer values, but the benefits of lazy cloning are more
// apparent when working with pointers. Non-pointer values may have worse performance than an eager
// deep copy would, since they're first shallow-copied on write, and again cloned on first shared read.
//
// All receiver methods of Map are thread-safe.
type Map[K comparable, V Value[V]] struct {
	// data is the underlying map.
	// This is copied on write if there's more than 1 reference.
	data map[K]*SharedValue[V]
	// lock synchronizes access to shared.
	lock sync.RWMutex
}

// NewMap creates a new [Map].
func NewMap[K comparable, V Value[V]]() *Map[K, V] {
	return &Map[K, V]{
		data: make(map[K]*SharedValue[V]),
	}
}

// Get gets the value associated with key.
// Returns the value and true if found.
// Returns zero-value and false if not found.
func (rcm *Map[K, V]) Get(key K) (V, bool) {
	rcm.lock.RLock()
	val, ok := rcm.data[key]
	rcm.lock.RUnlock()

	if !ok {
		var zero V
		return zero, false
	}

	return val.Get(), true
}

// Put associates the value with the key.
// It overwrites the existing value if the key exists.
// If the key does not exist it creates a new key/value pair.
//
// IMPORTANT: If val is a pointer or has internal pointers, the caller should not modify it
// after calling Put(). Doing so may give unexpected results. Ideally, the caller should not
// hang onto any references to val after calling Put().
func (rcm *Map[K, V]) Put(key K, val V) {
	newVal := NewSharedValue(val)

	rcm.lock.Lock()
	defer rcm.lock.Unlock()

	if existingVal, exists := rcm.data[key]; exists {
		existingVal.detach()
	}

	rcm.data[key] = newVal
}

// Delete deletes the value associated with the key from the map.
// Returns whether any value was actually removed (whether the value was present).
func (rcm *Map[K, V]) Delete(key K) bool {
	rcm.lock.Lock()
	defer rcm.lock.Unlock()

	if existingVal, exists := rcm.data[key]; exists {
		existingVal.detach()
		delete(rcm.data, key)
		return true
	}

	return false
}

// All returns an iterator over key-value pairs.
// See [maps.All].
func (rcm *Map[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		rcmClone := rcm.Clone()
		defer rcmClone.Clear()

		// Safe to do without lock, because any other reference to the underlying map
		// will cause rc>1, so writes will cause a copy of the underlying map.
		for k, v := range rcmClone.data {
			if !yield(k, v.Get()) {
				return
			}
		}
	}
}

// All returns an iterator over the values.
// See [maps.Values].
func (rcm *Map[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		rcmClone := rcm.Clone()
		defer rcmClone.Clear()

		// Safe to do without lock, because any other reference to the underlying map
		// will cause rc>1, so writes will cause a copy of the underlying map.
		for _, v := range rcmClone.data {
			if !yield(v.Get()) {
				return
			}
		}
	}
}

// ShareCount returns the number of references to the entry
// within the underlying data. Returns 0 if key does not exist.
func (rcm *Map[K, V]) ShareCount(key K) int64 {
	rcm.lock.RLock()
	val, ok := rcm.data[key]
	rcm.lock.RUnlock()

	if !ok {
		return 0
	}

	return val.RefCount()
}

// Clear clears the map.
func (rcm *Map[K, V]) Clear() {
	rcm.lock.Lock()
	defer rcm.lock.Unlock()

	for _, v := range rcm.data {
		v.detach()
	}

	rcm.data = make(map[K]*SharedValue[V])
}

// Clone creates a clone of the map.
func (rcm *Map[K, V]) Clone() *Map[K, V] {
	rcm.lock.Lock()
	clonedData := make(map[K]*SharedValue[V], len(rcm.data))
	for k, v := range rcm.data {
		clonedData[k] = v.fork()
	}
	rcm.lock.Unlock()

	return &Map[K, V]{
		data: clonedData,
	}
}

// ToMap returns a copy of the underlying map.
func (rcm *Map[K, V]) ToMap() map[K]V {
	rcmClone := rcm.Clone()
	defer rcmClone.Clear()

	result := make(map[K]V, len(rcmClone.data))
	for k, v := range rcmClone.data {
		result[k] = v.Get()
	}

	return result
}
