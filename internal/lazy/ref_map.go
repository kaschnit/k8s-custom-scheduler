package lazy

import (
	"iter"
	"sync"
)

// RefMap is a lazy map that uses reference counting to determine when writes are needed.
//
// The entire underlying data is shallow-copied when cloned.
// Each underlying value is cloned only when needed.
//
// RefMap provides an interface that looks like a deep-copyable data structure,
// but only data that needs to be deep-copied is actually deep-copied when it needs to be.
//
// RefMap can be used with non-pointer values, but the benefits of lazy cloning are more
// apparent when working with pointers. Non-pointer values may have worse performance than an eager
// deep copy would, since they're first shallow-copied on write, and again cloned on first shared read.
//
// All receiver methods of RefMap are thread-safe.
type RefMap[K comparable, V Value[V]] struct {
	// data is the underlying map.
	// This is copied on write if there's more than 1 reference.
	data map[K]*SharedValue[V]
	// lock synchronizes access to shared.
	lock sync.RWMutex
}

// NewRefMap creates a new [RefMap].
func NewRefMap[K comparable, V Value[V]]() *RefMap[K, V] {
	return &RefMap[K, V]{
		data: make(map[K]*SharedValue[V]),
	}
}

// Get gets the value associated with key.
// Returns the value and true if found.
// Returns zero-value and false if not found.
func (rcm *RefMap[K, V]) Get(key K) (V, bool) {
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
func (rcm *RefMap[K, V]) Put(key K, val V) {
	newVal := NewSharedValue(val)

	rcm.lock.Lock()
	defer rcm.lock.Unlock()

	if existingVal, exists := rcm.data[key]; exists {
		existingVal.Detach()
	}

	rcm.data[key] = newVal
}

// Delete deletes the value associated with the key from the map.
// Returns whether any value was actually removed (whether the value was present).
func (rcm *RefMap[K, V]) Delete(key K) bool {
	rcm.lock.Lock()
	defer rcm.lock.Unlock()

	if existingVal, exists := rcm.data[key]; exists {
		existingVal.Detach()
		delete(rcm.data, key)
		return true
	}

	return false
}

// All returns an iterator over key-value pairs.
// See [maps.All].
func (rcm *RefMap[K, V]) All() iter.Seq2[K, V] {
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
func (rcm *RefMap[K, V]) Values() iter.Seq[V] {
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

// RefCount returns the number of references to the entry
// within the underlying data. Returns 0 if key does not exist.
func (rcm *RefMap[K, V]) RefCount(key K) int64 {
	rcm.lock.RLock()
	val, ok := rcm.data[key]
	rcm.lock.RUnlock()

	if !ok {
		return 0
	}

	return val.RefCount()
}

// Clear clears the map.
func (rcm *RefMap[K, V]) Clear() {
	rcm.lock.Lock()
	defer rcm.lock.Unlock()

	for _, v := range rcm.data {
		v.Detach()
	}

	rcm.data = make(map[K]*SharedValue[V])
}

// Clone creates a clone of the map.
// This only creates a shallow clone, but can be treated as a deep clone
// because shared values are cloned when read.
func (rcm *RefMap[K, V]) Clone() *RefMap[K, V] {
	rcm.lock.Lock()
	clonedData := make(map[K]*SharedValue[V], len(rcm.data))
	for k, v := range rcm.data {
		clonedData[k] = v.Fork()
	}
	rcm.lock.Unlock()

	return &RefMap[K, V]{
		data: clonedData,
	}
}

// ToMap returns a copy of the underlying map.
func (rcm *RefMap[K, V]) ToMap() map[K]V {
	rcmClone := rcm.Clone()
	defer rcmClone.Clear()

	result := make(map[K]V, len(rcmClone.data))
	for k, v := range rcmClone.data {
		result[k] = v.Get()
	}

	return result
}
