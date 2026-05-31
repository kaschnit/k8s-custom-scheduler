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
	data map[K]*sharedValue[V]
	// lock synchronizes access to shared.
	lock sync.RWMutex
}

// NewMap creates a new [Map].
func NewMap[K comparable, V Value[V]]() *Map[K, V] {
	return &Map[K, V]{
		data: make(map[K]*sharedValue[V]),
	}
}

// Get gets the value associated with key.
// Returns the value and true if found.
// Returns zero-value and false if not found.
func (rcm *Map[K, V]) Get(key K) (V, bool) {
	rcm.lock.RLock()
	defer rcm.lock.RUnlock()

	val, ok := rcm.data[key]
	if !ok {
		var zero V
		return zero, false
	}

	return val.get(), true
}

// Put associates the value with the key.
// It overwrites the existing value if the key exists.
// If the key does not exist it creates a new key/value pair.
func (rcm *Map[K, V]) Put(key K, val V) {
	sharedVal := newSharedValue(val)

	rcm.lock.Lock()
	rcm.data[key] = sharedVal
	rcm.lock.Unlock()
}

// Delete deletes the value associated with the key from the map.
// Returns whether any value was actually removed (whether the value was present).
func (rcm *Map[K, V]) Delete(key K) bool {
	rcm.lock.Lock()
	defer rcm.lock.Unlock()

	val, exists := rcm.data[key]
	if !exists {
		return false
	}

	val.detach()
	delete(rcm.data, key)

	return true
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
			if !yield(k, v.get()) {
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
			if !yield(v.get()) {
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

	return val.refCount()
}

// Clear clears the map.
func (rcm *Map[K, V]) Clear() {
	rcm.lock.Lock()
	for _, v := range rcm.data {
		v.detach()
	}
	rcm.lock.Unlock()

	rcm.data = make(map[K]*sharedValue[V])
}

// Clone creates a clone of the map.
func (rcm *Map[K, V]) Clone() *Map[K, V] {
	rcm.lock.Lock()
	clonedData := make(map[K]*sharedValue[V], len(rcm.data))
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
		result[k] = v.get()
	}

	return result
}
