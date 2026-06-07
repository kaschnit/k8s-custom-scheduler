package lazy

import "sync"

// SharedValue is a reference-counted shared value.
// All exported methods are thread-safe.
type SharedValue[V Value[V]] struct {
	value V
	rc    *RefCounter
	lock  sync.Mutex
}

// NewSharedValue creates a new [SharedValue].
func NewSharedValue[V Value[V]](value V) *SharedValue[V] {
	return &SharedValue[V]{
		value: value,
		rc:    NewRefCounter(),
	}
}

// RefCount returns the number of references to lc's value.
func (lc *SharedValue[V]) RefCount() int64 {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	return lc.rc.Count()
}

// Get gets lc's value.
// This clones the value if it has more than 1 reference.
// If cloned, the number of references is reset to 1.
func (lc *SharedValue[V]) Get() V {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	if detached := lc.rc.DecIfShared(); detached {
		lc.value = lc.value.Clone()
		lc.rc = NewRefCounter()
	}

	return lc.value
}

// Fork makes a copy of the shared value with increased reference count.
func (lc *SharedValue[V]) Fork() *SharedValue[V] {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	lc.rc.Inc()

	return &SharedValue[V]{
		value: lc.value,
		rc:    lc.rc,
	}
}

// Detach removes the reference to the value.
func (lc *SharedValue[V]) Detach() {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	lc.rc.Dec()

	var zero V
	lc.value = zero
}
