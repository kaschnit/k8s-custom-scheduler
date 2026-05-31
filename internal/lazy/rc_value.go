package lazy

import "sync"

// RCValue is a reference-counted value.
// All exported methods are thread-safe.
type RCValue[V Value[V]] struct {
	value V
	rc    *RefCounter
	lock  sync.Mutex
}

// NewRCValue creates a new [RCValue].
func NewRCValue[V Value[V]](value V) *RCValue[V] {
	return &RCValue[V]{
		value: value,
		rc:    NewRefCounter(),
	}
}

// RefCount returns the number of references to lc's value.
func (lc *RCValue[V]) RefCount() int64 {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	return lc.rc.Count()
}

// Get gets lc's value.
func (lc *RCValue[V]) Get() V {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	if detached := lc.rc.DecIfShared(); detached {
		lc.value = lc.value.Clone()
		lc.rc = NewRefCounter()
	}

	return lc.value
}

func (lc *RCValue[V]) fork() *RCValue[V] {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	lc.rc.Inc()

	return &RCValue[V]{
		value: lc.value,
		rc:    lc.rc,
	}
}

func (lc *RCValue[V]) detach() {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	lc.rc.Dec()
}
