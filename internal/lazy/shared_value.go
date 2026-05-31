package lazy

import "sync"

type sharedValue[V Value[V]] struct {
	value V
	rc    *RefCounter
	lock  sync.Mutex
}

func newSharedValue[V Value[V]](value V) *sharedValue[V] {
	return &sharedValue[V]{
		value: value,
		rc:    NewRefCounter(),
	}
}

func (lc *sharedValue[V]) refCount() int64 {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	return lc.rc.Count()
}

func (lc *sharedValue[V]) get() V {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	if detached := lc.rc.DetachIfShared(); detached {
		lc.value = lc.value.Clone()
		lc.rc = NewRefCounter()
	}

	return lc.value
}

func (lc *sharedValue[V]) fork() *sharedValue[V] {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	lc.rc.Attach()

	return &sharedValue[V]{
		value: lc.value,
		rc:    lc.rc,
	}
}

func (lc *sharedValue[V]) detach() {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	lc.rc.Detach()
}
