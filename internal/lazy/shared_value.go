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

	lc.rc.Lock()
	if lc.rc.count > 1 {
		lc.rc.count--
		lc.rc.Unlock()

		lc.value = lc.value.Clone()
		lc.rc = NewRefCounter()
	} else {
		lc.rc.Unlock()
	}

	return lc.value
}

func (lc *sharedValue[V]) fork() *sharedValue[V] {
	lc.lock.Lock()
	defer lc.lock.Unlock()

	lc.rc.Lock()
	lc.rc.count++
	lc.rc.Unlock()

	return &sharedValue[V]{
		value: lc.value,
		rc:    lc.rc,
	}
}

func (lc *sharedValue[V]) detach() {
	lc.lock.Lock()
	lc.rc.Lock()
	lc.rc.count--
	lc.rc.Unlock()
	lc.lock.Unlock()
}
