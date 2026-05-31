package lazy

import "sync"

type RefCounter struct {
	// count is the reference count.
	count int64
	lock  sync.Mutex
}

func NewRefCounter() *RefCounter {
	return &RefCounter{
		count: 1,
	}
}

func (rc *RefCounter) Count() int64 {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	return rc.count
}

func (rc *RefCounter) Attach() {
	rc.lock.Lock()
	rc.count++
	rc.lock.Unlock()
}

func (rc *RefCounter) Detach() {
	rc.lock.Lock()
	rc.count--
	rc.lock.Unlock()
}

func (rc *RefCounter) DetachIfShared() bool {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	if rc.count > 1 {
		rc.count--
		return true
	}

	return false
}
