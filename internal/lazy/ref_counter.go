package lazy

import "sync"

type RefCounter struct {
	// count is the reference count.
	count int64
	sync.Mutex
}

func NewRefCounter() *RefCounter {
	return &RefCounter{
		count: 1,
	}
}

func (rc *RefCounter) Count() int64 {
	rc.Lock()
	defer rc.Unlock()

	return rc.count
}
