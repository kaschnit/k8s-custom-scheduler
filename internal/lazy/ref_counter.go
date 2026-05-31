package lazy

import "sync"

// RefCounter counts references.
// All exported methods are thread-safe.
type RefCounter struct {
	// count is the reference count.
	count int64
	lock  sync.Mutex
}

// NewRefCounter creates a new [RefCounter].
func NewRefCounter() *RefCounter {
	return &RefCounter{
		count: 1,
	}
}

// Count returns the
func (rc *RefCounter) Count() int64 {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	return rc.count
}

// Inc increments the reference count.
func (rc *RefCounter) Inc() {
	rc.lock.Lock()
	rc.count++
	rc.lock.Unlock()
}

// Dec decrements the reference count.
func (rc *RefCounter) Dec() {
	rc.lock.Lock()
	rc.count--
	rc.lock.Unlock()
}

// DecIfShared decrements the reference count if there is more than 1 reference.
func (rc *RefCounter) DecIfShared() bool {
	rc.lock.Lock()
	defer rc.lock.Unlock()

	if rc.count > 1 {
		rc.count--
		return true
	}

	return false
}
