package queue

import (
	"errors"
	"fmt"
	"iter"
	"sync"

	"github.com/kaschnit/kaschnit-scheduler/apis/scheduling"
	"github.com/kaschnit/kaschnit-scheduler/internal/lazy"

	corev1 "k8s.io/api/core/v1"
)

var (
	// ErrAddPodToQuota indicates an error adding pod to quota.
	ErrAddPodToQuota = errors.New("failed to add pod to quota")
	// ErrRemovePodFromQuota indicates an error removing pod from quota.
	ErrRemovePodFromQuota = errors.New("failed to remove pod from quota")
)

// Manager manages queues.
type Manager struct {
	sync.Mutex

	// queueByName are the queues indexed by name.
	// We use a lazy map for fast snapshotting of the quota.
	queueByName *lazy.Map[string, *Queue]
}

// NewManager creates a new [Manager].
func NewManager() *Manager {
	return &Manager{
		queueByName: lazy.NewMap[string, *Queue](),
	}
}

// Get gets the quota related to the pod, based on the pod's queue.
// If the pod is nil or has no queue, returns nil.
func (qm *Manager) Get(pod *corev1.Pod) *Queue {
	if pod == nil {
		return nil
	}

	name, ok := pod.Labels[scheduling.LabelKeyQueue]
	if !ok {
		// Ignore pod if it has no queue, it will not be tracked.
		return nil
	}

	return qm.GetByName(name)
}

func (qm *Manager) GetByName(name string) *Queue {
	q, _ := qm.queueByName.Get(name)
	return q
}

// Put creates or updates the quota for the queue.
func (qm *Manager) Put(name string, opts ...QueueOption) {
	qm.queueByName.Put(name, New(name, opts...))
}

// Update mutates the queue with the given name.
// It will create the queue if it does not exist.
func (qm *Manager) Update(name string, opts ...QueueOption) {
	if len(opts) == 0 {
		return
	}

	qm.Lock()
	defer qm.Unlock()

	q := qm.GetByName(name)
	if q == nil {
		qm.Put(name, opts...)
		return
	}

	q.ApplyOpts(opts...)
}

// Delete deletes the queue from the manager.
func (qm *Manager) Delete(name string) {
	qm.queueByName.Delete(name)
}

// QueueIter returns an sequence to iterate over each queue.
func (qm *Manager) QueueIter() iter.Seq[*Queue] {
	return qm.queueByName.Values()
}

// AddPodIfNotPresent adds the pod to the quota if the pod has a quota.
func (qm *Manager) AddPodIfNotPresent(pod *corev1.Pod) error {
	qm.Lock()
	defer qm.Unlock()

	return qm.addPodIfNotPresentNoLock(pod)
}

func (qm *Manager) addPodIfNotPresentNoLock(pod *corev1.Pod) error {
	if pod == nil {
		return nil
	}

	queueName, ok := pod.Labels[scheduling.LabelKeyQueue]
	if !ok {
		// Ignore pod if it has no queue, it will not be tracked.
		return nil
	}

	q := qm.GetByName(queueName)
	if q == nil {
		return fmt.Errorf("%w: queue '%s' does not exist", ErrAddPodToQuota, queueName)
	}

	q.Quota().AddPodIfNotPresent(pod)

	return nil
}

// DeletePodIfPresent removes the pod to the quota if the pod has a quota.
func (qm *Manager) DeletePodIfPresent(pod *corev1.Pod) error {
	qm.Lock()
	defer qm.Unlock()

	return qm.deletePodIfPresentNoLock(pod)
}

func (qm *Manager) deletePodIfPresentNoLock(pod *corev1.Pod) error {
	if pod == nil {
		return nil
	}

	queueName, ok := pod.Labels[scheduling.LabelKeyQueue]
	if !ok {
		// Ignore pod if it has no queue, it will not be tracked.
		return nil
	}

	q := qm.GetByName(queueName)
	if q == nil {
		return fmt.Errorf("%w: queue '%s' does not exist", ErrRemovePodFromQuota, queueName)
	}

	q.Quota().DeletePodIfPresent(pod)

	return nil
}

// Clone creates a clone of the [Manager].
func (qm *Manager) Clone() *Manager {
	return &Manager{
		queueByName: qm.queueByName.Clone(),
	}
}
