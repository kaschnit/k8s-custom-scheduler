package queue

import (
	"errors"
	"fmt"
	"iter"
	"maps"
	"sync"

	"github.com/kaschnit/kaschnit-scheduler/apis/scheduling"

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
	queueByName map[string]*Queue
	lock        sync.RWMutex
}

// NewManager creates a new [Manager].
func NewManager() *Manager {
	return &Manager{
		queueByName: make(map[string]*Queue),
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

// GetByName gets the queue by name.
func (qm *Manager) GetByName(name string) *Queue {
	qm.lock.RLock()
	defer qm.lock.RUnlock()

	return qm.queueByName[name]
}

// Put creates or updates the quota for the queue.
func (qm *Manager) Put(name string, opts ...QueueOption) {
	qm.lock.Lock()
	defer qm.lock.Unlock()

	qm.queueByName[name] = New(name, opts...)
}

// Update mutates the queue with the given name.
// It will create the queue if it does not exist.
func (qm *Manager) Update(name string, opts ...QueueOption) {
	if len(opts) == 0 {
		return
	}

	qm.lock.Lock()
	defer qm.lock.Unlock()

	q := qm.queueByName[name]
	if q == nil {
		qm.queueByName[name] = New(name, opts...)
		return
	}

	q.ApplyOpts(opts...)
}

// Delete deletes the queue from the manager.
func (qm *Manager) Delete(name string) {
	qm.lock.Lock()
	defer qm.lock.Unlock()

	delete(qm.queueByName, name)
}

// QueueIter returns an sequence to iterate over each queue.
func (qm *Manager) QueueIter() iter.Seq[*Queue] {
	return func(yield func(*Queue) bool) {
		qmClone := qm.Clone()
		for _, q := range qmClone.queueByName {
			if !yield(q) {
				return
			}
		}
	}
}

// AddPodIfNotPresent adds the pod to the quota if the pod has a quota.
func (qm *Manager) AddPodIfNotPresent(pod *corev1.Pod) error {
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
	qm.lock.RLock()
	queuesClone := maps.Clone(qm.queueByName)
	qm.lock.RUnlock()

	for name, queue := range queuesClone {
		queuesClone[name] = queue.Clone()
	}

	return &Manager{
		queueByName: queuesClone,
	}
}
