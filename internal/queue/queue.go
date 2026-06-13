package queue

import (
	"sync"

	"github.com/kaschnit/kaschnit-scheduler/internal/alloc"
	"github.com/kaschnit/kaschnit-scheduler/internal/match"
	"k8s.io/apimachinery/pkg/labels"
)

// Queue reperesents a queue for pods.
type Queue struct {
	// name is the name of the queue.
	name string
	// Quota is the queue's quota.
	quota *Quota
	// labels are this queue's labels.
	labels labels.Labels
	// victimSelector is the selector for victim queues.
	victimSelector match.LabelMatcher

	lock sync.RWMutex
}

// New creates a new queue with the provided options.
func New(name string, opts ...QueueOption) *Queue {
	q := &Queue{
		name:           name,
		quota:          NewQuota(nil),
		labels:         labels.Set{},
		victimSelector: labels.Nothing(),
	}

	q.ApplyOpts(opts...)

	return q
}

// Name returns the queue's name.
func (q *Queue) Name() string {
	if q == nil {
		return ""
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.name
}

// Quota returns the queue's quota.
func (q *Queue) Quota() *Quota {
	if q == nil {
		return nil
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.quota
}

func (q *Queue) IsVictimOf(other *Queue) bool {
	if q == nil {
		return false
	}

	return other.VictimSelector().Matches(q.Labels())
}

// Labels returns the queue's labels.
func (q *Queue) Labels() labels.Labels {
	if q == nil {
		return nil
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	if q.labels == nil {
		return make(labels.Set)
	}

	return q.labels
}

// VictimSelector returns the queue's victim queue selector.
func (q *Queue) VictimSelector() match.LabelMatcher {
	if q == nil {
		return labels.Nothing()
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	if q.victimSelector == nil {
		return labels.Nothing()
	}

	return q.victimSelector
}

// ApplyOpts applies the queue options, mutation the queue.
func (q *Queue) ApplyOpts(opts ...QueueOption) {
	q.lock.Lock()
	defer q.lock.Unlock()

	for _, opt := range opts {
		opt(q)
	}
}

// Clone clones the [Queue].
func (q *Queue) Clone() *Queue {
	if q == nil {
		return nil
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	return &Queue{
		name:           q.name,
		quota:          q.quota.Clone(),
		labels:         q.labels,
		victimSelector: q.victimSelector,
	}
}

// QueueOption is an option that can be applied to configure [Queue].
type QueueOption func(*Queue)

// WithQuotaMax configures the max quota of the queue.
func WithQuotaMax(max alloc.Resources) QueueOption {
	return func(q *Queue) {
		if q == nil {
			return
		}

		q.quota.SetMax(max)
	}
}

// WithLabels sets the labels of the queue.
func WithLabels(lbls labels.Labels) QueueOption {
	return func(q *Queue) {
		if q == nil {
			return
		}

		if lbls == nil {
			lbls = make(labels.Set)
		}

		q.labels = lbls
	}
}

// WithVictimSelector sets the victim selector of the queue.
func WithVictimSelector(victimSelector match.LabelMatcher) QueueOption {
	return func(q *Queue) {
		if q == nil {
			return
		}

		if victimSelector == nil {
			victimSelector = labels.Nothing()
		}

		q.victimSelector = victimSelector
	}
}
