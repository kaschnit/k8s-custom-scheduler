package queue

import (
	"sync"

	"github.com/kaschnit/kaschnit-scheduler/apis/scheduling"
	"github.com/kaschnit/kaschnit-scheduler/internal/alloc"
	corev1 "k8s.io/api/core/v1"
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
	// preemptionConfig is theis queue's preemption configuration.
	preemptionCfg *PreemptionConfig

	lock sync.RWMutex
}

// New creates a new queue with the provided options.
func New(name string, opts ...QueueOption) *Queue {
	q := &Queue{
		name:          name,
		quota:         NewQuota(nil),
		labels:        labels.Set{},
		preemptionCfg: &PreemptionConfig{},
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

func (q *Queue) CanPodPreemptOthers(pod *corev1.Pod) bool {
	if q == nil {
		return false
	}

	if pod == nil {
		return false
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	// Does pod belong to q?
	if pod.Labels[scheduling.LabelKeyQueue] != q.name {
		return false
	}

	// Can q prempt at all?
	if !q.preemptionCfg.preempts.canPreempt() {
		return false
	}

	// Does q allow pod to preempt others?
	if !q.preemptionCfg.preempts.fromPods.Matches(labels.Set(pod.Labels)) {
		return false
	}

	return true
}

func (q *Queue) CanPodBePreemptedByOthers(pod *corev1.Pod) bool {
	if q == nil {
		return false
	}

	if pod == nil {
		return false
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	// Does pod belong to q?
	if pod.Labels[scheduling.LabelKeyQueue] != q.name {
		return false
	}

	// Can q be preempted at all?
	if !q.preemptionCfg.preemptedBy.canBePreempted() {
		return false
	}

	// Does q allow pod to be preempted by others?
	if !q.preemptionCfg.preemptedBy.toPods.Matches(labels.Set(pod.Labels)) {
		return false
	}

	return true
}

func (q *Queue) CanPreemptTo(fromPod *corev1.Pod, toQ *Queue, toPod *corev1.Pod) bool {
	if q == nil || toQ == nil {
		return false
	}

	if fromPod == nil || toPod == nil {
		return false
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	// Does fromPod belong to q?
	if fromPod.Labels[scheduling.LabelKeyQueue] != q.name {
		return false
	}

	// Does toPod belong to toQ?
	if toPod.Labels[scheduling.LabelKeyQueue] != toQ.name {
		return false
	}

	// Can q preempt at all?
	if !q.preemptionCfg.preempts.canPreempt() {
		return false
	}

	// Does q allow fromPod to preempt?
	if !q.preemptionCfg.preempts.fromPods.Matches(labels.Set(fromPod.Labels)) {
		return false
	}

	// Can toQ be preempted by q?
	if !q.preemptionCfg.preempts.toQueues.Matches(toQ.Labels()) {
		return false
	}

	// Can toPod be preempted by q?
	if !q.preemptionCfg.preempts.toPods.Matches(labels.Set(toPod.Labels)) {
		return false
	}

	return true
}

func (q *Queue) CanBePreemptedBy(fromQ *Queue, fromPod, toPod *corev1.Pod) bool {
	if q == nil || fromQ == nil {
		return false
	}

	if fromPod == nil || toPod == nil {
		return false
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	// Does fromPod belong to fromQ?
	if fromPod.Labels[scheduling.LabelKeyQueue] != fromQ.name {
		return false
	}

	// Does toPod belong to q?
	if toPod.Labels[scheduling.LabelKeyQueue] != q.name {
		return false
	}

	// Can q be preempted at all?
	if !q.preemptionCfg.preemptedBy.canBePreempted() {
		return false
	}

	// Can q be preempted by fromQ?
	if !q.preemptionCfg.preemptedBy.fromQueues.Matches(fromQ.Labels()) {
		return false
	}

	// Can q be preempted by fromPod?
	if !q.preemptionCfg.preemptedBy.fromPods.Matches(labels.Set(fromPod.Labels)) {
		return false
	}

	// Does q allow toPod be preempted?
	if !q.preemptionCfg.preemptedBy.toPods.Matches(labels.Set(toPod.Labels)) {
		return false
	}

	return true
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

// ApplyOpts applies the queue options, mutation the queue.
func (q *Queue) ApplyOpts(opts ...QueueOption) {
	if q == nil {
		return
	}

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
		name:          q.name, // copy by value
		quota:         q.quota.Clone(),
		labels:        q.labels,        // read only
		preemptionCfg: q.preemptionCfg, // read only
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

func WithPreemptionConfig(config *PreemptionConfig) QueueOption {
	return func(q *Queue) {
		if q == nil {
			return
		}

		if config == nil {
			config = &PreemptionConfig{}
		}

		q.preemptionCfg = config
	}
}
