package queue

import (
	"fmt"
	"strings"
	"sync"

	"github.com/kaschnit/immut"
	"github.com/kaschnit/kaschnit-scheduler/internal/alloc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Quota tracks the max and available quota.
type Quota struct {
	// Max is the available resources.
	max alloc.Resources
	// Used is the used resources.
	used alloc.Resources
	// podsByID are pods that currently contribute to quota.
	// Immutable map is used to make clones cheap and lock-free.
	podsByID immut.Map[types.UID, *corev1.Pod]

	lock sync.RWMutex
}

// NewQuota creates a new [Quota].
func NewQuota(max alloc.Resources) *Quota {
	return &Quota{
		max:      max,
		used:     make(alloc.Resources),
		podsByID: immut.MakeMap[types.UID, *corev1.Pod](),
	}
}

// AddPodIfNotPresent adds the pod to the quota if it's not part of the quota.
func (q *Quota) AddPodIfNotPresent(pods ...*corev1.Pod) {
	if q == nil || len(pods) == 0 {
		return
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	for _, pod := range pods {
		if pod == nil {
			continue
		}

		if _, exists := q.podsByID.Get(pod.UID); !exists {
			q.podsByID = q.podsByID.Set(pod.UID, pod)
			q.used.Add(alloc.FromPodReq(pod))
		}
	}
}

// DeletePodIfPresent removes the pod from the quota if it's part of the quota.
func (q *Quota) DeletePodIfPresent(pods ...*corev1.Pod) {
	if q == nil || len(pods) == 0 {
		return
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	for _, pod := range pods {
		if pod == nil {
			continue
		}

		if _, exists := q.podsByID.Get(pod.UID); exists {
			q.podsByID = q.podsByID.Delete(pod.UID)
			q.used.Sub(alloc.FromPodReq(pod))
		}
	}
}

// DeletePodsFunc deletes the pods matching the predicate from the quota.
// The entire set of the quota's pods is iterated and checked against the predicate.
func (q *Quota) DeletePodsFunc(predicate func(*corev1.Pod) bool) {
	if q == nil || predicate == nil {
		return
	}

	var victims []*corev1.Pod

	q.lock.RLock()
	podsByID := q.podsByID
	q.lock.RUnlock()

	for otherPod := range podsByID.Values() {
		if predicate(otherPod) {
			victims = append(victims, otherPod)
		}
	}

	q.DeletePodIfPresent(victims...)
}

// Max gets the max quota.
func (q *Quota) Max() alloc.Resources {
	if q == nil {
		return nil
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.max
}

// SetMax sets the max quota to the provided max.
func (q *Quota) SetMax(max alloc.Resources) {
	if q == nil {
		return
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	q.max = max
}

// Used gets the used quota.
func (q *Quota) Used() alloc.Resources {
	if q == nil {
		return nil
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.used
}

// ContainsPod returns true if the pod is counted towards the quota.
func (q *Quota) ContainsPod(pod *corev1.Pod) bool {
	if q == nil {
		return false
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	_, ok := q.podsByID.Get(pod.UID)
	return ok
}

// WouldPutOverMax returns true if request would put the quota over its max
// when added to the used amount.
func (q *Quota) WouldPutOverMax(request alloc.Resources) bool {
	if q == nil {
		// No max on q if q is nil.
		return false
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.used.Plus(request).AnyGreaterIntersecting(q.max)
}

// Clone clones the [Quota].
func (q *Quota) Clone() *Quota {
	if q == nil {
		return nil
	}

	q.lock.RLock()
	defer q.lock.RUnlock()

	return &Quota{
		podsByID: q.podsByID, // immutable data structure
		max:      q.max.Clone(),
		used:     q.used.Clone(),
	}
}

// String converts q to a string representation.
func (q *Quota) String() string {
	return q.Clone().stringNoLock()
}

func (q *Quota) stringNoLock() string {
	if q == nil {
		return "<nil>"
	}

	const maxPodSamples = 3
	podSamples := make([]string, 0, maxPodSamples)
	for pod := range q.podsByID.Values() {
		if len(podSamples) >= maxPodSamples {
			break
		}
		if pod != nil {
			podSamples = append(podSamples, fmt.Sprintf("%s/%s", pod.Namespace, pod.Name))
		}
	}

	var podsSummary string
	if q.podsByID.Len() == 0 {
		podsSummary = "[]"
	} else if q.podsByID.Len() <= maxPodSamples {
		podsSummary = fmt.Sprintf("[%s]", strings.Join(podSamples, ", "))
	} else {
		podsSummary = fmt.Sprintf("[%s, ... (+%d more)]", strings.Join(podSamples, ", "), q.podsByID.Len()-maxPodSamples)
	}

	return fmt.Sprintf("{Max: %s, Used: %s, Pods: %s}", q.max, q.used, podsSummary)
}
