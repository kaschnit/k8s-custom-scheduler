package queue

import (
	"fmt"
	"maps"
	"strings"

	"github.com/kaschnit/kaschnit-scheduler/internal/alloc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Quota tracks the max and available quota.
type Quota struct {
	// Max is the available resources.
	Max alloc.Resources
	// Used is the used resources.
	Used alloc.Resources
	// PodsByID are pods that currently contribute to quota.
	PodsByID map[types.UID]*corev1.Pod
}

// NewQuota creates a new [Quota].
func NewQuota(max alloc.Resources) *Quota {
	return &Quota{
		Max:      max,
		Used:     make(alloc.Resources),
		PodsByID: make(map[types.UID]*corev1.Pod),
	}
}

// AddPodIfNotPresent adds the pod to the quota if it's not part of the quota.
func (q *Quota) AddPodIfNotPresent(pod *corev1.Pod) {
	if pod == nil {
		return
	}

	_, wasTrackingPod := q.PodsByID[pod.UID]

	q.PodsByID[pod.UID] = pod

	if !wasTrackingPod {
		q.Used.Add(alloc.FromPodReq(pod))
	}
}

// DeletePodIfPresent removes the pod from the quota if it's part of the quota.
func (q *Quota) DeletePodIfPresent(pod *corev1.Pod) {
	if pod == nil {
		return
	}

	if _, wasTrackingPod := q.PodsByID[pod.UID]; wasTrackingPod {
		delete(q.PodsByID, pod.UID)
		q.Used.Sub(alloc.FromPodReq(pod))
	}
}

// DeletePodsFunc deletes the pods matching the predicate from the quota.
// The entire set of the quota's pods is iterated and checked against the predicate.
func (q *Quota) DeletePodsFunc(predicate func(*corev1.Pod) bool) {
	for _, otherPod := range q.PodsByID {
		if predicate(otherPod) {
			q.DeletePodIfPresent(otherPod)
		}
	}
}

// ContainsPod returns true if the pod is counted towards the quota.
func (q *Quota) ContainsPod(pod *corev1.Pod) bool {
	_, ok := q.PodsByID[pod.UID]
	return ok
}

// WouldPutOverMax returns true if request would put the quota over its max
// when added to the used amount.
func (q *Quota) WouldPutOverMax(request alloc.Resources) bool {
	return q.Used.Plus(request).AnyGreaterIntersecting(q.Max)
}

// Clone clones the [Quota].
func (q *Quota) Clone() *Quota {
	newQuotaUsage := &Quota{
		PodsByID: maps.Clone(q.PodsByID),
	}

	if q.Max != nil {
		newQuotaUsage.Max = q.Max.Clone()
	}
	if q.Used != nil {
		newQuotaUsage.Used = q.Used.Clone()
	}

	return newQuotaUsage
}

// String converts q to a string representation.
func (q *Quota) String() string {
	const maxPodSamples = 3
	podSamples := make([]string, 0, maxPodSamples)
	for _, pod := range q.PodsByID {
		if len(podSamples) >= maxPodSamples {
			break
		}
		if pod != nil {
			// "namespace/name" is much more useful than a raw types.UID string
			podSamples = append(podSamples, fmt.Sprintf("%s/%s", pod.Namespace, pod.Name))
		}
	}

	var podsSummary string
	if len(q.PodsByID) == 0 {
		podsSummary = "[]"
	} else if len(q.PodsByID) <= maxPodSamples {
		podsSummary = fmt.Sprintf("[%s]", strings.Join(podSamples, ", "))
	} else {
		podsSummary = fmt.Sprintf("[%s, ... (+%d more)]", strings.Join(podSamples, ", "), len(q.PodsByID)-maxPodSamples)
	}

	return fmt.Sprintf("{Max: %s, Used: %s, Pods: %s}", q.Max, q.Used, podsSummary)
}
