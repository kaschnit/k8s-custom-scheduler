package quotaawarepreempt

import (
	"errors"
	"fmt"
	"math"

	"github.com/kaschnit/custom-scheduler/internal/resconv"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	UpperBoundOfMax = math.MaxInt64
)

var (
	ErrAddPodToQuota      = errors.New("failed to add pod to quota")
	ErrRemovePodFromQuota = errors.New("failed to remove pod from quota")
)

type QuotaUsages map[string]*QuotaUsage

func (qu QuotaUsages) clone() QuotaUsages {
	newQuotaUsages := make(QuotaUsages, len(qu))
	for key, quotaUsage := range qu {
		newQuotaUsages[key] = quotaUsage.clone()
	}
	return newQuotaUsages
}

func (qu QuotaUsages) getQuota(pod *corev1.Pod) (string, *QuotaUsage) {
	if pod == nil {
		return "", nil
	}

	queue, ok := pod.Annotations[AnnotationKeyQueue]
	if !ok {
		// Ignore pod if it has no queue, it will not be tracked.
		return queue, nil
	}

	return queue, qu[queue]
}

func (qu QuotaUsages) addPodIfNotPresent(pod *corev1.Pod) error {
	if pod == nil {
		return nil
	}

	queue, ok := pod.Annotations[AnnotationKeyQueue]
	if !ok {
		// Ignore pod if it has no queue, it will not be tracked.
		return nil
	}

	quotaUsage, ok := qu[queue]
	if !ok {
		return fmt.Errorf("%w: queue '%s' does not exist", ErrAddPodToQuota, queue)
	}

	return quotaUsage.addPodIfNotPresent(pod)
}

func (qu QuotaUsages) deletePodIfPresent(pod *corev1.Pod) error {
	if pod == nil {
		return nil
	}

	queue, ok := pod.Annotations[AnnotationKeyQueue]
	if !ok {
		// Ignore pod if it has no queue, it will not be tracked.
		return nil
	}

	quotaUsage, ok := qu[queue]
	if !ok {
		return fmt.Errorf("%w: queue '%s' does not exist", ErrRemovePodFromQuota, queue)
	}

	return quotaUsage.deletePodIfPresent(pod)
}

type QuotaUsage struct {
	Max  *framework.Resource
	Used *framework.Resource
	pods sets.Set[string]
}

func (qu *QuotaUsage) clone() *QuotaUsage {
	newQuotaUsage := &QuotaUsage{
		pods: sets.New[string](),
	}

	if qu.Max != nil {
		newQuotaUsage.Max = qu.Max.Clone()
	}
	if qu.Used != nil {
		newQuotaUsage.Used = qu.Used.Clone()
	}

	for pod := range qu.pods {
		newQuotaUsage.pods.Insert(pod)
	}

	return newQuotaUsage
}

func newQuotaUsage(max, used corev1.ResourceList) *QuotaUsage {
	if max == nil {
		max = corev1.ResourceList{
			corev1.ResourceCPU:              *resource.NewMilliQuantity(UpperBoundOfMax, resource.DecimalSI),
			corev1.ResourceMemory:           *resource.NewQuantity(UpperBoundOfMax, resource.BinarySI),
			corev1.ResourceEphemeralStorage: *resource.NewQuantity(UpperBoundOfMax, resource.BinarySI),
			corev1.ResourcePods:             *resource.NewQuantity(UpperBoundOfMax, resource.DecimalSI),
		}
	}

	return &QuotaUsage{
		pods: sets.New[string](),
		Max:  framework.NewResource(max),
		Used: framework.NewResource(used),
	}
}

func (qu *QuotaUsage) addPodIfNotPresent(pod *corev1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	if qu.pods.Has(key) {
		return nil
	}

	qu.pods.Insert(key)
	qu.reserveResource(*resconv.ExtractFwkFromPod(pod))

	return nil
}

func (qu *QuotaUsage) deletePodIfPresent(pod *corev1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}

	if !qu.pods.Has(key) {
		return nil
	}

	qu.pods.Delete(key)
	qu.unreserveResource(*resconv.ExtractFwkFromPod(pod))

	return nil
}

func (qu *QuotaUsage) reserveResource(request framework.Resource) {
	qu.Used.Memory += request.Memory
	qu.Used.MilliCPU += request.MilliCPU
	qu.Used.EphemeralStorage += request.EphemeralStorage
	qu.Used.AllowedPodNumber += request.AllowedPodNumber
	for name, value := range request.ScalarResources {
		qu.Used.SetScalar(name, qu.Used.ScalarResources[name]+value)
	}
}

func (qu *QuotaUsage) unreserveResource(request framework.Resource) {
	qu.Used.Memory -= request.Memory
	qu.Used.MilliCPU -= request.MilliCPU
	qu.Used.EphemeralStorage -= request.EphemeralStorage
	qu.Used.AllowedPodNumber -= request.AllowedPodNumber
	for name, value := range request.ScalarResources {
		qu.Used.SetScalar(name, qu.Used.ScalarResources[name]+value)
	}
}

func (qu *QuotaUsage) wouldPutOverMax(request *framework.Resource) bool {
	qu.reserveResource(*request)
	defer qu.unreserveResource(*request)

	return anyGreaterThanOnlyExisting(*qu.Used, *qu.Max)
}

func anyGreaterThanOnlyExisting(a framework.Resource, b framework.Resource) bool {
	if a.Memory > b.Memory {
		return true
	}

	if a.MilliCPU > b.MilliCPU {
		return true
	}

	if a.EphemeralStorage > b.EphemeralStorage {
		return true
	}

	if a.AllowedPodNumber > b.AllowedPodNumber {
		return true
	}

	// Iterate over b so we only compare a to the existing scalar resources of b.
	for name, bValue := range b.ScalarResources {
		if aValue, ok := a.ScalarResources[name]; ok {
			return aValue > bValue
		}
	}

	return false
}
