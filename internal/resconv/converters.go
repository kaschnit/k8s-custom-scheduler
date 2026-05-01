package resconv

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	fwk "k8s.io/kube-scheduler/framework"
	corev1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// FwkToCoreV1Lsit converts [fwk.Resource] to [corev1.ResourceList].
func FwkToCoreV1List(r fwk.Resource) corev1.ResourceList {
	result := corev1.ResourceList{
		corev1.ResourceCPU:              *resource.NewMilliQuantity(r.GetMilliCPU(), resource.DecimalSI),
		corev1.ResourceMemory:           *resource.NewQuantity(r.GetMemory(), resource.BinarySI),
		corev1.ResourcePods:             *resource.NewQuantity(int64(r.GetAllowedPodNumber()), resource.BinarySI),
		corev1.ResourceEphemeralStorage: *resource.NewQuantity(r.GetEphemeralStorage(), resource.BinarySI),
	}
	for rName, rQuant := range r.GetScalarResources() {
		if corev1helper.IsHugePageResourceName(rName) {
			result[rName] = *resource.NewQuantity(rQuant, resource.BinarySI)
		} else {
			result[rName] = *resource.NewQuantity(rQuant, resource.DecimalSI)
		}
	}
	return result
}

// ExtractFwkFromPod converts [corev1.Pod] to the [framework.Resource] its requests represent.
func ExtractFwkFromPod(pod *corev1.Pod) *framework.Resource {
	result := &framework.Resource{}
	for _, container := range pod.Spec.Containers {
		result.Add(container.Resources.Requests)
	}

	// take max_resource(sum_pod, any_init_container)
	for _, container := range pod.Spec.InitContainers {
		result.SetMaxResource(container.Resources.Requests)
	}

	// If Overhead is being utilized, add to the total requests for the pod
	if pod.Spec.Overhead != nil {
		result.Add(pod.Spec.Overhead)
	}

	return result
}

// AddFwkInPlace adds all of the provided resources to res.
// This function mutates res.
func AddFwkInPlace(res *framework.Resource, others ...*framework.Resource) {
	for _, otherRes := range others {
		res.Memory += otherRes.Memory
		res.MilliCPU += otherRes.MilliCPU
		res.EphemeralStorage += otherRes.EphemeralStorage
		res.AllowedPodNumber += otherRes.AllowedPodNumber
		for name, value := range otherRes.ScalarResources {
			res.AddScalar(name, value)
		}
	}
}

// AddFwk returns a sum all of the provided resources.
func AddFwk(res *framework.Resource, others ...*framework.Resource) *framework.Resource {
	resCopy := res.Clone()

	AddFwkInPlace(resCopy, others...)

	return resCopy
}

// SubtractFwk returns the result of subtracting the provided resources from res.
func SubtractFwkInPlace(res *framework.Resource, others ...*framework.Resource) {
	for _, otherRes := range others {
		res.Memory -= otherRes.Memory
		res.MilliCPU -= otherRes.MilliCPU
		res.EphemeralStorage -= otherRes.EphemeralStorage
		res.AllowedPodNumber -= otherRes.AllowedPodNumber
		for name, value := range otherRes.ScalarResources {
			res.AddScalar(name, -value)
		}
	}
}

// SubtractFwk subtracts the provided resources from res.
// This function mutates res.
func SubtractFwk(res *framework.Resource, others ...*framework.Resource) *framework.Resource {
	resCopy := res.Clone()

	SubtractFwkInPlace(res, others...)

	return resCopy
}
