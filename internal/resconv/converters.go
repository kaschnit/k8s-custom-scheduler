package resconv

import (
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	fwk "k8s.io/kube-scheduler/framework"
	corev1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func FwkToCoreV1List(r fwk.Resource) corev1.ResourceList {
	result := corev1.ResourceList{
		v1.ResourceCPU:              *resource.NewMilliQuantity(r.GetMilliCPU(), resource.DecimalSI),
		v1.ResourceMemory:           *resource.NewQuantity(r.GetMemory(), resource.BinarySI),
		v1.ResourcePods:             *resource.NewQuantity(int64(r.GetAllowedPodNumber()), resource.BinarySI),
		v1.ResourceEphemeralStorage: *resource.NewQuantity(r.GetEphemeralStorage(), resource.BinarySI),
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

func AddFwk(res *framework.Resource, others ...*framework.Resource) *framework.Resource {
	resCopy := res.Clone()
	for _, otherRes := range others {
		resCopy.Add(FwkToCoreV1List(otherRes))
	}
	return resCopy
}
