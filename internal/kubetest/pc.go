package kubetest

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulingv1client "k8s.io/client-go/kubernetes/typed/scheduling/v1"
)

func NewPC(name string, value int32, preempt bool) *schedulingv1.PriorityClass {
	var preemptionPolicy corev1.PreemptionPolicy
	if preempt {
		preemptionPolicy = corev1.PreemptLowerPriority
	} else {
		preemptionPolicy = corev1.PreemptNever
	}

	return &schedulingv1.PriorityClass{
		ObjectMeta:       metav1.ObjectMeta{Name: name},
		Value:            value,
		PreemptionPolicy: &preemptionPolicy,
	}
}

type PriorityClassManager struct {
	pcClient schedulingv1client.PriorityClassInterface
}

func NewPCManager(pcClient schedulingv1client.PriorityClassInterface) *PriorityClassManager {
	return &PriorityClassManager{
		pcClient: pcClient,
	}
}

func (mgr *PriorityClassManager) Create(
	ctx context.Context,
	pcs ...*schedulingv1.PriorityClass,
) ([]*schedulingv1.PriorityClass, error) {
	createdPCs := make([]*schedulingv1.PriorityClass, 0, len(pcs))
	for _, pc := range pcs {
		createdPC, err := mgr.pcClient.Create(ctx, pc, metav1.CreateOptions{})
		if err != nil {
			return createdPCs, err
		}

		createdPCs = append(createdPCs, createdPC)
	}

	return createdPCs, nil
}

func (mgr *PriorityClassManager) DeleteAll(ctx context.Context) error {
	return mgr.pcClient.DeleteCollection(ctx,
		metav1.DeleteOptions{
			GracePeriodSeconds: new(int64(0)),
			PropagationPolicy:  new(metav1.DeletePropagationBackground),
		},
		metav1.ListOptions{})
}
