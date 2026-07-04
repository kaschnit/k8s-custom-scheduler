package kubeassert

import (
	"context"
	"slices"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

// PodNominatedToPreempt asserts that preemptor appears to be nominated to preempt victim.
// This does not necessarily mean that victim is the actual victim or the only victim, but
// rather that victim is bound to the node that preemptor is nominated to preempt on.
func PodNominatedToPreempt(
	t assert.TestingT,
	ctx context.Context,
	preemptor *corev1.Pod,
	victim *corev1.Pod,
) {
	assert.NotEmpty(t, preemptor.Status.NominatedNodeName, "Preemptor should have nominatedNode")
	assert.Equalf(t, preemptor.Spec.NodeName, victim.Status.NominatedNodeName,
		"Preemptor's nominated node should be the node of victim %s/%s",
		victim.Namespace,
		victim.Name)
}

// PodRunningOnNode asserts that the pod is running and bound to the node with the provided name.
func PodRunningOnNode(t assert.TestingT, ctx context.Context, pod *corev1.Pod, nodeName string) {
	PodRunning(t, ctx, pod)
	assert.Equal(t, nodeName, pod.Spec.NodeName, "Pod does not have expected spec.NodeName")
}

// PodRunning asserts that the pod is running on some node.
func PodRunning(t assert.TestingT, ctx context.Context, pod *corev1.Pod) {
	assert.Equal(t, corev1.PodRunning, pod.Status.Phase, "Pod should be running")
	assert.NotEmpty(t, pod.Spec.NodeName, "Running pod should have spec.nodeName")
}

// PodUnschedulable asserts that the is unable to be scheduled.
func PodUnschedulable(t assert.TestingT, ctx context.Context, pod *corev1.Pod) {
	// Unschedulable pods must be pending, though pending phase does not necessarily mean
	// the pod is unschedulable.
	assert.Equal(t, corev1.PodPending, pod.Status.Phase, "Pod should be pending")
	// Unschedulable pods must not be bound to a node, though lack of node does not necessarily
	// mean the pod is unschedulable.
	assert.Empty(t, pod.Spec.NodeName, "Pod should not have spec.nodeName")
	// Pod will have an PodScheduled=False condition with Reason=Unschedulable if it meets the
	// above conditions due to actually being unschedulable.
	assert.Condition(t, func() bool {
		return slices.ContainsFunc(pod.Status.Conditions, func(cond corev1.PodCondition) bool {
			return cond.Type == corev1.PodScheduled &&
				cond.Status == corev1.ConditionFalse &&
				cond.Reason == corev1.PodReasonUnschedulable
		})
	}, "Pod should have unschedulable condition")
}
