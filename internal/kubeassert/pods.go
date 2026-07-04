package kubeassert

import (
	"slices"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

// PodNominatedForNode asserts that pod appears to be nominated to schedule on the node.
func PodNominatedForNode(t TestingT, pod *corev1.Pod, node string) {
	t.Helper()

	assert.NotEmpty(t, pod.Status.NominatedNodeName, "Pod should have nominatedNode")
	assert.Equalf(t, node, pod.Status.NominatedNodeName,
		"Pod's nominated node should be %s", node)
}

// PodNominatedForOneOfNodes asserts that preemptor appears to be nominated to schedule on one
// of the provided nodes.
func PodNominatedForOneOfNodes(t TestingT, pod *corev1.Pod, nodes []string) {
	t.Helper()

	assert.NotEmpty(t, pod.Status.NominatedNodeName, "Pod should have nominatedNode")
	assert.Containsf(t, nodes, pod.Status.NominatedNodeName,
		"Pod's nominated node should be one of: %s", nodes)
}

// PodRunningOnNode asserts that the pod is running and bound to the node with the provided name.
func PodRunningOnNode(t TestingT, pod *corev1.Pod, nodeName string) {
	t.Helper()

	PodRunning(t, pod)
	assert.Equal(t, nodeName, pod.Spec.NodeName, "Pod does not have expected spec.NodeName")
}

// PodRunning asserts that the pod is running on some node.
func PodRunning(t TestingT, pod *corev1.Pod) {
	t.Helper()

	assert.Equal(t, corev1.PodRunning, pod.Status.Phase, "Pod should be running")
	assert.NotEmpty(t, pod.Spec.NodeName, "Running pod should have spec.nodeName")
}

// PodUnschedulable asserts that the is unable to be scheduled.
func PodUnschedulable(t TestingT, pod *corev1.Pod) {
	t.Helper()

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
