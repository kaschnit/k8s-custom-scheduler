package kubeassert

import (
	"context"
	"slices"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

// func EventuallyPod(
// 	t *testing.T,
// 	getPod func() (*corev1.Pod, error),
// 	condition func(c *assert.CollectT, pod *corev1.Pod),
// 	opts ...EventuallyConfigOpt,
// ) {
// 	t.Helper()

// 	cfg := newEventuallyConfig(opts...)

// 	assert.EventuallyWithT(t, func(c *assert.CollectT) {
// 		pod, err := getPod()
// 		assert.NoError(c, err, "Failed to get pod")

// 		condition(c, pod)
// 	}, cfg.Timeout, cfg.PollInterval)
// }

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

func PodRunningOnNode(t assert.TestingT, ctx context.Context, pod *corev1.Pod, nodeName string) {
	PodRunning(t, ctx, pod)
	assert.Equal(t, nodeName, pod.Spec.NodeName, "Pod does not have expected spec.NodeName")
}

func PodRunning(t assert.TestingT, ctx context.Context, pod *corev1.Pod) {
	assert.Equal(t, corev1.PodRunning, pod.Status.Phase, "Pod should be running")
	assert.NotEmpty(t, pod.Spec.NodeName, "Running pod should have spec.nodeName")
}

func PodUnschedulable(t assert.TestingT, ctx context.Context, pod *corev1.Pod) {
	assert.Equal(t, corev1.PodPending, pod.Status.Phase, "Pod should be pending")
	assert.Empty(t, pod.Spec.NodeName, "Pod should not have spec.nodeName")
	assert.Condition(t, func() bool {
		return slices.ContainsFunc(pod.Status.Conditions, func(cond corev1.PodCondition) bool {
			return cond.Type == corev1.PodScheduled &&
				cond.Status == corev1.ConditionFalse &&
				cond.Reason == corev1.PodReasonUnschedulable
		})
	}, "Pod did not have unschedulable condition")
}
