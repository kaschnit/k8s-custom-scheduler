package pods

import corev1 "k8s.io/api/core/v1"

// PolicyAllowsPreemption returns whether the pod's PreemptionPolicy allows preemption.
func PolicyAllowsPreemption(pod *corev1.Pod) bool {
	if pod == nil {
		// Preemption of nil pod doesn't make sense, not allowed.
		return false
	}

	if pod.Spec.PreemptionPolicy == nil {
		// Defaults to PreemptLowerPriority if not provided.
		return true
	}

	switch *pod.Spec.PreemptionPolicy {
	case corev1.PreemptLowerPriority:
		return true
	case corev1.PreemptNever:
		return false
	case "":
		// Defaults to PreemptLowerPriority if not provided.
		return true
	default:
		// Unknown policy, does not allow preemption.
		return false
	}
}

// TerminatingByPreemption returns true if the pod is in the termination state caused by scheduler preemption.
// TODO: replace with preemption package API when available in scheduling framework release: https://github.com/kubernetes/kubernetes/blob/28a13bcbd0c199dd1914140a688fa1c14696c75e/pkg/scheduler/framework/preemption/util.go#L24
func TerminatingByPreemption(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp == nil {
		return false
	}

	return HasCondition(pod, corev1.DisruptionTarget, corev1.ConditionTrue,
		corev1.PodReasonPreemptionByScheduler)
}
