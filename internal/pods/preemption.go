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
