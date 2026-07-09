package pods

import corev1 "k8s.io/api/core/v1"

// HasCondition returns whether the pod has a condition matching the type, status, and reason.
func HasCondition(
	pod *corev1.Pod,
	typ corev1.PodConditionType,
	status corev1.ConditionStatus,
	reason string,
) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == typ && condition.Status == status && condition.Reason == reason {
			return true
		}
	}

	return false
}
