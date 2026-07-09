package pods

import corev1 "k8s.io/api/core/v1"

// IsTerminal returns true if the pod's phase indicates the pod is in a terminal state.
func IsTerminal(pod *corev1.Pod) bool {
	if pod == nil {
		return true
	}

	return IsTerminalPhase(pod.Status.Phase)
}

// IsTerminalPhase returns true if the pod phase indicates the pod is in a terminal state.
func IsTerminalPhase(phase corev1.PodPhase) bool {
	return phase == corev1.PodSucceeded || phase == corev1.PodFailed
}

// IsNonTerminal returns true if the pod's phase indicates the pod is in a non-terminal state.
func IsNonTerminal(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}

	return IsNonTerminalPhase(pod.Status.Phase)
}

// IsNonTerminalPhase returns true if the pod phase indicates the pod is in a non-terminal state.
func IsNonTerminalPhase(phase corev1.PodPhase) bool {
	return phase == corev1.PodPending || phase == corev1.PodRunning
}
