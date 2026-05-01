package podutil

import corev1 "k8s.io/api/core/v1"

// IsTerminal returns true if the pod phase indicates the pod is in a terminal state.
func IsTerminal(phase corev1.PodPhase) bool {
	return phase == corev1.PodSucceeded || phase == corev1.PodFailed
}

// IsNonTerminal returns true if the pod phase indicates the pod is in a non-terminal state.
func IsNonTerminal(phase corev1.PodPhase) bool {
	return phase == corev1.PodPending || phase == corev1.PodRunning
}
