package pods

import (
	corev1 "k8s.io/api/core/v1"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

// CompareImportanceDesc is a comparison function for sorting pods in descending order of importance.
func CompareImportanceDesc(a, b *corev1.Pod) int {
	if schedutil.MoreImportantPod(a, b) {
		return -1
	}
	if schedutil.MoreImportantPod(b, a) {
		return 1
	}

	return 0
}
