package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

func PreemptsEverything() PreemptsRule {
	return PreemptsRule{
		FromPods: &metav1.LabelSelector{},
		ToQueues: &metav1.LabelSelector{},
		ToPods:   &metav1.LabelSelector{},
	}
}

func PreemptsNothing() PreemptsRule {
	return PreemptsRule{}
}

func PreemptedByEverything() PreemptedByRule {
	return PreemptedByRule{
		FromQueues: &metav1.LabelSelector{},
		FromPods:   &metav1.LabelSelector{},
		ToPods:     &metav1.LabelSelector{},
	}
}

func PreemptedByNothing() PreemptedByRule {
	return PreemptedByRule{}
}
