package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// PreemptsEverything returns a rule that allows preemption of everything that is
// allowed to be preempted.
func PreemptsEverything() PreemptsRule {
	return PreemptsRule{
		FromPods: &metav1.LabelSelector{},
		ToQueues: &metav1.LabelSelector{},
		ToPods:   &metav1.LabelSelector{},
	}
}

// PreemptsNothing returns a rules that prevents preempting.
func PreemptsNothing() PreemptsRule {
	return PreemptsRule{}
}

// PreemptedByEverything returns a rule that allows being preempted by anything that is
// allowed to preempt.
func PreemptedByEverything() PreemptedByRule {
	return PreemptedByRule{
		FromQueues: &metav1.LabelSelector{},
		FromPods:   &metav1.LabelSelector{},
		ToPods:     &metav1.LabelSelector{},
	}
}

// PreemptedByNothing returns a rules that prevents being preempted.
func PreemptedByNothing() PreemptedByRule {
	return PreemptedByRule{}
}
