package pdbeval

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fwk "k8s.io/kube-scheduler/framework"

	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// CheckPodRemovalViolationsResult is the return value for [CheckPodRemovalViolations].
type CheckPodRemovalViolationsResult struct {
	// ViolatingPods are the pods that would cause PDB violation if they're removed.
	ViolatingPods []fwk.PodInfo
	// ViolatingPods are the pods that would not cause PDB violation if they're removed.
	NonViolatingPods []fwk.PodInfo
}

// CheckPodRemovalViolations groups the given "pods" into two groups of "violatingPods"
// and "nonViolatingPods" based on whether their PDBs will be violated if they are
// preempted.
// This function is stable and does not change the order of received pods. So, if it
// receives a sorted list, grouping will preserve the order of the input list.
func CheckPodRemovalViolations(podInfos []fwk.PodInfo, pdbs []*policyv1.PodDisruptionBudget) CheckPodRemovalViolationsResult {
	var result CheckPodRemovalViolationsResult

	pdbsAllowed := make([]int32, 0, len(pdbs))
	for _, pdb := range pdbs {
		pdbsAllowed = append(pdbsAllowed, pdb.Status.DisruptionsAllowed)
	}

	for _, podInfo := range podInfos {
		pod := podInfo.GetPod()
		pdbForPodIsViolated := false

		// A pod with no labels will not match any PDB. So, only need to check
		// if the pod has labels.
		if len(pod.Labels) > 0 {
			for i, pdb := range pdbs {
				if pdb.Namespace != pod.Namespace {
					continue
				}

				selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
				if err != nil {
					continue
				}

				// A PDB with a nil or empty selector matches nothing.
				if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
					continue
				}

				// Existing in DisruptedPods means it has been processed in API server,
				// we don't treat it as a violating case.
				if _, exist := pdb.Status.DisruptedPods[pod.Name]; exist {
					continue
				}

				// Only decrement the matched pdb when it's not in its DisruptedPods.
				// Otherwise, we may over-decrement the budget number.
				pdbsAllowed[i]--

				// Matching PDB found.
				if pdbsAllowed[i] < 0 {
					pdbForPodIsViolated = true
				}
			}
		}

		if pdbForPodIsViolated {
			result.ViolatingPods = append(result.ViolatingPods, podInfo)
		} else {
			result.NonViolatingPods = append(result.NonViolatingPods, podInfo)
		}
	}

	return result
}
