package quotaawarepreempt

import (
	"context"
	"fmt"
	"sort"

	"github.com/kaschnit/custom-scheduler/internal/boolstr"
	"github.com/kaschnit/custom-scheduler/internal/pdbutil"
	"github.com/kaschnit/custom-scheduler/internal/resconv"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

const (
	// AnnotationKeyIsPreemptor specifies whether this pod can preempt other pods.
	// If unspecified, empty, or invalid, defaults to false (this pod cannot preempt).
	AnnotationKeyIsPreemptor = AnnotationKeyPrefix + "is-preemptor"
	// AnnotationKeyIsVictim specifies whether this pod can be preempted by other pods.
	// If unspecified, empty, or invalid, defaults to false (this pod cannot be preempted).
	AnnotationKeyIsVictim = AnnotationKeyPrefix + "is-victim"
	// AnnotationKeyMinTimeToPreempt is the minimum time that this pod must be waiting before it
	// is allowed to preempt.
	// If unspecified, empty, or invalid, defaults to 0 (can immediately preempt).
	AnnotationKeyMinTimeToPreempt = AnnotationKeyPrefix + "min-time-to-preempt"
	// AnnotationKeyMinTimeToVictim is the minimum time that this pod must be scheduled before it
	// may be considered as a victim.
	// If unspecified, empty, or invalid, defaults to 0 (can immediately be a victim).
	AnnotationKeyMinTimeToVictim = AnnotationKeyPrefix + "min-time-to-victim"
)

type preemptor struct {
	logger   klog.Logger
	fh       fwk.Handle
	stateMgr *StateManager
}

var _ preemption.Interface = (*preemptor)(nil)

// CandidatesToVictimsMap implements [preemption.Interface].
func (p *preemptor) CandidatesToVictimsMap(candidates []preemption.Candidate) map[string]*extenderv1.Victims {
	m := make(map[string]*extenderv1.Victims, len(candidates))
	for _, c := range candidates {
		m[c.Name()] = c.Victims()
	}
	return m
}

// GetOffsetAndNumCandidates implements [preemption.Interface].
func (p *preemptor) GetOffsetAndNumCandidates(nodes int32) (int32, int32) {
	return 0, nodes
}

// OrderedScoreFuncs implements [preemption.Interface].
func (p *preemptor) OrderedScoreFuncs(
	ctx context.Context,
	nodesToVictims map[string]*extenderv1.Victims,
) []func(node string) int64 {
	return nil
}

// PodEligibleToPreemptOthers implements [preemption.Interface].
func (p *preemptor) PodEligibleToPreemptOthers(
	ctx context.Context,
	pod *corev1.Pod,
	nominatedNodeStatus *fwk.Status,
) (bool, string) {
	logger := p.logger

	// Check the PreemptionPolicy from the PriorityClass.
	// If not provided, preemption is allowed (default is PreemptLowerPriority).
	if pod.Spec.PreemptionPolicy != nil {
		switch *pod.Spec.PreemptionPolicy {
		case corev1.PreemptNever:
			logger.Info("Pod is not eligible to preempt because of its preemptionPolicy",
				"pod", klog.KObj(pod),
				"preemptionPolicy", corev1.PreemptNever)
			return false, "Not eligible to preempt due to preemptionPolicy=Never."
		case corev1.PreemptLowerPriority: // Preemption allowed
		case "": // Preemption allowed; use our annotation-based preemption if unspecified
		default:
			logger.Info("Pod is not eligible to preempt because of its preemptionPolicy",
				"pod", klog.KObj(pod),
				"preemptionPolicy", corev1.PreemptNever)
			return false, "Not eligible to preempt due to unknown preemptionPolicy."
		}
	}

	// Check the is-preemptor annotation.
	// The pod is only eligible to preempt if it has this annotation.
	if !boolstr.IsTrue(pod.Annotations[AnnotationKeyIsPreemptor]) {
		return false, "Not eligible to preempt due to is-preemptor!=true"
	}

	// If no nominated node for this pod, then it has not yet been considered for preemption.
	// Thus is should be considered.
	if len(pod.Status.NominatedNodeName) == 0 {
		return true, ""
	}

	// A previous preemption attempt nominated this node for this pod, but filters determined
	// that the node became UnschedulableAndUnresolvable after that nomination.
	// This could happen if the node is changed in some way (e.g. cordoned) after nomination.
	// Thus we must allow it to be considered for preemption again; running preemption again
	// will nominate it for a different node.
	// This logic is very similar to that of the in-tree DefaultPreemption plugin:
	// https://github.com/kubernetes/kubernetes/blob/90608d95012a53ab5c359cf8fe37f06601e2aaf7/pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go#L371-L375
	if nominatedNodeStatus.Code() == fwk.UnschedulableAndUnresolvable {
		return true, ""
	}

	// Fetch the node info and ensure it exists.
	nodeInfo, err := p.fh.SnapshotSharedLister().NodeInfos().Get(pod.Status.NominatedNodeName)
	if nodeInfo == nil || err != nil {
		logger.Info("Unable to find node info of nominated node",
			"nomNodeName", pod.Status.NominatedNodeName,
			"err", err)
	}

	// Fetch the prefilter state.
	preFilterState, err := p.stateMgr.ReadPreFilter()
	if err != nil {
		logger.Error(err, "Failed to read preFilterState from cycleState")
		return false, "Not eligible to preempt due to failed to read from cycleState"
	}

	// Fetch the quota snapshot from the prefilter.
	quotaSnapshotState, err := p.stateMgr.ReadQuotaUsageSnapshot()
	if err != nil {
		logger.Error(err, "Failed to read quotaSnapshotState from cycleState")
		return true, ""
	}

	// At this point, we have a pod that has a valid node nomination.
	// We must ensure that we should preempt on the nominated node.
	// We should preempt on this node if there are no terminating lower-priority pods
	// on the node, as such terminations may indicate that this pod already preempted.
	preemptorPriority := corev1helpers.PodPriority(pod)
	preemptorQ, preemptorQuota := quotaSnapshotState.quotaUsages.getQuota(pod)
	if preemptorQuota != nil { // Quota-aware preemption path
		wouldBeOverQuota := preemptorQuota.wouldPutOverMax(
			resconv.AddFwk(&preFilterState.request, &preFilterState.nominatedReqInQuota))

		// Check for terminating pods (marked for deletion) that will clear up space for preemptor.
		// This check prevents additional preemptions unnecessarily.
		for _, victimInfo := range nodeInfo.GetPods() {
			if victimInfo.GetPod().DeletionTimestamp == nil {
				// Potential victim is not being deleted, move on to the next.
				continue
			}
			if corev1helpers.PodPriority(victimInfo.GetPod()) >= preemptorPriority {
				// Terminating pod does not have lower priority.
				// Thus it is not a preemption victim, it's just a terminating pod.
				continue
			}
			if !boolstr.IsTrue(victimInfo.GetPod().Annotations[AnnotationKeyIsVictim]) {
				// Terminating pod is not allowed to be a vicitm.
				// This it is preemption victim, it's just a terminating pod.
				continue
			}

			victimQ, victimQuotaUsage := quotaSnapshotState.quotaUsages.getQuota(victimInfo.GetPod())
			if victimQuotaUsage == nil {
				// No quota to check for victim, move on to the next.
				continue
			}

			if preemptorQ == victimQ && corev1helpers.PodPriority(victimInfo.GetPod()) < preemptorPriority {
				// There is a terminating victim in the queue (sharing quota with preemptor) and of lower priority.
				// This may free up room to schedule the preemptor, so no need to preempt.
				return false, "Not eligible to preempt due to a terminating pod on the nominated node."
			}

			if preemptorQ != victimQ && !wouldBeOverQuota {
				// There is a terminating victim in a different queue (not sharing quota with preemptor).
				// The preemptor is also not going to be over its quota, and thus is schedulable in terms of quota.
				// So, waiting for this victim to finish terminating will allow the preemptor to schedule.
				return false, "Not eligible to preempt due to a terminating pod on the nominated node."
			}

		}
	} else { // Vanilla preemption path
		for _, victimPodInfo := range nodeInfo.GetPods() {
			if victimPodInfo.GetPod().DeletionTimestamp == nil {
				// Victim is not being deleted, move on to the next.
				continue
			}

			if _, vicQuotaUsage := quotaSnapshotState.quotaUsages.getQuota(victimPodInfo.GetPod()); vicQuotaUsage != nil {
				// Victim has a quota, do not evaluate for normal preemption path.
				continue
			}

			if corev1helpers.PodPriority(victimPodInfo.GetPod()) < preemptorPriority {
				// There is a terminating victim of lower priority.
				// This may free up room to schedule the preemptor, so no need to preempt.
				return false, "Not eligible to preempt due to a terminating pod on the nominated node."
			}
		}
	}

	// No reason has been found at this point for the pod to not be eligible for preemption.
	return true, ""
}

// SelectVictimsOnNode implements [preemption.Interface].
func (p *preemptor) SelectVictimsOnNode(
	ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	nodeInfo fwk.NodeInfo,
	pdbs []*policyv1.PodDisruptionBudget,
) ([]*corev1.Pod, int, *fwk.Status) {
	logger := p.logger.WithValues(
		"preemptor", klog.KObj(pod),
		"node", klog.KObj(nodeInfo.Node()))

	logger.Info("Selecting victims on node for preemption")

	preFilterState, err := p.stateMgr.ReadPreFilter()
	if err != nil {
		logger.Error(err, "Failed to read preFilterState from cycleState")
		return nil, 0, fwk.NewStatus(fwk.Unschedulable, "Failed to read preFilterState from cycleState")
	}

	quotaSnapshotState, err := p.stateMgr.ReadQuotaUsageSnapshot()
	if err != nil {
		logger.Error(err, "Failed to read quotaSnapshotState from cycleState")
		return nil, 0, fwk.NewStatus(fwk.Unschedulable, "Failed to read quotaSnapshotState from cycleState")
	}

	// Simulate removing pi from this node.
	// This adjusts the quota snapshot usage accordingly.
	removePod := func(pi fwk.PodInfo) error {
		if err := nodeInfo.RemovePod(logger, pi.GetPod()); err != nil {
			return err
		}
		status := p.fh.RunPreFilterExtensionRemovePod(ctx, state, pod, pi, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		if err := quotaSnapshotState.quotaUsages.deletePodIfPresent(pi.GetPod()); err != nil {
			return err
		}
		return nil
	}

	// Simulate adding pi to this node.
	// This adjusts the quota snapshot usage accordingly.
	addPod := func(pi fwk.PodInfo) error {
		nodeInfo.AddPodInfo(pi)
		status := p.fh.RunPreFilterExtensionAddPod(ctx, state, pod, pi, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		if err := quotaSnapshotState.quotaUsages.addPodIfNotPresent(pi.GetPod()); err != nil {
			return err
		}
		return nil
	}

	_, preemptorQuota := quotaSnapshotState.quotaUsages.getQuota(pod)
	preemptorPriority := corev1helpers.PodPriority(pod)

	logger.Info("Looking for potential preemption victim on node")

	// Identify all potential victims, simulating their removal.
	var potentialVictims []fwk.PodInfo
	if preemptorQuota != nil { // Quota-aware preemption path
		for _, victimInfo := range nodeInfo.GetPods() {
			if _, vicQuota := quotaSnapshotState.quotaUsages.getQuota(victimInfo.GetPod()); vicQuota == nil {
				// Not a victim if it has no queue/quota specified.
				continue
			}

			if corev1helpers.PodPriority(victimInfo.GetPod()) >= preemptorPriority {
				// Not a victim if it's same or higher priority than the preemptor.
				continue
			}

			potentialVictims = append(potentialVictims, victimInfo)
			if err := removePod(victimInfo); err != nil {
				return nil, 0, fwk.AsStatus(err)
			}
		}
	} else { // Vanilla preemption path
		for _, victimInfo := range nodeInfo.GetPods() {
			if _, vicQuota := quotaSnapshotState.quotaUsages.getQuota(victimInfo.GetPod()); vicQuota != nil {
				// Not a victim for vanilla preemption path if it has a quota.
				continue
			}

			if corev1helpers.PodPriority(victimInfo.GetPod()) >= preemptorPriority {
				// Not a victim if it's same or higher priority than the preemptor.
				continue
			}

			potentialVictims = append(potentialVictims, victimInfo)
			if err := removePod(victimInfo); err != nil {
				return nil, 0, fwk.AsStatus(err)
			}
		}
	}

	if len(potentialVictims) == 0 {
		// No potential victims are found, so we don't need to evaluate the node again since its state didn't change.
		logger.Info("Did not find any potential victims on node")
		return nil, 0, fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
			fmt.Sprintf("No victims found on node %s for preemptor pod %s", nodeInfo.Node().Name, pod.Name))
	}

	logger.Info("Found potential victims on node",
		"numPotentialVictims", len(potentialVictims))

	if status := p.fh.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo); !status.IsSuccess() {
		// If the new pod does not fit after removing all the lower priority pods,
		// this node is not suitable for preemption.
		logger.Info("Preemptor does not fit on node after removing potential victims")
		return nil, 0, status
	}

	if preemptorQuota != nil && preemptorQuota.wouldPutOverMax(&preFilterState.request) {
		// If there's a quota and it's exceeded even after removing all potential victims,
		// there's nothing we can do on this node to make pods schedule. So this node is
		// not eligible for preemption (i.e. has no eligible victims).
		logger.Info("Preemptor does not fit quota after removing potential victims from node",
			"requested", preFilterState.request,
			"used", preemptorQuota.Used,
			"max", preemptorQuota.Max)
		return nil, 0, fwk.NewStatus(fwk.Unschedulable,
			fmt.Sprintf("Not eligible for preemption because queue exceeds after preemption (used=%+v, max=%+v)",
				preemptorQuota.Used, preemptorQuota.Max))
	}

	// Sort potential victims in descending order of priority.
	// We want to try to reprieve the highest-priority pods first, so that we
	// only select the lowest-priority victims that we can.
	sort.Slice(potentialVictims, func(i, j int) bool {
		return schedutil.MoreImportantPod(potentialVictims[i].GetPod(), potentialVictims[j].GetPod())
	})

	// Final victims list, built from reprieval.
	var victims []*corev1.Pod

	// Potential victim "reprieval" - add potential victims back to the node,
	// and see if we can still fit the preemptor.
	// If the preemptor fits with pi added back, it's not a victim.
	// If the preemptor does not fit with the pi added back, it's a victim.
	// Returns whether pi is reprieved.
	maybeReprievePod := func(pi fwk.PodInfo) (bool, error) {
		// Add the potential victim back to the node
		if err := addPod(pi); err != nil {
			return false, err
		}

		// Check if the filter plugin passes with the preemptor on the node after adding
		// back the potential victim. This tells us whether the preemptor will still fit
		// on the node with the potential victim added back, and thus whether the potential
		// victim can be reprieved.
		status := p.fh.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo)
		if !status.IsSuccess() {
			// Pod did not fit on node with preemptor; this pod should indeed be a victim.
			if err := removePod(pi); err != nil {
				return false, err
			}
			victims = append(victims, pi.GetPod())
			logger.Info("Found a preemption victim on node",
				"pod", klog.KObj(pi.GetPod()),
				"node", klog.KObj(nodeInfo.Node()))

			return false, nil
		}

		// Check if the quotas are in violation after adding back the potential victim.
		// This is to ensure that victims are selected such that the quota is reduced
		// below the max to make room for the preemptor.
		if preemptorQuota != nil &&
			preemptorQuota.wouldPutOverMax(resconv.AddFwk(&preFilterState.request, &preFilterState.nominatedReqInQuota)) {
			// Pod did not fit in quota with preemptor; this pod should indeed be a victim.
			if err := removePod(pi); err != nil {
				return false, err
			}
			victims = append(victims, pi.GetPod())
			logger.Info("Found a preemption victim on node",
				"pod", klog.KObj(pi.GetPod()),
				"node", klog.KObj(nodeInfo.Node()))

			return false, nil
		}

		return true, nil
	}

	numPDBViolationVictims := 0
	pdbViolationEval := pdbutil.EvaluatePodRemovalViolations(potentialVictims, pdbs)

	logger.Info("Attempting reprieval on PDB-violating potential victims",
		"numPDBViolatingPotentialVictims", len(pdbViolationEval.ViolatingPods))
	for _, pi := range pdbViolationEval.ViolatingPods {
		reprieved, err := maybeReprievePod(pi)
		if err != nil {
			logger.Error(err, "Failed to reprieve PDB-violating potential victim",
				"pod", klog.KObj(pi.GetPod()))
			return nil, 0, fwk.AsStatus(err)
		}

		// PDB violation pod was not reprieved, so it's a victim.
		if !reprieved {
			numPDBViolationVictims++
		}
	}

	// Now we try to reprieve non-violating victims.
	logger.Info("Attempting reprieval on non-PDB-violating potential victims",
		"numNonPDBViolatingPotentialVictims", len(pdbViolationEval.NonViolatingPods))
	for _, pi := range pdbViolationEval.NonViolatingPods {
		if _, err := maybeReprievePod(pi); err != nil {
			logger.Error(err, "Failed to reprieve non-PDB-violating potential victim",
				"pod", klog.KObj(pi.GetPod()))
			return nil, 0, fwk.AsStatus(err)
		}
	}

	// PDB violation eval may cause victims to be out of order.
	// Ensure victims are kept in order from highest priority to lowest priority.
	sort.Slice(victims, func(i, j int) bool { return schedutil.MoreImportantPod(victims[i], victims[j]) })

	logger.Info("Finished selecting victims on node",
		"numVictims", len(victims),
		"numPDBViolationVictims", numPDBViolationVictims)

	return victims, numPDBViolationVictims, fwk.NewStatus(fwk.Success)
}
