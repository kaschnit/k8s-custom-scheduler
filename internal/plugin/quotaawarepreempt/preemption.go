package quotaawarepreempt

import (
	"context"
	"fmt"
	"math/rand"
	"slices"

	configv1 "github.com/kaschnit/kaschnit-scheduler/apis/config/v1"
	"github.com/kaschnit/kaschnit-scheduler/internal/alloc"
	"github.com/kaschnit/kaschnit-scheduler/internal/pdbeval"
	"github.com/kaschnit/kaschnit-scheduler/internal/pods"
	"github.com/kaschnit/kaschnit-scheduler/internal/queue"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
)

type preemptor struct {
	logger     klog.Logger
	fh         fwk.Handle
	cycleState fwk.CycleState
	cfg        configv1.PreemptionConfig
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
	// If max is provided, ensure number of candidates is no greater than the max.
	if max := p.cfg.MaxCandidateNodes; max != nil {
		nodes = min(nodes, *max)
	}

	// Use random offset to spread preemptions uniformly across candidate nodes.
	return rand.Int31(), max(1, nodes)
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
	logger := p.logger.WithValues("preemptorPod", klog.KObj(pod))
	stateMgr := NewStateManager(p.cycleState)

	// Check the PreemptionPolicy specified by the pod's PriorityClass.
	if !pods.PolicyAllowsPreemption(pod) {
		return false, "Not eligible to preempt due to configured preemptionPolicy"
	}

	// Fetch the queue snapshot.
	queueSnapshot, err := stateMgr.ReadQueueSnapshot()
	if err != nil {
		logger.Error(err, "Failed to read queueSnapshot from cycleState")
		return false, "Not eligible to preempt due to failed to read queue snapshot from cycleState."
	}

	// Pod is not eligible to preempt if its queue config says it can't preempt.
	preemptorQ := queueSnapshot.QueueMgr.Get(pod)
	if !preemptorQ.CanPodPreemptOthers(pod) {
		return false, "Not eligible to preempt due to queue's preemption config not allowing preemption."
	}

	// If no nominated node for this pod, then it has not yet been considered for preemption.
	// Thus is should be considered.
	if len(pod.Status.NominatedNodeName) == 0 {
		logger.V(5).Info("No nominated node, eligible to preempt")
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
		logger.Info("Nominated node is UnschedulableAndUnresolvable, eligible to preempt")
		return true, ""
	}

	// Fetch the node info and ensure it exists.
	nodeInfo, err := p.fh.SnapshotSharedLister().NodeInfos().Get(pod.Status.NominatedNodeName)
	if nodeInfo == nil || err != nil {
		logger.Info("Unable to find node info of nominated node",
			"nomNodeName", pod.Status.NominatedNodeName,
			"err", err)
		return false, "Unable to find info of nominated node."
	}

	// At this point, we have a pod that has a still-valid node nomination.
	// This means it has already selected victims on the nominated node.
	// We must check if we should again perform victim selection on the nominated node.
	//
	// We should select victims again if there are no terminating, lower-priority pods
	// on the nominated node; such terminations may indicate that the preemption resulting
	// from the previous victim selection is still in-progress.
	// We don't want to perform victim selection if we don't have to because it's expensive.
	requestedRes := alloc.FromPodReq(pod)
	preemptorPriority := corev1helpers.PodPriority(pod)
	if preemptorQ != nil { // Quota-aware preemption path
		wouldBeOverQuota := preemptorQ.Quota().WouldPutOverMax(requestedRes)

		// Check for terminating pods (marked for deletion) that will clear up space for preemptor.
		// This check prevents additional preemptions unnecessarily.
		logger.Info("Checking potential victim pods that would exclude eligibility")
		for _, victimInfo := range nodeInfo.GetPods() {
			victimLogger := logger.V(5).WithValues("victimPod", klog.KObj(victimInfo.GetPod()))

			victimLogger.Info("Checking potential victim pod")
			if !pods.TerminatingByPreemption(victimInfo.GetPod()) {
				// Potential victim is not being deleted by preemption, move on to the next.
				victimLogger.Info("Potential victim is not terminating via preemption, does not exclude eligibility")
				continue
			}
			if corev1helpers.PodPriority(victimInfo.GetPod()) >= preemptorPriority {
				// Terminating pod does not have lower priority.
				// Thus it is not a preemption victim, it's just a terminating pod.
				victimLogger.Info("Potential victim is not lower priority, does not exclude eligibility")
				continue
			}

			victimQ := queueSnapshot.QueueMgr.Get(victimInfo.GetPod())
			if victimQ == nil {
				// No quota to check for victim, move on to the next.
				victimLogger.Info("Potential victim does not have a queue, does not exclude eligibility")
				continue
			}
			if !victimQ.CanPodBePreemptedByOthers(victimInfo.GetPod()) {
				// Terminating pod is not allowed to be a victim per queue configuration.
				// So it is not a preemption victim, it's just a terminating pod.
				victimLogger.Info("Potential victim cannot be preempted, does not exclude eligibility")
				continue
			}

			if preemptorQ.Name() == victimQ.Name() {
				// There is a terminating victim in the queue (sharing quota with preemptor) and of lower priority.
				// This may free up room to schedule the preemptor, so no need to preempt.
				return false, "Not eligible to preempt due to a terminating preemption victim on the nominated node."
			}
			if preemptorQ.Name() != victimQ.Name() && !wouldBeOverQuota {
				// There is a terminating victim in a different queue (not sharing quota with preemptor).
				// The preemptor is also not going to be over its quota, and thus is schedulable in terms of quota.
				// So, waiting for this victim to finish terminating will allow the preemptor to schedule.
				return false, "Not eligible to preempt due to a terminating preemption victim on the nominated node."
			}
		}
	} else { // Vanilla preemption path
		for _, victimInfo := range nodeInfo.GetPods() {
			if !pods.TerminatingByPreemption(victimInfo.GetPod()) {
				// Victim is not being deleted by preemption, move on to the next.
				continue
			}
			if victimQuota := queueSnapshot.QueueMgr.Get(victimInfo.GetPod()); victimQuota != nil {
				// Victim has a quota, do not evaluate for normal preemption path.
				continue
			}

			if corev1helpers.PodPriority(victimInfo.GetPod()) < preemptorPriority {
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

	// Note that we use the state passed in to SelectVictimsOnNode.
	// This state is fresh on each call to SelectVictimsOnNode so we can simulate
	// preemption with a clean state each time.
	// It's not the same as the preemptor's cycleState because it persists changes
	// when calling simulated PreFilter/Filter/etc.
	stateMgr := NewStateManager(state)

	queueSnapshot, err := stateMgr.ReadQueueSnapshot()
	if err != nil {
		logger.Error(err, "Failed to read queueSnapshot from cycleState")
		return nil, 0, fwk.NewStatus(fwk.Unschedulable, "Failed to read queueSnapshot from cycleState")
	}

	// Simulate removing pi from this node.
	// This adjusts the queue snapshot's quota usage accordingly.
	removePod := func(pi fwk.PodInfo) error {
		if err := nodeInfo.RemovePod(logger, pi.GetPod()); err != nil {
			return err
		}
		status := p.fh.RunPreFilterExtensionRemovePod(ctx, state, pod, pi, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		return nil
	}
	// Simulate adding pi to this node.
	// This adjusts the queue snapshot's qutoa usage accordingly.
	addPod := func(pi fwk.PodInfo) error {
		nodeInfo.AddPodInfo(pi)
		status := p.fh.RunPreFilterExtensionAddPod(ctx, state, pod, pi, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		return nil
	}

	preemptorQ := queueSnapshot.QueueMgr.Get(pod)

	logger.Info("Looking for potential preemption victim on node")
	potentialVictims := findPotentialVictims(pod, preemptorQ, queueSnapshot, nodeInfo)

	if len(potentialVictims) == 0 {
		// No potential victims are found, so we don't need to evaluate the node again since its state didn't change.
		logger.Info("Did not find any potential victims on node")
		return nil, 0, fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
			fmt.Sprintf("No victims found on node %s for preemptor pod %s", nodeInfo.Node().Name, pod.Name))
	}

	logger.Info("Found potential victims on node",
		"numPotentialVictims", len(potentialVictims))

	// Simulate removal of potential victims from node.
	for _, victimInfo := range potentialVictims {
		if err := removePod(victimInfo); err != nil {
			return nil, 0, fwk.AsStatus(err)
		}
	}

	if status := p.fh.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo); !status.IsSuccess() {
		// If the new pod does not fit after removing all the lower priority pods,
		// this node is not suitable for preemption. We can skip the expensive reprieval logic.
		logger.Info("Preemptor does not fit on node after removing potential victims")
		return nil, 0, status
	}

	requestedRes := alloc.FromPodReq(pod)
	if preemptorQ != nil && preemptorQ.Quota().WouldPutOverMax(requestedRes) {
		// If there's a quota and it's exceeded even after removing all potential victims,
		// there's nothing we can do on this node to make pods schedule. So this node is
		// not eligible for preemption (i.e. has no eligible victims).
		logger.Info("Preemptor does not fit quota after removing potential victims from node",
			"requested", requestedRes,
			"used", preemptorQ.Quota().Used(),
			"max", preemptorQ.Quota().Max())
		return nil, 0, fwk.NewStatus(fwk.Unschedulable,
			fmt.Sprintf("Not eligible for preemption because queue exceeds after preemption (quota=%s)",
				preemptorQ.Quota()))
	}

	// Sort potential victims in descending order of priority.
	// We want to try to reprieve the highest-priority pods first, so that we
	// only select the lowest-priority victims that we can.
	slices.SortFunc(potentialVictims, func(a, b fwk.PodInfo) int {
		return pods.CompareImportanceDesc(a.GetPod(), b.GetPod())
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
		if preemptorQ != nil && preemptorQ.Quota().WouldPutOverMax(requestedRes) {
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
	pdbViolationEval := pdbeval.CheckPodRemovalViolations(potentialVictims, pdbs)

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
	slices.SortFunc(victims, pods.CompareImportanceDesc)

	logger.Info("Finished selecting victims on node",
		"numVictims", len(victims),
		"numPDBViolationVictims", numPDBViolationVictims)

	return victims, numPDBViolationVictims, fwk.NewStatus(fwk.Success)
}

// findPotentialVictims identifies all potential preemption victims for preemptor.
// preemptorQ may be nil if the preemptor does not belong to a queue. All other
// arguments are expected to be non-nil.
func findPotentialVictims(
	preemptorPod *corev1.Pod,
	preemptorQ *queue.Queue,
	queueSnapshot *QueueSnapshotState,
	nodeInfo fwk.NodeInfo,
) []fwk.PodInfo {
	preemptorPrio := corev1helpers.PodPriority(preemptorPod)

	var potentialVictims []fwk.PodInfo
	if preemptorQ != nil { // Quota-aware preemption path
		for _, victimInfo := range nodeInfo.GetPods() {
			victimQ := queueSnapshot.QueueMgr.Get(victimInfo.GetPod())
			if victimQ == nil {
				// Not a victim if it has no queue/quota specified.
				continue
			}

			if corev1helpers.PodPriority(victimInfo.GetPod()) >= preemptorPrio {
				// Not a victim if it's same or higher priority than the preemptor.
				continue
			}

			if !queue.IsPreemptionAllowed(preemptorQ, preemptorPod, victimQ, victimInfo.GetPod()) {
				// Not a victim if preemptor cannot preempt it.
				continue
			}

			potentialVictims = append(potentialVictims, victimInfo)
		}
	} else { // Vanilla preemption path
		for _, victimInfo := range nodeInfo.GetPods() {
			if victimQ := queueSnapshot.QueueMgr.Get(victimInfo.GetPod()); victimQ != nil {
				// Not a victim for vanilla preemption path if it has a quota.
				continue
			}

			if corev1helpers.PodPriority(victimInfo.GetPod()) >= preemptorPrio {
				// Not a victim if it's same or higher priority than the preemptor.
				continue
			}

			potentialVictims = append(potentialVictims, victimInfo)
		}
	}

	return potentialVictims
}
