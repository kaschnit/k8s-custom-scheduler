package quotaawarepreempt

import (
	"context"
	"fmt"

	"github.com/kaschnit/custom-scheduler/internal/boolstr"
	"github.com/kaschnit/custom-scheduler/internal/resconv"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
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
	// Ref 1: https://github.com/kubernetes-sigs/scheduler-plugins/blob/2c75c8b5cb943435e94ffd325d9f1542d01f175f/pkg/capacityscheduling/capacity_scheduling.go#L403-L484
	// Ref 2: https://github.com/kubernetes-sigs/scheduler-plugins/blob/2c75c8b5cb943435e94ffd325d9f1542d01f175f/pkg/preemptiontoleration/preemption_toleration.go#L339C33-L364
	logger := p.logger

	// Check the PreemptionPolicy from the PriorityClass.
	// If not provided, preemption is allowed (default is PreemptLowerPriority).
	if pod.Spec.PreemptionPolicy != nil {
		switch *pod.Spec.PreemptionPolicy {
		case corev1.PreemptNever:
			logger.V(5).Info("Pod is not eligible for preemption because of its preemptionPolicy",
				"pod", klog.KObj(pod),
				"preemptionPolicy", corev1.PreemptNever)
			return false, "not eligible due to preemptionPolicy=Never."
		case corev1.PreemptLowerPriority: // Preemption allowed
		case "": // Preemption allowed (default is PreemptLowerPriority)
		default:
			logger.Info("Pod is not eligible for preemption because of its preemptionPolicy",
				"pod", klog.KObj(pod),
				"preemptionPolicy", corev1.PreemptNever)
			return false, "not eligible due to unknown preemptionPolicy."
		}
	}

	// Check the is-preemptor annotation.
	if !boolstr.IsTrue(pod.Annotations[AnnotationKeyIsPreemptor]) {
		return false, "not eligible due to is-preemptor!=true"
	}

	nomNodeName := pod.Status.NominatedNodeName
	if len(nomNodeName) == 0 {
		return true, ""
	}

	// If the pod's nominated node is considered as UnschedulableAndUnresolvable by the filters,
	// then the pod should be considered for preempting again.
	if nominatedNodeStatus.Code() == fwk.UnschedulableAndUnresolvable {
		return true, ""
	}

	nodeInfo, err := p.fh.SnapshotSharedLister().NodeInfos().Get(nomNodeName)
	if nodeInfo == nil || err != nil {
		logger.V(5).Info("Unable to find node info of nominated node",
			"nomNodeName", nomNodeName,
			"err", err)
	}

	preFilterState, err := p.stateMgr.ReadPreFilter()
	if err != nil {
		logger.V(5).Error(err, "Failed to read preFilterState from cycleState")
		return false, "not eligible due to failed to read from cycleState"
	}

	quotaSnapshotState, err := p.stateMgr.ReadQuotaUsageSnapshot()
	if err != nil {
		logger.Error(err, "Failed to read quotaSnapshotState from cycleState")
		return true, ""
	}

	preemptorPriority := corev1helpers.PodPriority(pod)
	preemptorQ, preemptorQuotaUsage := quotaSnapshotState.quotaUsages.getQuota(pod)
	if preemptorQuotaUsage != nil { // Quota-aware preemption path
		wouldBeOverQuota := preemptorQuotaUsage.wouldPutOverMax(
			resconv.AddFwk(&preFilterState.request, &preFilterState.nominatedReqInQuota))

		// Check for terminating pods (marked for deletion) that will clear up space for preemptor.
		// This check prevents potentially unnecessary preemptions.
		for _, victimPodInfo := range nodeInfo.GetPods() {
			if victimPodInfo.GetPod().DeletionTimestamp == nil {
				// Victim is not being deleted, move on to the next.
				continue
			}

			victimQ, victimQuotaUsage := quotaSnapshotState.quotaUsages.getQuota(victimPodInfo.GetPod())
			if victimQuotaUsage == nil {
				// No quota to check for victim, move on to the next.
				continue
			}

			if preemptorQ == victimQ && corev1helpers.PodPriority(victimPodInfo.GetPod()) < preemptorPriority {
				// There is a terminating victim in the queue (sharing quota with preemptor) and of lower priority.
				// This may free up room to schedule the preemptor, so no need to preempt.
				return false, "not eligible due to a terminating pod on the nominated node."
			}

			if preemptorQ != victimQ && !wouldBeOverQuota {
				// There is a terminating victim in a different queue (not sharing quota with preemptor).
				// The preemptor is also not going to be over its quota, and thus is schedulable in terms of quota.
				// So, waiting for this victim to finish terminating will allow the preemptor to schedule.
				return false, "not eligible due to a terminating pod on the nominated node."
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
				return false, "not eligible due to a terminating pod on the nominated node."
			}
		}
	}

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
	// Ref 1: https://github.com/kubernetes-sigs/scheduler-plugins/blob/2c75c8b5cb943435e94ffd325d9f1542d01f175f/pkg/capacityscheduling/capacity_scheduling.go#L486-L677
	// Ref 2: https://github.com/kubernetes-sigs/scheduler-plugins/blob/2c75c8b5cb943435e94ffd325d9f1542d01f175f/pkg/preemptiontoleration/preemption_toleration.go#L188-L299
	logger := p.logger

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

	fmt.Printf("%+v, %+v", preFilterState, quotaSnapshotState)

	panic("unimplemented") // TODO finish this.
}

func getElasticQuotaSnapshotState(cycleState fwk.CycleState) (*QuotaUsageSnapshotState, error) {
	c, err := cycleState.Read(stateKeyQuotaSnapshot)
	if err != nil {
		// ElasticQuotaSnapshotState doesn't exist, likely PreFilter wasn't invoked.
		return nil, fmt.Errorf("error reading %q from cycleState: %w", stateKeyQuotaSnapshot, err)
	}

	s, ok := c.(*QuotaUsageSnapshotState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to QuotaAwarePreemption QuotaUsageSnapshotState error", c)
	}
	return s, nil
}
