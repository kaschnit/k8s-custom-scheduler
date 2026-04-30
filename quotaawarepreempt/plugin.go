package quotaawarepreempt

import (
	"context"
	"fmt"
	"sync"

	"github.com/kaschnit/custom-scheduler/apis/config"
	"github.com/kaschnit/custom-scheduler/internal/resconv"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

const (
	// Name is the name of the scheduling plugin.
	Name = "Quota"

	// GroupName is the API group name for this scheduling plugin.
	// TODO: move this to /apis when we add queue CRD.
	GroupName = "scheduling.kaschnit.github.io"

	// AnnotatioNKeyPrefix is the prefix of the annotations for this plugin.
	AnnotationKeyPrefix = "quota." + GroupName + "/"
)

// Plugin is a kube-scheduler framework plugin for quota-aware preemption.
type Plugin struct {
	sync.RWMutex
	quotas QuotaUsages
	logger klog.Logger
	fh     fwk.Handle
	args   config.QuotaArgs
}

var (
	_ fwk.PreFilterPlugin   = (*Plugin)(nil)
	_ fwk.PostFilterPlugin  = (*Plugin)(nil)
	_ fwk.ReservePlugin     = (*Plugin)(nil)
	_ fwk.EnqueueExtensions = (*Plugin)(nil)
)

// NewPlugin initializes a new [Plugin] and returns it.
func NewPlugin(ctx context.Context, rawArgs runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
	args, ok := rawArgs.(*config.QuotaArgs)
	if !ok {
		return nil, fmt.Errorf("got args of type %T, want *PreemptionTolerationArgs", args)
	}
	logger := klog.FromContext(ctx).WithValues("plugin", Name)

	plugin := Plugin{
		logger: logger,
		fh:     fh,
		args:   *args,
	}
	return &plugin, nil
}

// Name returns name of the plugin.
func (plugin *Plugin) Name() string {
	return Name
}

// PreFilter implements [framework.PreFilterPlugin].
func (plugin *Plugin) PreFilter(
	ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	nodes []fwk.NodeInfo,
) (*fwk.PreFilterResult, *fwk.Status) {
	stateMgr := NewStateManager(state)
	stateMgr.WriteQuotaUsageSnapshot(plugin.createQuotasSnapshot())

	podReq := resconv.ExtractFwkFromPod(pod)

	queue, quota := plugin.quotas.getQuota(pod)
	if quota == nil {
		stateMgr.WritePreFilter(&PreFilterState{
			request: *podReq,
		})
		return nil, fwk.NewStatus(fwk.Success)
	}

	nodeList, err := plugin.fh.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, fwk.NewStatus(fwk.Error, fmt.Sprintf("Error getting the node list: %v", err))
	}

	var nominatedReqInQuota framework.Resource
	for _, node := range nodeList {
		nominatedPods := plugin.fh.NominatedPodsForNode(node.Node().Name)
		for _, nomPodInfo := range nominatedPods {
			if nomPodInfo.GetPod().UID == pod.UID {
				continue
			}

			nomQueue, nomQuota := plugin.quotas.getQuota(nomPodInfo.GetPod())
			if nomQuota != nil {
				nomResourceRequest := resconv.FwkToCoreV1List(resconv.ExtractFwkFromPod(nomPodInfo.GetPod()))
				// If they are subject to the same quota and nomPod is scheduled ahead of (higher priority than) pod,
				// nomPod will be added to the nominatedReqInQuota.
				// If they aren't subject to the same quota and the usage of nomQuota does not exceed min,
				// p will be added to the totalNominatedResource.
				if nomQueue == queue && corev1helpers.PodPriority(nomPodInfo.GetPod()) >= corev1helpers.PodPriority(pod) {
					nominatedReqInQuota.Add(nomResourceRequest)
				}
			}
		}
	}

	stateMgr.WritePreFilter(&PreFilterState{
		request:             *podReq,
		nominatedReqInQuota: nominatedReqInQuota,
	})

	if quota.wouldPutOverMax(resconv.AddFwk(&nominatedReqInQuota, podReq)) {
		return nil, fwk.NewStatus(fwk.Unschedulable,
			fmt.Sprintf("Pod %v/%v is rejected in PreFilter because quota %s is more than Max", pod.Namespace, pod.Name, queue))
	}

	return nil, fwk.NewStatus(fwk.Success, "")
}

// PreFilterExtensions implements [framework.PreFilterPlugin].
func (plugin *Plugin) PreFilterExtensions() fwk.PreFilterExtensions {
	return plugin
}

// PostFilter implements [framework.PostFilterPlugin].
func (plugin *Plugin) PostFilter(
	ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	m fwk.NodeToStatusReader,
) (*fwk.PostFilterResult, *fwk.Status) {
	defer metrics.PreemptionAttempts.Inc()

	evaluator := preemption.NewEvaluator(
		plugin.Name(),
		plugin.fh,
		&preemptor{
			logger:   plugin.logger,
			fh:       plugin.fh,
			stateMgr: NewStateManager(state),
		},
		plugin.args.EnableAsyncPreemption,
	)

	return evaluator.Preempt(ctx, state, pod, m)
}

// AddPod implements [framework.PreFilterExtensions].
func (plugin *Plugin) AddPod(
	ctx context.Context,
	state fwk.CycleState,
	podToSchedule *corev1.Pod,
	podInfoToAdd fwk.PodInfo,
	nodeInfo fwk.NodeInfo,
) *fwk.Status {
	logger := klog.FromContext(klog.NewContext(ctx, plugin.logger))
	stateMgr := NewStateManager(state)

	quotaSnapshotState, err := stateMgr.ReadQuotaUsageSnapshot()
	if err != nil {
		logger.Error(err, "Failed to read quotaSnapshotState from cycleState")
		return fwk.NewStatus(fwk.Error, err.Error())
	}

	if err := quotaSnapshotState.quotaUsages.addPodIfNotPresent(podInfoToAdd.GetPod()); err != nil {
		logger.Error(err, "Failed to add Pod to its associated quota usage",
			"pod", klog.KObj(podInfoToAdd.GetPod()))
	}

	return fwk.NewStatus(fwk.Success, "")
}

// RemovePod implements [framework.PreFilterExtensions].
func (plugin *Plugin) RemovePod(
	ctx context.Context,
	state fwk.CycleState,
	podToSchedule *corev1.Pod,
	podInfoToRemove fwk.PodInfo,
	nodeInfo fwk.NodeInfo,
) *fwk.Status {
	logger := klog.FromContext(klog.NewContext(ctx, plugin.logger))
	stateMgr := NewStateManager(state)

	quotaSnapshotState, err := stateMgr.ReadQuotaUsageSnapshot()
	if err != nil {
		logger.Error(err, "Failed to read quotaSnapshotState from cycleState")
		return fwk.NewStatus(fwk.Error, err.Error())
	}

	if err := quotaSnapshotState.quotaUsages.deletePodIfPresent(podInfoToRemove.GetPod()); err != nil {
		logger.Error(err, "Failed to delete Pod from its associated quota usage",
			"pod", klog.KObj(podInfoToRemove.GetPod()))
	}

	return fwk.NewStatus(fwk.Success, "")
}

// Reserve implements [framework.ReservePlugin].
func (plugin *Plugin) Reserve(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) *fwk.Status {
	plugin.Lock()
	defer plugin.Unlock()
	logger := klog.FromContext(klog.NewContext(ctx, plugin.logger)).WithValues("ExtensionPoint", "Reserve")

	if err := plugin.quotas.addPodIfNotPresent(pod); err != nil {
		logger.Error(err, "Failed to add Pod to its associated queue quota", "pod", klog.KObj(pod))
		return fwk.NewStatus(fwk.Error, err.Error())
	}

	return fwk.NewStatus(fwk.Success, "")
}

// Unreserve implements [framework.ReservePlugin].
func (plugin *Plugin) Unreserve(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) {
	plugin.Lock()
	defer plugin.Unlock()
	logger := klog.FromContext(klog.NewContext(ctx, plugin.logger)).WithValues("ExtensionPoint", "Reserve")

	if err := plugin.quotas.deletePodIfPresent(pod); err != nil {
		logger.Error(err, "Failed to remove Pod from its associated queue quota", "pod", klog.KObj(pod))
	}
}

// EventsToRegister implements [framework.EnqueueExtensions].
func (plugin *Plugin) EventsToRegister(context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{
			Event: fwk.ClusterEvent{
				Resource:   fwk.Pod,
				ActionType: fwk.Delete,
			},
		},
		// We only need this if we depend on a custom resource for configuration.
		// Currently expects KubeSchedulerConfig for queue configuration, but we
		// should move to a custom resource.
		// TODO: update this to match custom resource when it exists.
		// {
		// 	Event: fwk.ClusterEvent{
		// 		Resource:   fwk.EventResource(fmt.Sprintf("quotas.v1alpha1.%s", GroupName)),
		// 		ActionType: fwk.All,
		// 	},
		// },
	}, nil
}

func (plugin *Plugin) createQuotasSnapshot() *QuotaUsageSnapshotState {
	plugin.RLock()
	defer plugin.RUnlock()

	return &QuotaUsageSnapshotState{
		quotaUsages: plugin.quotas.clone(),
	}
}
