package quotaawarepreempt

import (
	"context"
	"fmt"
	"sync"

	configv1 "github.com/kaschnit/kaschnit-scheduler/apis/config/v1"
	schedv1 "github.com/kaschnit/kaschnit-scheduler/apis/scheduling/v1"
	schedclients "github.com/kaschnit/kaschnit-scheduler/client/clientset/scheduling"
	schedinformers "github.com/kaschnit/kaschnit-scheduler/client/informers/externalversions"
	"github.com/kaschnit/kaschnit-scheduler/internal/alloc"
	"github.com/kaschnit/kaschnit-scheduler/internal/kubesched"
	"github.com/kaschnit/kaschnit-scheduler/internal/queue"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	schedruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

const (
	// PluginName is the name of the scheduling plugin.
	PluginName = "QuotaAwarePreemption"
)

func Register(registry schedruntime.Registry) error {
	return registry.Register(PluginName, PluginFactory)
}

func WithPlugin() app.Option {
	return app.WithPlugin(PluginName, PluginFactory)
}

func PluginFactory(ctx context.Context, configuration runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
	logger := klog.FromContext(ctx).WithValues("plugin", PluginName)

	fts := feature.NewSchedulerFeaturesFromGates(utilfeature.DefaultFeatureGate)

	logger.Info("Starting plugin",
		"features", fts)

	factory := schedruntime.FactoryAdapter(fts, NewPlugin)
	return factory(ctx, configuration, fh)
}

// Plugin is a kube-scheduler framework plugin for quota-aware preemption.
type Plugin struct {
	sync.RWMutex
	queueMgr          *queue.Manager
	queueSynchronizer *queue.Synchronizer
	logger            klog.Logger
	fh                fwk.Handle
	fts               feature.Features
	args              configv1.QuotaAwarePreemptionArgs
}

// Validate plugin implementation so multipoint configuration works as expected.
var (
	_ fwk.PreFilterPlugin     = (*Plugin)(nil)
	_ fwk.PreFilterExtensions = (*Plugin)(nil)
	_ fwk.PostFilterPlugin    = (*Plugin)(nil)
	_ fwk.ReservePlugin       = (*Plugin)(nil)
	_ fwk.EnqueueExtensions   = (*Plugin)(nil)
)

// NewPlugin initializes a new [Plugin] and returns it.
func NewPlugin(ctx context.Context, rawArgs runtime.Object, fh fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	logger := klog.FromContext(ctx).WithValues("plugin", PluginName)

	logger.Info("Parsing args for plugin")

	var args configv1.QuotaAwarePreemptionArgs
	if err := schedruntime.DecodeInto(rawArgs, &args); err != nil {
		return nil, err
	}

	logger.Info("Got args for plugin",
		"args", args)

	logger.Info("Setting up queue manager for plugin")
	queueMgr := queue.NewManager()

	logger.Info("Setting up queue synchronizer for plugin")
	schedClientset, err := schedclients.NewForConfig(fh.KubeConfig())
	if err != nil {
		return nil, err
	}

	queueSynchronizer, err := queue.NewSynchronizer(
		ctx,
		queueMgr,
		fh.SharedInformerFactory(),
		schedClientset.SchedulingV1().Queues(),
		schedinformers.NewSharedInformerFactory(schedClientset, 0),
	)
	if err != nil {
		return nil, err
	}

	logger.Info("Initialized plugin")
	return &Plugin{
		queueMgr:          queueMgr,
		queueSynchronizer: queueSynchronizer,
		logger:            logger,
		fh:                fh,
		args:              args,
	}, nil
}

// Name returns name of the plugin.
func (plugin *Plugin) Name() string {
	return PluginName
}

// PreFilter implements [framework.PreFilterPlugin].
func (plugin *Plugin) PreFilter(
	ctx context.Context,
	state fwk.CycleState,
	pod *corev1.Pod,
	nodes []fwk.NodeInfo,
) (*fwk.PreFilterResult, *fwk.Status) {
	logger := klog.FromContext(klog.NewContext(ctx, plugin.logger)).WithValues(
		"extensionPoint", "PreFilter",
		"pod", klog.KObj(pod))
	logger.Info("Running PreFilter")

	stateMgr := NewStateManager(state)
	requestedRes := alloc.FromPodReq(pod)
	qSnapshot := NewQueueSnapshotState(plugin.queueMgr)
	// Defer because below code may modify the snapshot's queueMgr.
	// Ensure we wait to write the state until we have made all modifications.
	defer stateMgr.WriteQueueSnapshot(qSnapshot)

	podQ := qSnapshot.QueueMgr.Get(pod)
	if podQ == nil {
		return nil, fwk.NewStatus(fwk.Success)
	}

	nodeList, err := plugin.fh.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, fwk.NewStatus(fwk.Error, fmt.Sprintf("Error getting the node list: %v", err))
	}

	// Count pods with a nominated node against the quota, since they have effectively reserved some
	// space on a target node. Only count pods that are subject to the same quota and are same or
	// higher priority, since these are effectively "ahead" of the current pod behing evaluated.
	for _, node := range nodeList {
		nominatedPods := plugin.fh.NominatedPodsForNode(node.Node().Name)
		for _, nomPodInfo := range nominatedPods {
			if nomPodInfo.GetPod().GetUID() == pod.GetUID() {
				// Don't count this pod to avoid double-counting.
				continue
			}
			if corev1helpers.PodPriority(nomPodInfo.GetPod()) < corev1helpers.PodPriority(pod) {
				// Lower priority, don't count against quota.
				continue
			}

			nomQ := qSnapshot.QueueMgr.Get(nomPodInfo.GetPod())
			if nomQ == nil || nomQ.Name() != podQ.Name() {
				// Not assigned to a queue (meaning it's not subject to any quota) or it's
				// not assigned to the same queue. In either case, it's not subject to the
				// same quota as pod.
				continue
			}

			// Count towards the quota.
			podQ.Quota().AddPodIfNotPresent(nomPodInfo.GetPod())
		}
	}

	// If the pod has a nominated node for preemption, we must re-compute the quota for this
	// pod's queue to ensure that quota snapshot is in sync with the state of the cycle snapshot.
	// Quota is normally updated async by informer, so may be inconsistent.
	//
	// If we don't recompute this, we may return Unschedulable even if the preemptor's victim
	// has already terminated. This will lead to preemption possibly running again, since the
	// scheduler skips Filter and goes straight to PostFilter if PreFilter fails. In the preemption
	// logic of PostFilter, we perform preemption again if the nominated node appears "stale" (has
	// no terminating victims).
	logger = logger.WithValues(
		"used", podQ.Quota().Used(),
		"max", podQ.Quota().Max(),
		"requestedRes", requestedRes)
	exceedsQuota := podQ.Quota().WouldPutOverMax(requestedRes)
	if len(pod.Status.NominatedNodeName) > 0 && exceedsQuota {
		logger.Info("Pod with nominated node does not fit in quota, recomputing quota")

		podIDsOnNode, err := kubesched.GetPodIDsOnNode(plugin.fh, pod.Status.NominatedNodeName)
		if err != nil {
			// Nominated node doesn't seem to exist anymore, so scheduling on this node
			// is not possible and not resolvable by preemption.
			return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable,
				fmt.Sprintf("Error getting pod IDs from nominated node %s: %v",
					pod.Status.NominatedNodeName, err))
		}

		podQ.Quota().DeletePodsFunc(func(podToDelete *corev1.Pod) bool {
			if podToDelete.Spec.NodeName != pod.Status.NominatedNodeName {
				// Pod is not on the nominated node, don't remove from quota.
				return false
			}
			if podIDsOnNode.Has(podToDelete.UID) {
				// Pod is still valid, don't remove from quota.
				return false
			}

			logger.Info("Removing pod from quota snapshot to keep in sync with cycle snapshot")
			return true
		})

		exceedsQuota = podQ.Quota().WouldPutOverMax(requestedRes)
		logger.Info("Synced pod's quota with snapshot",
			"exceedsQuotaAfterSync", exceedsQuota)
	}

	if exceedsQuota {
		logger.Info("Pod does not fit in quota")

		return nil, fwk.NewStatus(fwk.Unschedulable,
			fmt.Sprintf("Not eligible for scheduling because queue %s exceeds quota (quota=%s, requested=%s)",
				podQ.Name(), podQ.Quota(), requestedRes))
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
	logger := klog.FromContext(klog.NewContext(ctx, plugin.logger)).WithValues(
		"extensionPoint", "PostFilter",
		"pod", klog.KObj(pod))
	logger.Info("Running PostFilter")

	defer metrics.PreemptionAttempts.Inc()

	evaluator := preemption.NewEvaluator(
		plugin.Name(),
		plugin.fh,
		&preemptor{
			logger:     plugin.logger,
			fh:         plugin.fh,
			cycleState: state,
			cfg:        plugin.args.Preemption,
		},
		preemption.NewExecutor(plugin.fh, plugin.fts),
	)

	result, status := evaluator.Preempt(ctx, state, pod, m)
	logger.Info("Got preemption result for pod",
		"result", result,
		"status", status)

	return result, status
}

// AddPod implements [framework.PreFilterExtensions].
func (plugin *Plugin) AddPod(
	ctx context.Context,
	state fwk.CycleState,
	podToSchedule *corev1.Pod,
	podInfoToAdd fwk.PodInfo,
	nodeInfo fwk.NodeInfo,
) *fwk.Status {
	logger := klog.FromContext(klog.NewContext(ctx, plugin.logger)).WithValues(
		"extensionPoint", "AddPod",
		"podToSchedule", klog.KObj(podToSchedule),
		"podToAdd", klog.KObj(podInfoToAdd.GetPod()))

	quotaSnapshot, err := NewStateManager(state).ReadQueueSnapshot()
	if err != nil {
		logger.Error(err, "Failed to read quotaSnapshotState from cycleState")
		return fwk.NewStatus(fwk.Error, err.Error())
	}

	if err := quotaSnapshot.QueueMgr.AddPodIfNotPresent(podInfoToAdd.GetPod()); err != nil {
		logger.Error(err, "Failed to add Pod to its associated quota usage")
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
	logger := klog.FromContext(klog.NewContext(ctx, plugin.logger)).WithValues(
		"extensionPoint", "RemovePod",
		"podToSchedule", klog.KObj(podToSchedule),
		"podToRemove", klog.KObj(podInfoToRemove.GetPod()))

	quotaSnapshot, err := NewStateManager(state).ReadQueueSnapshot()
	if err != nil {
		logger.Error(err, "Failed to read quotaSnapshotState from cycleState")
		return fwk.NewStatus(fwk.Error, err.Error())
	}

	if err := quotaSnapshot.QueueMgr.DeletePodIfPresent(podInfoToRemove.GetPod()); err != nil {
		logger.Error(err, "Failed to delete Pod from its associated quota usage")
	}

	return fwk.NewStatus(fwk.Success, "")
}

// Reserve implements [framework.ReservePlugin].
func (plugin *Plugin) Reserve(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) *fwk.Status {
	logger := klog.FromContext(klog.NewContext(ctx, plugin.logger)).WithValues(
		"extensionPoint", "Reserve",
		"pod", klog.KObj(pod))

	if err := plugin.queueMgr.AddPodIfNotPresent(pod); err != nil {
		logger.Error(err, "Failed to add Pod to its associated queue quota")
		return fwk.NewStatus(fwk.Error, err.Error())
	}

	return fwk.NewStatus(fwk.Success, "")
}

// Unreserve implements [framework.ReservePlugin].
func (plugin *Plugin) Unreserve(ctx context.Context, state fwk.CycleState, pod *corev1.Pod, nodeName string) {
	logger := klog.FromContext(klog.NewContext(ctx, plugin.logger)).WithValues(
		"extensionPoint", "Unreserve",
		"pod", klog.KObj(pod))

	if err := plugin.queueMgr.DeletePodIfPresent(pod); err != nil {
		logger.Error(err, "Failed to remove Pod from its associated queue quota", "pod", klog.KObj(pod))
	}
}

// EventsToRegister implements [framework.EnqueueExtensions].
func (plugin *Plugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	// Return the events that may cause pods that this plugin failed to becomes schedulable.
	// This seems like it might have a bug related which causes events to not move pods off of the
	// unschedulable queue.
	// See: https://github.com/kubernetes/kubernetes/issues/110175
	// See: https://github.com/kubernetes/kubernetes/issues/87850
	return []fwk.ClusterEventWithHint{
		// Changes to a pod may cause previously unschedulable pods to become schedulable.
		{
			Event: fwk.ClusterEvent{
				Resource:   fwk.Pod,
				ActionType: fwk.Update | fwk.Delete,
			},
			QueueingHintFn: func(logger klog.Logger, pod *corev1.Pod, oldObj, newObj any) (fwk.QueueingHint, error) {
				return fwk.Queue, nil
			},
		},
		{
			Event: fwk.ClusterEvent{
				Resource:   fwk.EventResource(schedv1.QueueFQRN),
				ActionType: fwk.All,
			},
			QueueingHintFn: func(logger klog.Logger, pod *corev1.Pod, oldObj, newObj any) (fwk.QueueingHint, error) {
				return fwk.Queue, nil
			},
		},
	}, nil
}
