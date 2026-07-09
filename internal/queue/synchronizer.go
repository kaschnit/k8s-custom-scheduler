package queue

import (
	"context"
	"encoding/json"
	"time"

	schedv1 "github.com/kaschnit/kaschnit-scheduler/apis/scheduling/v1"
	schedinformers "github.com/kaschnit/kaschnit-scheduler/client/informers/externalversions"
	"github.com/kaschnit/kaschnit-scheduler/internal/alloc"
	"github.com/kaschnit/kaschnit-scheduler/internal/pods"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// Synchronizer keeps the queue manager in sync with the cluster.
type Synchronizer struct {
	queueMgr     *Manager
	queueUpdater schedv1QueueUpdater
	resTracker   *alloc.ClusterResourceTracker
}

// NewSynchronizer creates a new [Synchronizer].
func NewSynchronizer(
	ctx context.Context,
	queueMgr *Manager,
	informerFactory informers.SharedInformerFactory,
	queueUpdater schedv1QueueUpdater,
	schedInformerFactory schedinformers.SharedInformerFactory,
) (*Synchronizer, error) {
	syn := Synchronizer{
		queueMgr:     queueMgr,
		queueUpdater: queueUpdater,
	}

	queueInformer := schedInformerFactory.Scheduling().V1().Queues().Informer()
	if _, err := queueInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    syn.addQueue,
		UpdateFunc: syn.updateQueue,
		DeleteFunc: syn.deleteQueue,
	}); err != nil {
		return nil, err
	}

	schedInformerFactory.Start(ctx.Done())
	schedInformerFactory.WaitForCacheSync(ctx.Done())

	podInformer := informerFactory.Core().V1().Pods().Informer()
	if _, err := podInformer.AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj any) bool {
				switch t := obj.(type) {
				case *corev1.Pod:
					return len(t.Spec.NodeName) > 0
				case cache.DeletedFinalStateUnknown:
					if pod, ok := t.Obj.(*corev1.Pod); ok {
						return len(pod.Spec.NodeName) > 0
					}
					return false
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    syn.addPod,
				UpdateFunc: syn.updatePod,
				DeleteFunc: syn.deletePod,
			},
		},
	); err != nil {
		return nil, err
	}

	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	resTracker, err := alloc.NewClusterResourceTracker(ctx, informerFactory)
	if err != nil {
		return nil, err
	}

	syn.resTracker = resTracker

	go syn.statusUpdateLoop(ctx)

	return &syn, nil
}

func (syn *Synchronizer) statusUpdateLoop(ctx context.Context) {
	logger := klog.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			clusterTotalRes := syn.resTracker.GetTotal()

			for q := range syn.queueMgr.QueueIter() {
				statusPatch := schedv1.Queue{
					Status: schedv1.QueueStatus{
						Quota: schedv1.QuotaStatus{
							EffectiveMax: clusterTotalRes.TakeMinExisting(q.Quota().Max()).ToResourceList(),
							Used:         q.Quota().Used().ToResourceList(),
						},
					},
				}

				patchBytes, err := json.Marshal(statusPatch)
				if err != nil {
					logger.Error(err, "failed to convert status patch to bytes",
						"statusPatch", statusPatch)
					continue
				}

				if _, err := syn.queueUpdater.Patch(
					ctx,
					q.Name(),
					types.MergePatchType,
					patchBytes,
					metav1.PatchOptions{},
					"status",
				); err != nil {
					logger.Error(err, "failed to patch status",
						"statusPatch", statusPatch)
					continue
				}
			}
		}
	}
}

func (syn *Synchronizer) addQueue(obj any) {
	ctx := context.Background()
	logger := klog.FromContext(ctx)

	queueObj, ok := obj.(*schedv1.Queue)
	if !ok {
		logger.Info("failed to handle queue added, got unexpected object",
			"obj", obj)
	}

	logger.Info("handling queue added",
		"queue", klog.KObj(queueObj))

	preemptionCfg, err := NewPreemptionConfigFromSpec(queueObj.Spec.Preemption)
	if err != nil {
		logger.Error(err, "invalid spec.preemption",
			"preemptionSpec", queueObj.Spec.Preemption)
	}

	syn.queueMgr.Put(queueObj.Name,
		WithQuotaMax(alloc.FromResourceList(queueObj.Spec.Quota.Max)),
		WithLabels(labels.Set(queueObj.Labels)),
		WithPreemptionConfig(preemptionCfg))
}

func (syn *Synchronizer) updateQueue(oldObj, newObj any) {
	ctx := context.Background()
	logger := klog.FromContext(ctx)

	oldQueue, ok := oldObj.(*schedv1.Queue)
	if !ok {
		logger.Info("failed to handle pod updated, got unexpected old object",
			"oldObj", oldObj)
	}

	newQueue, ok := newObj.(*schedv1.Queue)
	if !ok {
		logger.Info("failed to handle pod updated, got unexpected new object",
			"newObj", newObj)
	}

	logger.Info("handling queue updated",
		"oldQueue", klog.KObj(oldQueue),
		"newQueue", klog.KObj(newQueue))

	preemptionCfg, err := NewPreemptionConfigFromSpec(newQueue.Spec.Preemption)
	if err != nil {
		logger.Error(err, "invalid spec.preemption",
			"preemptionSpec", newQueue.Spec.Preemption)
	}

	syn.queueMgr.Update(newQueue.Name,
		WithQuotaMax(alloc.FromResourceList(newQueue.Spec.Quota.Max)),
		WithLabels(labels.Set(newQueue.Labels)),
		WithPreemptionConfig(preemptionCfg))
}

func (syn *Synchronizer) deleteQueue(obj any) {
	ctx := context.Background()
	logger := klog.FromContext(ctx)

	queueObj, ok := obj.(*schedv1.Queue)
	if !ok {
		logger.Info("failed to handle queue deleted, got unexpected object",
			"obj", obj)
	}

	logger.Info("handling queue deleted",
		"queue", klog.KObj(queueObj))

	syn.queueMgr.Delete(queueObj.Name)
}

func (syn *Synchronizer) addPod(obj any) {
	ctx := context.Background()
	logger := klog.FromContext(ctx)

	pod, ok := obj.(*corev1.Pod)
	if !ok {
		logger.Info("failed to handle pod added, got unexpected object",
			"obj", obj)
	}

	if err := syn.queueMgr.AddPodIfNotPresent(pod); err != nil {
		logger.Error(err, "Failed to add Pod to its associated quota",
			"pod", klog.KObj(pod))
	}
}

func (syn *Synchronizer) updatePod(oldObj, newObj any) {
	ctx := context.Background()
	logger := klog.FromContext(ctx)

	oldPod, ok := oldObj.(*corev1.Pod)
	if !ok {
		logger.Info("failed to handle pod updated, got unexpected old object",
			"oldObj", oldObj)
	}

	newPod, ok := newObj.(*corev1.Pod)
	if !ok {
		logger.Info("failed to handle pod updated, got unexpected new object",
			"newObj", newObj)
	}

	if pods.IsTerminal(oldPod) || pods.IsNonTerminal(newPod) {
		return
	}

	if err := syn.queueMgr.DeletePodIfPresent(newPod); err != nil {
		logger.Error(err, "Failed to delete Pod from its associated quota",
			"pod", klog.KObj(newPod))
	}
}

func (syn *Synchronizer) deletePod(obj any) {
	ctx := context.Background()
	logger := klog.FromContext(ctx)

	pod, ok := obj.(*corev1.Pod)
	if !ok {
		logger.Info("failed to handle pod deleted, got unexpected object",
			"obj", obj)
	}

	if err := syn.queueMgr.DeletePodIfPresent(pod); err != nil {
		logger.Error(err, "Failed to delete Pod from its associated quota",
			"pod", klog.KObj(pod))
	}
}

type schedv1QueueUpdater interface {
	Patch(
		ctx context.Context,
		name string,
		pt types.PatchType,
		data []byte,
		opts metav1.PatchOptions,
		subresources ...string,
	) (result *schedv1.Queue, err error)
}
