//go:build envtest

package quotaawarepreempt_test

import (
	"testing"

	schedulingapi "github.com/kaschnit/kaschnit-scheduler/apis/scheduling"
	schedulingv1 "github.com/kaschnit/kaschnit-scheduler/apis/scheduling/v1"
	"github.com/kaschnit/kaschnit-scheduler/internal/kassert"
	"github.com/kaschnit/kaschnit-scheduler/internal/kubetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
)

// TODO: Additional tests to add:
//
// - Basic Scheduling
//   - Handling of pods not assigned to a queue
//   - Handling of pods assigned to an invalid queue
//
// - Quota counting
//   - Gate scheduling until quota freed up
//   - Extended resource interaction
//
// - Preemption
//   - Inter-queue preemption / quota, capacity, both
//   - Multiple victims for one preemptor / intra-queue, inter-queue, both / quota, capacity, both
//   - Interaction with taints/tolerations, node selectors, etc
//   - Various preemption policies allowing/preventing preemption
//   - Extended resources
//   - Preempt pod consuming resource X and Y when only requesting resource X
//   - Preempt pod consuming resource X and Y when requesting resources X and Z
//   - Interaction with PriorityClass.preemptionPolicy
//   - Interaction with pods not assigned to a queue
func TestPlugin(t *testing.T) {
	testEnv, err := kubetest.StartEnvTest()
	require.NoError(t, err, "Failed to start envtest")
	t.Cleanup(func() { testEnv.Stop() })

	tCtx, err := kubetest.NewSchedulerContext(t.Context(), testEnv.Config)
	require.NoError(t, err, "Failed to create test context")
	t.Cleanup(func() { tCtx.CleanUp(t.Context()) })

	parentT := t

	t.Run("Preempt one in same queue for capacity", func(t *testing.T) {
		tCtx, err := kubetest.NewSchedulerContext(t.Context(), testEnv.Config)
		require.NoError(t, err, "Failed to create test context")
		t.Cleanup(func() { tCtx.CleanUp(parentT.Context()) })

		_, err = tCtx.PCMgr.Create(t.Context(),
			kubetest.NewPC("high", 1000, true),
			kubetest.NewPC("low", -1000, true))
		require.NoError(t, err, "Failed to create priority classes")

		_, err = tCtx.NodeMgr.CreateAndWaitForReady(t.Context(), 1, kubetest.WaitForNodesReadyOpts{})
		require.NoError(t, err, "Failed to create nodes and wait for ready")

		allocatableByNode, err := tCtx.NodeMgr.GetAllocatableByNode(t.Context())
		require.NoError(t, err, "Failed to get node allocatable resource")
		require.Len(t, allocatableByNode, 1, "Expected exactly 1 node")

		var nodeAllocatable corev1.ResourceList
		for _, allocatable := range allocatableByNode {
			nodeAllocatable = corev1.ResourceList{
				corev1.ResourceCPU:    allocatable[corev1.ResourceCPU],
				corev1.ResourceMemory: allocatable[corev1.ResourceMemory],
			}
			break
		}

		_, err = tCtx.QMgr.Create(t.Context(), &schedulingv1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: "tenant-a"},
			Spec: schedulingv1.QueueSpec{
				Quota: schedulingv1.QuotaSpec{
					Max: corev1.ResourceList{
						corev1.ResourceCPU: func() resource.Quantity {
							// Ensure quota is well above capacity
							quotaCpu := nodeAllocatable.Cpu().DeepCopy()
							quotaCpu.Add(*nodeAllocatable.Cpu())
							quotaCpu.Add(*nodeAllocatable.Cpu())
							quotaCpu.Add(*nodeAllocatable.Cpu())
							return quotaCpu
						}(),
						corev1.ResourceMemory: func() resource.Quantity {
							// Ensure quota is well above capacity
							quotaMem := nodeAllocatable.Memory().DeepCopy()
							quotaMem.Add(*nodeAllocatable.Memory())
							quotaMem.Add(*nodeAllocatable.Memory())
							quotaMem.Add(*nodeAllocatable.Memory())
							return quotaMem
						}(),
					},
				},
				Preemption: schedulingv1.PreemptionSpec{
					Preempts:    schedulingv1.PreemptsEverything(),
					PreemptedBy: schedulingv1.PreemptedByEverything(),
				},
			},
		})
		require.NoError(t, err, "Failed to create queues")

		victim := newPodForQueue(tCtx, "tenant-a", "low", nodeAllocatable)
		_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Create(t.Context(),
			victim, metav1.CreateOptions{})
		require.NoError(t, err)

		// Schedule victim pod
		tCtx.Scheduler.ScheduleOne(t.Context())
		kassert.Eventually(t, func(c *assert.CollectT) {
			gotVictim, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				victim.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get victim pod")
			kassert.PodRunning(c, gotVictim)
		})

		gotVictim, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
			victim.Name, metav1.GetOptions{})
		require.NoError(t, err, "Failed to get victim pod")

		preemptor := newPodForQueue(tCtx, "tenant-a", "high", corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		})
		_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Create(t.Context(),
			preemptor, metav1.CreateOptions{})
		require.NoError(t, err)

		// Perform preemption, resulting in nominated node for preemptor pod
		tCtx.Scheduler.ScheduleOne(t.Context())
		kassert.Eventually(t, func(c *assert.CollectT) {
			gotPreemptor, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				preemptor.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get preemptor pod")

			// Preemptor pod nominated
			kassert.PodNominatedForNode(c, gotPreemptor, gotVictim.Spec.NodeName)

			// Victim pod deleted
			_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				victim.Name, metav1.GetOptions{})
			kassert.IsErrNotFound(c, err, "Victim pod should be deleted")
		})

		// Perform scheduling for nominated node
		tCtx.Scheduler.ScheduleOne(t.Context())
		kassert.Eventually(t, func(c *assert.CollectT) {
			gotPreemptor, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				preemptor.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get preemptor pod")
			kassert.PodRunningOnNode(c, gotPreemptor, gotVictim.Spec.NodeName)
		})

		// Create victim again
		_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Create(t.Context(),
			victim, metav1.CreateOptions{})
		require.NoError(t, err)

		// It should be unschedulable
		tCtx.Scheduler.ScheduleOne(t.Context())
		kassert.Eventually(t, func(c *assert.CollectT) {
			gotVictim, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				victim.Name, metav1.GetOptions{})
			require.NoError(t, err, "Failed to get victim pod")
			kassert.PodUnschedulable(c, gotVictim)
		})
	})

	t.Run("Preempt one in same queue for quota", func(t *testing.T) {
		tCtx, err := kubetest.NewSchedulerContext(t.Context(), testEnv.Config)
		require.NoError(t, err, "Failed to create test context")
		t.Cleanup(func() { tCtx.CleanUp(parentT.Context()) })

		_, err = tCtx.PCMgr.Create(t.Context(),
			kubetest.NewPC("high", 1000, true),
			kubetest.NewPC("low", -1000, true))
		require.NoError(t, err, "Failed to create priority classes")

		_, err = tCtx.NodeMgr.CreateAndWaitForReady(t.Context(), 10, kubetest.WaitForNodesReadyOpts{})
		require.NoError(t, err, "Failed to create nodes and wait for ready")

		_, err = tCtx.QMgr.Create(t.Context(), &schedulingv1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: "tenant-a"},
			Spec: schedulingv1.QueueSpec{
				Quota: schedulingv1.QuotaSpec{
					Max: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("3"),
						corev1.ResourceMemory: resource.MustParse("5Gi"),
					},
				},
				Preemption: schedulingv1.PreemptionSpec{
					Preempts:    schedulingv1.PreemptsEverything(),
					PreemptedBy: schedulingv1.PreemptedByEverything(),
				},
			},
		})
		require.NoError(t, err, "Failed to create queues")

		victim := newPodForQueue(tCtx, "tenant-a", "low", corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("3"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		})
		_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Create(t.Context(),
			victim, metav1.CreateOptions{})
		require.NoError(t, err)

		// Schedule victim pod
		tCtx.Scheduler.ScheduleOne(t.Context())
		kassert.Eventually(t, func(c *assert.CollectT) {
			gotVictim, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				victim.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get victim pod")
			kassert.PodRunning(c, gotVictim)
		})

		gotVictim, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
			victim.Name, metav1.GetOptions{})
		require.NoError(t, err, "Failed to get victim pod")

		preemptor := newPodForQueue(tCtx, "tenant-a", "high", corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		})
		_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Create(t.Context(),
			preemptor, metav1.CreateOptions{})
		require.NoError(t, err)

		// Perform preemption, resulting in nominated node for preemptor pod
		tCtx.Scheduler.ScheduleOne(t.Context())
		kassert.Eventually(t, func(c *assert.CollectT) {
			gotPreemptor, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				preemptor.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get preemptor pod")

			// Preemptor pod nominated
			kassert.PodNominatedForNode(c, gotPreemptor, gotVictim.Spec.NodeName)

			// Victim pod deleted
			_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				victim.Name, metav1.GetOptions{})
			kassert.IsErrNotFound(c, err, "Victim pod should be deleted")
		})

		// Perform scheduling for nominated node
		tCtx.Scheduler.ScheduleOne(t.Context())
		kassert.Eventually(t, func(c *assert.CollectT) {
			gotPreemptor, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				preemptor.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get preemptor pod")
			kassert.PodRunningOnNode(c, gotPreemptor, gotVictim.Spec.NodeName)
		})

		// Create victim again
		_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Create(t.Context(),
			victim, metav1.CreateOptions{})
		require.NoError(t, err)

		// It should be unschedulable
		tCtx.Scheduler.ScheduleOne(t.Context())
		kassert.Eventually(t, func(c *assert.CollectT) {
			gotVictim, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				victim.Name, metav1.GetOptions{})
			require.NoError(t, err, "Failed to get victim pod")
			kassert.PodUnschedulable(c, gotVictim)
		})
	})

	t.Run("Preempt multiple in same queue for quota", func(t *testing.T) {
		tCtx, err := kubetest.NewSchedulerContext(t.Context(), testEnv.Config)
		require.NoError(t, err, "Failed to create test context")
		t.Cleanup(func() { tCtx.CleanUp(parentT.Context()) })

		_, err = tCtx.PCMgr.Create(t.Context(),
			kubetest.NewPC("high", 1000, true),
			kubetest.NewPC("low", -1000, true))
		require.NoError(t, err, "Failed to create priority classes")

		_, err = tCtx.NodeMgr.CreateAndWaitForReady(t.Context(), 1, kubetest.WaitForNodesReadyOpts{})
		require.NoError(t, err, "Failed to create nodes and wait for ready")

		_, err = tCtx.QMgr.Create(t.Context(), &schedulingv1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: "tenant-a"},
			Spec: schedulingv1.QueueSpec{
				Quota: schedulingv1.QuotaSpec{
					Max: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("4"),
						corev1.ResourceMemory: resource.MustParse("5Gi"),
					},
				},
				Preemption: schedulingv1.PreemptionSpec{
					Preempts:    schedulingv1.PreemptsEverything(),
					PreemptedBy: schedulingv1.PreemptedByEverything(),
				},
			},
		})
		require.NoError(t, err, "Failed to create queues")

		// Combined the three victims take up entire quota.
		victims := [3]*corev1.Pod{
			newPodForQueue(tCtx, "tenant-a", "low", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			}),
			newPodForQueue(tCtx, "tenant-a", "low", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			}),
			newPodForQueue(tCtx, "tenant-a", "low", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			}),
		}
		for _, victim := range victims {
			_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Create(t.Context(),
				victim, metav1.CreateOptions{})
			require.NoError(t, err, "Failed to create pod")
		}

		// Schedule victim pods
		tCtx.Scheduler.ScheduleOne(t.Context())
		tCtx.Scheduler.ScheduleOne(t.Context())
		tCtx.Scheduler.ScheduleOne(t.Context())
		kassert.Eventually(t, func(c *assert.CollectT) {
			gotVictims, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).List(t.Context(),
				metav1.ListOptions{})
			require.NoError(c, err, "Failed to list pods")
			assert.Lenf(t, gotVictims.Items, len(victims), "Expected %d pods", len(victims))
			for _, gotVictim := range gotVictims.Items {
				require.NoError(c, err, "Failed to get pod")
				kassert.PodRunning(c, &gotVictim)
			}
		})

		gotVictims, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).List(t.Context(),
			metav1.ListOptions{})
		require.NoError(t, err, "Failed to list pods")
		assert.Lenf(t, gotVictims.Items, len(victims), "Expected %d pods", len(victims))

		preemptor := newPodForQueue(tCtx, "tenant-a", "high", corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("3"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		})
		preemptor, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Create(t.Context(),
			preemptor, metav1.CreateOptions{})
		require.NoError(t, err)

		// Perform preemption, resulting in nominated node for preemptor pod
		tCtx.Scheduler.ScheduleOne(t.Context())
		kassert.Eventually(t, func(c *assert.CollectT) {
			gotPreemptor, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				preemptor.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get preemptor pod")

			// Preemptor pod nominated for one of the victims' nodes.
			// Assumption is that all victims are on same node.
			// This is currently a limitation of the preemption algorithm.
			kassert.PodNominatedForNode(c, gotPreemptor, gotVictims.Items[0].Spec.NodeName)

			// Exactly two victim pods had to be chosen to make room for preemptor.
			// It's not important which two, choice is arbitrary.
			// That leaves 1 preemptor pod and 1 victim pod.
			remainingPods, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).List(t.Context(),
				metav1.ListOptions{})
			require.NoError(t, err, "Failed to list pods")
			assert.Len(t, remainingPods.Items, 2)
			kassert.PodInPodListByUID(t, gotPreemptor, remainingPods)
		})

		// Perform scheduling for nominated node
		tCtx.Scheduler.ScheduleOne(t.Context())
		kassert.Eventually(t, func(c *assert.CollectT) {
			gotPreemptor, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				preemptor.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get preemptor pod")
			kassert.PodRunningOnNode(c, gotPreemptor, gotVictims.Items[0].Spec.NodeName)
		})

		// Ensure two remaining pods, one preemptor and one victim.
		// Both now running.
		remainingPods, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).List(t.Context(),
			metav1.ListOptions{})
		require.NoError(t, err, "Failed to list pods")
		assert.Len(t, remainingPods.Items, 2)
		kassert.PodInPodListByUID(t, preemptor, remainingPods)
		kassert.PodListAllRunning(t, remainingPods)
	})
}

func newPodForQueue(
	tCtx *kubetest.SchedulerContext,
	queue string,
	priorityClassName string,
	requests corev1.ResourceList,
) *corev1.Pod {
	pod := newPod(tCtx, requests)
	pod.Labels[schedulingapi.LabelKeyQueue] = queue
	pod.Spec.PriorityClassName = priorityClassName
	return pod
}

func newPod(tCtx *kubetest.SchedulerContext, requests corev1.ResourceList) *corev1.Pod {
	if requests == nil {
		requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		}
	}

	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(uuid.NewUUID()),
			Namespace: tCtx.Namespace,
			Labels:    make(map[string]string),
		},
		Spec: corev1.PodSpec{
			SchedulerName: kubetest.SchedulerName,
			Containers: []corev1.Container{
				{
					Name:      "test-container",
					Image:     "fake-image",
					Resources: corev1.ResourceRequirements{Requests: requests},
				},
			},
		},
	}
}
