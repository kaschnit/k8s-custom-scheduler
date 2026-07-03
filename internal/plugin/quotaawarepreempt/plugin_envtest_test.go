//go:build envtest

package quotaawarepreempt_test

import (
	"testing"
	"time"

	schedulingapi "github.com/kaschnit/kaschnit-scheduler/apis/scheduling"
	schedulingv1 "github.com/kaschnit/kaschnit-scheduler/apis/scheduling/v1"
	"github.com/kaschnit/kaschnit-scheduler/internal/kubetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestPlugin(t *testing.T) {
	testEnv, err := kubetest.StartEnvTest()
	require.NoError(t, err, "Failed to start envtest")
	t.Cleanup(func() { testEnv.Stop() })

	tCtx, err := kubetest.NewEnvTestContext(t.Context(), testEnv.Config)
	require.NoError(t, err, "Failed to create test context")
	t.Cleanup(func() { tCtx.CleanUp(t.Context()) })

	parentT := t

	t.Run("Basic quota preemption", func(t *testing.T) {
		tCtx, err := kubetest.NewEnvTestContext(t.Context(), testEnv.Config)
		require.NoError(t, err, "Failed to create test context")
		t.Cleanup(func() { tCtx.CleanUp(parentT.Context()) })

		_, err = tCtx.PCMgr.Create(t.Context(),
			kubetest.NewPC("high", 1000, true),
			kubetest.NewPC("low", -1000, true))
		require.NoError(t, err, "Failed to create priority classes")

		_, err = tCtx.NodeMgr.CreateAndWaitForReady(t.Context(), "worker", 10,
			kubetest.WaitForNodesReadyOpts{
				PollInterval: time.Second,
				Timeout:      10 * time.Second,
			})
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
					Preempts: schedulingv1.PreemptsRule{
						FromPods: &metav1.LabelSelector{},
						ToQueues: &metav1.LabelSelector{},
						ToPods:   &metav1.LabelSelector{},
					},
					PreemptedBy: schedulingv1.PreemptedByRule{
						FromQueues: &metav1.LabelSelector{},
						FromPods:   &metav1.LabelSelector{},
						ToPods:     &metav1.LabelSelector{},
					},
				},
			},
		})
		require.NoError(t, err, "Failed to create queues")

		victim := newPodForQueue(types.NamespacedName{Name: "victim-1"}, "tenant-a", "low",
			corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			})
		_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Create(t.Context(),
			victim, metav1.CreateOptions{})
		require.NoError(t, err)

		// Schedule victim pod
		tCtx.Scheduler.ScheduleOne(t.Context())
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			victim, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				victim.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get victim pod")
			assert.Equal(c, corev1.PodRunning, victim.Status.Phase, "Victim should be running")
			assert.NotEmpty(c, victim.Spec.NodeName, "Victim should have nodeName")
		}, 3*time.Second, 1*time.Second)

		victim, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
			victim.Name, metav1.GetOptions{})
		require.NoError(t, err, "Failed to get victim pod")

		preemptor := newPodForQueue(types.NamespacedName{Name: "preemptor-1"}, "tenant-a", "high",
			corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			})
		_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Create(t.Context(),
			preemptor, metav1.CreateOptions{})
		require.NoError(t, err)

		// Perform preemption, resulting in nominated node for preemptor pod
		tCtx.Scheduler.ScheduleOne(t.Context())
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			preemptor, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				preemptor.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get preemptor pod")
			assert.NotEmpty(c, preemptor.Status.NominatedNodeName, "Preemptor should have nominatedNode")
			assert.Equal(c, victim.Spec.NodeName, preemptor.Status.NominatedNodeName,
				"Preemptor's nominated node should be the victim's node")

			_, err = tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				victim.Name, metav1.GetOptions{})
			assert.True(c, apierrors.IsNotFound(err), "Victim pod should be deleted")
		}, 3*time.Second, 1*time.Second)

		// Perform scheduling for nominated node
		tCtx.Scheduler.ScheduleOne(t.Context())
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			preemptor, err := tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).Get(t.Context(),
				preemptor.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get preemptor pod")
			assert.Equal(c, corev1.PodRunning, preemptor.Status.Phase, "Preemptor should be running after scheduling")
			assert.NotEmpty(c, preemptor.Spec.NodeName, "Preemptor should have spec.nodeName after scheduling")
			assert.Equal(c, victim.Spec.NodeName, preemptor.Spec.NodeName,
				"Preemptor's node should be the victim's node")
		}, 3*time.Second, 1*time.Second)
	})
}

func newPodForQueue(
	name types.NamespacedName,
	queue string,
	priorityClassName string,
	requests corev1.ResourceList,
) *corev1.Pod {
	pod := newPod(name, requests)
	pod.Labels[schedulingapi.LabelKeyQueue] = queue
	pod.Spec.PriorityClassName = priorityClassName
	return pod
}

func newPod(name types.NamespacedName, requests corev1.ResourceList) *corev1.Pod {
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
			Name:      name.Name,
			Namespace: name.Namespace,
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
