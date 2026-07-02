//go:build envtest

package quotaawarepreempt_test

import (
	"testing"
	"time"

	schedulingapi "github.com/kaschnit/kaschnit-scheduler/apis/scheduling"
	schedulingv1 "github.com/kaschnit/kaschnit-scheduler/apis/scheduling/v1"
	schedulingclient "github.com/kaschnit/kaschnit-scheduler/client/clientset/scheduling"
	"github.com/kaschnit/kaschnit-scheduler/internal/kubesched"
	"github.com/kaschnit/kaschnit-scheduler/internal/kubetest"
	"github.com/kaschnit/kaschnit-scheduler/internal/plugin/quotaawarepreempt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	kubeschedulingv1 "k8s.io/api/scheduling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/events"
	kubeschedcfgv1 "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/pkg/scheduler"
	kubeschedcfgapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	kubeschedq "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	nodefast "sigs.k8s.io/kwok/kustomize/stage/node/fast"
	podfast "sigs.k8s.io/kwok/kustomize/stage/pod/fast"
	kwokinternal "sigs.k8s.io/kwok/pkg/apis/internalversion"
	kwokclient "sigs.k8s.io/kwok/pkg/client/clientset/versioned"
	"sigs.k8s.io/kwok/pkg/config"
	kwokctrl "sigs.k8s.io/kwok/pkg/kwok/controllers"
)

const schedulerName = "kaschnit-scheduler"

func TestScheduler(t *testing.T) {
	const (
		numNodes = 10
	)

	clck := clock.RealClock{}

	t.Log("Setting up test env")
	testEnv := &envtest.Environment{
		CRDInstallOptions: envtest.CRDInstallOptions{
			Paths: []string{
				"../../../charts/kaschnit-scheduler/templates/scheduling.kaschnit.github.io_queues.yaml",
			},
		},
	}

	k8sConfig, err := testEnv.Start()
	require.NoError(t, err, "Failed to start envtest environment")

	t.Cleanup(func() { testEnv.Stop() })

	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	require.NoError(t, err, "Failed to create Kubernetes client")

	dynClient, err := dynamic.NewForConfig(k8sConfig)
	require.NoError(t, err, "Failed to create dynamic Kubernetes client")

	schedulingClient, err := schedulingclient.NewForConfig(k8sConfig)
	require.NoError(t, err, "Failed to create scheduling client")

	kwokClient, err := kwokclient.NewForConfig(k8sConfig)
	require.NoError(t, err, "Failed to create KWOK clientset")

	require.NoError(t, err, "Failed to apply KWOK stage")

	groupResources, err := restmapper.GetAPIGroupResources(k8sClient.Discovery())
	require.NoError(t, err, "Failed to get API group resources")

	nodeInitStage, err := config.UnmarshalWithType[*kwokinternal.Stage](nodefast.DefaultNodeInit)
	require.NoError(t, err, "Failed to unmarshal default node init stage")

	podInitStage, err := config.UnmarshalWithType[*kwokinternal.Stage](podfast.DefaultPodReady)
	require.NoError(t, err, "Failed to unmarshal default pod ready stage")

	podDeleteStage, err := config.UnmarshalWithType[*kwokinternal.Stage](podfast.DefaultPodDelete)
	require.NoError(t, err, "Failed to unmarshal default pod delete stage")

	kwokCtrl, err := kwokctrl.NewController(kwokctrl.Config{
		TypedClient:                       k8sClient,
		TypedKwokClient:                   kwokClient,
		RESTClient:                        k8sClient.RESTClient(),
		RESTMapper:                        restmapper.NewDiscoveryRESTMapper(groupResources),
		ManageNodesWithAnnotationSelector: "kwok.x-k8s.io/node=fake",
		CIDR:                              "10.0.0.0/24",
		NodeLeaseDurationSeconds:          40,
		NodeIP:                            "10.0.0.1",
		PodPlayStageParallelism:           32,
		NodePlayStageParallelism:          32,
		NodeLeaseParallelism:              4,
		EnablePodCache:                    true,
		Clock:                             clck,
		LocalStages: map[kwokinternal.StageResourceRef][]*kwokinternal.Stage{
			{APIGroup: "v1", Kind: "Node"}: {nodeInitStage},
			{APIGroup: "v1", Kind: "Pod"}:  {podInitStage, podDeleteStage},
		},
	})
	require.NoError(t, err, "Failed to create KWOK controller")

	err = kwokCtrl.Start(t.Context())
	require.NoError(t, err, "Failed to start KWOK controller")

	nodeMgr := kubetest.NewKWOKNodeManager(k8sClient.CoreV1().Nodes())

	registerResourceCleanup := func(t *testing.T) {
		t.Helper()
		t.Cleanup(func() { nodeMgr.DeleteAllNodes(t.Context()) })
		t.Cleanup(func() {
			k8sClient.CoreV1().Pods(corev1.NamespaceAll).DeleteCollection(t.Context(),
				*metav1.NewDeleteOptions(0), metav1.ListOptions{})
		})
	}

	priorityClasses := []*kubeschedulingv1.PriorityClass{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "high",
			},
			Value:            1000,
			PreemptionPolicy: new(corev1.PreemptLowerPriority),
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "low",
			},
			Value:            -1000,
			PreemptionPolicy: new(corev1.PreemptLowerPriority),
		},
	}
	for _, pc := range priorityClasses {
		_, err := k8sClient.SchedulingV1().PriorityClasses().Create(t.Context(), pc, metav1.CreateOptions{})
		require.NoError(t, err, "Failed to create priority class")
	}

	t.Run("Basic quota preemption", func(t *testing.T) {
		registerResourceCleanup(t)

		err = nodeMgr.CreateNodesAndWaitForReady(t.Context(), "worker", numNodes,
			kubetest.WaitForNodesReadyOpts{
				PollInterval: time.Second,
				Timeout:      10 * time.Second,
			})
		require.NoError(t, err, "Failed to create nodes and wait for ready")

		kubeScheduler := newKubeScheduler(t, k8sConfig, k8sClient, dynClient)

		_, err := schedulingClient.SchedulingV1().Queues().Create(t.Context(), &schedulingv1.Queue{
			TypeMeta: metav1.TypeMeta{
				APIVersion: schedulingv1.SchemeGroupVersion.String(),
				Kind:       "Queue",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "tenant-a",
			},
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
		}, metav1.CreateOptions{})
		require.NoError(t, err, "Failed to create queue")

		victim := newPodForQueue(types.NamespacedName{Name: "victim-1"}, "tenant-a", "low",
			corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("3"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			})
		_, err = k8sClient.CoreV1().Pods(corev1.NamespaceDefault).Create(t.Context(),
			victim, metav1.CreateOptions{})
		require.NoError(t, err)

		// Schedule victim pod
		kubeScheduler.ScheduleOne(t.Context())
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			victim, err := k8sClient.CoreV1().Pods(corev1.NamespaceDefault).Get(t.Context(),
				victim.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get victim pod")
			assert.Equal(c, corev1.PodRunning, victim.Status.Phase, "Victim should be running")
			assert.NotEmpty(c, victim.Spec.NodeName, "Victim should have nodeName")
		}, 3*time.Second, 1*time.Second)

		victim, err = k8sClient.CoreV1().Pods(corev1.NamespaceDefault).Get(t.Context(),
			victim.Name, metav1.GetOptions{})
		require.NoError(t, err, "Failed to get victim pod")

		preemptor := newPodForQueue(types.NamespacedName{Name: "preemptor-1"}, "tenant-a", "high",
			corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			})
		_, err = k8sClient.CoreV1().Pods(corev1.NamespaceDefault).Create(t.Context(),
			preemptor, metav1.CreateOptions{})
		require.NoError(t, err)

		// Perform preemption, resulting in nominated node for preemptor pod
		kubeScheduler.ScheduleOne(t.Context())
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			preemptor, err := k8sClient.CoreV1().Pods(corev1.NamespaceDefault).Get(t.Context(),
				preemptor.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get preemptor pod")
			assert.NotEmpty(c, preemptor.Status.NominatedNodeName, "Preemptor should have nominatedNode")
			assert.Equal(c, victim.Spec.NodeName, preemptor.Status.NominatedNodeName,
				"Preemptor's nominated node should be the victim's node")

			_, err = k8sClient.CoreV1().Pods(corev1.NamespaceDefault).Get(t.Context(),
				victim.Name, metav1.GetOptions{})
			assert.True(c, apierrors.IsNotFound(err), "Victim pod should be deleted")
		}, 3*time.Second, 1*time.Second)

		// Perform scheduling for nominated node
		kubeScheduler.ScheduleOne(t.Context())
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			preemptor, err := k8sClient.CoreV1().Pods(corev1.NamespaceDefault).Get(t.Context(),
				preemptor.Name, metav1.GetOptions{})
			require.NoError(c, err, "Failed to get preemptor pod")
			assert.Equal(c, corev1.PodRunning, preemptor.Status.Phase, "Preemptor should be running after scheduling")
			assert.NotEmpty(c, preemptor.Spec.NodeName, "Preemptor should have spec.nodeName after scheduling")
			assert.Equal(c, victim.Spec.NodeName, preemptor.Spec.NodeName,
				"Preemptor's node should be the victim's node")
		}, 3*time.Second, 1*time.Second)
	})
}

func newKubeScheduler(
	t *testing.T,
	k8sConfig *rest.Config,
	k8sClient kubernetes.Interface,
	dynClient dynamic.Interface,
) *scheduler.Scheduler {
	kubeSchedulerConfig := newKubeSchedulerConfig(t)

	t.Log("Creating scheduler informer factories")
	sharedInformerFactory := informers.NewSharedInformerFactory(k8sClient, 0)
	sharedInformerFactory.Start(t.Context().Done())
	sharedInformerFactory.WaitForCacheSync(t.Context().Done())

	dynInformerFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, 0)
	dynInformerFactory.Start(t.Context().Done())
	dynInformerFactory.WaitForCacheSync(t.Context().Done())

	t.Log("Creating scheduler")
	// TODO: consider using app.Setup() instead of scheduler.New(). This unfortunately
	// requires CLI-like inputs (path to kubeconfig file) so it's tricky to do with envtest;
	// however it makes it more aligned with the scheduler cmd's main.go and automates handling
	// of default plugin registration.
	kubeScheduler, err := scheduler.New(
		t.Context(),
		k8sClient,
		sharedInformerFactory,
		dynInformerFactory,
		events.NewEventBroadcasterAdapter(k8sClient).NewRecorder,
		scheduler.WithComponentConfigVersion("kubescheduler.config.k8s.io/v1"),
		scheduler.WithKubeConfig(k8sConfig),
		scheduler.WithFrameworkOutOfTreeRegistry(newPluginRegistry(t)),
		scheduler.WithProfiles(kubeSchedulerConfig.Profiles...),
		scheduler.WithPercentageOfNodesToScore(kubeSchedulerConfig.PercentageOfNodesToScore),
		scheduler.WithPodMaxBackoffSeconds(kubeSchedulerConfig.PodMaxBackoffSeconds),
		scheduler.WithPodInitialBackoffSeconds(kubeSchedulerConfig.PodInitialBackoffSeconds),
		scheduler.WithPodMaxInUnschedulablePodsDuration(kubeschedq.DefaultPodMaxInUnschedulablePodsDuration),
		scheduler.WithParallelism(kubeSchedulerConfig.Parallelism),
	)
	require.NoError(t, err, "Failed to create scheduler instance")

	return kubeScheduler
}

func newKubeSchedulerConfig(t *testing.T) kubeschedcfgapi.KubeSchedulerConfiguration {
	t.Log("Building scheduler profile")
	kubeSchedulerConfig, err := kubesched.ToConfigAPIWithDefaults(kubeschedcfgv1.KubeSchedulerConfiguration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: kubeschedcfgv1.SchemeGroupVersion.String(),
			Kind:       "KubeSchedulerConfiguration",
		},
		Profiles: []kubeschedcfgv1.KubeSchedulerProfile{
			{
				SchedulerName: new(schedulerName),
				Plugins: &kubeschedcfgv1.Plugins{
					MultiPoint: kubeschedcfgv1.PluginSet{
						Enabled: []kubeschedcfgv1.Plugin{
							{Name: quotaawarepreempt.PluginName},
						},
					},
					PostFilter: kubeschedcfgv1.PluginSet{
						Enabled: []kubeschedcfgv1.Plugin{
							{Name: quotaawarepreempt.PluginName},
						},
						Disabled: []kubeschedcfgv1.Plugin{
							{Name: "*"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to create KubeSchedulerConfiguration")

	return kubeSchedulerConfig
}

func newPluginRegistry(t *testing.T) runtime.Registry {
	t.Log("Building plugin registry")
	pluginRegistry := make(runtime.Registry)

	err := quotaawarepreempt.Register(pluginRegistry)
	require.NoError(t, err, "Failed to registery scheduler plugin")

	return pluginRegistry
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
			SchedulerName: schedulerName,
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
