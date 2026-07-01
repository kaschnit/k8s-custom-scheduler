//go:build envtest

package quotaawarepreempt_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/kaschnit/kaschnit-scheduler/internal/kubesched"
	"github.com/kaschnit/kaschnit-scheduler/internal/plugin/quotaawarepreempt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/events"
	schedulerconfigv1 "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerconfigapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	schedulerq "k8s.io/kubernetes/pkg/scheduler/backend/queue"
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

	kwokClient, err := kwokclient.NewForConfig(k8sConfig)
	require.NoError(t, err, "Failed to create KWOK clientset")

	require.NoError(t, err, "Failed to apply KWOK stage")

	groupResources, err := restmapper.GetAPIGroupResources(k8sClient.Discovery())
	require.NoError(t, err, "Failed to get API group resources")

	nodeInitStage, err := config.UnmarshalWithType[*kwokinternal.Stage](nodefast.DefaultNodeInit)
	require.NoError(t, err, "Failed to unmarshal default node init stage")

	podInitStage, err := config.UnmarshalWithType[*kwokinternal.Stage](podfast.DefaultPodReady)
	require.NoError(t, err, "Failed to unmarshal default pod read stage")

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
			{APIGroup: "v1", Kind: "Pod"}:  {podInitStage},
		},
	})
	require.NoError(t, err, "Failed to create KWOK controller")

	err = kwokCtrl.Start(t.Context())
	require.NoError(t, err, "Failed to start KWOK controller")

	t.Logf("Creating %d nodes", numNodes)
	for i := range numNodes {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:        fmt.Sprintf("worker-%d", i),
				Annotations: map[string]string{"kwok.x-k8s.io/node": "fake"},
				Labels:      map[string]string{"type": "kwok"},
			},
			Spec: corev1.NodeSpec{
				ProviderID: "kwok://fake-node",
				Taints:     []corev1.Taint{},
			},
		}

		_, err := k8sClient.CoreV1().Nodes().Create(t.Context(), node, metav1.CreateOptions{})
		require.NoError(t, err, "Failed to create worker node %d", i, numNodes)
	}

	t.Logf("Waiting for %d nodes to be ready", numNodes)
	// TODO switch to eventual assertion.
	err = wait.PollUntilContextTimeout(t.Context(), 1*time.Second, 10*time.Second, true,
		func(ctx context.Context) (done bool, err error) {
			nodes, err := k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
			if err != nil {
				t.Logf("Failed to list nodes: %s", err)
				return false, err
			}

			if len(nodes.Items) < numNodes {
				return false, nil
			}

			readyCount := 0
			for _, node := range nodes.Items {
				for _, cond := range node.Status.Conditions {
					if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
						readyCount++

						if len(node.Spec.Taints) > 0 {
							updatedTaints := slices.DeleteFunc(slices.Clone(node.Spec.Taints),
								func(tnt corev1.Taint) bool { return tnt.Key == "node.kubernetes.io/not-ready" })
							if len(updatedTaints) < len(node.Spec.Taints) {
								patch, err := json.Marshal(map[string]any{
									"spec": map[string]any{
										"taints": updatedTaints,
									},
								})
								require.NoError(t, err, "Failed to marshal node taint patch")

								_, err = k8sClient.CoreV1().Nodes().Patch(
									t.Context(),
									node.Name,
									types.MergePatchType,
									patch,
									metav1.PatchOptions{},
								)
								if err != nil {
									t.Logf("Failed to remove taint from node: %s", err)
									return false, err
								}
							}
						}

						break
					}
				}
			}

			t.Logf("Progress: %d/%d nodes are Ready", readyCount, numNodes)
			return readyCount == numNodes, nil
		})
	require.NoError(t, err, "Nodes never became ready")

	t.Run("schedule one pod", func(t *testing.T) {
		kubeScheduler := newKubeScheduler(t, k8sConfig, k8sClient, dynClient)

		pod := &corev1.Pod{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "Pod",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-1",
			},
			Spec: corev1.PodSpec{
				SchedulerName: schedulerName,
				Containers: []corev1.Container{
					{
						Name:            "test-container",
						Image:           "fake-image",
						ImagePullPolicy: corev1.PullIfNotPresent,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("2"),
								corev1.ResourceMemory: resource.MustParse("1Gi"),
							},
						},
					},
				},
			},
		}

		_, err = k8sClient.CoreV1().Pods(corev1.NamespaceDefault).Create(t.Context(), pod, metav1.CreateOptions{})
		require.NoError(t, err, "Failed to create pod")

		podList, err := k8sClient.CoreV1().Pods(corev1.NamespaceDefault).List(t.Context(), metav1.ListOptions{})
		require.NoError(t, err, "Failed to list pods before scheduling")
		assert.Len(t, podList.Items, 1, "Pod list should have 1 item")
		assert.Empty(t, podList.Items[0].Spec.NodeName, "Pod should not have spec.nodeName before scheduling")
		assert.Equal(t, corev1.PodPending, podList.Items[0].Status.Phase, "Pod should be pending before scheduling")

		kubeScheduler.ScheduleOne(t.Context())

		time.Sleep(3 * time.Second) // TODO use eventual assertion

		podList, err = k8sClient.CoreV1().Pods(corev1.NamespaceDefault).List(t.Context(), metav1.ListOptions{})
		require.NoError(t, err, "Failed to list pods after scheduling")
		assert.Len(t, podList.Items, 1, "Pod list should have 1 item")
		assert.NotEmpty(t, podList.Items[0].Spec.NodeName, "Pod should have spec.nodeName after scheduling")
		assert.NotEqual(t, corev1.PodPending, podList.Items[0].Status.Phase, "Pod should not be pending before scheduling")
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
		scheduler.WithPodMaxInUnschedulablePodsDuration(schedulerq.DefaultPodMaxInUnschedulablePodsDuration),
		scheduler.WithParallelism(kubeSchedulerConfig.Parallelism),
	)
	require.NoError(t, err, "Failed to create scheduler instance")

	return kubeScheduler
}

func newKubeSchedulerConfig(t *testing.T) schedulerconfigapi.KubeSchedulerConfiguration {
	t.Log("Building scheduler profile")
	kubeSchedulerConfig, err := kubesched.ToConfigAPIWithDefaults(schedulerconfigv1.KubeSchedulerConfiguration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: schedulerconfigv1.SchemeGroupVersion.String(),
			Kind:       "KubeSchedulerConfiguration",
		},
		Profiles: []schedulerconfigv1.KubeSchedulerProfile{
			{
				SchedulerName: new(schedulerName),
				Plugins: &schedulerconfigv1.Plugins{
					MultiPoint: schedulerconfigv1.PluginSet{
						Enabled: []schedulerconfigv1.Plugin{
							{Name: quotaawarepreempt.PluginName},
						},
					},
					PostFilter: schedulerconfigv1.PluginSet{
						Enabled: []schedulerconfigv1.Plugin{
							{Name: quotaawarepreempt.PluginName},
						},
						Disabled: []schedulerconfigv1.Plugin{
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
