//go:build envtest

package quotaawarepreempt_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/kaschnit/kaschnit-scheduler/internal/plugin/quotaawarepreempt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	testdefaults "k8s.io/kubernetes/pkg/scheduler/apis/config/testing/defaults"
	schedulerq "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	nodefast "sigs.k8s.io/kwok/kustomize/stage/node/fast"
	kwokinternal "sigs.k8s.io/kwok/pkg/apis/internalversion"
	kwokclient "sigs.k8s.io/kwok/pkg/client/clientset/versioned"
	"sigs.k8s.io/kwok/pkg/config"
	kwokctrl "sigs.k8s.io/kwok/pkg/kwok/controllers"
)

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
	require.NoError(t, err, "Failed to unmarshal default node init")

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
			Spec: corev1.NodeSpec{ProviderID: "kwok://fake-node"},
		}

		_, err := k8sClient.CoreV1().Nodes().Create(t.Context(), node, metav1.CreateOptions{})
		require.NoError(t, err, "Failed to create worker node %d", i, numNodes)
	}

	t.Logf("Waiting for %d nodes to be ready", numNodes)
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
						break
					}
				}
			}

			t.Logf("Progress: %d/%d nodes are Ready", readyCount, numNodes)
			return readyCount == numNodes, nil
		})
	require.NoError(t, err, "Nodes never became ready")

	t.Log("Creating scheduler plugin registry")
	schedulerPluginRegistry := make(runtime.Registry)
	quotaawarepreempt.WithPlugin()(schedulerPluginRegistry)

	t.Log("Building scheduler profile")
	plugins := testdefaults.ExpandedPluginsV1.DeepCopy()
	plugins.MultiPoint = schedulerconfig.PluginSet{
		Enabled: []schedulerconfig.Plugin{
			{Name: quotaawarepreempt.PluginName},
		},
	}
	plugins.PostFilter = schedulerconfig.PluginSet{
		Enabled: []schedulerconfig.Plugin{
			{Name: quotaawarepreempt.PluginName},
		},
		Disabled: []schedulerconfig.Plugin{
			{Name: "*"},
		},
	}
	pluginConfig := slices.Clone(testdefaults.PluginConfigsV1)

	t.Log("Creating scheduler")
	// TODO: consider using app.Setup() instead of scheduler.New(). This unfortunately
	// requires CLI-like inputs (path to kubeconfig file) so it's tricky to do with envtest;
	// however it makes it more aligned with the scheduler cmd's main.go and automates handling
	// of default plugin registration.
	scheduler, err := scheduler.New(
		t.Context(),
		k8sClient,
		informers.NewSharedInformerFactory(k8sClient, 0),
		dynamicinformer.NewDynamicSharedInformerFactory(dynClient, 0),
		events.NewEventBroadcasterAdapter(k8sClient).NewRecorder,
		scheduler.WithComponentConfigVersion("kubescheduler.config.k8s.io/v1"),
		scheduler.WithKubeConfig(k8sConfig),
		scheduler.WithProfiles(schedulerconfig.KubeSchedulerProfile{
			SchedulerName: "kaschnit-scheduler",
			Plugins:       plugins,
			PluginConfig:  pluginConfig,
		}),
		scheduler.WithPercentageOfNodesToScore(new(int32(schedulerconfig.DefaultPercentageOfNodesToScore))),
		scheduler.WithFrameworkOutOfTreeRegistry(schedulerPluginRegistry),
		scheduler.WithPodMaxBackoffSeconds(int64(schedulerq.DefaultPodMaxBackoffDuration.Seconds())),
		scheduler.WithPodInitialBackoffSeconds(int64(schedulerq.DefaultPodInitialBackoffDuration.Seconds())),
		scheduler.WithPodMaxInUnschedulablePodsDuration(schedulerq.DefaultPodMaxBackoffDuration),
		scheduler.WithParallelism(int32(parallelize.DefaultParallelism)),
	)
	require.NoError(t, err, "Failed to create scheduler instance")

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	t.Cleanup(cancel)

	t.Log("Running scheduler")
	scheduler.Run(ctx)

	// TODO: finish this test.
	assert.True(t, false, "Intentional fail to produce logs")
}
