package kubetest

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	schedulingclient "github.com/kaschnit/kaschnit-scheduler/client/clientset/scheduling"
	"github.com/kaschnit/kaschnit-scheduler/internal/kubesched"
	"github.com/kaschnit/kaschnit-scheduler/internal/plugin/quotaawarepreempt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	nodefast "sigs.k8s.io/kwok/kustomize/stage/node/fast"
	podfast "sigs.k8s.io/kwok/kustomize/stage/pod/fast"
	kwokinternal "sigs.k8s.io/kwok/pkg/apis/internalversion"
	kwokclient "sigs.k8s.io/kwok/pkg/client/clientset/versioned"
	kwokcfg "sigs.k8s.io/kwok/pkg/config"
	kwokctrl "sigs.k8s.io/kwok/pkg/kwok/controllers"
)

// StartEnvTest creates and envtest environment and starts it.
// This loads in all CRDs necessary for testing the scheduler.
func StartEnvTest() (*envtest.Environment, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return nil, errors.New("failed to locate root directory")
	}

	rootDir := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))

	testEnv := &envtest.Environment{
		CRDInstallOptions: envtest.CRDInstallOptions{
			Paths: []string{
				rootDir + "/charts/kaschnit-scheduler/templates/scheduling.kaschnit.github.io_queues.yaml",
			},
		},
	}

	_, err := testEnv.Start()
	if err != nil {
		return testEnv, err
	}

	return testEnv, nil
}

// SchedulerName is a constant scheduler name to use across tests for uniformity.
// This allows writing helper functions to build pods and the scheduler without wiring
// through the scheduler name to be used. The name itself does not really matter, it's
// purpose is just to match up a pod to a scheduling profile.
const SchedulerName = "kaschnit-scheduler"

// SchedulerContext contains all clients, configs, etc. needed for testing the scheduler.
type SchedulerContext struct {
	Namespace          string
	Clock              clock.Clock
	K8sCfg             *rest.Config
	K8sClient          *kubernetes.Clientset
	DynClient          *dynamic.DynamicClient
	SchedulingClient   *schedulingclient.Clientset
	KWOKClient         *kwokclient.Clientset
	InformerFactory    informers.SharedInformerFactory
	DynInformerFactory dynamicinformer.DynamicSharedInformerFactory
	RESTMapper         meta.RESTMapper
	Scheduler          *scheduler.Scheduler
	NodeMgr            *KWOKNodeManager
	PCMgr              *PriorityClassManager
	QMgr               *QueueManager
	KWOKController     *kwokctrl.Controller
}

// NewSchedulerContext builds an [SchedulerContext].
func NewSchedulerContext(ctx context.Context, k8sConfig *rest.Config) (*SchedulerContext, error) {
	clk := clock.RealClock{}

	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return nil, err
	}

	dynClient, err := dynamic.NewForConfig(k8sConfig)
	if err != nil {
		return nil, err
	}

	apiGoupResources, err := restmapper.GetAPIGroupResources(k8sClient.Discovery())
	if err != nil {
		return nil, err
	}

	restMapper := restmapper.NewDiscoveryRESTMapper(apiGoupResources)

	kwokClient, err := kwokclient.NewForConfig(k8sConfig)
	if err != nil {
		return nil, err
	}

	kwokCtrl, err := newKWOKContoller(ctx, k8sClient, kwokClient, restMapper, clk)
	if err != nil {
		return nil, err
	}

	schedulingClient, err := schedulingclient.NewForConfig(k8sConfig)
	if err != nil {
		return nil, err
	}

	infFactory := scheduler.NewInformerFactory(k8sClient, 0)
	infFactory.Start(ctx.Done())
	infFactory.WaitForCacheSync(ctx.Done())

	dynInfFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, 0)
	dynInfFactory.Start(ctx.Done())
	dynInfFactory.WaitForCacheSync(ctx.Done())

	kubeScheduler, err := newKubeScheduler(ctx, k8sConfig, k8sClient, infFactory, dynInfFactory)
	if err != nil {
		return nil, err
	}

	nsName := "test-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := k8sClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: nsName},
	}, metav1.CreateOptions{}); err != nil {
		return nil, err
	}

	return &SchedulerContext{
		Namespace:          nsName,
		Clock:              clk,
		K8sCfg:             k8sConfig,
		K8sClient:          k8sClient,
		DynClient:          dynClient,
		SchedulingClient:   schedulingClient,
		KWOKClient:         kwokClient,
		InformerFactory:    infFactory,
		DynInformerFactory: dynInfFactory,
		RESTMapper:         restMapper,
		Scheduler:          kubeScheduler,
		NodeMgr:            NewKWOKNodeManager(k8sClient.CoreV1().Nodes()),
		PCMgr:              NewPCManager(k8sClient.SchedulingV1().PriorityClasses()),
		QMgr:               NewQueueManager(schedulingClient.SchedulingV1().Queues()),
		KWOKController:     kwokCtrl,
	}, nil
}

// CleanUp deletes all relevant cluster-wide and namespaced resources.
// This can be used to simplify reuse of a single envtest environment across multiple tests.
func (tCtx *SchedulerContext) CleanUp(ctx context.Context) error {
	var errs error

	errs = errors.Join(errs,
		tCtx.K8sClient.CoreV1().Pods(tCtx.Namespace).DeleteCollection(ctx,
			metav1.DeleteOptions{
				GracePeriodSeconds: new(int64(0)),
				PropagationPolicy:  new(metav1.DeletePropagationBackground),
			}, metav1.ListOptions{}))
	errs = errors.Join(errs, tCtx.QMgr.DeleteAll(ctx))
	errs = errors.Join(errs, tCtx.PCMgr.DeleteAll(ctx))
	errs = errors.Join(errs, tCtx.NodeMgr.DeleteAllWait(ctx, WaitForNodesDeleteOpts{}))
	errs = errors.Join(errs,
		tCtx.K8sClient.CoreV1().Namespaces().Delete(ctx, tCtx.Namespace, metav1.DeleteOptions{
			GracePeriodSeconds: new(int64(0)),
			PropagationPolicy:  new(metav1.DeletePropagationBackground),
		}))

	if tCtx.InformerFactory != nil {
		tCtx.InformerFactory.Shutdown()
	}
	if tCtx.DynInformerFactory != nil {
		tCtx.DynInformerFactory.Shutdown()
	}

	return errs
}

func newKWOKContoller(
	ctx context.Context,
	k8sClient *kubernetes.Clientset,
	kwokClient *kwokclient.Clientset,
	restMapper meta.RESTMapper,
	clk clock.Clock,
) (*kwokctrl.Controller, error) {
	nodeInitStage, err := kwokcfg.UnmarshalWithType[*kwokinternal.Stage](nodefast.DefaultNodeInit)
	if err != nil {
		return nil, err
	}

	podReadyStage, err := kwokcfg.UnmarshalWithType[*kwokinternal.Stage](podfast.DefaultPodReady)
	if err != nil {
		return nil, err
	}

	podCompleteStage, err := kwokcfg.UnmarshalWithType[*kwokinternal.Stage](podfast.DefaultPodComplete)
	if err != nil {
		return nil, err
	}

	podDeleteStage, err := kwokcfg.UnmarshalWithType[*kwokinternal.Stage](podfast.DefaultPodDelete)
	if err != nil {
		return nil, err
	}

	kwokController, err := kwokctrl.NewController(kwokctrl.Config{
		TypedClient:                       k8sClient,
		TypedKwokClient:                   kwokClient,
		RESTClient:                        k8sClient.RESTClient(),
		RESTMapper:                        restMapper,
		ManageNodesWithAnnotationSelector: "kwok.x-k8s.io/node=fake",
		CIDR:                              "10.0.0.0/24",
		NodeLeaseDurationSeconds:          40,
		NodeIP:                            "10.0.0.1",
		PodPlayStageParallelism:           32,
		NodePlayStageParallelism:          32,
		NodeLeaseParallelism:              4,
		EnablePodCache:                    true,
		Clock:                             clk,
		LocalStages: map[kwokinternal.StageResourceRef][]*kwokinternal.Stage{
			{APIGroup: "v1", Kind: "Node"}: {nodeInitStage},
			{APIGroup: "v1", Kind: "Pod"}:  {podReadyStage, podCompleteStage, podDeleteStage},
		},
	})
	if err != nil {
		return nil, err
	}

	if err := kwokController.Start(ctx); err != nil {
		return nil, err
	}

	return kwokController, nil
}

func newKubeScheduler(
	ctx context.Context,
	k8sConfig *rest.Config,
	k8sClient *kubernetes.Clientset,
	infFactory informers.SharedInformerFactory,
	dynInfFactory dynamicinformer.DynamicSharedInformerFactory,
) (*scheduler.Scheduler, error) {
	kubeSchedulerConfig, err := newKubeSchedulerConfig()
	if err != nil {
		return nil, err
	}

	pluginRegistry, err := newPluginRegistry()
	if err != nil {
		return nil, err
	}

	// TODO: consider using app.Setup() instead of scheduler.New(). This unfortunately
	// requires CLI-like inputs (path to kubeconfig file) so it's tricky to do with envtest;
	// however it makes it more aligned with the scheduler cmd's main.go and automates handling
	// of default plugin registration.
	kubeScheduler, err := scheduler.New(
		ctx,
		k8sClient,
		infFactory,
		dynInfFactory,
		events.NewEventBroadcasterAdapterWithContext(ctx, k8sClient).NewRecorder,
		scheduler.WithComponentConfigVersion("kubescheduler.config.k8s.io/v1"),
		scheduler.WithKubeConfig(k8sConfig),
		scheduler.WithFrameworkOutOfTreeRegistry(pluginRegistry),
		scheduler.WithProfiles(kubeSchedulerConfig.Profiles...),
		scheduler.WithPercentageOfNodesToScore(kubeSchedulerConfig.PercentageOfNodesToScore),
		scheduler.WithPodMaxBackoffSeconds(kubeSchedulerConfig.PodMaxBackoffSeconds),
		scheduler.WithPodInitialBackoffSeconds(kubeSchedulerConfig.PodInitialBackoffSeconds),
		scheduler.WithPodMaxInUnschedulablePodsDuration(kubeschedq.DefaultPodMaxInUnschedulablePodsDuration),
		scheduler.WithParallelism(kubeSchedulerConfig.Parallelism),
	)
	if err != nil {
		return nil, err
	}

	return kubeScheduler, nil
}

func newKubeSchedulerConfig() (kubeschedcfgapi.KubeSchedulerConfiguration, error) {
	return kubesched.ToConfigAPIWithDefaults(kubeschedcfgv1.KubeSchedulerConfiguration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: kubeschedcfgv1.SchemeGroupVersion.String(),
			Kind:       "KubeSchedulerConfiguration",
		},
		Profiles: []kubeschedcfgv1.KubeSchedulerProfile{
			{
				SchedulerName: new(SchedulerName),
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
}

func newPluginRegistry() (fwkruntime.Registry, error) {
	pluginRegistry := make(fwkruntime.Registry)

	err := quotaawarepreempt.Register(pluginRegistry)
	if err != nil {
		return pluginRegistry, err
	}

	return pluginRegistry, nil
}
