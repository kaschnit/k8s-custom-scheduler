package kubetest

import (
	"context"

	"github.com/kaschnit/kaschnit-scheduler/internal/pods"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

type PodManager struct {
	namespace    string
	corev1Client corev1client.CoreV1Interface
}

func NewPodManager(corev1Client corev1client.CoreV1Interface, namespace string) *PodManager {
	return &PodManager{
		namespace:    namespace,
		corev1Client: corev1Client,
	}
}

func (mgr *PodManager) Create(ctx context.Context, opts ...pods.Option) (*corev1.Pod, error) {
	opts = append(
		[]pods.Option{
			pods.WithNamespace(mgr.namespace),
			pods.WithSchedulerName(SchedulerName),
		},
		opts...,
	)

	return mgr.corev1Client.Pods(mgr.namespace).Create(ctx,
		pods.New(string(uuid.NewUUID()), opts...),
		metav1.CreateOptions{})
}

func (mgr *PodManager) Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Pod, error) {
	return mgr.corev1Client.Pods(mgr.namespace).Get(ctx, name, opts)
}
func (mgr *PodManager) List(ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
	return mgr.corev1Client.Pods(mgr.namespace).List(ctx, opts)
}

func (mgr *PodManager) DeleteAll(ctx context.Context) error {
	return mgr.corev1Client.Pods(mgr.namespace).DeleteCollection(ctx,
		metav1.DeleteOptions{
			GracePeriodSeconds: new(int64(0)),
			PropagationPolicy:  new(metav1.DeletePropagationBackground),
		}, metav1.ListOptions{})
}

func WithDummyContainer(requests corev1.ResourceList) pods.Option {
	if requests == nil {
		requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		}
	}

	return func(p *corev1.Pod) {
		p.Spec.Containers = []corev1.Container{{
			Name:      "test-container",
			Image:     "fake-image",
			Resources: corev1.ResourceRequirements{Requests: requests},
		}}
	}
}
