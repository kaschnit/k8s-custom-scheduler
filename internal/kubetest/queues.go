package kubetest

import (
	"context"

	schedulingv1 "github.com/kaschnit/kaschnit-scheduler/apis/scheduling/v1"
	schedulingv1client "github.com/kaschnit/kaschnit-scheduler/client/clientset/scheduling/typed/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type QueueManager struct {
	queueClient schedulingv1client.QueueInterface
}

func NewQueueManager(queueClient schedulingv1client.QueueInterface) *QueueManager {
	return &QueueManager{
		queueClient: queueClient,
	}
}

func (mgr *QueueManager) Create(
	ctx context.Context,
	queues ...*schedulingv1.Queue,
) ([]*schedulingv1.Queue, error) {
	createdQs := make([]*schedulingv1.Queue, 0, len(queues))
	for _, pc := range queues {
		createdQ, err := mgr.queueClient.Create(ctx, pc, metav1.CreateOptions{})
		if err != nil {
			return createdQs, err
		}

		createdQs = append(createdQs, createdQ)
	}

	return createdQs, nil
}

func (mgr *QueueManager) DeleteAll(ctx context.Context) error {
	return mgr.queueClient.DeleteCollection(ctx,
		metav1.DeleteOptions{
			GracePeriodSeconds: new(int64(0)),
			PropagationPolicy:  new(metav1.DeletePropagationBackground),
		},
		metav1.ListOptions{})
}
