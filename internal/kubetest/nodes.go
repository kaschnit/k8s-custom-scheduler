package kubetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/klog/v2"
)

func NewKWOKNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: map[string]string{"kwok.x-k8s.io/node": "fake"},
			Labels:      map[string]string{"type": "kwok"},
		},
		Spec: corev1.NodeSpec{
			ProviderID: "kwok://fake-node",
			Taints:     []corev1.Taint{},
		},
	}
}

type KWOKNodeManager struct {
	nodeClient corev1client.NodeInterface
}

func NewKWOKNodeManager(nodeClient corev1client.NodeInterface) *KWOKNodeManager {
	return &KWOKNodeManager{
		nodeClient: nodeClient,
	}
}

func (mgr *KWOKNodeManager) CreateNodes(ctx context.Context, prefix string, count int) ([]*corev1.Node, error) {
	var errs error
	nodes := make([]*corev1.Node, 0, count)

	for i := range count {
		nodeToCreate := NewKWOKNode(fmt.Sprintf("worker-%d", i))

		createdNode, err := mgr.nodeClient.Create(ctx, nodeToCreate, metav1.CreateOptions{})
		if err != nil {
			errs = errors.Join(errs, err)
		} else {
			nodes = append(nodes, createdNode)
		}
	}

	return nodes, errs
}

type WaitForNodesReadyOpts struct {
	PollInterval time.Duration
	Timeout      time.Duration
}

func (mgr *KWOKNodeManager) WaitForNodesReady(ctx context.Context, opts WaitForNodesReadyOpts) error {
	logger := klog.FromContext(ctx)

	if opts.PollInterval == 0 {
		opts.PollInterval = time.Second
	}
	if opts.Timeout == 0 {
		opts.Timeout = time.Minute
	}

	return wait.PollUntilContextTimeout(ctx, opts.PollInterval, opts.Timeout, true,
		func(ctx context.Context) (done bool, err error) {
			nodes, err := mgr.nodeClient.List(ctx, metav1.ListOptions{LabelSelector: "type=kwok"})
			if err != nil {
				return false, err
			}

			readyCount := 0
			for _, node := range nodes.Items {
				for _, cond := range node.Status.Conditions {
					if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
						updatedTaints := slices.DeleteFunc(slices.Clone(node.Spec.Taints),
							func(tnt corev1.Taint) bool {
								return tnt.Key == "node.kubernetes.io/not-ready"
							})

						// Remove not-ready taint if it was present.
						if len(updatedTaints) < len(node.Spec.Taints) {
							patch, err := json.Marshal(map[string]any{
								"spec": map[string]any{
									"taints": updatedTaints,
								},
							})
							if err != nil {
								return false, err
							}

							_, err = mgr.nodeClient.Patch(
								ctx,
								node.Name,
								types.MergePatchType,
								patch,
								metav1.PatchOptions{},
							)
							if err != nil {
								return false, err
							}
						}

						readyCount++
						break
					}
				}
			}

			logger.V(5).Info("Checked nodes ready",
				"ready", readyCount,
				"total", len(nodes.Items))

			return readyCount == len(nodes.Items), nil
		})
}

func (mgr *KWOKNodeManager) CreateNodesAndWaitForReady(
	ctx context.Context,
	prefix string,
	count int,
	opts WaitForNodesReadyOpts,
) error {

	if _, err := mgr.CreateNodes(ctx, prefix, count); err != nil {
		return err
	}

	if err := mgr.WaitForNodesReady(ctx, opts); err != nil {
		return err
	}

	return nil
}

func (mgr *KWOKNodeManager) DeleteAllNodes(ctx context.Context) error {
	return mgr.nodeClient.DeleteCollection(ctx, *metav1.NewDeleteOptions(0),
		metav1.ListOptions{LabelSelector: "type=kwok"})
}
