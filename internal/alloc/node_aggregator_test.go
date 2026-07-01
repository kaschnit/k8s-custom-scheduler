//go:build unit

package alloc_test

import (
	"testing"

	"github.com/kaschnit/kaschnit-scheduler/internal/alloc"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestCounter(t *testing.T) {
	node1 := newNode("node1", corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewQuantity(4, resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(16384, resource.BinarySI),
	})
	node2 := newNode("node2", corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewQuantity(8, resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(32768, resource.BinarySI),
		"nvidia.com/gpu":      *resource.NewQuantity(2, resource.DecimalSI),
	})

	t.Run("construct with nodes", func(t *testing.T) {
		counter1 := alloc.NewNodeAllocatableAggregator(node1)
		assert.Equal(t, framework.Resource{
			MilliCPU: 4000,
			Memory:   16384,
		}, *counter1.GetTotal().ToFrameworkResource())

		counter2 := alloc.NewNodeAllocatableAggregator(node2)
		assert.Equal(t, framework.Resource{
			MilliCPU: 8000,
			Memory:   32768,
			ScalarResources: map[corev1.ResourceName]int64{
				"nvidia.com/gpu": 2,
			},
		}, *counter2.GetTotal().ToFrameworkResource())

		counter3 := alloc.NewNodeAllocatableAggregator(node1, node2)
		assert.Equal(t, framework.Resource{
			MilliCPU: 12000,
			Memory:   49152,
			ScalarResources: map[corev1.ResourceName]int64{
				"nvidia.com/gpu": 2,
			},
		}, *counter3.GetTotal().ToFrameworkResource())
	})

	t.Run("basic lifecycle", func(t *testing.T) {
		counter := alloc.NewNodeAllocatableAggregator()

		t.Run("starts in empty state", func(t *testing.T) {
			assert.Equal(t, framework.Resource{}, *counter.GetTotal().ToFrameworkResource())
		})

		t.Run("deletefrom empty state does nothing", func(t *testing.T) {
			counter.Delete(node1)
			assert.Equal(t, framework.Resource{}, *counter.GetTotal().ToFrameworkResource())

			counter.Delete(node2)
			assert.Equal(t, framework.Resource{}, *counter.GetTotal().ToFrameworkResource())
		})

		t.Run("add to counter", func(t *testing.T) {
			counter.Put(node1)
			assert.Equal(t, framework.Resource{
				MilliCPU: 4000,
				Memory:   16384,
			}, *counter.GetTotal().ToFrameworkResource())
		})

		t.Run("add to counter with idempotency", func(t *testing.T) {
			expected := framework.Resource{
				MilliCPU: 12000,
				Memory:   49152,
				ScalarResources: map[corev1.ResourceName]int64{
					"nvidia.com/gpu": 2,
				},
			}

			counter.PutAll([]*corev1.Node{node1, node2})
			assert.Equal(t, expected, *counter.GetTotal().ToFrameworkResource())

			counter.Put(node2)
			assert.Equal(t, expected, *counter.GetTotal().ToFrameworkResource())
		})

		t.Run("remove from counter", func(t *testing.T) {
			counter.Delete(node1)
			assert.Equal(t, framework.Resource{
				MilliCPU: 8000,
				Memory:   32768,
				ScalarResources: map[corev1.ResourceName]int64{
					"nvidia.com/gpu": 2,
				},
			}, *counter.GetTotal().ToFrameworkResource())
		})
	})
}

func newNode(name string, allocatable corev1.ResourceList) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "name",
			UID:  uuid.NewUUID(),
		},
		Status: corev1.NodeStatus{
			Allocatable: allocatable,
		},
	}
}
