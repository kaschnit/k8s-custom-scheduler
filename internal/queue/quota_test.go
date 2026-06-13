package queue_test

import (
	"testing"

	"github.com/kaschnit/kaschnit-scheduler/internal/queue"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestQuota(t *testing.T) {
	t.Run("quota counting", func(t *testing.T) {
		quota := queue.NewQuota(nil)
		pod1 := newPodWithReq(corev1.ResourceList{corev1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI)})
		pod2 := newPodWithReq(corev1.ResourceList{corev1.ResourceCPU: *resource.NewQuantity(2, resource.DecimalSI)})

		t.Run("add pods to quota", func(t *testing.T) {
			// Add pod1 cpu
			quota.AddPodIfNotPresent(pod1)
			assert.Equal(t, framework.Resource{MilliCPU: 1000, AllowedPodNumber: 1},
				*quota.Used().ToFrameworkResource())
			assert.Equal(t, framework.Resource{MilliCPU: 1000, AllowedPodNumber: 1},
				*quota.Used().ToFrameworkResource())

			// Add pod2 cpu
			quota.AddPodIfNotPresent(pod2)
			assert.Equal(t, framework.Resource{MilliCPU: 3000, AllowedPodNumber: 2},
				*quota.Used().ToFrameworkResource())

			t.Run("idempotent", func(t *testing.T) {
				// Add pod1 cpu again does nothing
				quota.AddPodIfNotPresent(pod1)
				assert.Equal(t, framework.Resource{MilliCPU: 3000, AllowedPodNumber: 2},
					*quota.Used().ToFrameworkResource()) // No change
			})
		})

		t.Run("delete pods from quota", func(t *testing.T) {
			// Delete pod1 cpu
			quota.DeletePodIfPresent(pod1)
			assert.Equal(t, framework.Resource{MilliCPU: 2000, AllowedPodNumber: 1},
				*quota.Used().ToFrameworkResource())

			t.Run("idempotent", func(t *testing.T) {
				// Remove pod1 again does nothing
				quota.DeletePodIfPresent(pod1)
				assert.Equal(t, framework.Resource{MilliCPU: 2000, AllowedPodNumber: 1},
					*quota.Used().ToFrameworkResource()) // No change
			})

			t.Run("delete random pod does nothing", func(t *testing.T) {
				podNotInQuota := newPodWithReq(corev1.ResourceList{corev1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI)})
				quota.DeletePodIfPresent(podNotInQuota)
				assert.Equal(t, framework.Resource{MilliCPU: 2000, AllowedPodNumber: 1},
					*quota.Used().ToFrameworkResource()) // No change
			})

			// Delete pod2 cpu
			quota.DeletePodIfPresent(pod2)
			assert.Equal(t, framework.Resource{}, *quota.Used().ToFrameworkResource())

			t.Run("delete random pod from empty quota does nothing", func(t *testing.T) {
				podNotInQuota := newPodWithReq(corev1.ResourceList{corev1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI)})
				quota.DeletePodIfPresent(podNotInQuota)
				assert.Equal(t, framework.Resource{}, *quota.Used().ToFrameworkResource()) // No change
			})
		})
	})
}

func newPodWithReq(req corev1.ResourceList) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-" + string(uuid.NewUUID()),
			Namespace: "default",
			UID:       uuid.NewUUID(),
		},
		Spec: corev1.PodSpec{
			Resources: &corev1.ResourceRequirements{
				Requests: req,
			},
		},
	}
}
