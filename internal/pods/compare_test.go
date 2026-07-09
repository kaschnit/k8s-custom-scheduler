//go:build unit

package pods

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCompareImportanceDesc(t *testing.T) {
	t.Run("Sorts pods in descending order", func(t *testing.T) {
		origPods := []*corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-a"},
				Spec:       corev1.PodSpec{Priority: new(int32(50))},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-b"},
				Spec:       corev1.PodSpec{Priority: new(int32(10))},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-c"},
				Spec:       corev1.PodSpec{Priority: new(int32(30))},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-d"},
				Spec:       corev1.PodSpec{Priority: new(int32(-100))},
			},
		}

		sortedPods := slices.Clone(origPods)
		sortedPods = append(sortedPods, sortedPods[2]) // duplicate entry

		slices.SortFunc(sortedPods, CompareImportanceDesc)

		assert.Equal(t, []*corev1.Pod{
			origPods[0],
			origPods[2],
			origPods[2],
			origPods[1],
			origPods[3],
		}, sortedPods)
	})
}
