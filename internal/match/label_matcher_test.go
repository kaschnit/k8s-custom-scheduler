//go:build unit

package match_test

import (
	"testing"

	"github.com/kaschnit/kaschnit-scheduler/internal/match"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func TestNothing(t *testing.T) {
	t.Run("nil label selector returns true", func(t *testing.T) {
		selector, err := metav1.LabelSelectorAsSelector(nil)
		require.NoError(t, err)
		assert.True(t, match.Nothing(selector))
	})
	t.Run("labels.Nothing() returns true", func(t *testing.T) {
		assert.True(t, match.Nothing(labels.Nothing()))
	})
	t.Run("match one label returns false", func(t *testing.T) {
		labelSelector := &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"label1": "true",
			},
		}
		selector, err := metav1.LabelSelectorAsSelector(labelSelector)
		require.NoError(t, err)
		assert.False(t, match.Nothing(selector))
	})
	t.Run("match everything returns false", func(t *testing.T) {
		assert.False(t, match.Nothing(labels.Everything()))
	})
}
