//go:build unit

package alloc_test

import (
	"testing"

	"github.com/kaschnit/kaschnit-scheduler/internal/alloc"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestResourcePlus(t *testing.T) {
	cpuOnlyRes := alloc.Resources{
		alloc.ResourceNameCPU: *resource.NewMilliQuantity(1, resource.DecimalSI),
	}
	cpuMemRes1 := alloc.Resources{
		alloc.ResourceNameCPU:    *resource.NewMilliQuantity(5, resource.DecimalSI),
		alloc.ResourceNameMemory: *resource.NewQuantity(3, resource.BinarySI),
	}
	cpuMemRes2 := alloc.Resources{
		alloc.ResourceNameCPU:    *resource.NewMilliQuantity(4, resource.DecimalSI),
		alloc.ResourceNameMemory: *resource.NewQuantity(100, resource.BinarySI),
	}
	gpuRes1 := alloc.Resources{
		"nvidia.com/gpu": *resource.NewQuantity(8, resource.DecimalSI),
	}
	gpuRes2 := alloc.Resources{
		"nvidia.com/gpu": *resource.NewQuantity(1, resource.DecimalSI),
	}

	t.Run("sum resources with some overlap", func(t *testing.T) {
		result := cpuOnlyRes.Plus(cpuMemRes1)
		assert.Equal(t, alloc.Resources{
			alloc.ResourceNameCPU:    *resource.NewMilliQuantity(6, resource.DecimalSI),
			alloc.ResourceNameMemory: *resource.NewQuantity(3, resource.BinarySI),
		}, result)
	})

	t.Run("sum resources with strict overlap", func(t *testing.T) {
		result := cpuMemRes1.Plus(cpuMemRes2)
		assert.Equal(t, alloc.Resources{
			alloc.ResourceNameCPU:    *resource.NewMilliQuantity(9, resource.DecimalSI),
			alloc.ResourceNameMemory: *resource.NewQuantity(103, resource.BinarySI),
		}, result)
	})

	t.Run("sum multiple resources with overlap and no overlap", func(t *testing.T) {
		result := cpuOnlyRes.Plus(cpuMemRes1).Plus(gpuRes1).Plus(gpuRes2)
		assert.Equal(t, alloc.Resources{
			alloc.ResourceNameCPU:    *resource.NewMilliQuantity(6, resource.DecimalSI),
			alloc.ResourceNameMemory: *resource.NewQuantity(3, resource.BinarySI),
			"nvidia.com/gpu":         *resource.NewQuantity(9, resource.DecimalSI),
		}, result)
	})
}

func TesResourceMinus(t *testing.T) {
	cpuOnlyRes := alloc.Resources{
		alloc.ResourceNameCPU: *resource.NewMilliQuantity(1, resource.DecimalSI),
	}
	cpuMemRes1 := alloc.Resources{
		alloc.ResourceNameCPU:    *resource.NewMilliQuantity(5, resource.DecimalSI),
		alloc.ResourceNameMemory: *resource.NewQuantity(3, resource.BinarySI),
	}
	cpuMemRes2 := alloc.Resources{
		alloc.ResourceNameCPU:    *resource.NewMilliQuantity(4, resource.DecimalSI),
		alloc.ResourceNameMemory: *resource.NewQuantity(100, resource.BinarySI),
	}
	gpuRes1 := alloc.Resources{
		"nvidia.com/gpu": *resource.NewQuantity(8, resource.DecimalSI),
	}
	gpuRes2 := alloc.Resources{
		"nvidia.com/gpu": *resource.NewQuantity(1, resource.DecimalSI),
	}

	t.Run("subtract resources with some overlap", func(t *testing.T) {
		result := cpuMemRes1.Minus(cpuOnlyRes)
		assert.Equal(t, alloc.Resources{
			alloc.ResourceNameCPU:    *resource.NewMilliQuantity(4, resource.DecimalSI),
			alloc.ResourceNameMemory: *resource.NewQuantity(3, resource.BinarySI),
		}, result)
	})

	t.Run("subtract resources with strict overlap", func(t *testing.T) {
		result := cpuMemRes1.Minus(cpuMemRes2)

		assert.Equal(t, alloc.Resources{
			alloc.ResourceNameCPU:    *resource.NewMilliQuantity(1, resource.DecimalSI),
			alloc.ResourceNameMemory: *resource.NewQuantity(-97, resource.BinarySI),
		}, result)

		result = cpuMemRes2.Minus(cpuMemRes1)
		assert.Equal(t, alloc.Resources{
			alloc.ResourceNameCPU:    *resource.NewMilliQuantity(-1, resource.DecimalSI),
			alloc.ResourceNameMemory: *resource.NewQuantity(97, resource.BinarySI),
		}, result)
	})

	t.Run("sum multiple resources with overlap and no overlap", func(t *testing.T) {
		result := cpuMemRes1.Minus(cpuOnlyRes).Minus(gpuRes1).Minus(gpuRes2)
		assert.Equal(t, alloc.Resources{
			alloc.ResourceNameCPU:    *resource.NewMilliQuantity(4, resource.DecimalSI),
			alloc.ResourceNameMemory: *resource.NewQuantity(3, resource.BinarySI),
			"nvidia.com/gpu":         *resource.NewQuantity(-9, resource.DecimalSI),
		}, result)
	})
}
