package immut_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/benbjohnson/immutable"
	"github.com/kaschnit/kaschnit-scheduler/internal/immut"
)

// Helper to generate a predictable permutation of unique string keys
func generateKeys(count int) []string {
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("key-structured-prefix-vector-%06d", i)
	}
	return keys
}

func BenchmarkMap_Put(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		keys := generateKeys(size)

		// 1. Benchmark your implementation (CHAMP)
		b.Run(fmt.Sprintf("immut.Map/Size-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m := immut.NewMap[string, int]()
				for j, key := range keys {
					m = m.Put(key, j)
				}
			}
		})

		// 2. Benchmark benbjohnson/immutable (HAMT)
		b.Run(fmt.Sprintf("benbjohnson.Map/Size-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Passing a nil Hasher automatically defaults to a built-in string hasher
				m := immutable.NewMap[string, int](nil)
				for j, key := range keys {
					m = m.Set(key, j)
				}
			}
		})
	}
}

func BenchmarkMap_Get(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		keys := generateKeys(size)

		// Pre-populate your map
		myMap := immut.NewMap[string, int]()
		for j, key := range keys {
			myMap = myMap.Put(key, j)
		}

		// Pre-populate benbjohnson's map
		benMap := immutable.NewMap[string, int](nil)
		for j, key := range keys {
			benMap = benMap.Set(key, j)
		}

		// 1. Benchmark your implementation's read path
		b.Run(fmt.Sprintf("immut.Map/Size-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Safe, dynamic index generation per iteration step
				key := keys[rand.IntN(size)]
				_, _ = myMap.Get(key)
			}
		})

		// 2. Benchmark benbjohnson's read path
		b.Run(fmt.Sprintf("benbjohnson.Map/Size-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := keys[rand.IntN(size)]
				_, _ = benMap.Get(key)
			}
		})
	}
}
