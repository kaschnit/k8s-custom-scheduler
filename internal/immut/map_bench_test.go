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
	for i := range count {
		keys[i] = fmt.Sprintf("key-structured-prefix-vector-%06d", i)
	}
	return keys
}

func BenchmarkMap_Put(b *testing.B) {
	sizes := []int{10, 100, 1_000, 10_000, 100_000, 500_000}

	for _, size := range sizes {
		keys := generateKeys(size)

		// 1. Benchmark immut.Map (CHAMP) write
		b.Run(fmt.Sprintf("immut.Map/Size-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m := immut.MakeMap[string, int]()
				for j, key := range keys {
					m = m.Put(key, j)
				}
			}
		})

		// 2. Benchmark benbjohnson/immutable (HAMT) write
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
	sizes := []int{10, 100, 1_000, 10_000, 100_000, 500_000}

	for _, size := range sizes {
		keys := generateKeys(size)

		// Pre-populate immut.Map map
		myMap := immut.MakeMap[string, int]()
		for j, key := range keys {
			myMap = myMap.Put(key, j)
		}

		// Pre-populate benbjohnson/immutable Map
		benMap := immutable.NewMap[string, int](nil)
		for j, key := range keys {
			benMap = benMap.Set(key, j)
		}

		// 1. Benchmark immut.Map (CHAMP) read
		b.Run(fmt.Sprintf("immut.Map/Size-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := keys[rand.IntN(size)]
				_, _ = myMap.Get(key)
			}
		})

		// 2. Benchmark benbjohnson/immutable (HAMT) read
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
