package lazy_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/kaschnit/kaschnit-scheduler/internal/lazy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockItem implements Value[mockItem]. It wraps a pointer to a struct
// so we can explicitly track if mutations affect shared instances.
type mockItem struct {
	data *itemData
}

type itemData struct {
	val int
}

func newMockItem(v int) *mockItem {
	return &mockItem{data: &itemData{val: v}}
}

func (m *mockItem) Clone() *mockItem {
	return &mockItem{
		data: &itemData{val: m.data.val},
	}
}

func TestMap_BasicCRUD(t *testing.T) {
	m := lazy.NewMap[string, *mockItem]()

	// Test Put and Get
	m.Put("a", newMockItem(1))
	val, ok := m.Get("a")
	require.True(t, ok)
	assert.Equal(t, 1, val.data.val)

	// Test ShareCount for single reference
	assert.Equal(t, int64(1), m.ShareCount("a"))

	// Test Update
	m.Put("a", newMockItem(2))
	val, ok = m.Get("a")
	require.True(t, ok)
	assert.Equal(t, 2, val.data.val)

	// Test Delete
	assert.True(t, m.Delete("a"), "Expected first Delete to return true")
	_, ok = m.Get("a")
	assert.False(t, ok, "Expected key to be deleted")
	assert.False(t, m.Delete("a"), "Expected subsequent Delete to return false")
}

func TestMap_LazyCloneAndIsolation(t *testing.T) {
	m1 := lazy.NewMap[string, *mockItem]()
	m1.Put("key", newMockItem(100))

	// Clone the map
	m2 := m1.Clone()

	// Both maps should initially point to the exact same underlying itemData instance,
	// and the share count should reflect both references.
	assert.Equal(t, int64(2), m1.ShareCount("key"))

	// Call Get on m2. This triggers DetachIfShared inside get(), cloning the value.
	v2, ok := m2.Get("key")
	require.True(t, ok, "Failed to get key from m2")

	// Ensure m1's copy still has its original count or has safely adapted
	assert.Equal(t, int64(1), m1.ShareCount("key"))
	assert.Equal(t, int64(1), m2.ShareCount("key"))

	// Mutate m2's item data. It should not affect m1.
	v2.data.val = 999

	v1, ok := m1.Get("key")
	require.True(t, ok)
	assert.Equal(t, 100, v1.data.val, "Isolation broken! m1 value was mutated")
}

func TestMap_IteratorsAndToMap(t *testing.T) {
	m := lazy.NewMap[string, *mockItem]()
	m.Put("a", newMockItem(1))
	m.Put("b", newMockItem(2))

	// Test ToMap
	goMap := m.ToMap()
	if assert.Len(t, goMap, 2) {
		assert.Equal(t, 1, goMap["a"].data.val)
		assert.Equal(t, 2, goMap["b"].data.val)
	}

	// Test All() iterator
	count := 0
	for k, v := range m.All() {
		count++
		if k == "a" {
			assert.Equal(t, 1, v.data.val)
		}
	}
	assert.Equal(t, 2, count)

	// Test Clear
	m.Clear()
	assert.Empty(t, m.ToMap())
}

func TestMap_DeepCloneChainCascadingGet(t *testing.T) {
	const chainDepth = 10
	chains := make([]*lazy.Map[string, *mockItem], chainDepth)

	chains[0] = lazy.NewMap[string, *mockItem]()
	chains[0].Put("heavy", newMockItem(100))

	for i := 1; i < chainDepth; i++ {
		chains[i] = chains[i-1].Clone()
	}

	// Verify reference count stacked up correctly
	assert.Equal(t, int64(chainDepth), chains[0].ShareCount("heavy"))

	var wg sync.WaitGroup
	// Simultaneous reads across the entire lineage of clones
	for i := range chainDepth {
		wg.Go(func() {
			val, ok := chains[i].Get("heavy")
			if assert.True(t, ok) {
				assert.Equal(t, 100, val.data.val)
			}
		})
	}
	wg.Wait()

	// After all gets resolve, every single instance should be perfectly isolated (Count = 1)
	for i := range chainDepth {
		assert.Equal(t, int64(1), chains[i].ShareCount("heavy"))
	}
}

func TestMap_OverwriteIsolationInvariant(t *testing.T) {
	m1 := lazy.NewMap[string, *mockItem]()
	m1.Put("k", newMockItem(10))

	m2 := m1.Clone()

	// Overwrite the key in m1 with a totally new item.
	// This detaches the old sharedValue wrapper from m1.
	m1.Put("k", newMockItem(20))

	// Trigger a lazy clone on m2 by reading it
	v2, ok := m2.Get("k")
	require.True(t, ok)
	assert.Equal(t, 10, v2.data.val)

	// Verify m1 has the new value completely independently
	v1, ok := m1.Get("k")
	require.True(t, ok)
	assert.Equal(t, 20, v1.data.val)
}

func TestMap_ConcurrentReadsAndWrites(t *testing.T) {
	m := lazy.NewMap[int, *mockItem]()
	const workers = 10
	const iterations = 500

	var wg sync.WaitGroup

	// Concurrent Writers
	for i := range workers {
		wg.Go(func() {
			for j := range iterations {
				key := (i * iterations) + j
				m.Put(key, newMockItem(key))
			}
		})
	}

	// Concurrent Readers interacting with whatever is available
	for range workers {
		wg.Go(func() {
			for j := range iterations {
				// Random reads across possible keyspace
				m.Get(j)
			}
		})
	}

	wg.Wait()

	// Verify final total count matching expectations
	finalMap := m.ToMap()
	assert.Len(t, finalMap, workers*iterations)
}

func TestMap_ConcurrentCloningAndReads(t *testing.T) {
	m := lazy.NewMap[string, *mockItem]()
	m.Put("shared", newMockItem(42))

	const readers = 20
	const iterations = 200
	var wg sync.WaitGroup

	// Thread triggering maps to frequently clone
	wg.Go(func() {
		for range iterations {
			_ = m.Clone()
		}
	})

	// Concurrent readers trying to read and lazily evaluate the same underlying objects
	for i := range readers {
		wg.Go(func() {
			for range iterations {
				val, ok := m.Get("shared")
				if assert.True(t, ok, "Reader %d failed to find 'shared' key", i) {
					assert.Equal(t, 42, val.data.val, "Reader %d hit data corruption", i)
				}
			}
		})
	}

	wg.Wait()
}

func TestMap_ConcurrentIteratorIsolation(t *testing.T) {
	m := lazy.NewMap[string, *mockItem]()
	for i := range 100 {
		m.Put(fmt.Sprintf("key_%d", i), newMockItem(i))
	}

	var wg sync.WaitGroup

	// Loop calling iterators while other loops actively mutate the map
	wg.Go(func() {
		for range 50 {
			for range m.All() {
				// Just consuming iterator to trigger cloning and reading loops
			}
		}
	})

	// Parallel Mutator modifying the original map structure
	wg.Go(func() {
		for i := range 100 {
			m.Put(fmt.Sprintf("key_%d", i), newMockItem(i*10))
			m.Delete(fmt.Sprintf("key_del_%d", i))
		}
	})

	wg.Wait()
}

func TestMap_ConcurrentValuesIterator(t *testing.T) {
	m := lazy.NewMap[string, *mockItem]()
	for i := range 50 {
		m.Put(fmt.Sprintf("key_%d", i), newMockItem(i))
	}

	var wg sync.WaitGroup

	// Test the completely missing Values() API concurrently
	wg.Go(func() {
		for range 50 {
			for range m.Values() {
				// Exercise Values() iterator loop
			}
		}
	})

	// Parallel Mutator doing Puts and Deletes
	wg.Go(func() {
		for i := range 50 {
			m.Put(fmt.Sprintf("key_%d", i), newMockItem(i*2))
		}
	})

	wg.Wait()
}

func TestMap_ConcurrentClearAndReads(t *testing.T) {
	m := lazy.NewMap[int, *mockItem]()

	var wg sync.WaitGroup

	// Continuous Writers and Clearers
	wg.Go(func() {
		for range 100 {
			for j := range 10 {
				m.Put(j, newMockItem(j))
			}
			m.Clear() // Aggressively clearing while readers read
		}
	})

	// Continuous Readers and ToMap exporters
	wg.Go(func() {
		for range 100 {
			for j := range 10 {
				_, _ = m.Get(j)
			}
			_ = m.ToMap()
		}
	})

	wg.Wait()
}

func TestMap_ConcurrentGetOnMultipleClones(t *testing.T) {
	m := lazy.NewMap[string, *mockItem]()
	m.Put("target", newMockItem(777))

	// Create many clones that all share the exact same underlying ref counter
	const cloneCount = 20
	clones := make([]*lazy.Map[string, *mockItem], cloneCount)
	for i := range cloneCount {
		clones[i] = m.Clone()
	}

	var wg sync.WaitGroup

	// Simulating multiple threads trying to lazily clone the *same* item
	// from *different* map instances at the exact same time.
	for i := range cloneCount {
		wg.Go(func() {
			val, ok := clones[i].Get("target")
			if assert.True(t, ok) {
				assert.Equal(t, 777, val.data.val)
			}
		})
	}

	wg.Wait()
}

func TestMap_ConcurrentIteratorPassiveBreak(t *testing.T) {
	m := lazy.NewMap[int, *mockItem]()
	for i := range 100 {
		m.Put(i, newMockItem(i))
	}

	var wg sync.WaitGroup

	// Worker breaking out of iterators early
	wg.Go(func() {
		for range 50 {
			for range m.All() {
				break // Intentional early break to trigger deferred Clear()
			}
		}
	})

	// Concurrent mutator modifying the data being iterated over
	wg.Go(func() {
		for j := range 50 {
			m.Put(j, newMockItem(j*10))
			m.Delete(j + 50)
		}
	})

	wg.Wait()
}
