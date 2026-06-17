package immut_test

import (
	"fmt"
	"iter"
	"sync"
	"testing"

	"github.com/kaschnit/kaschnit-scheduler/internal/immut"
	"github.com/stretchr/testify/assert"
)

// helper to collect elements from iter.Seq2 into a slice of pairs
type pair[K any, V any] struct {
	k K
	v V
}

func collectSeq2[K comparable, V any](seq iter.Seq2[K, V]) []pair[K, V] {
	var res []pair[K, V]
	seq(func(k K, v V) bool {
		res = append(res, pair[K, V]{k, v})
		return true // Tells the iterator to keep going
	})
	return res
}

func TestMap_BasicCRUDAndImmutability(t *testing.T) {
	m1 := immut.NewMap[string, int]()

	// 1. Get on empty map
	_, found := m1.Get("foo")
	assert.False(t, found, "Expected key 'foo' to not be found in empty map")

	// 2. Put returns a new map, leaves old map unchanged
	m2 := m1.Put("foo", 42)
	_, found = m1.Get("foo")
	assert.False(t, found, "Mutation leak: original map m1 was altered after Put")

	val, found := m2.Get("foo")
	assert.True(t, found)
	assert.Equal(t, 42, val)

	// 3. Overwrite returns a new map, retains historical snapshots
	m3 := m2.Put("foo", 100)
	valM2, _ := m2.Get("foo")
	valM3, _ := m3.Get("foo")
	assert.Equal(t, 42, valM2)
	assert.Equal(t, 100, valM3)

	// 4. Delete leaves subsequent snapshots isolated
	m4 := m3.Delete("foo")
	_, found = m4.Get("foo")
	assert.False(t, found, "Expected 'foo' to be deleted from m4")

	_, found = m3.Get("foo")
	assert.True(t, found, "Mutation leak: 'foo' was deleted from historical snapshot m3")

	// 5. Deleting non-existent key returns identical map pointer
	m5 := m4.Delete("non-existent")
	assert.Same(t, m4, m5, "Expected Delete of missing key to return the exact same map reference")
}

func TestMap_MassiveInsertionAndStructuralStability(t *testing.T) {
	m := immut.NewMap[int, int]()
	count := 5000

	snapshots := make([]*immut.Map[int, int], count)

	for i := range count {
		m = m.Put(i, i*10)
		snapshots[i] = m
	}

	// Assert everything can be fetched perfectly from final state
	for i := range count {
		val, found := m.Get(i)
		assert.True(t, found)
		assert.Equal(t, i*10, val)
	}

	// Verify timeline integrity (no cross-contamination across updates)
	for i := range count {
		snap := snapshots[i]
		_, found := snap.Get(i + 1)
		assert.False(t, found, "Snapshot timeline leak: snap %d can see key %d", i, i+1)

		val, found := snap.Get(i)
		assert.True(t, found)
		assert.Equal(t, i*10, val)
	}
}

func TestMap_Iterators(t *testing.T) {
	m := immut.NewMap[string, string]().
		Put("A", "Apple").
		Put("B", "Banana").
		Put("C", "Cherry")

	// 1. Test All()
	pairs := collectSeq2(m.All())
	assert.Len(t, pairs, 3)

	expectedPairs := map[string]string{"A": "Apple", "B": "Banana", "C": "Cherry"}
	for _, p := range pairs {
		assert.Equal(t, expectedPairs[p.k], p.v)
	}

	// 2. Test Keys()
	var keys []string
	m.Keys()(func(k string) bool {
		keys = append(keys, k)
		return true
	})
	assert.Len(t, keys, 3)
	assert.Contains(t, keys, "A", "B", "C")

	// 3. Test Values()
	var values []string
	m.Values()(func(v string) bool {
		values = append(values, v)
		return true
	})
	assert.Len(t, values, 3)
	assert.Contains(t, values, "Apple", "Banana", "Cherry")

	// 4. Test Iterator Early Break
	breakCount := 0
	m.All()(func(k string, v string) bool {
		breakCount++
		return false
	})
	assert.Equal(t, 1, breakCount, "Iterator yield logic ignored early-termination signal")
}

func TestMap_NodeCollapsingCanonicalInvariants(t *testing.T) {
	mEmpty := immut.NewMap[string, int]()
	mWithBase := mEmpty.Put("BaseKey", 1)
	mPushed := mWithBase.Put("CollidingSibling", 2)
	mCollapsed := mPushed.Delete("CollidingSibling")

	val, found := mCollapsed.Get("BaseKey")
	assert.True(t, found)
	assert.Equal(t, 1, val)

	pairs := collectSeq2(mCollapsed.All())
	assert.Len(t, pairs, 1)
	assert.Equal(t, "BaseKey", pairs[0].k)
}

func TestMap_MaxDepthFullHashCollisionRouting(t *testing.T) {
	m := immut.NewMap[string, int]()

	for i := range 50 {
		key := fmt.Sprintf("CollisionKeyPrefix-%d", i)
		m = m.Put(key, i)
	}

	for i := range 50 {
		key := fmt.Sprintf("CollisionKeyPrefix-%d", i)
		val, found := m.Get(key)
		assert.True(t, found)
		assert.Equal(t, i, val)
	}
}

func TestMap_TotalBranchDrainToEmptyLeakPrevention(t *testing.T) {
	m := immut.NewMap[string, int]()

	// Build up a nested branch depth
	m = m.Put("Alpha", 100)
	m = m.Put("Beta", 200)

	// Verify existence
	_, fA := m.Get("Alpha")
	_, fB := m.Get("Beta")
	assert.True(t, fA)
	assert.True(t, fB)

	// Completely delete everything along that sub-path branch
	m = m.Delete("Alpha")
	m = m.Delete("Beta")

	// Ensure structural iteration returns absolutely zero dangling components
	pairs := collectSeq2(m.All())
	assert.Empty(t, pairs, "Expected tree to be structurally empty, but elements or dead nodes leaked out")
}

func TestMap_ZeroValueStorageIntegrity(t *testing.T) {
	m := immut.NewMap[string, int]()

	// Put explicit integer zero values
	m = m.Put("ZeroKey", 0)

	val, found := m.Get("ZeroKey")
	assert.True(t, found, "Expected explicitly added zero value key to be found")
	assert.Equal(t, 0, val, "Stored zero value was altered or corrupted")

	// Validate with a map containing pointer/interface types or empty strings
	mStr := immut.NewMap[string, string]().Put("EmptyStrKey", "")
	strVal, strFound := mStr.Get("EmptyStrKey")
	assert.True(t, strFound)
	assert.Equal(t, "", strVal)
}

func TestMap_ConcurrentReadsAndIsolatedWrites(t *testing.T) {
	// 1. Build a pristine baseline map.
	// Once initialized, this specific instance is NEVER updated globally.
	baselineMap := immut.NewMap[string, int]()
	itemCount := 500

	for i := range itemCount {
		baselineMap = baselineMap.Put(fmt.Sprintf("key-%d", i), i*10)
	}

	var wg sync.WaitGroup
	numWorkers := 20

	// Worker Group 1: Pure Readers
	// Every thread simultaneously reads from the exact same baseline map instance.
	// This proves the Get() path handles zero-lock concurrent reads without memory races.
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := range itemCount {
				targetKey := fmt.Sprintf("key-%d", i)
				val, found := baselineMap.Get(targetKey)

				assert.True(t, found, "Worker %d lost key %s", workerID, targetKey)
				assert.Equal(t, i*10, val)
			}
		}(w)
	}

	// Worker Group 2: Concurrent Iterators
	// Simultaneously ranges over the same baseline structure to verify bitmap read safety.
	for w := range 5 {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for range 10 {
				localCount := 0
				for k, v := range baselineMap.All() {
					localCount++
					if k == "" {
						t.Errorf("Worker %d found corrupt empty key", workerID)
					}
					_ = v
				}
				assert.Equal(t, itemCount, localCount)
			}
		}(w)
	}

	// Worker Group 3: Isolated Mutators
	// Each worker derives its own independent extensions from the baseline map.
	// This proves that structural path-copying leaves the shared ancestral nodes undamaged.
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Start from the shared baseline
			localMap := baselineMap

			// Append worker-specific keys unique to this thread
			for i := range 50 {
				uniqueKey := fmt.Sprintf("worker-%d-private-%d", workerID, i)
				localMap = localMap.Put(uniqueKey, workerID)
			}

			// Verify the worker's private keys exist in its isolated universe
			for i := range 50 {
				uniqueKey := fmt.Sprintf("worker-%d-private-%d", workerID, i)
				val, found := localMap.Get(uniqueKey)
				assert.True(t, found)
				assert.Equal(t, workerID, val)
			}

			// CRITICAL SANITY CHECK: Verify the shared baseline map
			// was NOT cross-contaminated by this worker's writes.
			for i := range 50 {
				uniqueKey := fmt.Sprintf("worker-%d-private-%d", workerID, i)
				_, found := baselineMap.Get(uniqueKey)
				assert.False(t, found, "Isolation Leak: Private worker key escaped into baseline map")
			}
		}(w)
	}

	wg.Wait()
}
