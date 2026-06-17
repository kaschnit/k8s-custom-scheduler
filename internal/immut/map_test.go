package immut_test

import (
	"fmt"
	"iter"
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

	for i := 0; i < count; i++ {
		m = m.Put(i, i*10)
		snapshots[i] = m
	}

	// Assert everything can be fetched perfectly from final state
	for i := 0; i < count; i++ {
		val, found := m.Get(i)
		assert.True(t, found)
		assert.Equal(t, i*10, val)
	}

	// Verify timeline integrity (no cross-contamination across updates)
	for i := 0; i < count; i++ {
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

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("CollisionKeyPrefix-%d", i)
		m = m.Put(key, i)
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("CollisionKeyPrefix-%d", i)
		val, found := m.Get(key)
		assert.True(t, found)
		assert.Equal(t, i, val)
	}
}
