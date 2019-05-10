/*
   Copyright 2018-2019 Banco Bilbao Vizcaya Argentaria, S.A.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package hyper

import (
	"encoding/binary"
	"testing"

	"github.com/bbva/qed/balloon/cache"
	"github.com/bbva/qed/hashing"
	"github.com/bbva/qed/log"
	"github.com/bbva/qed/storage"
	metrics_utils "github.com/bbva/qed/testutils/metrics"
	"github.com/bbva/qed/testutils/rand"
	storage_utils "github.com/bbva/qed/testutils/storage"
	"github.com/bbva/qed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdd(t *testing.T) {

	log.SetLogger("TestAdd", log.SILENT)

	testCases := []struct {
		eventDigest      hashing.Digest
		expectedRootHash hashing.Digest
	}{
		{hashing.Digest{0x0}, hashing.Digest{0x0}},
		{hashing.Digest{0x1}, hashing.Digest{0x1}},
		{hashing.Digest{0x2}, hashing.Digest{0x3}},
		{hashing.Digest{0x3}, hashing.Digest{0x0}},
		{hashing.Digest{0x4}, hashing.Digest{0x4}},
		{hashing.Digest{0x5}, hashing.Digest{0x1}},
		{hashing.Digest{0x6}, hashing.Digest{0x7}},
		{hashing.Digest{0x7}, hashing.Digest{0x0}},
		{hashing.Digest{0x8}, hashing.Digest{0x8}},
		{hashing.Digest{0x9}, hashing.Digest{0x1}},
	}

	store, closeF := storage_utils.OpenBPlusTreeStore()
	defer closeF()

	tree := NewHyperTree(hashing.NewFakeXorHasher, store, cache.NewSimpleCache(10))

	for i, c := range testCases {
		version := uint64(i)
		rootHash, mutations, err := tree.Add(c.eventDigest, version)
		require.NoErrorf(t, err, "This should not fail for version %d", i)
		err = tree.store.Mutate(mutations)
		require.NoErrorf(t, err, "Error inserting mutations for version %d", i)
		assert.Equalf(t, c.expectedRootHash, rootHash, "Incorrect root hash for index %d", i)
	}
}

func TestAddBulk(t *testing.T) {

	log.SetLogger("TestAddBulk", log.SILENT)

	testCases := []struct {
		eventDigests     []hashing.Digest
		versions         []uint64
		expectedRootHash hashing.Digest
	}{
		{
			[]hashing.Digest{
				hashing.Digest{0x0},
			},
			[]uint64{0},
			hashing.Digest{0x0},
		},
		{
			[]hashing.Digest{
				hashing.Digest{0x0}, hashing.Digest{0x1}, hashing.Digest{0x2}, hashing.Digest{0x3},
				hashing.Digest{0x4}, hashing.Digest{0x5}, hashing.Digest{0x6}, hashing.Digest{0x7},
				hashing.Digest{0x8}, hashing.Digest{0x9},
			},
			[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			hashing.Digest{0x1},
		},
	}

	store, closeF := storage_utils.OpenBPlusTreeStore()
	defer closeF()

	tree := NewHyperTree(hashing.NewFakeXorHasher, store, cache.NewSimpleCache(10))

	for i, c := range testCases {
		rootHash, mutations, err := tree.AddBulk(c.eventDigests, c.versions)
		require.NoErrorf(t, err, "This should not fail in test %d", i)
		err = tree.store.Mutate(mutations)
		require.NoErrorf(t, err, "Error inserting mutations in test %d", i)
		assert.Equalf(t, c.expectedRootHash, rootHash, "Incorrect root hash in test %d", i)
	}
}

func TestConsistencyBetweenAddAndAddBulk(t *testing.T) {

	log.SetLogger("TestAddVsAddBulk", log.SILENT)

	testCases := []struct {
		eventDigests     []hashing.Digest
		versions         []uint64
		expectedRootHash hashing.Digest
	}{
		{
			[]hashing.Digest{
				hashing.Digest{0x0},
			},
			[]uint64{0},
			hashing.Digest{0x0},
		},
		{
			[]hashing.Digest{
				hashing.Digest{0x0}, hashing.Digest{0x1}, hashing.Digest{0x2}, hashing.Digest{0x3},
				hashing.Digest{0x4}, hashing.Digest{0x5}, hashing.Digest{0x6}, hashing.Digest{0x7},
				hashing.Digest{0x8}, hashing.Digest{0x9},
			},
			[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			hashing.Digest{0x1},
		},
	}

	store, closeF := storage_utils.OpenBPlusTreeStore()
	defer closeF()
	addCache := cache.NewSimpleCache(10)
	addTree := NewHyperTree(hashing.NewFakeXorHasher, store, addCache)

	store2, closeF2 := storage_utils.OpenBPlusTreeStore()
	defer closeF2()
	addBulkCache := cache.NewSimpleCache(10)
	addBulkTree := NewHyperTree(hashing.NewFakeXorHasher, store2, addBulkCache)

	for i, c := range testCases {
		// Add
		var lastRootHash hashing.Digest
		for j, _ := range c.eventDigests {
			rootHash, mutations, err := addTree.Add(c.eventDigests[j], c.versions[j])
			require.NoErrorf(t, err, "This should not fail in test %d", j)
			err = addTree.store.Mutate(mutations)
			require.NoErrorf(t, err, "Error inserting mutations in test %d", j)
			lastRootHash = rootHash
		}

		// Add Bulk
		rootHashBulk, mutations, err := addBulkTree.AddBulk(c.eventDigests, c.versions)
		require.NoErrorf(t, err, "This should not fail in test %d", i)
		err = addBulkTree.store.Mutate(mutations)
		require.NoErrorf(t, err, "Error inserting mutations in test %d", i)

		// Root Hashes
		assert.Equalf(t, lastRootHash, rootHashBulk, "Incorrect root hash in test %d", i)
		assert.Equalf(t, c.expectedRootHash, rootHashBulk, "Incorrect root hash in test %d", i)

		// Caches
		assert.True(t, addCache.Equal(addBulkCache), "Caches are different in test %d", i)

		// Stores
		// All elements from addTree are in addBulkTree
		reader := addTree.store.GetAll(storage.HyperTable)
		for {
			entries := make([]*storage.KVPair, 1)
			n, _ := reader.Read(entries)
			if n == 0 {
				break
			}
			_, err := addBulkTree.store.Get(storage.HyperTable, entries[0].Key)
			assert.NoError(t, err, "Entry from addTree not found in addBulkTree")
		}
		reader.Close()

		// All elements from addBulkTree are in addTree
		reader = addBulkTree.store.GetAll(storage.HyperTable)
		for {
			entries := make([]*storage.KVPair, 1)
			n, _ := reader.Read(entries)
			if n == 0 {
				break
			}
			_, err := addTree.store.Get(storage.HyperTable, entries[0].Key)
			assert.NoError(t, err, "Entry from addBulkTree not found in addTree")
		}
		reader.Close()
	}
}

func TestProveMembership(t *testing.T) {

	log.SetLogger("TestProveMembership", log.SILENT)

	testCases := []struct {
		addedKeys         map[uint64]hashing.Digest
		expectedAuditPath AuditPath
		expectedValue     []byte
	}{
		{
			addedKeys: map[uint64]hashing.Digest{
				uint64(0): {0x0},
			},
			expectedAuditPath: AuditPath{
				"0x80|7": hashing.Digest{0x0},
				"0x40|6": hashing.Digest{0x0},
				"0x20|5": hashing.Digest{0x0},
				"0x10|4": hashing.Digest{0x0},
			},
			expectedValue: []byte{0x0},
		},
		{
			addedKeys: map[uint64]hashing.Digest{
				uint64(0): {0x0},
				uint64(1): {0x1},
				uint64(2): {0x2},
			},
			expectedAuditPath: AuditPath{
				"0x80|7": hashing.Digest{0x0},
				"0x40|6": hashing.Digest{0x0},
				"0x20|5": hashing.Digest{0x0},
				"0x10|4": hashing.Digest{0x0},
				"0x08|3": hashing.Digest{0x0},
				"0x04|2": hashing.Digest{0x0},
				"0x02|1": hashing.Digest{0x2},
				"0x01|0": hashing.Digest{0x1},
			},
			expectedValue: []byte{0x0},
		},
	}

	hasher := hashing.NewFakeXorHasher()
	searchedDigest := hasher.Do(hashing.Digest{0x0})

	for i, c := range testCases {
		store, closeF := storage_utils.OpenBPlusTreeStore()
		defer closeF()
		simpleCache := cache.NewSimpleCache(10)
		tree := NewHyperTree(hashing.NewFakeXorHasher, store, simpleCache)

		for index, digest := range c.addedKeys {
			_, mutations, err := tree.Add(digest, index)
			require.NoErrorf(t, err, "This should not fail for index %d", i)
			err = tree.store.Mutate(mutations)
			require.NoErrorf(t, err, "This should not fail for index %d", i)
		}

		proof, err := tree.QueryMembership(searchedDigest)
		require.NoErrorf(t, err, "Error adding to the tree: %v for case %d", err, i)
		assert.Equalf(t, c.expectedValue, proof.Value, "Incorrect value for case %d", i)
		assert.Equalf(t, c.expectedAuditPath, proof.AuditPath, "Incorrect audit path for case %d", i)
	}

}

func TestAddAndVerify(t *testing.T) {

	log.SetLogger("TestAddAndVerify", log.SILENT)

	value := uint64(0)

	testCases := []struct {
		hasherF func() hashing.Hasher
	}{
		{hasherF: hashing.NewXorHasher},
		{hasherF: hashing.NewSha256Hasher},
		{hasherF: hashing.NewPearsonHasher},
	}

	for i, c := range testCases {

		hasher := c.hasherF()
		store, closeF := storage_utils.OpenBPlusTreeStore()
		defer closeF()
		simpleCache := cache.NewSimpleCache(10)
		tree := NewHyperTree(c.hasherF, store, simpleCache)

		key := hasher.Do(hashing.Digest("a test event"))
		valueBytes := util.Uint64AsPaddedBytes(value, len(key))
		valueBytes = valueBytes[len(valueBytes)-len(key):] // adjust to the key size

		rootHash, mutations, err := tree.Add(key, value)
		require.NoErrorf(t, err, "Add operation should not fail for index %d", i)
		_ = tree.store.Mutate(mutations)

		proof, err := tree.QueryMembership(key)
		require.Nilf(t, err, "The membership query should not fail for index %d", i)
		assert.Equalf(t, valueBytes, proof.Value, "Incorrect actual value for index %d", i)

		correct := proof.Verify(key, rootHash)
		assert.Truef(t, correct, "Key %x should be a member for index %d", key, i)
	}

}

func TestDeterministicAdd(t *testing.T) {

	log.SetLogger("TestDeterministicAdd", log.SILENT)

	hasher := hashing.NewSha256Hasher()

	// create two trees
	cache1 := cache.NewSimpleCache(0)
	cache2 := cache.NewSimpleCache(0)
	store1, closeF1 := storage_utils.OpenBPlusTreeStore()
	store2, closeF2 := storage_utils.OpenBPlusTreeStore()
	defer closeF1()
	defer closeF2()
	tree1 := NewHyperTree(hashing.NewSha256Hasher, store1, cache1)
	tree2 := NewHyperTree(hashing.NewSha256Hasher, store2, cache2)

	// insert a bunch of events in both trees
	for i := 0; i < 100; i++ {
		event := rand.Bytes(32)
		eventDigest := hasher.Do(event)
		version := uint64(i)
		_, m1, _ := tree1.Add(eventDigest, version)
		_ = store1.Mutate(m1)
		_, m2, _ := tree2.Add(eventDigest, version)
		_ = store2.Mutate(m2)
	}

	// check cache store equality
	reader12 := store1.GetAll(storage.HyperTable)
	reader22 := store2.GetAll(storage.HyperTable)
	defer reader12.Close()
	defer reader22.Close()
	buff12 := make([]*storage.KVPair, 0)
	buff22 := make([]*storage.KVPair, 0)
	for {
		b := make([]*storage.KVPair, 100)
		n, err := reader12.Read(b)
		if err != nil || n == 0 {
			break
		}
		buff12 = append(buff12, b...)
	}
	for {
		b := make([]*storage.KVPair, 100)
		n, err := reader22.Read(b)
		if err != nil || n == 0 {
			break
		}
		buff22 = append(buff22, b...)
	}
	require.Equalf(t, buff12, buff22, "The stored cached digests should be equal")

	// check cache equality
	require.True(t, cache1.Equal(cache2), "Both caches should be equal")

}

func TestRebuildCache(t *testing.T) {

	log.SetLogger("TestRebuildCache", log.SILENT)

	store, closeF := storage_utils.OpenBPlusTreeStore()
	defer closeF()
	hasherF := hashing.NewSha256Hasher
	hasher := hasherF()

	firstCache := cache.NewSimpleCache(10)
	tree := NewHyperTree(hasherF, store, firstCache)
	require.True(t, firstCache.Size() == 0, "The cache should be empty")

	// store multiple elements
	for i := 0; i < 1000; i++ {
		key := hasher.Do(rand.Bytes(32))
		_, mutations, _ := tree.Add(key, uint64(i))
		_ = store.Mutate(mutations)
	}
	expectedSize := firstCache.Size()

	// Close tree and reopen with a new fresh cache
	tree.Close()
	secondCache := cache.NewSimpleCache(10)
	tree = NewHyperTree(hasherF, store, secondCache)

	require.Equal(t, expectedSize, secondCache.Size(), "The size of the caches should match")
	require.True(t, firstCache.Equal(secondCache), "The caches should be equal")
}

func BenchmarkAdd(b *testing.B) {

	log.SetLogger("BenchmarkAdd", log.SILENT)

	store, closeF := storage_utils.OpenRocksDBStore(b, "/var/tmp/hyper_tree_test.db")
	defer closeF()

	hasher := hashing.NewSha256Hasher()
	freeCache := cache.NewFreeCache(CacheSize)
	tree := NewHyperTree(hashing.NewSha256Hasher, store, freeCache)

	hyperMetrics := metrics_utils.CustomRegister(AddTotal)
	srvCloseF := metrics_utils.StartMetricsServer(hyperMetrics) //, store)
	defer srvCloseF()

	b.ResetTimer()
	b.N = 1000000
	for i := 0; i < b.N; i++ {
		index := make([]byte, 8)
		binary.LittleEndian.PutUint64(index, uint64(i))
		elem := append(rand.Bytes(32), index...)
		_, mutations, err := tree.Add(hasher.Do(elem), uint64(i))
		require.NoError(b, err)
		require.NoError(b, store.Mutate(mutations))
		AddTotal.Inc()
	}

}

func BenchmarkAddBulk(b *testing.B) {

	log.SetLogger("BenchmarkAddBulk", log.SILENT)

	store, closeF := storage_utils.OpenRocksDBStore(b, "/var/tmp/hyper_tree_test.db")
	defer closeF()

	hasher := hashing.NewSha256Hasher()
	freeCache := cache.NewFreeCache(CacheSize)
	tree := NewHyperTree(hashing.NewSha256Hasher, store, freeCache)

	hyperMetrics := metrics_utils.CustomRegister(AddTotal)
	srvCloseF := metrics_utils.StartMetricsServer(hyperMetrics) //, store)
	defer srvCloseF()

	bulkSize := uint64(20)
	eventDigests := make([]hashing.Digest, bulkSize)
	versions := make([]uint64, bulkSize)

	b.ResetTimer()
	b.N = 200000000
	for i := uint64(0); i < uint64(b.N); i++ {
		idx := i % bulkSize

		index := make([]byte, 8)
		binary.LittleEndian.PutUint64(index, i)
		event := append(rand.Bytes(32), index...)

		eventDigests[idx] = hasher.Do(event)
		versions[idx] = i

		if idx == bulkSize-1 {
			_, mutations, err := tree.AddBulk(eventDigests, versions)
			require.NoError(b, err)
			require.NoError(b, store.Mutate(mutations))
			AddTotal.Add(float64(bulkSize))
		}
	}

}

func BenchmarkAddMmap(b *testing.B) {

	log.SetLogger("BenchmarkAdd", log.SILENT)
	path := "/var/tmp/hyper_tree_test.db"
	store, closeF := storage_utils.OpenRocksDBStore(b, path)
	defer closeF()

	hasher := hashing.NewSha256Hasher()

	seeker := NewBatchSeeker(hasher.Len(), 4, 31*31, 6)
	cache, err := cache.NewMmapCache(path, 1118481, 31*31, seeker)
	if err != nil {
		b.Fatalf("Error creating cache: %v", err)
	}
	tree := NewHyperTree(hashing.NewSha256Hasher, store, cache)

	hyperMetrics := metrics_utils.CustomRegister(AddTotal)
	srvCloseF := metrics_utils.StartMetricsServer(hyperMetrics) //, store)
	defer srvCloseF()

	b.ResetTimer()
	b.N = 1000000
	for i := 0; i < b.N; i++ {
		index := make([]byte, 8)
		binary.LittleEndian.PutUint64(index, uint64(i))
		elem := append(rand.Bytes(32), index...)
		_, mutations, err := tree.Add(hasher.Do(elem), uint64(i))
		require.NoError(b, err)
		require.NoError(b, store.Mutate(mutations))
		AddTotal.Inc()
	}

}