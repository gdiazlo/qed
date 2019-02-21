/*
   Copyright 2018 Banco Bilbao Vizcaya Argentaria, S.A.

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

package hyperbatch

import (
	"testing"

	"github.com/bbva/qed/hashing"
	"github.com/bbva/qed/storage/bplus"
	"github.com/bbva/qed/testutils/rand"
	"github.com/bbva/qed/testutils/storage"
	assert "github.com/stretchr/testify/require"
)

func equals(a, b Node) bool {
	return a.height == b.height && a.ibatch == b.ibatch
}

func pos(i, h uint) position {
	return position{i, h}
}

func step(id string, p position, op Operation) state {
	return state{id, p, op}
}

type mut struct {
	s *MemStore
	n Node
}

func newDest(hasher hashing.Hasher, digest []byte, value uint64) Node {
	dst := NewNode(digest, value)
	copy(dst.hash[:], hasher.Do(dst.value[:])[:])
	// dst.index[0] = 0x80
	return dst
}

func TestAdd(t *testing.T) {

	batches := make(Batches)

	dht := genDHT(hashing.NewXorHasher())
	db := bplus.NewBPlusTreeStore()

	next := newNextTree(8, 4, batches, db, dht)
	persist := newPersist(batches, db)

	testCases := []struct {
		eventDigest      []byte
		expectedRootHash []byte
	}{
		{[]byte{0x0}, []byte{0x0}},
		{[]byte{0x1}, []byte{0x1}},
		{[]byte{0x2}, []byte{0x3}},
		{[]byte{0x3}, []byte{0x0}},
		{[]byte{0x4}, []byte{0x4}},
		{[]byte{0x5}, []byte{0x1}},
		{[]byte{0x6}, []byte{0x7}},
		{[]byte{0x7}, []byte{0x0}},
		{[]byte{0x8}, []byte{0x8}},
		{[]byte{0x9}, []byte{0x1}},
	}

	hasher := hashing.NewXorHasher()

	root := NewNode([]byte{}, 0)
	root.index[0] = 0x80
	root.height = 8

	for i, c := range testCases {
		executor := &Tester{
			c: &Compute{
				h: hasher,
			},
		}

		dst := newDest(hasher, c.eventDigest, uint64(i))
		tree := next(root)
		root = tree.Root(root, dst)
		root = Traverse(tree, root, dst, executor, false)
		persist()
		assert.Equalf(t, c.expectedRootHash, root.hash[:1], "Incorrect root hash for index %d", i)
	}
}

// Test a binary tree of 8 levels,
// 4 in memory, 4 in a kv store
func TestMultiBatchDiskTree(t *testing.T) {

	batches := make(Batches)

	dht := genDHT(hashing.NewXorHasher())
	db := bplus.NewBPlusTreeStore()

	next := newNextTree(8, 4, batches, db, dht)
	persist := newPersist(batches, db)

	testCases := [][]byte{
		[]byte{0x0},
		[]byte{0x1},
		[]byte{0x2},
		[]byte{0x3},
		[]byte{0x4},
	}

	expectedPaths := [][]state{
		{
			step("batch 0", pos(0, 8), Shortcut),
		},
		{
			step("batch 0", pos(0, 8), Left),
			step("batch 0", pos(1, 7), Left),
			step("batch 0", pos(3, 6), Left),
			step("batch 0", pos(7, 5), Left),
			step("batch 2", pos(0, 4), Left),
			step("batch 2", pos(1, 3), Left),
			step("batch 2", pos(3, 2), Left),
			step("batch 2", pos(7, 1), Right),
			step("batch 4", pos(0, 0), Leaf),
		},
		{
			step("batch 0", pos(0, 8), Left),
			step("batch 0", pos(1, 7), Left),
			step("batch 0", pos(3, 6), Left),
			step("batch 0", pos(7, 5), Left),
			step("batch 2", pos(0, 4), Left),
			step("batch 2", pos(1, 3), Left),
			step("batch 2", pos(3, 2), Right),
			step("batch 2", pos(8, 1), Shortcut),
		},
		{
			step("batch 0", pos(0, 8), Left),
			step("batch 0", pos(1, 7), Left),
			step("batch 0", pos(3, 6), Left),
			step("batch 0", pos(7, 5), Left),
			step("batch 2", pos(0, 4), Left),
			step("batch 2", pos(1, 3), Left),
			step("batch 2", pos(3, 2), Right),
			step("batch 2", pos(8, 1), Right),
			step("batch 4", pos(0, 0), Leaf),
		},
		{
			step("batch 0", pos(0, 8), Left),
			step("batch 0", pos(1, 7), Left),
			step("batch 0", pos(3, 6), Left),
			step("batch 0", pos(7, 5), Left),
			step("batch 2", pos(0, 4), Left),
			step("batch 2", pos(1, 3), Right),
			step("batch 2", pos(4, 2), Shortcut),
		},
	}

	hasher := hashing.NewXorHasher()

	root := NewNode([]byte{}, 0)
	root.index[0] = 0x80
	root.height = 8

	for i, digest := range testCases {
		executor := &Tester{
			c: &Compute{
				h: hasher,
			},
		}

		dst := newDest(hasher, digest, uint64(i))
		tree := next(root)
		root = tree.Root(root, dst)
		Traverse(tree, root, dst, executor, false)
		persist()
		assert.Equal(t, expectedPaths[i], executor.path, "Incorrect path in test case %d", i)
	}
}

func TestCachedDiskTree(t *testing.T) {
	batches := make(Batches)
	cached := make(Batches)

	dht := genDHT(hashing.NewXorHasher())

	db, closeF := storage.OpenBadgerStore(t, "/var/tmp/byperbatch")
	defer closeF()
	cache := bplus.NewBPlusTreeStore()

	next := newCachedNextTreeFn(4, 4, batches, cached, db, cache, dht)
	persist := newCachedPersistFn(batches, cached, db, cache)

	testCases := [][]byte{
		[]byte{0x0},
		[]byte{0x1},
		[]byte{0x2},
		[]byte{0x3},
		[]byte{0x4},
	}

	expectedPaths := [][]state{
		{
			step("cache 0", pos(0, 8), Left),
			step("cache 0", pos(1, 7), Left),
			step("cache 0", pos(3, 6), Left),
			step("cache 0", pos(7, 5), Left),
			step("db 1", pos(0, 4), Left),
			step("db 1", pos(1, 3), Shortcut),
		},
		{
			step("cache 0", pos(0, 8), Left),
			step("cache 0", pos(1, 7), Left),
			step("cache 0", pos(3, 6), Left),
			step("cache 0", pos(7, 5), Left),
			step("db 1", pos(0, 4), Left),
			step("db 1", pos(1, 3), Left),
			step("db 1", pos(3, 2), Left),
			step("db 1", pos(7, 1), Right),
			step("db 3", pos(0, 0), Leaf),
		},
		{
			step("cache 0", pos(0, 8), Left),
			step("cache 0", pos(1, 7), Left),
			step("cache 0", pos(3, 6), Left),
			step("cache 0", pos(7, 5), Left),
			step("db 1", pos(0, 4), Left),
			step("db 1", pos(1, 3), Left),
			step("db 1", pos(3, 2), Right),
			step("db 1", pos(8, 1), Shortcut),
		},
		{
			step("cache 0", pos(0, 8), Left),
			step("cache 0", pos(1, 7), Left),
			step("cache 0", pos(3, 6), Left),
			step("cache 0", pos(7, 5), Left),
			step("db 1", pos(0, 4), Left),
			step("db 1", pos(1, 3), Left),
			step("db 1", pos(3, 2), Right),
			step("db 1", pos(8, 1), Right),
			step("db 3", pos(0, 0), Leaf),
		},
		{
			step("cache 0", pos(0, 8), Left),
			step("cache 0", pos(1, 7), Left),
			step("cache 0", pos(3, 6), Left),
			step("cache 0", pos(7, 5), Left),
			step("db 1", pos(0, 4), Left),
			step("db 1", pos(1, 3), Right),
			step("db 1", pos(4, 2), Shortcut),
		},
	}

	hasher := hashing.NewXorHasher()

	root := NewNode([]byte{}, 0)
	root.index[0] = 0x80
	root.height = 8

	for i, digest := range testCases {
		executor := &Tester{
			c: &Compute{
				h: hasher,
			},
		}

		dst := newDest(hasher, digest, uint64(i))
		tree := next(root)
		root = tree.Root(root, dst)
		Traverse(tree, root, dst, executor, false)
		persist()
		assert.Equal(t, expectedPaths[i], executor.path, "Incorrect path in test case %d", i)
	}
}

func BenchmarkAdd(b *testing.B) {

	batches := make(Batches)
	cached := make(Batches)

	dht := genDHT(hashing.NewSha256Hasher())

	db, closeF := storage.OpenBadgerStore(b, "/var/tmp/byperbatch")
	defer closeF()
	cache := bplus.NewBPlusTreeStore()
	next := newCachedNextTreeFn(4, 232, batches, cached, db, cache, dht)
	persist := newCachedPersistFn(batches, cached, db, cache)

	hasher := hashing.NewSha256Hasher()

	executor := &Compute{
		h: hasher,
	}

	root := NewNode([]byte{}, 0)
	root.index[0] = 0x80
	root.height = 256

	b.ResetTimer()
	b.N = 10000
	for i := 0; i < b.N; i++ {
		key := hasher.Do(rand.Bytes(32))
		dst := newDest(hasher, key, uint64(i))
		tree := next(root)
		root = tree.Root(root, dst)
		Traverse(tree, root, dst, executor, false)
		persist()
	}
}

func BenchmarkSC(b *testing.B) {

	batches := make(Batches)
	cache := NewMemStore(24)

	dht := genDHT(hashing.NewSha256Hasher())

	db, closeF := storage.OpenBadgerStore(b, "/var/tmp/byperbatch")
	defer closeF()

	counter := uint(0)
	next := newSingleCachedNextTreeFn(&counter, 4, batches, cache, db, 232, dht)
	persist := newSingleCachedPersistFn(batches, db)

	hasher := hashing.NewSha256Hasher()

	executor := &Compute{
		h: hasher,
	}

	root := NewNode([]byte{}, 0)
	root.index[0] = 0x80
	root.height = 256

	b.ResetTimer()
	b.N = 1000000
	for i := 0; i < b.N; i++ {
		key := hasher.Do(rand.Bytes(32))
		dst := newDest(hasher, key, uint64(i))
		tree := next(root)
		root = tree.Root(root, dst)
		Traverse(tree, root, dst, executor, false)
		counter = 0
		persist()
	}
}
