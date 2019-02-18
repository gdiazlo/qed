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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/bbva/qed/hashing"
	"github.com/bbva/qed/storage"
	"github.com/bbva/qed/storage/bplus"
	"github.com/bbva/qed/testutils/rand"
	storage_utils "github.com/bbva/qed/testutils/storage"
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

func newNextTree(treeHeight, batchHeight uint, batches map[[40]byte]*MemStore, db storage.Store, dht [][]byte) NextTree {
	var next NextTree
	next = func(n Node) Tree {
		if n.height%4 == 0 {
			var store *MemStore
			key := n.Key()
			store, ok := batches[key]
			if !ok {
				batches[key] = NewMemStore(4)
				store = batches[key]
				kv, err := db.Get(storage.IndexPrefix, key[:])
				if err == nil {
					store.Unmarshal(kv.Value)
				} else {
					if err != storage.ErrKeyNotFound {
						panic(fmt.Sprintf("Error in storage: %v", err))
					}
				}
			} 
			return NewSubtree(fmt.Sprintf("batch %d", len(batches)-1), store, dht, 4, true, next)
		}
		return nil

	}
	return next
}

func newPersist(batches map[[40]byte]*MemStore, db storage.Store) func() {
	return func() {
		var muts []*storage.Mutation
		for key, b := range batches {
			if b.writes > 0 {
				var m storage.Mutation
				m.Prefix = storage.IndexPrefix
				m.Key = append(m.Key, key[:]...)
				m.Value = append(m.Value, b.Marshal()...)
				muts = append(muts, &m)
			} 
			delete(batches, key)
		}

		err := db.Mutate(muts)
		if err != nil {
			panic(fmt.Sprintf("Storage error %v", err))
		}
	}

}

func newDest(hasher hashing.Hasher, digest []byte, value uint64) Node {
	var dst Node
	copy(dst.key[:], digest)
	binary.LittleEndian.PutUint64(dst.value[:], value)
	copy(dst.hash[:], hasher.Do(dst.value[:])[:])
	dst.index[0] = 0x80
	return dst
}

func TestAdd(t *testing.T) {

	batches := make(map[[40]byte]*MemStore)

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

	batches := make(map[[40]byte]*MemStore)

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
		fmt.Println("------------")
	}
}

func TestCachedDiskTree(t *testing.T) {
	batches := make(map[[40]byte]*MemStore)
	cache := NewMemStore(4)

	dht := genDHT(hashing.NewXorHasher())
	// db, _ := badger.NewBadgerStore("/var/tmp/byperbatch")
	// assert.Nil(t, err)

	db := bplus.NewBPlusTreeStore()

	next := newNextTree(8, 4, batches, db, dht)
	persist := newPersist(batches, db)

	testCases := [][]state{
		{
			step("top", pos(0, 8), Left),
			step("top", pos(1, 7), Left),
			step("top", pos(3, 6), Left),
			step("top", pos(7, 5), Left),
			step("batch 1", pos(0, 4), Shortcut),
		},
		{
			step("top", pos(0, 8), Left),
			step("top", pos(1, 7), Left),
			step("top", pos(3, 6), Left),
			step("top", pos(7, 5), Left),
			step("batch 1", pos(0, 4), Left),
			step("batch 1", pos(1, 3), Left),
			step("batch 1", pos(3, 2), Left),
			step("batch 1", pos(7, 1), Right),
			step("batch 1", pos(16, 0), Leaf),
		},
		{
			step("top", pos(0, 8), Left),
			step("top", pos(1, 7), Left),
			step("top", pos(3, 6), Left),
			step("top", pos(7, 5), Left),
			step("batch 1", pos(0, 4), Left),
			step("batch 1", pos(1, 3), Left),
			step("batch 1", pos(3, 2), Right),
			step("batch 1", pos(8, 1), Left),
			step("batch 1", pos(17, 0), Leaf),
		},
		{
			step("top", pos(0, 8), Left),
			step("top", pos(1, 7), Left),
			step("top", pos(3, 6), Left),
			step("top", pos(7, 5), Left),
			step("batch 1", pos(0, 4), Left),
			step("batch 1", pos(1, 3), Left),
			step("batch 1", pos(3, 2), Right),
			step("batch 1", pos(8, 1), Right),
			step("batch 1", pos(18, 0), Leaf),
		},
	}

	hasher := hashing.NewXorHasher()

	root := NewNode([]byte{}, 0)
	root.index[0] = 0x80
	root.height = 8

	batches[root.Key()] = cache

	tree := NewSubtree("batch 0", cache, dht, 8, true, next)

	for i, expectedPath := range testCases {
		executor := &Tester{
			c: &Compute{
				h: hasher,
			},
		}

		dst := NewNode([]byte{byte(i)}, uint64(i))
		Traverse(tree, tree.Root(Node{height: 8}, dst), dst, executor, false)
		assert.Equal(t, expectedPath, executor.path, "Incorrect path in test case %d", i)
		persist()
	}
}

func TestCachedDiskTreeSha(t *testing.T) {
	batches := make(map[[40]byte]*MemStore)
	cache := NewMemStore(4)

	dht := genDHT(hashing.NewXorHasher())
	// db, _ := badger.NewBadgerStore("/var/tmp/byperbatch")
	// assert.Nil(t, err)

	db := bplus.NewBPlusTreeStore()

	next := newNextTree(8, 4, batches, db, dht)
	persist := newPersist(batches, db)

	hasher := hashing.NewXorHasher()

	root := NewNode([]byte{}, 0)
	root.index[0] = 0x80
	root.height = 8

	batches[root.Key()] = cache

	tree := NewSubtree("batch 0", cache, dht, 8, true, next)
	for i := 0; i < 10; i++ {
		executor := &Tester{
			c: &Compute{
				h: hasher,
			},
		}

		dst := NewNode([]byte{byte(i)}, uint64(i))
		Traverse(tree, tree.Root(Node{height: 256}, dst), dst, executor, false)
		// assert.Equal(t, expectedPath, executor.path, "Incorrect path in test case %d", i)
		persist()
	}
}

func BenchmarkAdd(b *testing.B) {

	batches := make(map[[40]byte]*MemStore)
	cache := NewMemStore(25)
	dht := genDHT(hashing.NewSha256Hasher())
	db, closeF := storage_utils.OpenBadgerStore(b, "/var/tmp/hyper_tree_test.db")
	defer closeF()

	next := newNextTree(256, 4, batches, db, dht)
	persist := newPersist(batches, db)

	tree := NewSubtree("batch 0", cache, dht, 256, false, next)

	hasher := hashing.NewSha256Hasher()

	executor := &Compute{
		h: hasher,
	}

	b.ResetTimer()
	b.N = 100000
	for i := 0; i < b.N; i++ {
		key := hasher.Do(rand.Bytes(32))
		dst := NewNode(key, uint64(i))
		Traverse(tree, tree.Root(Node{height: 256}, dst), dst, executor, false)
		persist()
	}
}
