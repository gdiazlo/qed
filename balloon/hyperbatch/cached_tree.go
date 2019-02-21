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
	"fmt"
	"strconv"

	"github.com/bbva/qed/storage"
)

func newCachedNextTreeFn(batchHeight, cacheHeight uint, dbb, cached Batches, dbs, cache storage.Store, dht [][]byte) NextTreeFn {

	var next NextTreeFn
	var store storage.Store
	var batches Batches
	var region string
	var shortcuts bool

	next = func(n Node) Tree {
		store = dbs
		batches = dbb
		shortcuts = true
		region = "db "
		if n.height > cacheHeight {
			store = cache
			batches = cached
			shortcuts = false
			region = "cache "
		}
		if n.height%batchHeight == 0 {
			var mem *MemStore
			key := n.Key()
			mem, ok := batches[key]
			if !ok {
				batches[key] = NewMemStore(batchHeight)
				mem = batches[key]
				kv, err := store.Get(storage.IndexPrefix, key[:])
				if err == nil {
					mem.Unmarshal(kv.Value)
				} else {
					if err != storage.ErrKeyNotFound {
						panic(fmt.Sprintf("Error in storage: %v", err))
					}
				}
			}
			return NewSubtree(region+strconv.FormatUint(uint64(len(batches)-1), 10), mem, dht, batchHeight, shortcuts, next)
		}
		return nil

	}
	return next
}

func newCachedPersistFn(dbb, cached Batches, dbs, cache storage.Store) PersistFn {

	persist := func(batches Batches, db storage.Store) {
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

	return func() {
		persist(dbb, dbs)
		persist(cached, cache)
	}
}
