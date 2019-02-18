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

	"github.com/bbva/qed/storage"
)

func newNextTree(treeHeight, batchHeight uint, batches Batches, db storage.Store, dht [][]byte) NextTreeFn {
	var next NextTreeFn
	next = func(n Node) Tree {
		if n.height%batchHeight == 0 {
			var store *MemStore
			key := n.Key()
			store, ok := batches[key]
			if !ok {
				batches[key] = NewMemStore(batchHeight)
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
			return NewSubtree(fmt.Sprintf("batch %d", len(batches)-1), store, dht, batchHeight, true, next)
		}
		return nil

	}
	return next
}

func newPersist(batches Batches, db storage.Store) PersistFn {
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