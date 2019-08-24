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
package postgres

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/bbva/qed/storage"
	"github.com/bbva/qed/testutils/spec"
	"github.com/bbva/qed/util"
)

func TestMutate(t *testing.T) {
	store, closeF := openPostgresStore(t)
	defer closeF()

	tests := []struct {
		testname      string
		table         storage.Table
		key, value    []byte
		expectedError error
	}{
		{"Mutate Key=Value", storage.HistoryTable, []byte("Key"), []byte("Version"), nil},
	}

	for _, test := range tests {
		err := store.Mutate([]*storage.Mutation{
			{Table: test.table, Key: test.key, Value: test.value},
		}, nil)
		spec.Equal(t, test.expectedError, err, "Error mutating in test", test.testname)
		_, err = store.Get(test.table, test.key)
		spec.Equal(t, test.expectedError, err, "Error getting key in test", test.testname)
	}
}

func TestGetExistentKey(t *testing.T) {

	store, closeF := openPostgresStore(t)
	defer closeF()

	testCases := []struct {
		table         storage.Table
		key, value    []byte
		expectedError error
	}{
		{storage.HistoryTable, []byte("Key1"), []byte("Value1"), nil},
		{storage.HistoryTable, []byte("Key2"), []byte("Value2"), nil},
		{storage.HyperTable, []byte("Key3"), []byte("Value3"), nil},
		{storage.HyperTable, []byte("Key4"), []byte("Value4"), storage.ErrKeyNotFound},
	}

	for _, test := range testCases {
		if test.expectedError == nil {
			err := store.Mutate([]*storage.Mutation{
				{
					Table: test.table,
					Key:   test.key,
					Value: test.value,
				},
			}, nil)
			spec.NoError(t, err)
		}

		stored, err := store.Get(test.table, test.key)
		if test.expectedError == nil {
			spec.NoError(t, err)
			spec.Equal(t, stored.Key, test.key, "The stored key does not match the original")
			spec.Equal(t, stored.Value, test.value, "The stored value does not match the original")
		} else {
			spec.Error(t, test.expectedError)
		}

	}

}

func TestGetRange(t *testing.T) {
	store, closeF := openPostgresStore(t)
	defer closeF()

	var testCases = []struct {
		size       int
		start, end byte
	}{
		{40, 10, 50},
		{0, 1, 9},
		{11, 1, 20},
		{10, 40, 60},
		{0, 60, 100},
		{0, 20, 10},
	}

	table := storage.HistoryTable
	for i := 10; i <= 50; i++ {
		store.Mutate([]*storage.Mutation{
			{table, []byte{byte(i)}, []byte("Value")},
		}, nil)
	}

	for _, test := range testCases {
		slice, err := store.GetRange(table, []byte{test.start}, []byte{test.end})
		spec.NoError(t, err)
		spec.Equal(t, test.size, len(slice), "Slice length invalid")
	}

}

func TestGetAll(t *testing.T) {

	table := storage.HyperTable
	numElems := uint16(1000)
	testCases := []struct {
		batchSize    int
		numBatches   int
		lastBatchLen int
	}{
		{10, 100, 10},
		{20, 50, 20},
		{17, 59, 14},
	}

	store, closeF := openPostgresStore(t)
	defer closeF()

	// insert
	for i := uint16(0); i < numElems; i++ {
		key := util.Uint16AsBytes(i)
		store.Mutate([]*storage.Mutation{
			{table, key, key},
		}, nil)
	}

	for i, c := range testCases {
		reader := store.GetAll(table)
		numBatches := 0
		var lastBatchLen int
		for {
			entries := make([]*storage.KVPair, c.batchSize)
			n, _ := reader.Read(entries)
			if n == 0 {
				break
			}
			numBatches++
			lastBatchLen = n
		}
		reader.Close()
		istr := fmt.Sprintf("%d", i)
		spec.Equal(t, c.numBatches, numBatches, "The number of batches should match for case", istr)
		spec.Equal(t, c.lastBatchLen, lastBatchLen, "The size of the last batch len should match for case", istr)
	}

}

func openPostgresStore(t testing.TB) (*PostgresStore, func()) {
	path := mustTempDir()
	store, err := NewPostgresStore("localhost", "qed", "qed")
	if err != nil {
		t.Errorf("Error opening rocksdb store: %v", err)
		t.FailNow()
	}
	return store, func() {
		store.db.Query("DELETE from history")
		store.db.Query("DELETE from hyper")
		store.db.Query("DELETE from hypercache")
		store.db.Query("DELETE from fsm")
		store.Close()
		deleteFile(path)
	}
}

func mustTempDir() string {
	var err error
	path, err := ioutil.TempDir("/var/tmp", "rocksdbstore-test-")
	if err != nil {
		panic("failed to create temp dir")
	}
	return path
}

func deleteFile(path string) {
	err := os.RemoveAll(path)
	if err != nil {
		fmt.Printf("Unable to remove db file %s", err)
	}
}
