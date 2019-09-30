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
package rocks

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bbva/qed/storage"
	"github.com/bbva/qed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMutate(t *testing.T) {
	store, closeF := openRocksDBStore(t)
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
		require.Equalf(t, test.expectedError, err, "Error mutating in test: %s", test.testname)
		_, err = store.Get(test.table, test.key)
		require.Equalf(t, test.expectedError, err, "Error getting key in test: %s", test.testname)
	}
}
func TestGetExistentKey(t *testing.T) {

	store, closeF := openRocksDBStore(t)
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
			require.NoError(t, err)
		}

		stored, err := store.Get(test.table, test.key)
		if test.expectedError == nil {
			require.NoError(t, err)
			require.Equalf(t, stored.Key, test.key, "The stored key does not match the original: expected %d, actual %d", test.key, stored.Key)
			require.Equalf(t, stored.Value, test.value, "The stored value does not match the original: expected %d, actual %d", test.value, stored.Value)
		} else {
			require.Error(t, test.expectedError)
		}

	}

}

func TestGetRange(t *testing.T) {
	store, closeF := openRocksDBStore(t)
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
	for i := 10; i < 50; i++ {
		store.Mutate([]*storage.Mutation{
			{table, []byte{byte(i)}, []byte("Value")},
		}, nil)
	}

	for _, test := range testCases {
		slice, err := store.GetRange(table, []byte{test.start}, []byte{test.end})
		require.NoError(t, err)
		require.Equalf(t, len(slice), test.size, "Slice length invalid: expected %d, actual %d", test.size, len(slice))
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

	store, closeF := openRocksDBStore(t)
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
		assert.Equalf(t, c.numBatches, numBatches, "The number of batches should match for test case %d", i)
		assert.Equal(t, c.lastBatchLen, lastBatchLen, "The size of the last batch len should match for test case %d", i)
	}

}

func TestGetLast(t *testing.T) {
	store, closeF := openRocksDBStore(t)
	defer closeF()

	// insert
	numElems := uint64(20)
	tables := []storage.Table{storage.HistoryTable, storage.HyperTable}
	for _, table := range tables {
		for i := uint64(0); i < numElems; i++ {
			key := util.Uint64AsBytes(i)
			store.Mutate([]*storage.Mutation{
				{table, key, key},
			}, nil)
		}
	}

	// get last element for history table
	kv, err := store.GetLast(storage.HistoryTable)
	require.NoError(t, err)
	require.Equalf(t, util.Uint64AsBytes(numElems-1), kv.Key, "The key should match the last inserted element")
	require.Equalf(t, util.Uint64AsBytes(numElems-1), kv.Value, "The value should match the last inserted element")
}

func TestFetchAndLoadSnapshot(t *testing.T) {
	store, closeF := openRocksDBStore(t)
	defer closeF()

	// insert
	numElems := uint64(100)
	tables := []storage.Table{storage.HistoryTable, storage.HyperTable}
	for j, table := range tables {
		for i := uint64(0); i < numElems; i++ {
			key := util.Uint64AsBytes(i)
			err := store.Mutate(
				[]*storage.Mutation{
					{Table: table, Key: key, Value: key},
				},
				util.Uint64AsBytes(numElems*uint64(j)+i),
			)
			require.NoError(t, err)
		}
	}

	// get last WAL seq num
	until := store.LastWALSequenceNumber()
	require.Equal(t, numElems*uint64(len(tables)), until)

	// fetch snapshot
	ioBuf := new(bufCloser)
	require.NoError(t, store.FetchSnapshot(ioBuf, 0, until, func(meta []byte) (bool, error) {
		return util.BytesAsUint64(meta) >= 0, nil // we start from the beginning
	}))

	// load snapshot in another instance
	restore, recloseF := openRocksDBStore(t)
	defer recloseF()
	require.NoError(t, restore.LoadSnapshot(ioBuf))

	// check elements
	for _, table := range tables {
		reader := store.GetAll(table)
		for {
			entries := make([]*storage.KVPair, 1000)
			n, _ := reader.Read(entries)
			if n == 0 {
				break
			}
			for i := 0; i < n; i++ {
				kv, err := restore.Get(table, entries[i].Key)
				require.NoError(t, err)
				require.Equal(t, entries[i].Value, kv.Value, "The values should match")
			}
		}
		reader.Close()
	}
}

func TestFetchAndLoadUntilSeqNum(t *testing.T) {
	store, closeF := openRocksDBStore(t)
	defer closeF()

	numElems := uint64(100)
	// insert
	for i := uint64(0); i < numElems; i++ {
		key := util.Uint64AsBytes(i)
		err := store.Mutate(
			[]*storage.Mutation{
				{Table: storage.HistoryTable, Key: key, Value: key},
			},
			key,
		)
		require.NoError(t, err)
	}

	// set last WAL seq num
	until := uint64(1)

	// fetch snapshot
	ioBuf := new(bufCloser)
	require.NoError(t, store.FetchSnapshot(ioBuf, 0, until,func(meta []byte) (bool, error) {
		return util.BytesAsUint64(meta) >= 0, nil // we start from the beginning
	}))

	// load snapshot in another instance
	restore, recloseF := openRocksDBStore(t)
	defer recloseF()
	require.NoError(t, restore.LoadSnapshot(ioBuf))

	// check elements
	reader := store.GetAll(storage.HistoryTable)
	defer reader.Close()
	entries := make([]*storage.KVPair, numElems)
	n, _ := reader.Read(entries)
	require.Equal(t, numElems, uint64(n))

	for i := uint64(0); i < until; i++ {
		kv, err := restore.Get(storage.HistoryTable, entries[i].Key)
		require.NoError(t, err)
		require.Equal(t, entries[i].Value, kv.Value, "The values should match")
	}
	for i := until; i < numElems; i++ {
		_, err := restore.Get(storage.HistoryTable, entries[i].Key)
		require.Error(t, err)
		require.Equal(t, storage.ErrKeyNotFound, err, "The error should match")
	}
}

func TestFetchAndLoadSnapshotSinceVersion(t *testing.T) {
	store, closeF := openRocksDBStore(t)
	defer closeF()

	numElems := uint64(100)
	lastVersion := numElems / 2
	// insert
	for i := uint64(0); i < numElems; i++ {
		key := util.Uint64AsBytes(i)
		err := store.Mutate(
			[]*storage.Mutation{
				{Table: storage.HistoryTable, Key: key, Value: key},
			},
			key,
		)
		require.NoError(t, err)
	}

	// get last WAL seq num
	until := store.LastWALSequenceNumber()
	require.Equal(t, numElems, until)

	// fetch snapshot
	ioBuf := new(bufCloser)
	require.NoError(t, store.FetchSnapshot(ioBuf, 0, until, func(meta []byte) (bool, error) {
		return util.BytesAsUint64(meta) >= lastVersion, nil // we start at the middle
	}))

	// load snapshot in another instance
	restore, recloseF := openRocksDBStore(t)
	defer recloseF()
	require.NoError(t, restore.LoadSnapshot(ioBuf))

	// check elements
	reader := store.GetAll(storage.HistoryTable)
	defer reader.Close()
	entries := make([]*storage.KVPair, numElems)
	n, _ := reader.Read(entries)
	require.Equal(t, numElems, uint64(n))

	for i := uint64(0); i < lastVersion; i++ {
		_, err := restore.Get(storage.HistoryTable, entries[i].Key)
		require.Error(t, err)
		require.Equal(t, storage.ErrKeyNotFound, err, "The error should match")
	}
	for i := lastVersion; i < numElems; i++ {
		kv, err := restore.Get(storage.HistoryTable, entries[i].Key)
		require.NoError(t, err)
		require.Equal(t, entries[i].Value, kv.Value, "The values should match")
	}
}

func openRocksDBStore(t require.TestingT) (*RocksDBStore, func()) {
	path := mustTempDir()
	store, err := NewRocksDBStore(filepath.Join(path, "rockdsdb_store_test.db"), 0)
	if err != nil {
		t.Errorf("Error opening rocksdb store: %v", err)
		t.FailNow()
	}
	return store, func() {
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

type bufCloser struct {
	bytes.Buffer
}

func (b bufCloser) Close() error {
	return nil
}