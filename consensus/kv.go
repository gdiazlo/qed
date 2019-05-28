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

package consensus

import (
	"bytes"
	"errors"

	"github.com/bbva/qed/rocksdb"
	"github.com/bbva/qed/storage"
	"github.com/lni/dragonboat/raftio"
)

var (
	// ErrKeyNotFound is an error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// table groups related key-value pairs under a
// consistent space.
type table uint32

const (
	defaultTable table = iota
	stableTable
	logTable
)

func (t table) String() string {
	var s string
	switch t {
	case defaultTable:
		s = "default"
	case stableTable:
		s = "stable"
	case logTable:
		s = "log"
	}
	return s
}

// RocksdbKV implements the IKVStore interface, used by the RDB struct to access the underlying Key-Value store.
type RocksdbKV struct {
	// db is the underlying handle to the db.
	db *rocksdb.DB

	stats *rocksdb.Statistics

	// The path to the RocksDB database directory.
	path string
	ro   *rocksdb.ReadOptions
	wo   *rocksdb.WriteOptions

	// column family handlers
	cfHandles rocksdb.ColumnFamilyHandles

	// global options
	globalOpts *rocksdb.Options
	// stable options
	stableBbto *rocksdb.BlockBasedTableOptions
	stableOpts *rocksdb.Options
	// log options
	logBbto *rocksdb.BlockBasedTableOptions
	logOpts *rocksdb.Options
	// block cache
	blockCache *rocksdb.Cache

	// metrics
	metrics *rocksDBMetrics
}

// Options contains all the configuration used to open the RocksDB instance.
type Options struct {
	// Path is the directory path to the RocksDB instance to use.
	Path string
	// TODO decide if we should use a diferent directory for the Rocks WAL

	// NoSync causes the database to skip fsync calls after each
	// write to the log. This is unsafe, so it should be used
	// with caution.
	NoSync bool

	EnableStatistics bool
}

// Name is the IKVStore name.
func (kv *RocksdbKV) Name() string {
	return "rocksdb"
}

// NewRocksDBKV takes a file path and returns a connected Raft backend.
func NewRocksDBStore(path string) (*RocksdbKV, error) {
	return New(Options{Path: path, NoSync: true})
}

// New uses the supplied options to open the RocksDB instance and prepare it for
// use as a raft backend.
func New(options Options) (*RocksdbKV, error) {

	// we need two column families, one for stable store and one for log store:
	// stable : used for storing key configurations.
	// log 	  : used for storing logs in a durable fashion.
	cfNames := []string{defaultTable.String(), stableTable.String(), logTable.String()}

	defaultOpts := rocksdb.NewDefaultOptions()

	// global options
	globalOpts := rocksdb.NewDefaultOptions()
	globalOpts.SetCreateIfMissing(true)
	globalOpts.SetCreateIfMissingColumnFamilies(true)
	blockCache := rocksdb.NewDefaultLRUCache(512 * 1024 * 1024)
	var stats *rocksdb.Statistics
	if options.EnableStatistics {
		stats = rocksdb.NewStatistics()
		globalOpts.SetStatistics(stats)
	}

	// stable store options
	stableBbto := rocksdb.NewDefaultBlockBasedTableOptions()
	stableOpts := rocksdb.NewDefaultOptions()
	stableOpts.SetBlockBasedTableFactory(stableBbto)

	// log store options
	logBbto := rocksdb.NewDefaultBlockBasedTableOptions()
	logBbto.SetBlockSize(32 * 1024)
	logBbto.SetCacheIndexAndFilterBlocks(true)
	logBbto.SetBlockCache(blockCache)
	logOpts := rocksdb.NewDefaultOptions()
	logOpts.SetUseFsync(!options.NoSync)
	// dio := directIOSupported(options.Path)
	// if dio {
	// 	logOpts.SetUseDirectIOForFlushAndCompaction(true)
	// }
	logOpts.SetCompression(rocksdb.NoCompression)

	// in normal mode, by default, we try to minimize write amplification,
	// so we set:
	//
	// L0 size = 256MBytes * 2 (min_write_buffer_number_to_merge) * \
	//              8 (level0_file_num_compaction_trigger)
	//         = 4GBytes
	// L1 size close to L0, 4GBytes, max_bytes_for_level_base = 4GBytes,
	//   max_bytes_for_level_multiplier = 2
	// L2 size is 8G, L3 is 16G, L4 is 32G, L5 64G...
	//
	// note this is the size of a shard, and the content of the store is expected
	// to be compacted by raft.
	//
	logOpts.SetMaxSubCompactions(2) // TODO what's this?
	logOpts.SetEnablePipelinedWrite(true)
	logOpts.SetWriteBufferSize(256 * 1024 * 1024)
	logOpts.SetMinWriteBufferNumberToMerge(2)
	logOpts.SetLevel0FileNumCompactionTrigger(8)
	logOpts.SetLevel0SlowdownWritesTrigger(17)
	logOpts.SetLevel0StopWritesTrigger(24)
	logOpts.SetMaxWriteBufferNumber(5)
	logOpts.SetNumLevels(7)
	// MaxBytesForLevelBase is the total size of L1, should be close to
	// the size of L0
	logOpts.SetMaxBytesForLevelBase(4 * 1024 * 1024 * 1024)
	logOpts.SetMaxBytesForLevelMultiplier(2)
	// files in L1 will have TargetFileSizeBase bytes
	logOpts.SetTargetFileSizeBase(256 * 1024 * 1024)
	logOpts.SetTargetFileSizeMultiplier(1)
	// IO parallelism
	logOpts.SetMaxBackgroundCompactions(2)
	logOpts.SetMaxBackgroundFlushes(2)

	cfOpts := []*rocksdb.Options{defaultOpts, stableOpts, logOpts}
	db, cfHandles, err := rocksdb.OpenDBColumnFamilies(options.Path, globalOpts, cfNames, cfOpts)
	if err != nil {
		return nil, err
	}

	// read/write options
	wo := rocksdb.NewDefaultWriteOptions()
	wo.SetSync(!options.NoSync)
	ro := rocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	store := &RocksdbKV{
		db:         db,
		stats:      stats,
		path:       options.Path,
		cfHandles:  cfHandles,
		stableBbto: stableBbto,
		stableOpts: stableOpts,
		logBbto:    logBbto,
		logOpts:    logOpts,
		blockCache: blockCache,
		globalOpts: globalOpts,
		ro:         ro,
		wo:         wo,
	}

	if stats != nil {
		store.metrics = newRocksDBMetrics(store)
	}

	return store, nil
}

// Close closes the underlying Key-Value store.
func (kv *RocksdbKV) Close() error {

	for _, cf := range kv.cfHandles {
		cf.Destroy()
	}
	if kv.db != nil {
		kv.db.Close()
	}
	if kv.stableBbto != nil {
		kv.stableBbto.Destroy()
	}
	if kv.stableOpts != nil {
		kv.stableOpts.Destroy()
	}
	if kv.blockCache != nil {
		kv.blockCache.Destroy()
	}
	if kv.logBbto != nil {
		kv.logBbto.Destroy()
	}
	if kv.logOpts != nil {
		kv.logOpts.Destroy()
	}
	if kv.stats != nil {
		kv.stats.Destroy()
	}
	if kv.wo != nil {
		kv.wo.Destroy()
	}
	if kv.ro != nil {
		kv.ro.Destroy()
	}
	kv.db = nil
	return nil

}

// IterateValue iterates the key range specified by the first key fk and
// last key lk. The inc boolean flag indicates whether it is inclusive for
// the last key lk. For each iterated entry, the specified op will be invoked
// on that key-value pair, the specified op func returns a boolean flag to
// indicate whether IterateValue should continue to iterate entries.
func (kv *RocksdbKV) IterateValue(fk []byte, lk []byte, inc bool, op func(key []byte, data []byte) (bool, error)) error {
	iter := kv.db.NewIteratorCF(rocksdb.NewDefaultReadOptions(), kv.cfHandles[logTable])

	defer iter.Close()

	for iter.Seek(fk); ; iter.Next() {
		if !iter.Valid() {
			break
		}
		keySlice := iter.Key()
		key := make([]byte, keySlice.Size())
		copy(key, keySlice.Data())
		keySlice.Free()
		if inc {
			if bytes.Compare(key, lk) > 0 {
				return nil
			}
		} else {
			if bytes.Compare(key, lk) >= 0 {
				return nil
			}
		}

		valueSlice := iter.Value()
		value := make([]byte, valueSlice.Size())
		copy(value, valueSlice.Data())
		cont, err := op(key, value)
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}
	return nil
}

// GetValue queries the value specified the input key, the returned value
// byte slice is passed to the specified op func.
func (kv *RocksdbKV) GetValue(key []byte, op func([]byte) error) error {

	v, err := kv.db.GetBytesCF(kv.ro, kv.cfHandles[logTable], key)
	if err != nil {
		return err
	}
	if v == nil {
		return storage.ErrKeyNotFound
	}

	return op(v)
}

// Save value saves the specified key value pair to the underlying key-value
// pair.
func (kv *RocksdbKV) SaveValue(key []byte, value []byte) error {
	batch := rocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.PutCF(kv.cfHandles[logTable], key, value)
	return kv.db.Write(kv.wo, batch)
}

// DeleteValue deletes the key-value pair specified by the input key.
func (kv *RocksdbKV) DeleteValue(key []byte) error {
	batch := rocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.DeleteCF(kv.cfHandles[logTable], key)
	return kv.db.Write(kv.wo, batch)
}

// GetWriteBatch returns an IWriteBatch object to be used by RDB.
func (kv *RocksdbKV) GetWriteBatch(ctx raftio.IContext) *rocksdb.WriteBatch {
	if ctx != nil {
		wb := ctx.GetWriteBatch()
		if wb != nil {
			return ctx.GetWriteBatch().(*rocksdb.WriteBatch)
		}
	}
	return rocksdb.NewWriteBatch()
}

// CommitWriteBatch atomically writes everything included in the write batch
// to the underlying key-value store.
func (kv *RocksdbKV) CommitWriteBatch(wb *rocksdb.WriteBatch) error {
	return kv.db.Write(kv.wo, wb)
}

// CommitDeleteBatch atomically deletes everything included in the write
// batch from the underlying key-value store.
func (kv *RocksdbKV) CommitDeleteBatch(wb *rocksdb.WriteBatch) error {
	return kv.CommitWriteBatch(wb)
}

// BulkRemoveEntries removes entries specified by the range [firstKey,
// lastKey). BulkRemoveEntries is called in the main execution thread of raft,
// it is suppose to immediately return without significant delay.
// BulkRemoveEntries is usually implemented in KV store's range delete feature.
func (kv *RocksdbKV) BulkRemoveEntries(firstKey []byte, lastKey []byte) error {
	batch := rocksdb.NewWriteBatch()
	defer batch.Destroy()
	batch.DeleteRangeCF(kv.cfHandles[logTable], firstKey, lastKey)
	return kv.db.Write(kv.wo, batch)
}

// CompactEntries is called by the compaction goroutine for the specified
// range [firstKey, lastKey). CompactEntries is triggered by the
// BulkRemoveEntries method for the same key range.
func (kv *RocksdbKV) CompactEntries(firstKey []byte, lastKey []byte) error {
	if err := r.deleteRange(fk, lk); err != nil {
		return err
	}
	opts := gorocksdb.NewCompactionOptions()
	opts.SetExclusiveManualCompaction(false)
	opts.SetChangeLevel(true)
	opts.SetTargetLevel(-1)
	defer opts.Destroy()
	rng := gorocksdb.Range{
		Start: fk,
		Limit: lk,
	}
	kv.db.CompactRangeWithOptions(opts, rng)
	return nil
}

// FullCompaction compact the entire key space.
func (kv *RocksdbKV) FullCompaction() error {
	fk := make([]byte, kv.MaxKeyLength)
	lk := make([]byte, kv.MaxKeyLength)
	for i := uint64(0); i < kv.MaxKeyLength; i++ {
		fk[i] = 0
		lk[i] = 0xFF
	}
	opts := gorocksdb.NewCompactionOptions()
	opts.SetExclusiveManualCompaction(false)
	opts.SetChangeLevel(true)
	opts.SetTargetLevel(-1)
	defer opts.Destroy()
	rng := gorocksdb.Range{
		Start: fk,
		Limit: lk,
	}
	kv.db.CompactRangeWithOptions(opts, rng)
	return nil
}
