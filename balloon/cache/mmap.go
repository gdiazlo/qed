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

package cache

import (
	"bytes"
	"fmt"
	"os"
	"sync"

	"github.com/bbva/qed/storage"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

type Seeker interface {
	Seek([]byte) (uint64, error)
}

type MmapCache struct {
	path       string
	fd         *os.File // file descriptor for the memory-mapped file
	fmap       []byte
	entryCount int
	bucketSize uint64
	// This is a lock on the log file. It guards the fd’s value, the file’s
	// existence and the file’s memory map.
	//
	// Use shared ownership when reading/writing the file or memory map, use
	// exclusive ownership to open/close the descriptor, unmap or remove the file.
	lock sync.RWMutex
	Seeker
}

func NewMmapCache(path string, numBuckets, bucketSize uint64, seek Seeker) (*MmapCache, error) {
	c := &MmapCache{
		path:       path,
		bucketSize: bucketSize,
		Seeker:     seek,
	}

	filePath := path + "/hypercache.dat"
	flags := os.O_RDWR | os.O_CREATE | os.O_EXCL
	var err error
	c.fd, err = os.OpenFile(filePath, flags, 0660)
	if err != nil {
		return nil, err
	}

	if err := c.fd.Truncate(int64(numBuckets * bucketSize)); err != nil {
		return nil, err
	}

	c.fmap, err = unix.Mmap(int(c.fd.Fd()), 0, int(numBuckets*bucketSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|(31<<unix.MAP_HUGE_SHIFT)|unix.MAP_POPULATE)
	if err != nil {
		return nil, err
	}

	unix.Madvise(c.fmap, unix.MADV_WILLNEED)
	unix.Mlock(c.fmap)
	unix.Munlock(c.fmap)

	return c, nil
}

func (c *MmapCache) Close() error {
	var err error
	if munmapErr := c.munmap(); munmapErr != nil {
		err = munmapErr
	}
	if closeErr := c.fd.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}

func (c MmapCache) Get(key []byte) ([]byte, bool) {
	offset, err := c.Seek(key)
	if err != nil {
		panic(fmt.Sprintf("You are trying to seek for the offset of a non-cacheable key: %d", key))
	}
	var value []byte
	if value = c.fmap[offset : offset+c.bucketSize]; bytes.Count(value, []byte{0x0}) != len(value) {
		return value, true
	}

	return nil, false
}

func (c *MmapCache) Put(key []byte, value []byte) {
	block := value

	offset, err := c.Seek(key)
	if err != nil {
		panic(fmt.Sprintf("You are trying to seek for the offset of a non-cacheable key: %d", key))
	}
	if prev := c.fmap[offset : offset+c.bucketSize]; bytes.Count(prev, []byte{0x0}) == len(block) {
		c.entryCount++
	}
	copy(c.fmap[offset:], block)
	// unix.Msync(c.fmap, unix.MS_ASYNC)
}

func (c *MmapCache) Fill(r storage.KVPairReader) (err error) {
	return nil
}

func (c MmapCache) Size() int {
	return c.entryCount
}

func (c MmapCache) Equal(o *MmapCache) bool {
	return bytes.Equal(c.fmap, o.fmap)
}

func (c *MmapCache) munmap() (err error) {
	if err := unix.Munmap(c.fmap); err != nil {
		return errors.Wrapf(err, "Unable to munmap cache: %q", c.path)
	}
	return nil
}
