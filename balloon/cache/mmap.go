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
	"syscall"
	"unsafe"

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
	/* c.fmap = make([]byte, numBuckets*bucketSize)
	return c, nil */

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

	if err = c.mmap(int64(numBuckets * bucketSize)); err != nil {
		return nil, err
	}
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
	unix.Msync(c.fmap, unix.MS_ASYNC)
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

func (c *MmapCache) mmap(size int64) (err error) {
	c.fmap, err = Mmap(c.fd, true, size)
	if err == nil {
		err = Madvise(c.fmap, false) // disable readahead
	}
	return err
}

func (c *MmapCache) munmap() (err error) {
	if err := Munmap(c.fmap); err != nil {
		return errors.Wrapf(err, "Unable to munmap cache: %q", c.path)
	}
	return nil
}

// Mmap uses the mmap system call to memory-map a file. If writable is true,
// memory protection of the pages is set so that they may be written to
// as well.
func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_PRIVATE)
}

// Munmap unmaps a previously mapped slice.
func Munmap(b []byte) error {
	return unix.Munmap(b)
}

// Madvise uses the madvise system call to give advise about the use of memory
// when using a slice that is memory-mapped to a file. Set the readahead flag to
// false if page references are expected in random order.
func Madvise(b []byte, readahead bool) error {
	flags := unix.MADV_NORMAL
	if !readahead {
		flags = unix.MADV_RANDOM
	}
	return madvise(b, flags)
}

// This is required because the unix package does not support the madvise system call on OS X.
func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
