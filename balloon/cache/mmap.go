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
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/bbva/qed/storage"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

type BatchLocator interface {
	Seek(key []byte) uint64
	BatchSize() uint64
}

type MmapCache struct {
	path       string
	fd         *os.File // file descriptor for the memory-mapped file
	fmap       []byte
	entryCount int
	// This is a lock on the log file. It guards the fd’s value, the file’s
	// existence and the file’s memory map.
	//
	// Use shared ownership when reading/writing the file or memory map, use
	// exclusive ownership to open/close the descriptor, unmap or remove the file.
	lock    sync.RWMutex
	locator BatchLocator
}

func NewMmapCache(path string, size int64, locator BatchLocator) (*MmapCache, error) {
	c := &MmapCache{
		path:    path,
		locator: locator,
	}
	filePath := path + "/hypercache.dat"
	flags := os.O_RDWR | os.O_CREATE | os.O_EXCL
	var err error
	c.fd, err = os.OpenFile(filePath, flags, 0660)
	if err != nil {
		return nil, err
	}
	if err = c.mmap(size); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *MmapCache) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
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
	c.lock.RLock()
	defer c.lock.RUnlock()
	if value := c.fmap[c.locator.Seek(key):c.locator.BatchSize()]; string(value[0:4]) != "" {
		return value, true
	}
	return nil, false
}

func (c *MmapCache) Put(key []byte, value []byte) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if value := c.fmap[c.locator.Seek(key):c.locator.BatchSize()]; string(value[0:4]) == "" {
		c.entryCount++
	}
	copy(c.fmap[c.locator.Seek(key):], value)
}

func (c *MmapCache) Fill(r storage.KVPairReader) (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return nil
}

func (c MmapCache) Size() int {
	return c.entryCount
}

func (c MmapCache) Equal(o *MmapCache) bool {
	return bytes.Equal(c.fmap, o.fmap)
}

func (c *MmapCache) mmap(size int64) (err error) {
	c.fmap, err = Mmap(c.fd, false, size)
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
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
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
