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
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeSeeker struct{}

func (f fakeSeeker) Seek(key []byte) (uint64, error) {
	return uint64(uint8(key[0])), nil
}

func TestMmapCache(t *testing.T) {

	testCases := []struct {
		key    []byte
		value  []byte
		cached bool
	}{
		{[]byte{0x0, 0x0}, []byte{0x1}, true},
		{[]byte{0x1, 0x0}, []byte{0x2}, true},
		{[]byte{0x2, 0x0}, []byte{0x3}, false},
	}

	path, err := ioutil.TempDir("", "mmap-test")
	require.NoError(t, err, "Error creating temp dir")

	cache, err := NewMmapCache(path, uint64(len(testCases)), uint64(len(testCases[0].value)), &fakeSeeker{})
	require.NoError(t, err, "Error creating cache")

	for i, c := range testCases {
		if c.cached {
			cache.Put(c.key, c.value)
		}

		cachedValue, ok := cache.Get(c.key)

		if c.cached {
			require.Truef(t, ok, "The key should exists in cache in test case %d", i)
			require.Equalf(t, c.value, cachedValue, "The cached value should be equal to stored value in test case %d", i)
		} else {
			require.Falsef(t, ok, "The key should not exist in cache in test case %d", i)
		}
	}
}