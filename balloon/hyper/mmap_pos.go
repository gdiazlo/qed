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

package hyper

import (
	"encoding/binary"
	"math/bits"

	"github.com/bbva/qed/util"
)

type BatchSeeker struct {
	treeHeight  uint16
	batchHeight uint16
	batchSize   uint64
	offsets     []uint64
}

func NewBatchSeeker(treeHeight, batchHeight uint16, batchSize uint64, batchLevels uint8) *BatchSeeker {
	offsets := func() []uint64 {
		o := make([]uint64, batchLevels+1)
		n := uint64(0)
		for i := uint8(0); i < uint8(batchLevels); i++ {
			n = n + (1 << (i * uint8(batchHeight)))
			o[i+1] = n
		}
		return o
	}()

	loc := &BatchSeeker{
		treeHeight:  treeHeight,
		batchHeight: batchHeight,
		batchSize:   batchSize,
		offsets:     offsets,
	}
	return loc
}

func (l BatchSeeker) Seek(key []byte) (uint64, error) {
	height := util.BytesAsUint16(key[0:2])
	index := key[2:]

	iPos := uint64(bits.Reverse32(binary.BigEndian.Uint32(index[0:4])))
	level := ((l.treeHeight - height) / l.batchHeight)
	iPos += l.offsets[level]

	return iPos * l.batchSize, nil
}
