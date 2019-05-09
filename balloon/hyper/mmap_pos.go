package hyper

import (
	"encoding/binary"
	"math/bits"

	"github.com/bbva/qed/util"
)

type BatchLocator struct {
	treeHeight  uint16
	batchHeight uint16
	batchSize   uint64
	offsets     []uint64
}

func NewBatchLocator(treeHeight, batchHeight uint16, batchSize uint64, batchLevels uint8) *BatchLocator {
	offsets := func() []uint64 {
		o := make([]uint64, batchLevels+1)
		n := uint64(0)
		for i := uint8(0); i < uint8(batchLevels); i++ {
			n = n + (1 << (i * uint8(batchHeight)))
			o[i+1] = n
		}
		return o
	}()

	loc := &BatchLocator{
		treeHeight:  treeHeight,
		batchHeight: batchHeight,
		batchSize:   batchSize,
		offsets:     offsets,
	}
	return loc
}

func (l BatchLocator) Seek(key []byte) uint64 {
	height := util.BytesAsUint16(key[0:2])
	index := key[2:]
	iPos := uint64(bits.Reverse32(binary.BigEndian.Uint32(index[0:4]))) - 1
	level := ((l.treeHeight - height) / l.batchHeight)
	iPos += l.offsets[level]
	return iPos * l.batchSize
}

func (l BatchLocator) BatchSize() uint64 {
	return l.batchSize
}
