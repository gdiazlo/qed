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
	"bytes"
	"encoding/binary"
	"fmt"
)

type Node struct {
	ibatch    uint
	height    uint
	base      [32]byte
	index     [32]byte
	value     []byte
	key       []byte
	hash      []byte
	donotsave bool
	shortcut  bool
	isNew     bool
}

func (n Node) String() string {
	return fmt.Sprintf("(s %v n %v i %v h %v f %v b %v x %v k %v v %v d %v)", n.shortcut, n.isNew, n.ibatch, n.height, n.hash[0], n.base[0], n.index[0], n.key[0], n.value[0], n.donotsave)
}

func (n Node) Equal(o Node) bool {
	return bytes.Compare(n.hash, o.hash) == 0 && bytes.Compare(n.key, o.key) == 0 && bytes.Compare(n.value, o.value) == 0
}

func (n Node) Key() (k Key) {
	copy(k[:], n.base[:])
	binary.LittleEndian.PutUint64(k[32:], uint64(n.height))
	return
}

func (src Node) Dir(dst Node) Operation {
	bit := src.height - 1
	if dst.key[bit>>3]&(1<<(bit&7)) == 0 {
		return Left
	}

	return Right
}

func NewNode(key []byte, value uint64) Node {
	var n Node
	n.key = key
	n.value = make([]byte, 8)
	binary.LittleEndian.PutUint64(n.value[:], value)
	return n
}
