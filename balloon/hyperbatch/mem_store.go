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

type Record byte

const (
	EmptyRecord       Record = 0x0
	NodeRecord        Record = 0x1
	ShortcutRecord    Record = 0x2
	ShortcutValRecord Record = 0x3
)

type MemStore struct {
	mem           [][33]byte
	reads, writes uint64
}

func NewMemStore(bits uint) *MemStore {
	store := make([][33]byte, (2<<bits)-1)
	return &MemStore{mem: store}
}

func (c *MemStore) Len() int {
	return len(c.mem)
}

func (c *MemStore) Get(i uint) (r Record, val [32]byte) {
	c.reads++
	r = Record(c.mem[i][0])
	switch r {
	case EmptyRecord:
		return
	case NodeRecord:
		copy(val[:], c.mem[i][1:])
	case ShortcutRecord:
		copy(val[:], c.mem[i][1:])
	case ShortcutValRecord:
		copy(val[:], c.mem[i][1:])
	}

	return
}

func (c *MemStore) Set(r Record, i uint, val [32]byte) {
	c.writes++
	c.mem[i][0] = byte(r)
	copy(c.mem[i][1:], val[:])
}

func (c *MemStore) Unset(i uint) {
	c.mem[i][0] = byte(EmptyRecord)
	c.writes--
}

func (c *MemStore) Marshal() []byte {
	m := make([]byte, len(c.mem)+1>>3)
	for i := 0; i < len(c.mem); i++ {
		if c.mem[i][0] != byte(EmptyRecord) {
			// set bit
			m[i>>3] |= 1 << (uint(i) & 7)
			m = append(m, c.mem[i][:]...)
		}
	}
	return m
}

func (c *MemStore) Unmarshal(m []byte) {
	step := len(c.mem[0])
	offset := len(c.mem) + 1>>3
	j := 0
	for i := uint(0); i < uint(len(c.mem)); i++ {
		// is bit set?
		if m[i>>3]&(1<<(i&7)) != 0 {
			start := offset + step*j
			copy(c.mem[i][:], m[start:start+step])
			j++
		}
	}
}
