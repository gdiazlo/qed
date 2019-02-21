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
	"fmt"

	"github.com/bbva/qed/hashing"
)

func genDHT(h hashing.Hasher) [][]byte {
	dht := make([][]byte, h.Len())
	dht[0] = h.Do([]byte{0x0})
	for i := 1; i < int(h.Len()); i++ {
		sum := h.Do(append(dht[i-1][:], dht[i-1][:]...))
		dht[i] = sum[:]
	}
	return dht
}

type NextTreeFn func(Node) Tree
type PersistFn func()

type Subtree struct {
	id        string
	batch     Store
	nBits     uint
	size      uint
	dht       [][]byte
	next      NextTreeFn
	shortcuts bool
}

func (s Subtree) Id() string {
	return s.id
}

var ErrNotFound error = fmt.Errorf("subtree.Get(): Node not found")
var ErrShortcutVal error = fmt.Errorf("subtree.Get(): Shortcut value here, when access directly its an empty node")

func (s *Subtree) Get(n Node) (Node, error) {
	r, v := s.batch.Get(n.ibatch)
	switch r {
	case EmptyRecord:
		return n, ErrNotFound
	case ShortcutRecord:
		copy(n.hash[:], v[:])
		n.shortcut = true
		_, n.key = s.batch.Get(2*n.ibatch + 1)
		_, n.value = s.batch.Get(2*n.ibatch + 2)
	case NodeRecord:
		copy(n.hash[:], v[:])
	case ShortcutValRecord:
		n.isNew = true
		return n, ErrShortcutVal
	}

	return n, nil
}

func (s *Subtree) Reset(n Node) {
	s.batch.Unset(n.ibatch)
}

func (s *Subtree) Set(n Node) {
	if n.donotsave {
		s.batch.Unset(n.ibatch)
		return
	}

	if n.shortcut {
		s.batch.Set(ShortcutRecord, n.ibatch, n.hash[:])
		s.batch.Set(ShortcutValRecord, 2*n.ibatch+1, n.key)
		s.batch.Set(ShortcutValRecord, 2*n.ibatch+2, n.value)
		return
	}
	s.batch.Set(NodeRecord, n.ibatch, n.hash[:])
}

func (s *Subtree) LoadNode(isShortcut bool, n, d Node) Node {
	var err error
	n, err = s.Get(n)
	if err != nil {
		n.isNew = true
		if isShortcut {
			n.hash = d.hash
			n.key = d.key
			n.value = d.value
			n.shortcut = true
			return n
		}
		copy(n.hash[:], s.dht[n.height][:])
		n.donotsave = true
		return n
	}
	n.isNew = false
	return n
}

func (s *Subtree) LeftChild(op Operation, current, destination Node) (Tree, Node) {
	var leftChild Node
	var t Tree

	leftChild.ibatch = 2*current.ibatch + 1
	leftChild.height = current.height - 1

	leftChild.base = current.base
	leftChild.index = current.base
	leftChild.index[leftChild.height>>3] |= 1 << ((leftChild.height) & 7)

	if t = s.NextTree(leftChild); t == nil {
		t = s
	} else {
		leftChild.ibatch = 0
	}
	return t, t.LoadNode(op == Left && s.shortcuts, leftChild, destination)
}

func (s *Subtree) RightChild(op Operation, current, destination Node) (Tree, Node) {
	var rightChild Node
	var t Tree

	rightChild.ibatch = 2*current.ibatch + 2
	rightChild.height = current.height - 1

	rightChild.base = current.index
	rightChild.index = current.index
	rightChild.index[rightChild.height>>3] |= 1 << ((rightChild.height) & 7)

	if t = s.NextTree(rightChild); t == nil {
		t = s
	} else {
		rightChild.ibatch = 0
	}

	return t, t.LoadNode(op == Right && s.shortcuts, rightChild, destination)
}

func (s *Subtree) IsLeaf(n Node) bool {
	return n.height == 0
}

func (s *Subtree) NextTree(n Node) Tree {
	return s.next(n)
}

func (s *Subtree) Root(current, destination Node) Node {
	current.ibatch = 0
	current, err := s.Get(current)
	if err != nil && s.shortcuts {
		current.isNew = true
		current.key = destination.key
		current.value = destination.value
		current.shortcut = true
	}
	return current
}

func NewSubtree(id string, batch Store, dht [][]byte, nbits uint, shortcuts bool, fn NextTreeFn) *Subtree {
	return &Subtree{
		id:        id,
		batch:     batch,
		nBits:     nbits,
		size:      (1 << nbits) - 1,
		dht:       dht,
		next:      fn,
		shortcuts: shortcuts,
	}
}
