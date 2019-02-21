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
	"github.com/bbva/qed/hashing"
	"github.com/bbva/qed/storage"
)

type Operation int

func (op Operation) String() string {
	switch op {
	case Left:
		return "Left"
	case Right:
		return "Right"
	case Leaf:
		return "Leaf"
	case Shortcut:
		return "Shortcut"
	}
	return "Unknown"
}

const (
	Left Operation = iota
	Right
	Leaf
	Shortcut
)

type Tree interface {
	Id() string
	LeftChild(Operation, Node, Node) (Tree, Node)
	RightChild(Operation, Node, Node) (Tree, Node)
	LoadNode(bool, Node, Node) Node
	IsLeaf(Node) bool
	Set(Node)
	Get(Node) (Node, error)
	Reset(Node)
	Root(Node, Node) Node
}

type SMT struct {
	next    NextTreeFn
	persist PersistFn
	tree    Tree
	root    Node
	hasher  hashing.Hasher
}

func NewCachedSMT(batchHeight, cacheHeight uint, db, cache storage.Store, hasher hashing.Hasher) (s SMT) {
	batches := make(Batches)
	cached := make(Batches)
	dht := genDHT(hasher)

	s.next = newCachedNextTreeFn(batchHeight, cacheHeight, batches, cached, db, cache, dht)
	s.persist = newCachedPersistFn(batches, cached, db, cache)

	s.root = NewNode([]byte{}, 0)
	s.root.index[0] = 0x80
	s.root.height = 8

	s.tree = s.next(s.root)
	s.hasher = hasher

	return s
}

func (s SMT) Insert(key []byte, value uint64) []byte {
	dst := newDest(s.hasher, key, value)
	n := Traverse(s.tree, s.tree.Root(s.root, dst), dst, &Compute{h: s.hasher}, false)
	return n.hash[:]
}

func (s SMT) Prove() [][]byte {

	return nil
}