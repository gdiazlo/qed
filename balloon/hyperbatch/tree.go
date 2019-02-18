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
	"encoding/binary"
	"fmt"
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

type Node struct {
	ibatch    uint
	height    uint
	base      [32]byte
	index     [32]byte
	value     [32]byte
	key       [32]byte
	hash      [32]byte
	donotsave bool
	shortcut  bool
	isNew     bool
}

func (n Node) String() string {
	return fmt.Sprintf("(s %v n %v i %v h %v f %v b %v x %v k %v v %v d %v)", n.shortcut, n.isNew, n.ibatch, n.height, n.hash[0], n.base[0], n.index[0], n.key[0], n.value[0], n.donotsave)
}

func (n Node) Equal(o Node) bool {
	return n.hash == o.hash && n.key == o.key && n.value == o.value
}

func (n Node) Key() (key [40]byte) {
	copy(key[:], n.base[:])
	binary.LittleEndian.PutUint64(key[32:], uint64(n.height))
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
	copy(n.key[:], key)
	binary.LittleEndian.PutUint64(n.value[:], value)
	return n
}

func Pushdown(curr, dest Node) (Node, Node) {
	dest.hash = curr.hash
	dest.key = curr.key
	dest.value = curr.value
	dest.shortcut = true
	curr.shortcut = false
	dest.donotsave = false
	curr.donotsave = false
	return curr, dest
}

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

func Traverse(subtree Tree, current, destination Node, executor Visitor, carry bool) Node {
	var leftChild, rightChild Node
	var tr, tl Tree

	if subtree.IsLeaf(current) {
		current = executor.Visit(subtree, Leaf, current, destination)
		subtree.Set(current)
		return current
	}

	dir := current.Dir(destination)

	tr, rightChild = subtree.RightChild(dir, current, destination)
	tl, leftChild = subtree.LeftChild(dir, current, destination)

	if current.isNew && rightChild.isNew && leftChild.isNew {
		carry = false
	}

	if current.shortcut && current.Equal(destination) && !carry {
		current = executor.Visit(subtree, Shortcut, current, destination)
		subtree.Set(current)
		return current
	}

	if current.shortcut {
		pDir := current.Dir(current)
		switch {
		case pDir == Left && leftChild.isNew:
			current, leftChild = Pushdown(current, leftChild)
			tl.Set(leftChild)
			subtree.Reset(current)
			return Traverse(subtree, current, destination, executor, true)
		case pDir == Right && rightChild.isNew:
			current, rightChild = Pushdown(current, rightChild)
			tr.Set(rightChild)
			subtree.Reset(current)
			return Traverse(subtree, current, destination, executor, true)
		}

	}

	switch dir {
	case Left:
		leftChild = Traverse(tl, leftChild, destination, executor, carry)
	case Right:
		rightChild = Traverse(tr, rightChild, destination, executor, carry)
	}

	current = executor.Visit(subtree, dir, current, leftChild, rightChild)
	subtree.Set(current)
	return current
}
