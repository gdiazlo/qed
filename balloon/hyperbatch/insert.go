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

func pushdown(curr, dest Node) (Node, Node) {
	dest.hash = curr.hash
	dest.key = curr.key
	dest.value = curr.value
	dest.shortcut = true
	curr.shortcut = false
	dest.donotsave = false
	curr.donotsave = false
	return curr, dest
}

func Traverse(tree Tree, cur, dst Node, executor Visitor, carry bool) Node {
	var leftChild, rightChild Node
	var rightTree, leftTree Tree

	if tree.IsLeaf(cur) {
		cur = executor.Visit(tree, Leaf, cur, dst)
		tree.Set(cur)
		return cur
	}

	dir := cur.Dir(dst)

	rightTree, rightChild = tree.RightChild(dir, cur, dst)
	leftTree, leftChild = tree.LeftChild(dir, cur, dst)

	if cur.isNew && rightChild.isNew && leftChild.isNew {
		carry = false
	}

	if cur.shortcut && cur.Equal(dst) && !carry {
		cur = executor.Visit(tree, Shortcut, cur, dst)
		tree.Set(cur)
		return cur
	}

	if cur.shortcut {
		pDir := cur.Dir(cur)
		switch {
		case pDir == Left && leftChild.isNew:
			cur, leftChild = pushdown(cur, leftChild)
			leftTree.Set(leftChild)
			tree.Reset(cur)
			return Traverse(tree, cur, dst, executor, true)
		case pDir == Right && rightChild.isNew:
			cur, rightChild = pushdown(cur, rightChild)
			rightTree.Set(rightChild)
			tree.Reset(cur)
			return Traverse(tree, cur, dst, executor, true)
		}

	}

	switch dir {
	case Left:
		leftChild = Traverse(leftTree, leftChild, dst, executor, carry)
	case Right:
		rightChild = Traverse(rightTree, rightChild, dst, executor, carry)
	}

	cur = executor.Visit(tree, dir, cur, leftChild, rightChild)
	tree.Set(cur)
	return cur
}
