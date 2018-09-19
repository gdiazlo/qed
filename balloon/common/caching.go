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

package common

import (
	"github.com/bbva/qed/hashing"
)

type CachedElement struct {
	Pos    Position
	Digest hashing.Digest
}

func NewCachedElement(pos Position, digest hashing.Digest) *CachedElement {
	return &CachedElement{pos, digest}
}

type CachingVisitor struct {
	decorated PostOrderVisitor
	elements  []CachedElement
}

func NewCachingVisitor(decorated PostOrderVisitor) *CachingVisitor {
	return &CachingVisitor{
		decorated: decorated,
		elements:  make([]CachedElement, 0),
	}
}

func (v *CachingVisitor) Result() []CachedElement {
	return v.elements
}

func (v *CachingVisitor) VisitRoot(pos Position, leftResult, rightResult interface{}) interface{} {
	// by-pass
	return v.decorated.VisitRoot(pos, leftResult, rightResult).(hashing.Digest)
}

func (v *CachingVisitor) VisitNode(pos Position, leftResult, rightResult interface{}) interface{} {
	// by-pass
	return v.decorated.VisitNode(pos, leftResult, rightResult).(hashing.Digest)
}

func (v *CachingVisitor) VisitPartialNode(pos Position, leftResult interface{}) interface{} {
	// by-pass
	return v.decorated.VisitPartialNode(pos, leftResult)
}

func (v *CachingVisitor) VisitLeaf(pos Position, eventDigest []byte) interface{} {
	// by-pass
	return v.decorated.VisitLeaf(pos, eventDigest).(hashing.Digest)
}

func (v *CachingVisitor) VisitCached(pos Position, cachedDigest hashing.Digest) interface{} {
	// by-pass
	return v.decorated.VisitCached(pos, cachedDigest)
}

func (v *CachingVisitor) VisitCollectable(pos Position, result interface{}) interface{} {
	element := NewCachedElement(pos, result.(hashing.Digest))
	v.elements = append(v.elements, *element)
	return result
}