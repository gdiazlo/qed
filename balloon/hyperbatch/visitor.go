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

type Visitor interface {
	Visit(Tree, Operation, Node, ...Node) Node
}

type Compute struct {
	h hashing.Hasher
}

func (c Compute) Visit(t Tree, d Operation, n Node, child ...Node) Node {
	if d == Leaf || d == Shortcut {
		copy(n.hash[:], c.h.Do(child[0].value[:]))
	} else {
		copy(n.hash[:], c.h.Do(child[0].hash[:], child[1].hash[:]))
	}

	return n
}

type position struct {
	ibatch, height uint
}

type state struct {
	treeId string
	p      position
	op     Operation
}

type Tester struct {
	path  []state
	graph []string
	c     Visitor
}

func (p *Tester) Visit(t Tree, op Operation, n Node, child ...Node) Node {
	p.path = append([]state{state{t.Id(), position{n.ibatch, n.height}, op}}, p.path...)
	n = p.c.Visit(t, op, n, child...)

	if op != Leaf && op != Shortcut {
		nf := "\"node%s\"[label=\"H %08b | B %08b | I %08b | %s\"]"
		cf := "\"node%s\" -> \"node%s\" [label=\"%v \"]"
		c0 := child[0]
		c1 := child[1]
		nid := fmt.Sprintf("%d-%d", n.ibatch, n.height)
		c0id := fmt.Sprintf("%d-%d", c0.ibatch, c0.height)
		c1id := fmt.Sprintf("%d-%d", c1.ibatch, c1.height)
		p.graph = append(p.graph, fmt.Sprintf(nf, nid, n.hash[:1], n.base[:1], n.index[:1], nid))
		p.graph = append(p.graph, fmt.Sprintf(nf, c0id, c0.hash[:1], c0.base[:1], c0.index[:1], c0id))
		p.graph = append(p.graph, fmt.Sprintf(nf, c1id, c1.hash[:1], c1.base[:1], c1.index[:1], c1id))
		p.graph = append(p.graph, fmt.Sprintf(cf, nid, c0id, t.Id()))
		p.graph = append(p.graph, fmt.Sprintf(cf, nid, c1id, t.Id()))
	}

	return n
}

func (p *Tester) print() {
	for _, s := range p.path {
		fmt.Printf("In %s pos %v next %v\n", s.treeId, s.p, s.op)
	}
	fmt.Println("")
}

func (p *Tester) dot() {
	fmt.Println("digraph G {")
	for _, s := range p.graph {
		fmt.Println(s)
	}
	fmt.Println("}")
}
