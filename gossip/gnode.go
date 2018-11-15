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

package gossip

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bbva/qed/log"
	"github.com/cnf/structhash"
	"github.com/hashicorp/memberlist"
)

type NodeType uint8

const (
	AuditorNode NodeType = 1
	MonitorNode NodeType = 2
	ServerNode  NodeType = 3
)

type Config struct {
	SendQueueSize        int
	RecvQueueSize        int
	BatchMsgSize         int
	GossipPeersToSend    int
	BatchSelectionSize   int
	QedSimulatorInterval time.Duration
	RendevouzInterval    time.Duration
	SenderInterval       time.Duration
}

var DefaultConfig Config = Config{
	SendQueueSize:        100,
	RecvQueueSize:        100,
	BatchMsgSize:         100,
	GossipPeersToSend:    2,
	BatchSelectionSize:   10,
	QedSimulatorInterval: 10 * time.Second,
	RendevouzInterval:    1 * time.Second,
	SenderInterval:       2 * time.Second,
}

type NodeAddr struct {
	id       string // unique name for node. If not set, fallback to hostname
	kind     NodeType
	bindAddr string // gossip bind address
	joinAddr string // comma-delimited list of nodes, through which a cluster can be joined (protocol://host:port)
}

func NewNodeAddr(id, bindAddr, joinAddr string, kind NodeType) *NodeAddr {
	return &NodeAddr{
		id,
		kind,
		bindAddr,
		joinAddr,
	}
}

type GNode struct {
	node   *NodeAddr
	config *Config

	members    *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue

	sendQueue chan BatchMsg // Batches already processed to send to others
	recvQueue chan BatchMsg // Batches arriving at this node
}

func NewGNode(n *NodeAddr, c *Config) (*GNode, error) {

	gnode := &GNode{
		node:      n,
		config:    c,
		sendQueue: make(chan BatchMsg, c.SendQueueSize),
		recvQueue: make(chan BatchMsg, c.RecvQueueSize),
	}

	return gnode, nil
}

func (gn *GNode) Start() error {

	log.Info("Start(): Starting GNode...")

	config := memberlist.DefaultLocalConfig()
	config.Name = gn.node.id
	config.Delegate = gn
	// this does not work as I want...
	config.Logger = log.GetLogger()
	// set bind address and port
	host, port, err := net.SplitHostPort(gn.node.bindAddr)
	if err != nil {
		return err
	}
	config.BindPort, err = strconv.Atoi(port)
	if err != nil {
		return err
	}
	config.BindAddr = host

	// register callback delegates

	// create the initial memberlist
	gn.members, err = memberlist.Create(config)
	if err != nil {
		return err
	}

	// join the existing cluster
	log.Debugf("Start(): Trying to join the cluster using members: %v", gn.node.joinAddr)
	if len(gn.node.joinAddr) > 0 {
		parts := strings.Split(gn.node.joinAddr, ",")
		contacted, err := gn.members.Join(parts)
		if err != nil {
			return err
		}
		log.Debugf("Start(): Number of nodes contacted: %d", contacted)
	}

	// Ask for members of the cluster
	for _, member := range gn.members.Members() {
		log.Debugf("Start(): Member: %s %s:%d", member.Name, member.Addr, member.Port)
	}

	// Print local member info
	node := gn.members.LocalNode()
	log.Debug("Start(): Local member %s:%d", node.Addr, node.Port)

	// Set broadcast queue
	gn.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return gn.members.NumMembers()
		},
		RetransmitMult: 0,
	}
	// QED emision generation
	if gn.node.kind == ServerNode {
		go gn.qedSimulator()
	}
	go gn.rendevouz()
	awaitTermSignal(gn.Stop)

	log.Debugf("Start(): Stopping auditor, about to exit...")

	return nil
}

func (gn *GNode) qedSimulator() {
	log.Info("qedSimulator(): Starting QED snapshot generation")
	i := uint64(0)
	for {
		batch := newBatch(i, 100)
		msg, err := Encode(BatchMsgType, batch)
		if err != nil {
			log.Debugf("Error encoding batch message due to error: %v", err)
			continue
		}
		time.Sleep(10 * time.Second)

		nodes := gn.withoutMe(gn.members.Members())
		if len(nodes) < gn.config.GossipPeersToSend {
			continue
		}

		// this is not a crypto rand...its an operative one
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		nextHops := r.Perm(len(nodes))

		for i, _ := range nextHops[0:gn.config.GossipPeersToSend] {
			gn.members.SendToTCP(nodes[i], msg)
		}

		i++
	}
}

func (gn *GNode) Stop() {
	if err := gn.members.Leave(2 * time.Second); err != nil {
		log.Errorf("Stop(): Error while leaving cluster: %v", err)
	}
	if err := gn.members.Shutdown(); err != nil {
		log.Fatalf("Stop(): Unable to gracefully shutdown node: %v", err)
	}
}

func awaitTermSignal(closeFn func()) {

	signals := make(chan os.Signal, 1)
	// sigint: Ctrl-C, sigterm: kill command
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// block main and wait for a signal
	sig := <-signals
	log.Infof("awaitTermSignal(): Signal received: %v", sig)

	closeFn()

}

func newSnapshot(v uint64) *SnapshotMsg {
	return &SnapshotMsg{
		HistoryDigest: []byte{0x01},
		HyperDigest:   []byte{0x01},
		Version:       v,
		EventDigest:   []byte{0x01},
	}
}

func newBatch(id, size uint64) BatchMsg {
	var b BatchMsg
	start := id * size
	for j := start; j < start+size; j++ {
		b.Snaps = append(b.Snaps, newSnapshot(j))
	}
	b.Id = id
	return b
}

// Delegate interface

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (gn *GNode) NodeMeta(limit int) []byte {
	return []byte{}
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed
func (gn *GNode) NotifyMsg(msg []byte) {
	if len(msg) == 0 {
		return
	}

	// decode msg
	msgType := MsgType(msg[0])

	switch msgType {
	case SnapshotMsgType:
		var snapshot SnapshotMsg
		if err := Decode(msg[1:], &snapshot); err != nil {
			log.Errorf("NotifyMsg(): Error decoding snapshot: %v", err)
		}
		log.Debugf("Snapshot recieved: %v", snapshot)
	case BatchMsgType:
		var b BatchMsg
		if err := Decode(msg[1:], &b); err != nil {
			log.Errorf("NotifyMsg(): Error decoding snapshot: %v", err)
		}
		if b.Ttl <= 20 {
			gn.recvQueue <- b
		} else {
			fmt.Println("Message ", b.Id, " discarted with  ", len(b.Snaps), " remaining snaps and ttl 20")
		}
		log.Debugf("NotifyMsg(): Batch %v received and enqueued", b.Id, len(b.Snaps))
	default:
	}
}

func (gn *GNode) randomSelection(batch *BatchMsg) BatchMsg {
	var selected BatchMsg
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	selected.Id = batch.Id
	selection := r.Perm(len(batch.Snaps))[0:gn.config.BatchSelectionSize]

	for _, i := range selection {
		selected.Snaps = append(selected.Snaps, batch.Snaps[i])
	}
	batch = deleteElem(batch, selection)
	return selected
}

func (gn *GNode) first10Selection(batch *BatchMsg) BatchMsg {
	var selected BatchMsg
	selected.Id = batch.Id
	selected.Snaps = batch.Snaps[0:gn.config.BatchSelectionSize]
	batch.Snaps = batch.Snaps[gn.config.BatchSelectionSize:]
	return selected
}

func (gn *GNode) rendevouz() {
	ticker := time.NewTicker(gn.config.RendevouzInterval)
	recv := make(map[string]bool)

	for {
		select {
		case batch := <-gn.recvQueue:
			key, _ := structhash.Hash(batch.Snaps, int(batch.Id))
			if _, ok := recv[key]; ok {
				log.Debugf("rendevouz(): Batch %v already seen with id %v\n", batch.Id, key)
				break
			}
			var selected BatchMsg
			if len(batch.Snaps) > gn.config.BatchSelectionSize {
				// selected = gn.randomSelection(&batch)
				selected = gn.first10Selection(&batch)
				log.Debugf("rendevouz(): New batch %v with %v snaps", batch.Id, len(batch.Snaps))
				batch.Ttl += 1
				gn.sendQueue <- batch
			} else {
				selected = batch
				fmt.Println("rendevouz() ended batch ", batch.Id)
			}
			go gn.process(selected, key)
			recv[key] = true
		case <-ticker.C:
			log.Debugf("rendevouz(): send tick: sendQueue: %v, recvQueue: %v", len(gn.sendQueue), len(gn.recvQueue))
			go gn.sender()
		}
	}
}

func (gn *GNode) process(b BatchMsg, key string) {
	for i := 0; i < len(b.Snaps); i++ {
		if b.Snaps[i] == nil {
			continue
		}
		res, err := http.Get(fmt.Sprintf("http://127.0.0.1:8080/%d", b.Id))
		if err != nil || res == nil {
			log.Debugf("Error contacting service with error %v", err)
		}
		// to reuse connections we need to do this
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()

		time.Sleep(1 * time.Second)
	}

	log.Debugf("process(): Processed %v elements of batch id %v", len(b.Snaps), key)
}

func (gn *GNode) sender() {
	var b BatchMsg
	for {
		select {
		case b = <-gn.sendQueue:
		default:
			return
		}

		log.Debugf("sender(): sending batch %v", b.Id)
		if len(b.Snaps) == 0 {
			log.Infof("sender(): batch %v has no snaps, remove it from the sendQueue", b.Id)
			continue
		}

		// TODO: what's the strategy if there are not enough nodes, or not nodes at all?
		nodes := gn.withoutMe(gn.members.Members())

		if len(nodes) < gn.config.GossipPeersToSend {
			fmt.Println("sender(): batch", b.Id, ",has no snaps, remove it from the sendQueue")
			log.Infof("sender(): not enough nodes avaiable!! dropping batches!!")
			continue
		}

		// this is not a crypto rand...its an operative one
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		nextHops := r.Perm(len(nodes))
		b.Ttl += 1
		msg, _ := Encode(BatchMsgType, b)
		for _, i := range nextHops[0:gn.config.GossipPeersToSend] {
			log.Debugf("sender() Sending batch %d with %d snaps to %v", b.Id, len(b.Snaps), nodes[i].Address())
			gn.members.SendToTCP(nodes[i], msg)
		}
		time.Sleep(gn.config.SenderInterval)
	}
}

func (gn *GNode) withoutMe(nodes []*memberlist.Node) []*memberlist.Node {
	var new []*memberlist.Node
	for _, n := range nodes {
		if n.Address() != gn.node.bindAddr {
			new = append(new, n)
		}
	}
	return new
}

func deleteElem(b *BatchMsg, selection []int) *BatchMsg {
	sort.Ints(selection)
	for i := 0; i < len(selection); i++ {
		pos := selection[i] - i
		copy(b.Snaps[pos:], b.Snaps[pos+1:])
		b.Snaps[len(b.Snaps)-1] = nil // or the zero value of T
		b.Snaps = b.Snaps[:len(b.Snaps)-1]
	}
	return b
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
func (gn *GNode) GetBroadcasts(overhead, limit int) [][]byte {
	return gn.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (gn *GNode) LocalState(join bool) []byte {
	msg, _ := Encode(MetaMsgType, *gn.node)
	return msg
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (gn *GNode) MergeRemoteState(buf []byte, join bool) {

}
