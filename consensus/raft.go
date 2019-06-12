/*
   Copyright 2018-2019 Banco Bilbao Vizcaya Argentaria, S.A.

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

package consensus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bbva/qed/balloon"
	"github.com/bbva/qed/hashing"
	"github.com/bbva/qed/log"
	"github.com/bbva/qed/metrics"
	"github.com/bbva/qed/protocol"
	"github.com/bbva/qed/storage"
	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/config"
	"github.com/lni/dragonboat/statemachine"
)

const (
	retainSnapshotCount = 2
	leaderWaitDelay     = 1 * time.Second
	raftLogCacheSize    = 512
)

var (
	// ErrBalloonInvalidState is returned when a Balloon is in an invalid
	// state for the requested operation.
	ErrBalloonInvalidState = errors.New("balloon not in valid state")

	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")
)

type NodeInfo struct {
	HTTPAddr    string
	RaftAddr    string
	MgmtAddr    string
	MetricsAddr string
}

type Metadata struct {
	NodeId   uint64
	LeaderId uint64
	Nodes    map[uint64]*NodeInfo
}

// RaftBalloon is the interface Raft-backed balloons must implement.
type RaftBalloonApi interface {
	Add(event []byte) (*balloon.Snapshot, error)
	AddBulk(bulk [][]byte) ([]*balloon.Snapshot, error)
	QueryDigestMembershipConsistency(keyDigest hashing.Digest, version uint64) (*balloon.MembershipProof, error)
	QueryMembershipConsistency(event []byte, version uint64) (*balloon.MembershipProof, error)
	QueryDigestMembership(keyDigest hashing.Digest) (*balloon.MembershipProof, error)
	QueryMembership(event []byte) (*balloon.MembershipProof, error)
	QueryConsistency(start, end uint64) (*balloon.IncrementalProof, error)
	Join(nodeId, clusterId uint64, addr string) error
	Info() (*Metadata, error)
	Metadata() error
}

// RaftBalloon is a replicated verifiable key-value store, where changes are made via Raft consensus.
type RaftBalloon struct {
	conf           *Config
	clusterConfig  config.Config
	nodeHostConfig config.NodeHostConfig
	nodeHost       *dragonboat.NodeHost

	sync.Mutex

	closed bool
	wg     sync.WaitGroup
	done   chan struct{}

	fsm         *BalloonFSM             // balloon's finite state machine
	snapshotsCh chan *protocol.Snapshot // channel to publish snapshots

	metrics *raftBalloonMetrics
}

// NewRaftBalloon returns a new RaftBalloon.
func NewRaftBalloon(conf *Config, store storage.ManagedStore, snapshotsCh chan *protocol.Snapshot) (*RaftBalloon, error) {

	clusterConfig := config.Config{
		NodeID:             conf.NodeId,
		ClusterID:          conf.ClusterId,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    100000, // TODO set this to the "size of a complete SST file"-equivalent
		CompactionOverhead: 5,
	}

	nodeHostConfig := config.NodeHostConfig{
		WALDir:         conf.RaftPath + "/wal",
		NodeHostDir:    conf.RaftPath + "/nh",
		RTTMillisecond: 200,
		RaftAddress:    conf.RaftAddr,
	}

	// Instantiate balloon FSM
	fsm, err := NewBalloonFSM(store, hashing.NewSha256Hasher)
	if err != nil {
		return nil, fmt.Errorf("new balloon fsm: %s", err)
	}

	nodeHost, err := dragonboat.NewNodeHost(nodeHostConfig)
	if err != nil {
		return nil, err
	}

	rb := &RaftBalloon{
		conf:           conf,
		clusterConfig:  clusterConfig,
		nodeHostConfig: nodeHostConfig,
		nodeHost:       nodeHost,
		done:           make(chan struct{}),
		fsm:            fsm,
		snapshotsCh:    snapshotsCh,
	}

	rb.metrics = newRaftBalloonMetrics(rb)

	return rb, nil
}

// Open opens the Balloon. If no joinAddr is provided, then there are no existing peers,
// then this node becomes the first node, and therefore, leader of the cluster.
func (b *RaftBalloon) Open(bootstrap bool) error {
	b.Lock()
	defer b.Unlock()

	if b.closed {
		return ErrBalloonInvalidState
	}

	log.Infof("opening balloon with node ID %v", b.conf.NodeId)

	peers := make(map[uint64]string)

	if bootstrap {
		peers[b.conf.NodeId] = b.conf.RaftAddr
	}

	fsmFactory := func(x, y uint64) statemachine.IOnDiskStateMachine { return b.fsm }
	err := b.nodeHost.StartOnDiskCluster(peers, !bootstrap, fsmFactory, b.clusterConfig)

	return err
}

// Close closes the RaftBalloon. If wait is true, waits for a graceful shutdown.
// Once closed, a RaftBalloon may not be re-opened.
func (b *RaftBalloon) Close(wait bool) error {
	b.Lock()
	defer b.Unlock()
	if b.closed {
		return nil
	}
	defer func() {
		b.closed = true
	}()

	close(b.done)
	b.wg.Wait()

	// shutdown raft

	b.nodeHost.Stop()
	b.nodeHost = nil

	b.metrics = nil

	// nodeHost.Close() calls FSM close
	// Close FSM
	// b.fsm.Close()
	// b.fsm = nil

	return nil
}

// Join joins a node, identified by id and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
// This must be called from the Leader or it will fail.
func (b *RaftBalloon) Join(nodeId, clusterId uint64, addr string) error {

	log.Infof("received join request for remote node %s at %s", nodeId, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	m, err := b.nodeHost.GetClusterMembership(ctx, clusterId)
	if err != nil {
		return err
	}

	resp, err := b.nodeHost.RequestAddNode(clusterId, nodeId, addr, m.ConfigChangeID, 1*time.Second)

	_, err = waitForResp(resp, 1*time.Second)
	if err != nil {
		return err
	}

	log.Infof("node %s at %s joined successfully", nodeId, addr)
	return nil
}

// Wait until node becomes leader or time is out
func (b *RaftBalloon) WaitForLeader(timeout time.Duration) (uint64, error) {
	var err error
	var id uint64
	var ok bool

	tck := time.NewTicker(leaderWaitDelay)
	defer tck.Stop()

	timeoutTck := time.NewTicker(timeout)
	defer timeoutTck.Stop()

	for {
		select {
		case <-tck.C:
			id, ok, err = b.nodeHost.GetLeaderID(b.clusterConfig.ClusterID)
			if ok && err == nil {
				return id, nil
			}

		case <-timeoutTck.C:
			return 0, fmt.Errorf("timeout expired id=%v ok=%v err=%v", id, ok, err)
		}
	}
}

func (b *RaftBalloon) IsLeader() bool {
	id, ok, err := b.nodeHost.GetLeaderID(b.clusterConfig.ClusterID)
	if !ok || err != nil {
		return false
	}
	return id == b.clusterConfig.NodeID
}

// Addr returns the address of the store.
func (b *RaftBalloon) Addr() string {
	return b.nodeHost.RaftAddress()
}

// LeaderAddr returns the Raft address of the current leader. Returns a
// blank string if there is no leader.
func (b *RaftBalloon) LeaderAddr() (string, error) {
	id, err := b.LeaderId()
	if err != nil {
		return "", err
	}
	nodes, err := b.Nodes()
	if err != nil {
		return "", err
	}

	return nodes[id], nil
}

// ID returns the Raft ID of the store.
func (b *RaftBalloon) ID() uint64 {
	return b.conf.NodeId
}

// LeaderID returns the node ID of the Raft leader. Returns a
// blank string if there is no leader, or an error.
func (b *RaftBalloon) LeaderId() (uint64, error) {
	id, ok, err := b.nodeHost.GetLeaderID(b.clusterConfig.ClusterID)
	if !ok || err != nil {
		return 0, fmt.Errorf("Error geting leader information: %v", err)
	}
	return id, nil
}

// Nodes returns the slice of nodes in the cluster, sorted by ID ascending.
func (b *RaftBalloon) Nodes() (map[uint64]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m, err := b.nodeHost.GetClusterMembership(ctx, b.clusterConfig.ClusterID)
	if err != nil {
		return nil, err
	}
	return m.Nodes, nil
}

// Remove removes a node from the store, specified by ID.
func (b *RaftBalloon) Remove(id uint64) error {
	log.Infof("received request to remove node %s", id)
	if err := b.remove(id); err != nil {
		log.Infof("failed to remove node %s: %s", id, err.Error())
		return err
	}

	log.Infof("node %s removed successfully", id)
	return nil
}

// remove removes the node, with the given ID, from the cluster.
func (b *RaftBalloon) remove(id uint64) error {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m, err := b.nodeHost.GetClusterMembership(ctx, b.clusterConfig.ClusterID)
	if err != nil {
		return err
	}

	resp, err := b.nodeHost.RequestDeleteNode(b.clusterConfig.ClusterID, id, m.ConfigChangeID, 1*time.Second)

	_, err = waitForResp(resp, 1*time.Second)
	return err
}

func (b *RaftBalloon) Info() (*Metadata, error) {

	// Update Leader info
	leaderId, err := b.LeaderId()
	if err != nil {
		return nil, err
	}

	// Update nodes info
	nodes, err := b.Nodes()
	if err != nil {
		return nil, err
	}

	b.fsm.metaUpdate(leaderId, nodes)

	// Return current metadata
	return b.fsm.Info(), nil
}

func (b *RaftBalloon) RegisterMetrics(registry metrics.Registry) {
	registry.MustRegister(b.metrics.collectors()...)
}

func (b *RaftBalloon) Add(event []byte) (*balloon.Snapshot, error) {
	var snapshot []*balloon.Snapshot

	cmd := NewCommand(AddEventCommandType)
	err := cmd.Encode(event)
	if err != nil {
		return nil, fmt.Errorf("failed to encode command: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session := b.nodeHost.GetNoOPSession(b.clusterConfig.ClusterID)

	// defer b.nodeHost.CloseSession(ctx, session)

	result, err := b.nodeHost.SyncPropose(ctx, session, cmd.data)
	if err != nil {
		return nil, fmt.Errorf("Error proposing Add command: %v", err)
	}

	b.metrics.Adds.Inc()
	err = decodeMsgPack(result.Data, &snapshot)
	if err != nil {
		return nil, fmt.Errorf("Error decoding result from Add command: %v", err)
	}

	p := protocol.Snapshot(*snapshot[0])

	//Send snapshot to the snapshot channel
	b.snapshotsCh <- &p // TODO move this to an upper layer (shard manager?)

	return snapshot[0], nil
}

func (b *RaftBalloon) AddBulk(bulk [][]byte) ([]*balloon.Snapshot, error) {

	cmd := NewCommand(AddEventsBulkCommandType)
	err := cmd.Encode(bulk)
	if err != nil {
		return nil, fmt.Errorf("failed to encode command: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session := b.nodeHost.GetNoOPSession(b.clusterConfig.ClusterID)

	// defer b.nodeHost.CloseSession(ctx, session)

	result, err := b.nodeHost.SyncPropose(ctx, session, cmd.data)
	if err != nil {
		return nil, err
	}

	b.metrics.Adds.Add(float64(len(bulk)))
	snapshots := make([]*balloon.Snapshot, 0)
	err = decodeMsgPack(result.Data, &snapshots)
	if err != nil {
		return nil, fmt.Errorf("Decoding the response of AddBulk from raft got: %v", err)
	}

	//Send snapshot to the snapshot channel
	// TODO move this to an upper layer (shard manager?)
	for _, s := range snapshots {
		p := protocol.Snapshot(*s)
		b.snapshotsCh <- &p
	}

	return snapshots, nil
}

func (b *RaftBalloon) Metadata() error {

	// Update Leader info
	leaderId, err := b.LeaderId()
	if err != nil {
		return err
	}

	meta := new(Metadata)

	meta.NodeId = b.conf.NodeId
	meta.LeaderId = leaderId
	meta.Nodes = make(map[uint64]*NodeInfo)
	meta.Nodes[meta.NodeId] = &NodeInfo{
		HTTPAddr:    b.conf.HTTPAddr,
		RaftAddr:    b.conf.RaftAddr,
		MgmtAddr:    b.conf.MgmtAddr,
		MetricsAddr: b.conf.MetricsAddr,
	}

	cmd := NewCommand(MetadataUpdateCommandType)
	err = cmd.Encode(meta)
	if err != nil {
		return fmt.Errorf("failed to encode command: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session := b.nodeHost.GetNoOPSession(b.clusterConfig.ClusterID)

	// defer b.nodeHost.CloseSession(ctx, session)

	_, err = b.nodeHost.SyncPropose(ctx, session, cmd.data)
	if err != nil {
		return err
	}

	return nil
}

func (b *RaftBalloon) QueryDigestMembershipConsistency(keyDigest hashing.Digest, version uint64) (*balloon.MembershipProof, error) {
	b.metrics.DigestMembershipQueries.Inc()
	return b.fsm.QueryDigestMembershipConsistency(keyDigest, version)
}

func (b *RaftBalloon) QueryMembershipConsistency(event []byte, version uint64) (*balloon.MembershipProof, error) {
	b.metrics.MembershipQueries.Inc()
	return b.fsm.QueryMembershipConsistency(event, version)
}

func (b *RaftBalloon) QueryDigestMembership(keyDigest hashing.Digest) (*balloon.MembershipProof, error) {
	b.metrics.DigestMembershipQueries.Inc()
	return b.fsm.QueryDigestMembership(keyDigest)
}

func (b *RaftBalloon) QueryMembership(event []byte) (*balloon.MembershipProof, error) {
	b.metrics.MembershipQueries.Inc()
	return b.fsm.QueryMembership(event)
}

func (b *RaftBalloon) QueryConsistency(start, end uint64) (*balloon.IncrementalProof, error) {
	b.metrics.IncrementalQueries.Inc()
	return b.fsm.QueryConsistency(start, end)
}

func waitForResp(s *dragonboat.RequestState, timeout time.Duration) (*statemachine.Result, error) {
	for {
		select {
		case r := <-s.CompletedC:
			if r.Completed() {
				result := r.GetResult()
				return &result, nil
			}
			if r.Timeout() {
				return nil, fmt.Errorf("Request timed out while processing")
			}
			if r.Rejected() {
				return nil, fmt.Errorf("Request rejected. Session is probably invalid.")
			}
			if r.Terminated() {
				return nil, fmt.Errorf("Request terminated because cluster is shutting down.")
			}
		case <-time.After(timeout):
			return nil, fmt.Errorf("timeout")
		}
	}
}
