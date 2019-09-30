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
	"net"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"

	"github.com/bbva/qed/balloon"
	"github.com/bbva/qed/crypto/hashing"
	"github.com/bbva/qed/log"
	"github.com/bbva/qed/metrics"
	"github.com/bbva/qed/protocol"
	"github.com/bbva/qed/storage"
)

const (
	leaderWaitDelay = 100 * time.Millisecond
)

var (
	// ErrNotLeader is raised when no a raft node tries to execute an operation
	// on a non-primary node.
	ErrNotLeader = errors.New("Not cluster leader")

	// ErrCannotJoin is raised when a node cannot join to any specified seed
	// (e.g. not leader).
	ErrCannotJoin = errors.New("Unable to join to the cluster")

	// ErrCannotSync is raised when a node cannot synchronize its cluster info.
	ErrCannotSync = errors.New("Unable to sync cluster info")
)

// ClusteringOptions contains node options related to clustering.
type ClusteringOptions struct {
	NodeID            string   // ID of the node within the cluster.
	Addr              string   // IP address where to listen for Raft commands.
	MgmtAddr          string   // IP address where to listen for management operations.
	HttpAddr          string   // IP address where clients can connect (this is used to populate node info)
	Bootstrap         bool     // Bootstrap the cluster as a seed node if there is no existing state.
	Seeds             []string // List of cluster peer node IDs to bootstrap the cluster state.
	RaftLogPath       string   // Path to Raft log store directory.
	LogCacheSize      int      // Number of Raft log entries to cache in memory to reduce disk IO.
	LogSnapshots      int      // Number of Raft log snapshots to retain.
	SnapshotThreshold uint64   // Controls how many outstanding logs there must be before we perform a snapshot.
	TrailingLogs      uint64   // Number of logs left after a snapshot.
	Sync              bool     // Do a file sync after every write to the Raft log and stable store.
	RaftLogging       bool     // Enable logging of Raft library (disabled by default since really verbose).

	// These will be set to some sane defaults. Change only if experiencing raft issues.
	RaftHeartbeatTimeout time.Duration
	RaftElectionTimeout  time.Duration
	RaftLeaseTimeout     time.Duration
	RaftCommitTimeout    time.Duration
	RaftApplyTimeout     time.Duration // Amount of time we wait for the command to be started.
}

func DefaultClusteringOptions() *ClusteringOptions {
	return &ClusteringOptions{
		NodeID:            "",
		Addr:              "",
		Bootstrap:         false,
		Seeds:             make([]string, 0),
		RaftLogPath:       "",
		LogCacheSize:      512,
		LogSnapshots:      2,
		SnapshotThreshold: 8192,
		TrailingLogs:      10240,
		RaftApplyTimeout:  10 * time.Second,
		Sync:              false,
		RaftLogging:       false,
	}
}

type RaftNode struct {
	info *NodeInfo

	applyTimeout time.Duration

	db        storage.ManagedStore    // Persistent database
	raftLog   *raftLog                // Underlying rocksdb-backed persistent log store
	snapshots *raft.FileSnapshotStore // Persistent snapstop store

	raft       *raft.Raft             // The consensus mechanism
	transport  *raft.NetworkTransport // Raft network transport
	raftConfig *raft.Config           // Config provides any necessary configuration for the Raft server.

	balloon     *balloon.Balloon // Balloon's finite state machine
	state       *fsmState
	snapshotsCh chan *protocol.Snapshot // channel to publish snapshots

	hasherF     func() hashing.Hasher
	metrics     *raftNodeMetrics     // Raft node metrics.
	raftMetrics *raftInternalMetrics // Raft internal metrics.

	log log.Logger

	sync.Mutex
	closed bool
	done   chan struct{}
}

func NewRaftNode(opts *ClusteringOptions, store storage.ManagedStore, snapshotsCh chan *protocol.Snapshot) (*RaftNode, error) {
	return NewRaftNodeWithLogger(opts, store, snapshotsCh, log.L())
}

func NewRaftNodeWithLogger(opts *ClusteringOptions, store storage.ManagedStore, snapshotsCh chan *protocol.Snapshot, logger log.Logger) (*RaftNode, error) {

	// We try to resolve the raft addr to avoid binding to hostnames
	// because Raft library does not support FQDNs
	addr, err := net.ResolveTCPAddr("tcp", opts.Addr)
	if err != nil {
		return nil, err
	}

	// We create s.raft early because once NewRaft() is called, the
	// raft code may asynchronously invoke FSM.Apply() and FSM.Restore()
	// So we want the object to exist so we can check on leader atomic, etc..
	info := &NodeInfo{
		NodeId:   opts.NodeID,
		RaftAddr: addr.String(),
		MgmtAddr: opts.MgmtAddr,
		HttpAddr: opts.HttpAddr,
	}
	node := &RaftNode{
		info:         info,
		snapshotsCh:  snapshotsCh,
		log:          logger,
		applyTimeout: opts.RaftApplyTimeout,
		done:         make(chan struct{}),
	}

	// Create the log store
	raftLog, err := newRaftLogOpts(raftLogOptions{
		Path:             opts.RaftLogPath + "/wal",
		NoSync:           !opts.Sync,
		EnableStatistics: true},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create a new Raft log: %s", err)
	}
	logStore, err := raft.NewLogCache(opts.LogCacheSize, raftLog)
	if err != nil {
		return nil, fmt.Errorf("cannot create a new cached log store: %s", err)
	}
	node.db = store
	node.raftLog = raftLog

	// Set hashing function
	hasherF := hashing.NewSha256Hasher
	node.hasherF = hasherF

	// Instantiate balloon FSM
	node.balloon, err = balloon.NewBalloonWithLogger(store, hasherF, node.log.Named("balloon"))
	if err != nil {
		return nil, err
	}
	err = node.loadState()
	if err != nil {
		node.log.Error("There was an error recovering the FSM state!!")
		return nil, err
	}

	// setup Raft configuration
	conf := raft.DefaultConfig()
	if opts.RaftHeartbeatTimeout != 0 {
		conf.HeartbeatTimeout = opts.RaftHeartbeatTimeout
	}
	if opts.RaftElectionTimeout != 0 {
		conf.ElectionTimeout = opts.RaftElectionTimeout
	}
	if opts.RaftLeaseTimeout != 0 {
		conf.LeaderLeaseTimeout = opts.RaftLeaseTimeout
	}
	if opts.RaftCommitTimeout != 0 {
		conf.CommitTimeout = opts.RaftCommitTimeout
	}
	conf.TrailingLogs = opts.TrailingLogs
	conf.SnapshotThreshold = opts.SnapshotThreshold
	conf.LocalID = raft.ServerID(opts.NodeID)

	conf.LogLevel = "error"
	if opts.RaftLogging {
		conf.Logger = log.NewHclogAdapter(node.log.Named("raft"))
	} else {
		conf.Logger = hclog.NewNullLogger()
	}

	node.raftConfig = conf

	node.transport, err = NewCMuxTCPTransportWithLogger(node, 3, 10*time.Second, node.log) // TODO export params
	if err != nil {
		return nil, err
	}

	// create the snapshot store. This allows the Raft to truncate the log.
	// The library creates a folder to store the snapshots in.
	node.snapshots, err = raft.NewFileSnapshotStoreWithLogger(opts.RaftLogPath, opts.LogSnapshots, node.log.StdLogger(&log.StdLoggerOptions{
		InferLevels: true,
	}))
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %s", err)
	}

	// instantiate the raft server
	node.raft, err = raft.NewRaft(node.raftConfig, node, logStore, node.raftLog, node.snapshots, node.transport)
	if err != nil {
		node.transport.Close()
		node.raftLog.Close()
		return nil, fmt.Errorf("new raft: %s", err)
	}

	// register metrics
	node.metrics = newRaftNodeMetrics(node)
	node.raftMetrics = newRaftInternalMetrics(node.raft)

	// check existing state
	existingState, err := raft.HasExistingState(logStore, node.raftLog, node.snapshots)
	if err != nil {
		node.Close(true)
		return nil, err
	}
	if existingState {
		node.log.Debug("Loaded existing state for Raft.")
	} else {
		node.log.Debug("No existing state found for Raft.")
		// Bootstrap if there is no previous state and we are starting this node as
		// a seed or a cluster configuration is provided.
		if opts.Bootstrap {
			node.log.Info("Bootstraping cluster...")
			if err := node.bootstrapCluster(); err != nil {
				node.Close(true)
				return nil, err
			}
			node.log.Info("Cluster successfully bootstraped.")
		} else {
			node.log.Info("Attempting to join the cluster.")
			// Attempt to join the cluster if we're not bootstraping.
			err := node.attemptToJoinCluster(opts.Seeds)
			if err != nil {
				node.Close(true)
				return nil, fmt.Errorf("failed to join Raft cluster: %v", err)
			}
			node.log.Info("Join operation finished successfully.")
		}
	}

	return node, nil
}

// Close closes the RaftNode. If wait is true, waits for a graceful shutdown.
// Once closed, a RaftNode may not be re-opened.
func (n *RaftNode) Close(wait bool) error {
	n.Lock()

	n.log.Info("RaftNode is cosing down")

	if n.closed {
		n.Unlock()
		return nil
	}

	n.closed = true
	n.Unlock()

	// shutdown Raft
	if n.raft != nil {
		f := n.raft.Shutdown()
		if wait {
			if e := f.(raft.Future); e.Error() != nil {
				return e.Error()
			}
		}
		n.log.Info("RaftNode closed Raft")
		n.raft = nil
	}

	if n.transport != nil {
		if err := n.transport.Close(); err != nil {
			return err
		}
		n.log.Info("RaftNode closed transport")
		n.transport = nil
	}

	if n.raftLog != nil {
		if err := n.raftLog.Close(); err != nil {
			return err
		}
		n.raftLog = nil
		n.log.Info("RaftNode closed raft log")
	}

	// close fsm
	if n.balloon != nil {
		n.balloon.Close()
		n.balloon = nil
		n.log.Info("RaftNode closed balloon")
	}

	// close the database
	if n.db != nil {
		if err := n.db.Close(); err != nil {
			return err
		}
		n.db = nil
		n.log.Info("RaftNode closed database")
	}

	n.log.Info("RaftNode completely stopped")
	return nil
}

func (n *RaftNode) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// WaitForLeader waits until the node becomes leader or time is out.
func (n *RaftNode) WaitForLeader(timeout time.Duration) error {
	tck := time.NewTicker(leaderWaitDelay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			l := string(n.raft.Leader())
			if l != "" {
				return nil
			}
		case <-tmr.C:
			return fmt.Errorf("timeout expired")
		}
	}
}

// JoinCluster joins a node, identified by id and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
// This must be called from the Leader or it will fail.
func (n *RaftNode) JoinCluster(ctx context.Context, req *RaftJoinRequest) (*RaftJoinResponse, error) {

	// Drop the request if we're not the leader. There's no race condition
	// after this check because even if we proceed with the cluster add, it
	// will fail if the node is not the leader as cluster changes go
	// through the Raft log.
	if !n.IsLeader() {
		return nil, ErrNotLeader
	}

	n.log.Infof("received join request for remote node %s at %s", req.NodeId, req.RaftAddr)

	// Add the node as a voter. This is idempotent. No-op if the request
	// came from ourselves.
	f := n.raft.AddVoter(raft.ServerID(req.NodeId), raft.ServerAddress(req.RaftAddr), 0, 0)
	if err := f.Error(); err != nil {
		return nil, err
	}

	return new(RaftJoinResponse), nil
}

func (n *RaftNode) attemptToJoinCluster(addrs []string) error {
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()
		client := NewClusterServiceClient(conn)
		req := new(RaftJoinRequest)
		req.NodeId = n.info.NodeId
		req.RaftAddr = string(n.transport.LocalAddr())
		_, err = client.JoinCluster(context.Background(), req)
		if err == nil {
			return nil
		}
	}
	return ErrCannotJoin
}

// RegisterMetrics register raft metrics: prometheus collectors and raftLog metrics.
func (n *RaftNode) RegisterMetrics(registry metrics.Registry) {
	if registry != nil {
		n.raftLog.RegisterMetrics(registry)
	}
	registry.MustRegister(n.metrics.collectors()...)
	registry.MustRegister(n.raftMetrics.collectors()...)
}

func (n *RaftNode) bootstrapCluster() error {
	// include ourself in the cluster
	servers := []raft.Server{
		{
			ID:      n.raftConfig.LocalID,
			Address: n.transport.LocalAddr(),
		},
	}
	err := n.raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error()
	if err != nil {
		return err
	}
	return n.WaitForLeader(5 * time.Second)
}

// blank string if there is no leader, or an error.
func (n *RaftNode) leaderId() (string, error) {
	n.Lock()
	if n.closed {
		n.Unlock()
		return "", errors.New("Node is closed")
	}
	defer n.Unlock()
	addr := n.raft.Leader()

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		n.log.Infof("failed to get raft configuration: %v", err)
		return "", err
	}
	for _, srv := range configFuture.Configuration().Servers {
		if srv.Address == raft.ServerAddress(addr) {
			return string(srv.ID), nil
		}
	}

	return "", errors.New("No leader")
}

// applies a command into the Raft.
func (n *RaftNode) propose(cmd *command) (interface{}, error) {
	future := n.raft.Apply(cmd.data, n.applyTimeout)
	if err := future.Error(); err != nil {
		return nil, err
	}
	return future.Response(), nil
}

func (n *RaftNode) leaveLeadership() error {
	if err := n.raft.LeadershipTransfer().Error(); err != nil {
		return err
	}
	return nil
}
