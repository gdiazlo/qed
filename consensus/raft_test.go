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
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/bbva/qed/log"
	"github.com/bbva/qed/protocol"
	"github.com/bbva/qed/storage/rocks"
	metrics_utils "github.com/bbva/qed/testutils/metrics"
	utilrand "github.com/bbva/qed/testutils/rand"
	"github.com/bbva/qed/testutils/spec"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/logger"
)

func init() {
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.ERROR)
	logger.GetLogger("transport").SetLevel(logger.ERROR)
	logger.GetLogger("grpc").SetLevel(logger.ERROR)
	logger.GetLogger("dragonboat").SetLevel(logger.ERROR)
	logger.GetLogger("logdb").SetLevel(logger.ERROR)
}

func raftAddr(id uint64) string {
	return fmt.Sprintf("127.0.0.1:1830%d", id)
}

func snapshotsDrainer(snapshotsCh chan *protocol.Snapshot) {
	go func() {
		for {
			_, ok := <-snapshotsCh
			if !ok {
				return
			}
		}
	}()
}

func newNode(join bool, port, nodeId, clusterId uint64) (*RaftBalloon, func(bool), error) {

	storePath := fmt.Sprintf("/var/tmp/raft-test-cluster%d/node%d/db", clusterId, nodeId)

	err := os.MkdirAll(storePath, os.FileMode(0755))
	if err != nil {
		return nil, nil, err
	}
	store, err := rocks.NewRocksDBStore(storePath)
	if err != nil {
		return nil, nil, err
	}

	raftPath := fmt.Sprintf("/var/tmp/raft-test-cluster%d/node%d/raft", clusterId, nodeId)
	err = os.MkdirAll(raftPath, os.FileMode(0755))
	if err != nil {
		return nil, nil, err
	}

	snapshotsCh := make(chan *protocol.Snapshot, 10000)
	snapshotsDrainer(snapshotsCh)
	rbconfig := &Config{
		NodeId:      nodeId,
		ClusterId:   clusterId,
		HTTPAddr:    "http://127.0.0.1:18800",
		RaftAddr:    raftAddr(port),
		MgmtAddr:    "http://127.0.0.1:18801",
		MetricsAddr: "http://127.0.0.1:18802",
		RaftPath:    raftPath,
	}
	node, err := NewRaftBalloon(rbconfig, store, snapshotsCh)
	if err != nil {
		return nil, nil, err
	}

	srvCloseF := metrics_utils.StartMetricsServer(node, store)

	return node, func(clean bool) {
		store.Close()
		srvCloseF()
		close(snapshotsCh)
		if clean {
			os.RemoveAll(storePath)
			os.RemoveAll(raftPath)
		}
	}, nil

}

func Test_Raft_IsLeader(t *testing.T) {

	log.SetLogger("Test_Raft_IsLeader", log.SILENT)

	r, clean, err := newNode(false, 0, 100, 100)
	spec.NoError(t, err, "Error creating testing node")
	defer clean(true)

	err = r.Open(true)
	spec.NoError(t, err, "Error opening raft node")

	defer func() {
		err = r.Close(true)
		spec.NoError(t, err, "Error closing raft node")
	}()

	_, err = r.WaitForLeader(10 * time.Second)
	spec.NoError(t, err, "Error waiting for leader")

	spec.True(t, r.IsLeader(), "single node is not leader!")

}

func TestRaft_OpenStore_CloseSingleNode(t *testing.T) {

	r, clean, err := newNode(false, 0, 200, 100)
	spec.NoError(t, err, "Error creating testing node")
	defer clean(true)

	err = r.Open(true)
	spec.NoError(t, err, "Error opening raft node")

	_, err = r.WaitForLeader(10 * time.Second)
	spec.NoError(t, err, "Error waiting for leader")

	err = r.Close(true)
	spec.NoError(t, err, "Error closing raft node")

	err = r.Open(true)
	spec.Equal(t, err, ErrBalloonInvalidState, "incorrect error returned on re-open attempt")

}

func Test_Raft_MultiNode_Join(t *testing.T) {

	log.SetLogger("Test_Raft_MultiNodeJoin", log.SILENT)

	r0, clean0, err := newNode(false, 0, 100, 200)
	spec.NoError(t, err, "Error creating raft node 0")
	defer func() {
		err := r0.Close(true)
		spec.NoError(t, err, "Error clogins raft node 0")
		clean0(true)
	}()

	err = r0.Open(true)
	spec.NoError(t, err, "Error opening raft node 0")

	_, err = r0.WaitForLeader(10 * time.Second)
	spec.NoError(t, err, "Error waiting for leader")

	r1, clean1, err := newNode(false, 1, 200, 200)
	spec.NoError(t, err, "Error creating raft node 1")
	defer func() {
		err := r1.Close(true)
		spec.NoError(t, err, "Error closing raft node 1")
		clean1(true)
	}()

	err = r0.Join(200, 200, r1.Addr())
	spec.NoError(t, err, "Error joining raft node 0")

	err = r1.Open(false)
	spec.NoError(t, err, "Error opening raft node 1")

	// This Nodes() seems to work only agains the leader
	// TODO: test this further
	n0, err := r0.Nodes()
	spec.NoError(t, err, "Error getting node list from raft node 0")
	spec.Equal(t, len(n0), 2, "Number of nodes must be 2")
}

func Test_Raft_MultiNode_JoinRemove(t *testing.T) {

	r0, clean0, err := newNode(false, 0, 100, 300)
	spec.NoError(t, err, "Error creating raft node 0")
	defer func() {
		err := r0.Close(true)
		spec.NoError(t, err, "Error closing raft node 0")
		clean0(true)
	}()

	err = r0.Open(true)
	spec.NoError(t, err, "Error opening raft node 0")

	_, err = r0.WaitForLeader(10 * time.Second)
	spec.NoError(t, err, "Error waiting for leader")

	r1, clean1, err := newNode(false, 1, 200, 300)
	spec.NoError(t, err, "Error creating raft node 1")
	defer func() {
		err := r1.Close(true)
		spec.NoError(t, err, "Error closing raft node 1")
		clean1(true)
	}()

	err = r0.Join(200, 300, r1.Addr())
	spec.NoError(t, err, "Error joining raft node 0")

	err = r1.Open(false)
	spec.NoError(t, err, "Error opening raft node 1")

	_, err = r0.WaitForLeader(10 * time.Second)
	spec.NoError(t, err, "Error waiting for leader in raft node 0")

	// Check leader state on follower.
	leaderAddr, err := r1.LeaderAddr()
	spec.NoError(t, err, "Error getting leader address from raft node 1")
	spec.Equal(t, leaderAddr, r0.Addr(), "wrong leader address returned")

	id, err := r1.LeaderId()
	spec.NoError(t, err, "Error getting leader id from raft node 1")

	spec.Equal(t, id, r0.ID(), "wrong leader ID returned")

	n0, err := r0.Nodes()
	spec.NoError(t, err, "Error getting node list from raft node 0")
	spec.Equal(t, len(n0), 2, "Number of nodes must be 2")

	// Remove a node.
	err = r0.Remove(r1.ID())
	spec.NoError(t, err, "Error removing raft node 1 from raft node 0")

	nodes, err := r0.Nodes()
	spec.NoError(t, err, "Error getting nodes from raft node 0")

	spec.Equal(t, len(nodes), 1, "size of cluster is not correct post remove")
	spec.Equal(t, r0.Addr(), nodes[r0.ID()], "cluster does not have correct nodes post remove")

}

func Test_Raft_SingleNode_SnapshotOnDisk(t *testing.T) {
	r0, clean0, err := newNode(false, 0, 100, 400)
	spec.NoError(t, err, "Error creating raft node 0")

	err = r0.Open(true)
	spec.NoError(t, err, "Error opening raft node 0")

	_, err = r0.WaitForLeader(10 * time.Second)
	spec.NoError(t, err, "Error waiting for leader in raft node 0")

	// Add event
	rand.Seed(42)
	expectedBalloonVersion := uint64(rand.Intn(50))
	for i := uint64(0); i < expectedBalloonVersion; i++ {
		_, err = r0.Add([]byte(fmt.Sprintf("Test Event %d", i)))
		spec.NoError(t, err, "Error adding event to raft node 0")
	}

	// request snapshot
	resp, err := r0.nodeHost.RequestSnapshot(400, dragonboat.SnapshotOption{}, 5*time.Second)
	spec.NoError(t, err, "Error requesting snapshot from raft node 0")
	_, err = waitForResp(resp, 5*time.Second)
	spec.NoError(t, err, "Error in snapshot request")

	// close node
	err = r0.Close(true)
	spec.NoError(t, err, "Error closing raft node 0")
	clean0(false)

	// restart node and check if recovers from snapshot
	r1, clean1, err := newNode(false, 0, 100, 400)
	spec.NoError(t, err, "Error creating raft node 1")
	defer func() {
		err = r1.Close(true)
		spec.NoError(t, err, "Error closing raft node 1")
		clean1(true)
	}()

	err = r1.Open(true)
	spec.NoError(t, err, "Error opening raft node 1")

	_, err = r1.WaitForLeader(10 * time.Second)
	spec.NoError(t, err, "Error waiting for leader in raft node 1")

	spec.Equal(t, expectedBalloonVersion, r1.fsm.balloon.Version(), "Error in state recovery from snapshot")

}

func Test_Raft_MultiNode_WithMetadata(t *testing.T) {

	log.SetLogger("Test_Raft_MultiNode_WithMetadata", log.SILENT)

	r0, clean0, err := newNode(false, 2, 100, 200)
	spec.NoError(t, err, "Error creating raft node 0")
	defer func() {
		err := r0.Close(true)
		spec.NoError(t, err, "Error closing raft node 0")
		clean0(true)
	}()

	err = r0.Open(true)
	spec.NoError(t, err, "Error opening raft node 0")

	_, err = r0.WaitForLeader(10 * time.Second)
	spec.NoError(t, err, "Error waiting for leader")

	r1, clean1, err := newNode(false, 3, 200, 200)
	spec.NoError(t, err, "Error creating raft node 1")
	defer func() {
		err := r1.Close(true)
		spec.NoError(t, err, "Error closing raft node 1")
		clean1(true)
	}()

	err = r0.Join(200, 200, r1.Addr())
	spec.NoError(t, err, "Error joining raft node 0")

	err = r1.Open(false)
	spec.NoError(t, err, "Error opening raft node 1")

	_, err = r1.WaitForLeader(10 * time.Second)
	spec.NoError(t, err, "Error waiting for leader")

	info0, err := r0.Info()
	spec.NoError(t, err, "Error getting info from node 0")
	info1, err := r1.Info()
	spec.NoError(t, err, "Error getting info from node 1")

	spec.Equal(t, info0.Nodes, info1.Nodes, "Both nodes must have the same metadata.")
}

func BenchmarkRaftAdd(b *testing.B) {

	log.SetLogger("BenchmarkRaftAdd", log.SILENT)

	raftNodeA, clean, err := newNode(false, 0, 1, 1)
	spec.NoError(b, err, "Error creating testing node")
	defer clean(true)

	err = raftNodeA.Open(true)
	spec.NoError(b, err, "Error opening raft node A")

	id, err := raftNodeA.WaitForLeader(10 * time.Second)
	spec.NoError(b, err, "Error waiting for leader")
	fmt.Println("Leader ID A: ", id)

	// b.N should be eq or greater than 500k to avoid benchmark framework spreading more than one goroutine.
	// b.N should be always greater than SnapshotEntries value to avoid taking snapshot while closing the DB.
	b.N = 110000
	b.ResetTimer()
	b.SetParallelism(100)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			event := utilrand.Bytes(128)
			_, err := raftNodeA.Add(event)
			spec.NoError(b, err, "Error adding event to raft node A")
		}
	})
}

func BenchmarkRaftAddBulk(b *testing.B) {

	log.SetLogger("BenchmarkRaftAddBulk", log.SILENT)

	log.SetLogger("BenchmarkRaftAdd", log.SILENT)

	raftNode, _, err := newNode(false, 0, 1, 1)
	spec.NoError(b, err, "Error creating testing node")
	// defer clean()

	err = raftNode.Open(true)
	spec.NoError(b, err, "Error opening raft node")

	id, err := raftNode.WaitForLeader(10 * time.Second)
	spec.NoError(b, err, "Error waiting for leader")
	fmt.Println("Leader ID A: ", id)

	// b.N shoul be eq or greater than 500k to avoid benchmark framework spreading more than one goroutine.
	// b.N should be always greater than SnapshotEntries value to avoid taking snapshot while closing the DB.
	b.N = 110000
	b.ResetTimer()
	b.SetParallelism(100)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			events := [][]byte{utilrand.Bytes(128)}
			_, err := raftNode.AddBulk(events)
			spec.NoError(b, err, "Error adding bulk events to raft node")
		}
	})

}
