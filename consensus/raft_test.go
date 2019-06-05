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
	"github.com/lni/dragonboat"
	"github.com/lni/dragonboat/logger"
	"github.com/stretchr/testify/require"
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

	node, err := NewRaftBalloon(raftPath, raftAddr(port), clusterId, nodeId, store, snapshotsCh)
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
	require.NoError(t, err, "Error creating testing node")
	defer clean(true)

	err = r.Open(true, map[string]string{"foo": "bar"})
	require.NoError(t, err)

	defer func() {
		err = r.Close(true)
		require.NoError(t, err)
	}()

	_, err = r.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	require.True(t, r.IsLeader(), "single node is not leader!")

}

func TestRaft_OpenStore_CloseSingleNode(t *testing.T) {

	r, clean, err := newNode(false, 0, 200, 100)
	require.NoError(t, err, "Error creating testing node")
	defer clean(true)

	err = r.Open(true, map[string]string{"foo": "bar"})
	require.NoError(t, err)

	_, err = r.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	err = r.Close(true)
	require.NoError(t, err)

	err = r.Open(true, map[string]string{"foo": "bar"})
	require.Equal(t, err, ErrBalloonInvalidState, err, "incorrect error returned on re-open attempt")

}

func Test_Raft_MultiNode_Join(t *testing.T) {

	log.SetLogger("Test_Raft_MultiNodeJoin", log.SILENT)

	r0, clean0, err := newNode(false, 0, 100, 200)
	require.NoError(t, err, "Error creating raft node 0")
	defer func() {
		err := r0.Close(true)
		require.NoError(t, err)
		clean0(true)
	}()

	err = r0.Open(true, map[string]string{"foo": "bar"})
	require.NoError(t, err, "Error opening raft node 0")

	_, err = r0.WaitForLeader(10 * time.Second)
	require.NoError(t, err, "Error waiting for leader")

	r1, clean1, err := newNode(false, 1, 200, 200)
	require.NoError(t, err, "Error creating raft node 1")
	defer func() {
		err := r1.Close(true)
		require.NoError(t, err)
		clean1(true)
	}()

	err = r0.Join(200, 200, r1.Addr(), map[string]string{"foo": "bar"})
	require.NoError(t, err, "Error joining raft node 0")

	err = r1.Open(false, map[string]string{"foo": "bar"})
	require.NoError(t, err, "Error opening raft node 1")

	// This Nodes() seems to work only agains the leader
	// TODO: test this further
	n0, err := r0.Nodes()
	require.NoError(t, err, "Error getting node list from raft node 0")
	require.Equal(t, len(n0), 2, "Number of nodes must be 2")
}

func Test_Raft_MultiNode_JoinRemove(t *testing.T) {

	r0, clean0, err := newNode(false, 0, 100, 300)
	require.NoError(t, err, "Error creating raft node 0")
	defer func() {
		err := r0.Close(true)
		require.NoError(t, err)
		clean0(true)
	}()

	err = r0.Open(true, map[string]string{"foo": "bar"})
	require.NoError(t, err)

	_, err = r0.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	r1, clean1, err := newNode(false, 1, 200, 300)
	require.NoError(t, err, "Error creating raft node 1")
	defer func() {
		err := r1.Close(true)
		require.NoError(t, err)
		clean1(true)
	}()

	err = r0.Join(200, 300, r1.Addr(), map[string]string{"foo": "bar"})
	require.NoError(t, err, "Error joining raft node 0")

	err = r1.Open(false, map[string]string{"foo": "bar"})
	require.NoError(t, err, "Error opening raft node 1")

	_, err = r0.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	// Check leader state on follower.
	leaderAddr, err := r1.LeaderAddr()
	require.NoError(t, err, "Error getting leader address")
	require.Equal(t, leaderAddr, r0.Addr(), "wrong leader address returned")

	id, err := r1.LeaderID()
	require.NoError(t, err)

	require.Equal(t, id, r0.ID(), "wrong leader ID returned")

	n0, err := r0.Nodes()
	require.NoError(t, err, "Error getting node list from raft node 0")
	require.Equal(t, len(n0), 2, "Number of nodes must be 2")

	// Remove a node.
	err = r0.Remove(r1.ID())
	require.NoError(t, err)

	nodes, err := r0.Nodes()
	require.NoError(t, err)

	require.Equal(t, len(nodes), 1, "size of cluster is not correct post remove")
	require.Equal(t, r0.Addr(), nodes[r0.ID()], "cluster does not have correct nodes post remove")

}

func Test_Raft_SingleNode_SnapshotOnDisk(t *testing.T) {
	r0, clean0, err := newNode(false, 0, 100, 400)
	require.NoError(t, err, "Error creating raft node 0")

	err = r0.Open(true, map[string]string{"foo": "bar"})
	require.NoError(t, err)

	_, err = r0.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	// Add event
	rand.Seed(42)
	expectedBalloonVersion := uint64(rand.Intn(50))
	for i := uint64(0); i < expectedBalloonVersion; i++ {
		_, err = r0.Add([]byte(fmt.Sprintf("Test Event %d", i)))
		require.NoError(t, err)
	}

	// request snapshot
	resp, err := r0.nodeHost.RequestSnapshot(400, dragonboat.SnapshotOption{}, 5*time.Second)
	require.NoError(t, err, "Error requesting snapshot")
	_, err = waitForResp(resp, 5*time.Second)
	require.NoError(t, err, "Error in snapshot request")

	// close node
	err = r0.Close(true)
	require.NoError(t, err)
	clean0(false)

	// restart node and check if recovers from snapshot
	r1, clean1, err := newNode(false, 0, 100, 400)
	require.NoError(t, err, "Error creating raft node 1")
	defer func() {
		err = r1.Close(true)
		require.NoError(t, err)
		clean1(true)
	}()

	err = r1.Open(true, map[string]string{"foo": "bar"})
	require.NoError(t, err)

	_, err = r1.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	require.Equal(t, expectedBalloonVersion, r1.fsm.balloon.Version(), "Error in state recovery from snapshot")

}

func Test_Raft_MultiNode_WithMetadata(t *testing.T) {

	log.SetLogger("Test_Raft_MultiNode_WithMetadata", log.SILENT)

	r0, clean0, err := newNode(false, 2, 100, 200)
	require.NoError(t, err, "Error creating raft node 0")
	defer func() {
		err := r0.Close(true)
		require.NoError(t, err)
		clean0(true)
	}()

	err = r0.Open(true, map[string]string{"foo": "bar"})
	require.NoError(t, err, "Error opening raft node 0")

	_, err = r0.WaitForLeader(10 * time.Second)
	require.NoError(t, err, "Error waiting for leader")

	r1, clean1, err := newNode(false, 3, 200, 200)
	require.NoError(t, err, "Error creating raft node 1")
	defer func() {
		err := r1.Close(true)
		require.NoError(t, err)
		clean1(true)
	}()

	err = r0.Join(200, 200, r1.Addr(), map[string]string{"foo": "bar"})
	require.NoError(t, err, "Error joining raft node 0")

	err = r1.Open(false, map[string]string{"foo": "bar"})
	require.NoError(t, err, "Error opening raft node 1")

	require.Equal(t, r0.Info()["meta"], r1.Info()["meta"], "Both nodes must have the same metadata.")
}

func BenchmarkRaftAdd(b *testing.B) {

	log.SetLogger("BenchmarkRaftAdd", log.SILENT)

	raftNodeA, _, err := newNode(false, 0, 1, 1)
	require.NoError(b, err, "Error creating testing node")
	// defer clean()

	err = raftNodeA.Open(true, map[string]string{"foo": "bar"})
	require.NoError(b, err)

	id, err := raftNodeA.WaitForLeader(10 * time.Second)
	require.NoError(b, err, "Error waiting for leader")
	fmt.Println("Leader ID A: ", id)

	// b.N shoul be eq or greater than 500k to avoid benchmark framework spreading more than one goroutine.
	b.N = 100000
	b.ResetTimer()
	b.SetParallelism(100)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			event := utilrand.Bytes(128)
			_, err := raftNodeA.Add(event)
			require.NoError(b, err)
		}
	})
}

func BenchmarkRaftAddBulk(b *testing.B) {

	log.SetLogger("BenchmarkRaftAddBulk", log.SILENT)

	log.SetLogger("BenchmarkRaftAdd", log.SILENT)

	raftNode, _, err := newNode(false, 0, 1, 1)
	require.NoError(b, err, "Error creating testing node")
	// defer clean()

	err = raftNode.Open(true, map[string]string{"foo": "bar"})
	require.NoError(b, err)

	id, err := raftNode.WaitForLeader(10 * time.Second)
	require.NoError(b, err, "Error waiting for leader")
	fmt.Println("Leader ID A: ", id)

	// b.N shoul be eq or greater than 500k to avoid benchmark framework spreading more than one goroutine.
	b.N = 2000000
	b.ResetTimer()
	b.SetParallelism(100)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			events := [][]byte{utilrand.Bytes(128)}
			_, err := raftNode.AddBulk(events)
			require.NoError(b, err)
		}
	})

}
