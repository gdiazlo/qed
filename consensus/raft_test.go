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
	"os"
	"testing"
	"time"

	"github.com/bbva/qed/log"
	"github.com/bbva/qed/protocol"
	"github.com/bbva/qed/storage/rocks"
	metrics_utils "github.com/bbva/qed/testutils/metrics"
	utilrand "github.com/bbva/qed/testutils/rand"
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

func newNode(join bool, port, nodeId, clusterId uint64) (*RaftBalloon, func(), error) {

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

	return node, func() {
		srvCloseF()
		close(snapshotsCh)
		os.RemoveAll(storePath)
		os.RemoveAll(raftPath)
	}, nil

}

func Test_Raft_IsLeader(t *testing.T) {

	log.SetLogger("Test_Raft_IsLeader", log.SILENT)

	r, clean, err := newNode(false, 0, 1, 1)
	require.NoError(t, err, "Error creating testing node")
	defer clean()

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

func Test_Raft_OpenStore_CloseSingleNode(t *testing.T) {

	r, clean, err := newNode(false, 0, 1, 1)
	require.NoError(t, err, "Error creating testing node")
	defer clean()

	err = r.Open(true, map[string]string{"foo": "bar"})
	require.NoError(t, err)

	_, err = r.WaitForLeader(10 * time.Second)
	require.NoError(t, err)

	err = r.Close(true)
	require.NoError(t, err)

	err = r.Open(true, map[string]string{"foo": "bar"})
	require.Equal(t, err, ErrBalloonInvalidState, err, "incorrect error returned on re-open attempt")

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
