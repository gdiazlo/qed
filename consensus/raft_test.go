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

	"github.com/bbva/qed/log"
	"github.com/bbva/qed/protocol"
	"github.com/bbva/qed/storage/rocks"
	metrics_utils "github.com/bbva/qed/testutils/metrics"
	utilrand "github.com/bbva/qed/testutils/rand"
	"github.com/stretchr/testify/require"
)

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

func newNodeBench(b *testing.B, id uint64) (*RaftBalloon, func()) {
	storePath := fmt.Sprintf("/var/tmp/raft-test/node%d/db", id)

	err := os.MkdirAll(storePath, os.FileMode(0755))
	require.NoError(b, err)
	store, err := rocks.NewRocksDBStore(storePath)
	require.NoError(b, err)

	raftPath := fmt.Sprintf("/var/tmp/raft-test/node%d/raft", id)
	err = os.MkdirAll(raftPath, os.FileMode(0755))
	require.NoError(b, err)

	snapshotsCh := make(chan *protocol.Snapshot, 10000)
	snapshotsDrainer(snapshotsCh)

	node, err := NewRaftBalloon(raftPath, raftAddr(id), id, id, store, snapshotsCh)
	require.NoError(b, err)

	srvCloseF := metrics_utils.StartMetricsServer(node, store)

	return node, func() {
		srvCloseF()
		close(snapshotsCh)
		os.RemoveAll(fmt.Sprintf("/var/tmp/raft-test/node%d", id))
	}

}
func BenchmarkRaftAdd(b *testing.B) {

	log.SetLogger("BenchmarkRaftAdd", log.SILENT)

	raftNode, clean := newNodeBench(b, 1)
	defer clean()

	err := raftNode.Open(true, map[string]string{"foo": "bar"})
	require.NoError(b, err)

	// b.N shoul be eq or greater than 500k to avoid benchmark framework spreading more than one goroutine.
	b.N = 2000000
	b.ResetTimer()
	b.SetParallelism(100)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			event := utilrand.Bytes(128)
			_, err := raftNode.Add(event)
			require.NoError(b, err)
		}
	})

}
