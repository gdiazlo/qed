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

package apihttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/bbva/qed/balloon"
	"github.com/bbva/qed/balloon/history"
	"github.com/bbva/qed/balloon/hyper"
	"github.com/bbva/qed/consensus"
	"github.com/bbva/qed/hashing"
	"github.com/bbva/qed/protocol"
	"github.com/bbva/qed/storage/rocks"
	metrics_utils "github.com/bbva/qed/testutils/metrics"
	"github.com/bbva/qed/testutils/rand"
	"github.com/stretchr/testify/require"
	assert "github.com/stretchr/testify/require"
)

type fakeRaftBalloon struct {
	dbPath       string
	raftDir      string
	raftBindAddr string
	raftID       string
}

func (b fakeRaftBalloon) Add(event []byte) (*balloon.Snapshot, error) {
	return &balloon.Snapshot{
		EventDigest:   hashing.Digest{0x02},
		HistoryDigest: hashing.Digest{0x00},
		HyperDigest:   hashing.Digest{0x01},
		Version:       0}, nil
}

func (b fakeRaftBalloon) AddBulk(bulk [][]byte) ([]*balloon.Snapshot, error) {
	return []*balloon.Snapshot{
		{
			EventDigest:   hashing.Digest{0x02},
			HistoryDigest: hashing.Digest{0x00},
			HyperDigest:   hashing.Digest{0x01},
			Version:       0,
		},
		{
			EventDigest:   hashing.Digest{0x05},
			HistoryDigest: hashing.Digest{0x03},
			HyperDigest:   hashing.Digest{0x04},
			Version:       1,
		},
	}, nil
}

func (b fakeRaftBalloon) Join(nodeId, clusterId uint64, addr string, metadata map[string]string) error {
	return nil
}

func (b fakeRaftBalloon) QueryDigestMembershipConsistency(keyDigest hashing.Digest, version uint64) (*balloon.MembershipProof, error) {
	return &balloon.MembershipProof{
		Exists:         true,
		HyperProof:     hyper.NewQueryProof([]byte{0x0}, []byte{0x0}, hyper.AuditPath{}, nil),
		HistoryProof:   history.NewMembershipProof(0, 0, history.AuditPath{}, nil),
		CurrentVersion: 1,
		QueryVersion:   1,
		ActualVersion:  2,
		KeyDigest:      keyDigest,
		Hasher:         hashing.NewFakeXorHasher(),
	}, nil
}
func (b fakeRaftBalloon) QueryDigestMembership(keyDigest hashing.Digest) (*balloon.MembershipProof, error) {
	return &balloon.MembershipProof{
		Exists:         true,
		HyperProof:     hyper.NewQueryProof([]byte{0x0}, []byte{0x0}, hyper.AuditPath{}, nil),
		HistoryProof:   history.NewMembershipProof(0, 0, history.AuditPath{}, nil),
		CurrentVersion: 1,
		QueryVersion:   1,
		ActualVersion:  2,
		KeyDigest:      keyDigest,
		Hasher:         hashing.NewFakeXorHasher(),
	}, nil
}

func (b fakeRaftBalloon) QueryMembershipConsistency(event []byte, version uint64) (*balloon.MembershipProof, error) {
	hasher := hashing.NewFakeXorHasher()
	return &balloon.MembershipProof{
		Exists:         true,
		HyperProof:     hyper.NewQueryProof([]byte{0x0}, []byte{0x0}, hyper.AuditPath{}, nil),
		HistoryProof:   history.NewMembershipProof(0, 0, history.AuditPath{}, nil),
		CurrentVersion: 1,
		QueryVersion:   1,
		ActualVersion:  2,
		KeyDigest:      hasher.Do(event),
		Hasher:         hasher,
	}, nil
}

func (b fakeRaftBalloon) QueryMembership(event []byte) (*balloon.MembershipProof, error) {
	hasher := hashing.NewFakeXorHasher()
	return &balloon.MembershipProof{
		Exists:         true,
		HyperProof:     hyper.NewQueryProof([]byte{0x0}, []byte{0x0}, hyper.AuditPath{}, nil),
		HistoryProof:   history.NewMembershipProof(0, 0, history.AuditPath{}, nil),
		CurrentVersion: 1,
		QueryVersion:   1,
		ActualVersion:  2,
		KeyDigest:      hasher.Do(event),
		Hasher:         hasher,
	}, nil
}

func (b fakeRaftBalloon) QueryConsistency(start, end uint64) (*balloon.IncrementalProof, error) {
	var pathKey [10]byte
	ip := balloon.IncrementalProof{
		Start:     2,
		End:       8,
		AuditPath: history.AuditPath{pathKey: hashing.Digest{0x00}},
		Hasher:    hashing.NewFakeXorHasher(),
	}
	return &ip, nil
}

func (b fakeRaftBalloon) Info() map[string]interface{} {
	return make(map[string]interface{})
}

func TestHealthCheckHandler(t *testing.T) {
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	req, err := http.NewRequest("HEAD", "/healthcheck", nil)
	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(HealthCheckHandler)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusNoContent {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusNoContent)
	}

	// Check the response body is what we expect.
	if rr.Body.String() != "" {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), "")
	}
}

func TestAdd(t *testing.T) {
	// Create a request to pass to our handler. We pass a message as a data.
	// If it's nil it will fail.
	data, _ := json.Marshal(&protocol.Event{[]byte("this is a sample event")})

	req, err := http.NewRequest("POST", "/events", bytes.NewBuffer(data))
	if len(data) == 0 {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := Add(fakeRaftBalloon{})

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusCreated)
	}

	// Check the body response
	snapshot := &protocol.Snapshot{}

	_ = json.Unmarshal([]byte(rr.Body.String()), snapshot)

	if !bytes.Equal(snapshot.HyperDigest, []byte{0x1}) {
		t.Errorf("HyperDigest is not consistent: %s", snapshot.HyperDigest)
	}

	if !bytes.Equal(snapshot.HistoryDigest, []byte{0x0}) {
		t.Errorf("HistoryDigest is not consistent %s", snapshot.HistoryDigest)
	}

	if snapshot.Version != 0 {
		t.Errorf("Version is not consistent")
	}
}

func TestAddBulk(t *testing.T) {
	// Create a request to pass to our handler. We pass a message as a data.
	// If it's nil it will fail.
	data, _ := json.Marshal(protocol.EventsBulk{Events: [][]byte{
		[]byte("this is event 1"),
		[]byte("this is event 2"),
	}})

	req, err := http.NewRequest("POST", "/events/bulk", bytes.NewBuffer(data))
	if len(data) == 0 {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := AddBulk(fakeRaftBalloon{})

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusCreated)
	}

	// Check the body response
	bs := []*protocol.Snapshot{}
	_ = json.Unmarshal([]byte(rr.Body.String()), &bs)

	expectedHyperDigests := [][]byte{[]byte{0x1}, []byte{0x4}}
	expectedHistoryDigests := [][]byte{[]byte{0x0}, []byte{0x3}}
	expectedVersions := []uint64{0, 1}

	for i, snap := range bs {
		if !bytes.Equal(snap.HyperDigest, expectedHyperDigests[i]) {
			t.Errorf("HyperDigest is not consistent: %s", snap.HyperDigest)
		}
		if !bytes.Equal(snap.HistoryDigest, expectedHistoryDigests[i]) {
			t.Errorf("HistoryDigest is not consistent: %s", snap.HistoryDigest)
		}
		if snap.Version != expectedVersions[i] {
			t.Errorf("Version is not consistent")
		}
	}
}

func TestMembership(t *testing.T) {

	key := []byte("this is a sample event")

	query, _ := json.Marshal(protocol.MembershipQuery{
		Key: key,
	})

	req, err := http.NewRequest("POST", "/proofs/membership", bytes.NewBuffer(query))
	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := Membership(fakeRaftBalloon{})
	expectedResult := &protocol.MembershipResult{
		Exists:         true,
		Hyper:          map[string]hashing.Digest{},
		History:        map[string]hashing.Digest{},
		CurrentVersion: 0x1,
		QueryVersion:   0x1,
		ActualVersion:  0x2,
		KeyDigest:      []uint8{0x17},
		Key:            key,
	}

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the body response
	actualResult := new(protocol.MembershipResult)
	json.Unmarshal([]byte(rr.Body.String()), actualResult)

	assert.Equal(t, expectedResult, actualResult, "Incorrect proof")

}

func TestMembershipConsistency(t *testing.T) {
	var version uint64 = 1
	key := []byte("this is a sample event")

	query, _ := json.Marshal(protocol.MembershipQuery{
		Key:     key,
		Version: &version,
	})

	req, err := http.NewRequest("POST", "/proofs/membership", bytes.NewBuffer(query))
	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := Membership(fakeRaftBalloon{})
	expectedResult := &protocol.MembershipResult{
		Exists:         true,
		Hyper:          map[string]hashing.Digest{},
		History:        map[string]hashing.Digest{},
		CurrentVersion: 0x1,
		QueryVersion:   0x1,
		ActualVersion:  0x2,
		KeyDigest:      []uint8{0x17},
		Key:            key,
	}

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the body response
	actualResult := new(protocol.MembershipResult)
	json.Unmarshal([]byte(rr.Body.String()), actualResult)

	assert.Equal(t, expectedResult, actualResult, "Incorrect proof")

}

func TestDigestMembership(t *testing.T) {

	hasher := hashing.NewSha256Hasher()
	eventDigest := hasher.Do([]byte("this is a sample event"))

	query, _ := json.Marshal(protocol.MembershipDigest{
		KeyDigest: eventDigest,
	})

	req, err := http.NewRequest("POST", "/proofs/digest-membership", bytes.NewBuffer(query))
	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := DigestMembership(fakeRaftBalloon{})
	expectedResult := &protocol.MembershipResult{
		Exists:         true,
		Hyper:          map[string]hashing.Digest{},
		History:        map[string]hashing.Digest{},
		CurrentVersion: 0x1,
		QueryVersion:   0x1,
		ActualVersion:  0x2,
		KeyDigest:      eventDigest,
		Key:            []byte(nil),
	}

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the body response
	actualResult := new(protocol.MembershipResult)
	json.Unmarshal([]byte(rr.Body.String()), actualResult)

	assert.Equal(t, expectedResult, actualResult, "Incorrect proof")

}

func TestDigestMembershipConsistency(t *testing.T) {

	version := uint64(1)
	hasher := hashing.NewSha256Hasher()
	eventDigest := hasher.Do([]byte("this is a sample event"))

	query, _ := json.Marshal(protocol.MembershipDigest{
		eventDigest,
		&version,
	})

	req, err := http.NewRequest("POST", "/proofs/digest-membership", bytes.NewBuffer(query))
	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := DigestMembership(fakeRaftBalloon{})
	expectedResult := &protocol.MembershipResult{
		Exists:         true,
		Hyper:          map[string]hashing.Digest{},
		History:        map[string]hashing.Digest{},
		CurrentVersion: 0x1,
		QueryVersion:   0x1,
		ActualVersion:  0x2,
		KeyDigest:      eventDigest,
		Key:            []byte(nil),
	}

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the body response
	actualResult := new(protocol.MembershipResult)
	json.Unmarshal([]byte(rr.Body.String()), actualResult)

	assert.Equal(t, expectedResult, actualResult, "Incorrect proof")

}

func TestIncremental(t *testing.T) {
	start := uint64(2)
	end := uint64(8)
	query, _ := json.Marshal(protocol.IncrementalRequest{
		start,
		end,
	})

	req, err := http.NewRequest("POST", "/proofs/incremental", bytes.NewBuffer(query))
	assert.NoError(t, err, "Error querying for incremental proof")

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := Incremental(fakeRaftBalloon{})
	expectedResult := &protocol.IncrementalResponse{
		start,
		end,
		map[string]hashing.Digest{"0|0": []uint8{0x0}},
	}

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	status := rr.Code
	assert.Equalf(t, http.StatusOK, status, "handler returned wrong status code: got %v want %v", status, http.StatusOK)

	// Check the body response
	actualResult := new(protocol.IncrementalResponse)
	json.Unmarshal([]byte(rr.Body.String()), actualResult)

	assert.Equal(t, expectedResult, actualResult, "Incorrect proof")
}

func TestAuthHandlerMiddleware(t *testing.T) {

	req, err := http.NewRequest("HEAD", "/healthcheck", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Set Api-Key header
	req.Header.Set("Api-Key", "this-is-my-api-key")

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := AuthHandlerMiddleware(HealthCheckHandler)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusNoContent {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusNoContent)
	}
}

func BenchmarkNoAuth(b *testing.B) {

	req, err := http.NewRequest("GET", "/health-check", nil)
	if err != nil {
		b.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(HealthCheckHandler)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Define our http client
	client := &http.Client{}

	for i := 0; i < b.N; i++ {
		client.Do(req)
	}
}

func BenchmarkAuth(b *testing.B) {

	req, err := http.NewRequest("GET", "/health-check", nil)
	if err != nil {
		b.Fatal(err)
	}

	// Set Api-Key header
	req.Header.Set("Api-Key", "this-is-my-api-key")

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := AuthHandlerMiddleware(HealthCheckHandler)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Define our http client
	client := &http.Client{}

	for i := 0; i < b.N; i++ {
		client.Do(req)
	}
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

func raftAddr(id uint64) string {
	return fmt.Sprintf("127.0.0.1:1830%d", id)
}

func newNode(join bool, port, nodeId, clusterId uint64) (*consensus.RaftBalloon, func(bool), error) {

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

	node, err := consensus.NewRaftBalloon(raftPath, raftAddr(port), clusterId, nodeId, store, snapshotsCh)
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

func BenchmarkApiAdd(b *testing.B) {

	raftNode, _, err := newNode(false, 0, 100, 100)
	require.NoError(b, err, "Error creating testing node")
	// defer clean()

	err = raftNode.Open(true, map[string]string{"foo": "bar"})
	require.NoError(b, err)

	_, err = raftNode.WaitForLeader(10 * time.Second)
	require.NoError(b, err, "Error waiting for leader")

	handler := Add(raftNode)

	time.Sleep(2 * time.Second)
	b.ResetTimer()
	b.N = 10000
	for i := 0; i < b.N; i++ {
		data, _ := json.Marshal(&protocol.Event{rand.Bytes(128)})
		req, _ := http.NewRequest("POST", "/events", bytes.NewBuffer(data))
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if status := rr.Code; status != http.StatusCreated {
			b.Errorf("handler returned wrong status code: got %v want %v",
				status, http.StatusCreated)
		}
	}
}
