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
	"bytes"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"

	"github.com/bbva/qed/balloon"
	"github.com/bbva/qed/crypto/hashing"
	"github.com/bbva/qed/protocol"
	"github.com/bbva/qed/storage"
)

type fsmResponse struct {
	err error
	val interface{}
}

type VersionMetadata struct {
	PreviousVersion uint64
	NewVersion      uint64
}

func (m *VersionMetadata) encode() ([]byte, error) {
	return encodeMsgPack(m)
}

func (m *VersionMetadata) decode(value []byte) error {
	return decodeMsgPack(value, m)
}

type fsmState struct {
	Index, Term, BalloonVersion uint64
}

func (s *fsmState) encode() ([]byte, error) {
	return encodeMsgPack(s)
}

func (s *fsmState) decode(value []byte) error {
	return decodeMsgPack(value, s)
}

func (s *fsmState) shouldApply(f *fsmState) bool {

	if s.Term > f.Term {
		return false
	}

	if s.Term == f.Term && s.Index >= f.Index && s.Index != 0 {
		return false
	}

	if f.BalloonVersion > 0 && s.BalloonVersion >= f.BalloonVersion {
		panic(fmt.Sprintf("balloonVersion panic! old: %+v, new %+v", s, f))
	}

	return true
}

func (n *RaftNode) loadState() error {
	kvstate, err := n.db.Get(storage.FSMStateTable, storage.FSMStateTableKey)
	if err == storage.ErrKeyNotFound {
		n.log.Infof("Unable to find previous state: assuming a clean instance")
		n.state = new(fsmState)
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "loading state failed")
	}
	var state fsmState
	state.decode(kvstate.Value)
	if err != nil {
		return errors.Wrap(err, "unable to decode state")
	}
	n.state = &state
	return nil
}

/*
	RaftBalloon API implements the Ballon API in the RAFT system
*/

// Add function applies an add operation into a Raft balloon.
// As a result, it returns a shapshot, but previously it sends the snapshot
// to the agents channel, in order to be published/queried.
func (n *RaftNode) Add(event []byte) (*balloon.Snapshot, error) {
	snapshots, err := n.AddBulk(append([][]byte{}, event))
	if err != nil {
		return nil, err
	}
	return snapshots[0], nil
}

// AddBulk function applies an add bulk operation into a Raft balloon.
// As a result, it returns a bulk of shapshots, but previously it sends each snapshot
// of the bulk to the agents channel, in order to be published/queried.
func (n *RaftNode) AddBulk(bulk [][]byte) ([]*balloon.Snapshot, error) {
	// Hash events
	var eventHashBulk []hashing.Digest
	for _, event := range bulk {
		eventHashBulk = append(eventHashBulk, n.hasherF().Do(event))
	}

	// Create and apply command.
	cmd := newCommand(addEventCommandType)
	cmd.encode(eventHashBulk)
	resp, err := n.propose(cmd)
	if err != nil {
		return nil, err
	}

	snapshotBulk := resp.(*fsmResponse).val.([]*balloon.Snapshot)

	//Send snapshot to the snapshot channel
	// TODO move this to an upper layer (shard manager?)
	for _, s := range snapshotBulk {
		p := protocol.Snapshot(*s)
		n.snapshotsCh <- &p
	}

	return snapshotBulk, nil
}

// QueryDigestMembershipConsistency acts as a passthrough when an event digest is given to
// request a membership proof against a certain balloon version.
func (n *RaftNode) QueryDigestMembershipConsistency(keyDigest hashing.Digest, version uint64) (*balloon.MembershipProof, error) {
	n.metrics.DigestMembershipQueries.Inc()
	return n.balloon.QueryDigestMembershipConsistency(keyDigest, version)
}

// QueryMembershipConsistency acts as a passthrough when an event is given to request a
// membership proof against a certain balloon version.
func (n *RaftNode) QueryMembershipConsistency(event []byte, version uint64) (*balloon.MembershipProof, error) {
	n.metrics.MembershipQueries.Inc()
	return n.balloon.QueryMembershipConsistency(event, version)
}

// QueryDigestMembership acts as a passthrough when an event digest is given to request a
// membership proof against the last balloon version.
func (n *RaftNode) QueryDigestMembership(keyDigest hashing.Digest) (*balloon.MembershipProof, error) {
	n.metrics.DigestMembershipQueries.Inc()
	return n.balloon.QueryDigestMembership(keyDigest)
}

// QueryMembership acts as a passthrough when an event is given to request a membership proof
// against the last balloon version.
func (n *RaftNode) QueryMembership(event []byte) (*balloon.MembershipProof, error) {
	n.metrics.MembershipQueries.Inc()
	return n.balloon.QueryMembership(event)
}

// QueryConsistency acts as a passthrough when requesting an incremental proof.
func (n *RaftNode) QueryConsistency(start, end uint64) (*balloon.IncrementalProof, error) {
	n.metrics.IncrementalQueries.Inc()
	return n.balloon.QueryConsistency(start, end)
}

/**************** END OF API ******************/

// Apply applies a Raft log entry to the database.
func (n *RaftNode) Apply(l *raft.Log) interface{} {
	cmd := newCommandFromRaft(l.Data)

	// We should ignore unknown message types.
	// Otherwise, apply if possible. If not, we have to panic so
	// that we crash and our state doesn't diverge.

	switch cmd.id {
	case addEventCommandType:
		var eventDigests []hashing.Digest
		if err := cmd.decode(&eventDigests); err != nil {
			panic(fmt.Sprintf("Unable to decode command: %v", err))
		}
		newState := &fsmState{l.Index, l.Term, n.balloon.Version() + uint64(len(eventDigests)) - 1}
		if n.state.shouldApply(newState) {
			return n.applyAdd(eventDigests, newState)
		}
		return &fsmResponse{fmt.Errorf("state already applied!: %+v -> %+v", n.state, newState), nil}

	default:
		// ignore
		n.log.Warnf("Unknown command: %v", cmd.id)
		return nil

	}
}

// StoreConfiguration is invoked once a log entry containing a configuration
// change is committed. It takes the index at which the configuration was
// written and the configuration value.
func (n *RaftNode) StoreConfiguration(index uint64, config raft.Configuration) {
	n.log.Debugf("Node [%s] - Configuration changed on index [%d]: %+v", n.info.NodeId, index, config)
}

// Snapshot returns a snapshot of the key-value store. The caller must ensure that
// no Raft transaction is taking place during this call. Hashicorp Raft
// guarantees that this function will not be called concurrently with Apply.
func (n *RaftNode) Snapshot() (raft.FSMSnapshot, error) {
	lastSeqNum := n.db.LastWALSequenceNumber()
	n.log.Debugf("Generating snapshot until seqNum: %d (balloon version %d)", lastSeqNum, n.balloon.Version())
	return &fsmSnapshot{lastSeqNum, n.balloon.Version()}, nil
}

// Restore restores the node to a previous state.
func (n *RaftNode) Restore(rc io.ReadCloser) error {

	n.log.Infof("Recovering from snapshot (last applied version: %d)...", n.state.BalloonVersion)

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return err
	}
	var snap fsmSnapshot
	if err := snap.decode(buf.Bytes()); err != nil {
		return err
	}

	if n.raft != nil { // we are not restoring on startup

		// we make a remote call to fetch the snapshot
		reader, err := n.attemptToFetchSnapshot(snap.LastSeqNum, n.state.BalloonVersion)
		if err != nil {
			return err
		}

		if err := n.db.LoadSnapshot(reader); err != nil {

			return err
		}
	}

	n.loadState()
	n.balloon.RefreshVersion()

	n.log.Infof("Recovering finished, new version: %d", n.state.BalloonVersion)

	return nil
}

func (n *RaftNode) applyAdd(hashes []hashing.Digest, state *fsmState) *fsmResponse {

	resp := new(fsmResponse)
	snapshotBulk, mutations, err := n.balloon.AddBulk(hashes)
	if err != nil {
		n.log.Panicf("Unable to add bulk: %v", err)
	}

	stateBuff, err := state.encode()
	if err != nil {
		n.log.Panicf("Unable to encode state: %v", err)
	}
	mutations = append(mutations, storage.NewMutation(storage.FSMStateTable, storage.FSMStateTableKey, stateBuff))

	meta := &VersionMetadata{
		PreviousVersion: n.state.BalloonVersion,
		NewVersion:      state.BalloonVersion,
	}
	metaBytes, err := meta.encode()
	if err != nil {
		n.log.Panicf("Unable to encode version metadata: %v", err)
	}

	err = n.db.Mutate(mutations, metaBytes)
	if err != nil {
		n.log.Panicf("Unable to mutate database: %v", err)
	}
	n.state = state
	resp.val = snapshotBulk
	n.metrics.Adds.Add(float64(len(hashes)))

	return resp
}
