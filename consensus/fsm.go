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
	"io"
	"sync"

	"github.com/bbva/qed/balloon"
	"github.com/bbva/qed/hashing"
	"github.com/bbva/qed/storage"

	"github.com/lni/dragonboat/statemachine"
	"github.com/prometheus/common/log"
)

type fsmResponse struct {
	snapshots []*balloon.Snapshot
	version   uint64
	err       error
}

type fsmState struct {
	Index, BalloonVersion uint64
}

func (s fsmState) shouldApply(f *fsmState) bool {

	if f.Index <= s.Index {
		return false
	}

	if f.BalloonVersion > 0 && s.BalloonVersion >= f.BalloonVersion {
		panic(fmt.Sprintf("balloonVersion panic! old: %+v, new %+v", s, f))
	}

	return true
}

type BalloonFSM struct {
	hasherF func() hashing.Hasher

	store   storage.ManagedStore
	balloon *balloon.Balloon
	state   *fsmState

	metaMu sync.RWMutex
	meta   *Metadata

	restoreMu sync.RWMutex // Restore needs exclusive access to database.
}

func NewBalloonFSM(store storage.ManagedStore, hasherF func() hashing.Hasher) (*BalloonFSM, error) {
	meta := new(Metadata)
	meta.Nodes = make(map[uint64]*NodeInfo)
	hasherF = hashing.NewSha256Hasher

	b, err := balloon.NewBalloon(store, hasherF)
	if err != nil {
		return nil, err
	}

	return &BalloonFSM{
		hasherF: hasherF,
		store:   store,
		balloon: b,
		meta:    meta,
	}, nil
}

// Open opens the existing on disk state machine to be used or it creates a
// new state machine with empty state if it does not exist. Open returns the
// most recent index value of the Raft log it has persisted, or it returns 0
// when the state machine is a new one.
//
// The provided read only chan struct{} channel is used to notify the Open
// method that the node has been stopped and the Open method can choose to
// abort by returning an ErrOpenStopped error.
//
// Open is called shortly after the Raft node is started. The Update method
// and the Lookup method will not be called until call to the Open method is
// successfully completed.
func (fsm *BalloonFSM) Open(stopc <-chan struct{}) (uint64, error) {
	var err error

	fsm.state, err = loadState(fsm.store)
	if err != nil {
		log.Infof("There was an error recovering the FSM state!!")
		return 0, err
	}

	return fsm.state.BalloonVersion, nil
}

// Update updates the IOnDiskStateMachine instance. The input Entry slice
// is a list of continuous proposed and committed commands from clients, they
// are provided together as a batch so the IOnDiskStateMachine implementation
// can choose to batch them and apply together to hide latency. Update returns
// the input entry slice with the Result field of all its members set.
//
// The Index field of each input Entry instance is the Raft log index of each
// entry, it is IOnDiskStateMachine's responsibility to atomically persist the
// Index value together with the corresponding Entry update.
//
// The Update method can choose to synchronize all its in-core state with that
// on disk. This can minimize the number of committed Raft entries that need
// to be re-applied after reboot. Update is also allowed to postpone such
// synchronization until the Sync method is invoked, this approach produces
// higher throughput during fault free running at the cost that some of the
// most recent Raft entries not fully synchronized onto disks will have to be
// re-applied after reboot.
//
// The Update method must be deterministic, meaning given the same initial
// state of IOnDiskStateMachine and the same input sequence, it should reach
// to the same updated state and outputs the same results. The input entry
// slice should be the only input to this method. Reading from the system
// clock, random number generator or other similar external data sources will
// violate the deterministic requirement of the Update method.
//
// Concurrent call to the Lookup method and the SaveSnapshot method is allowed
// when the state machine is being updated by the Update method.
//
// The IOnDiskStateMachine implementation should not keep a reference to the
// input entry slice after return.
//
// Update returns an error when there is unrecoverable error for updating the
// on disk state machine, e.g. disk failure when trying to update the state
// machine.
func (fsm *BalloonFSM) Update(entries []statemachine.Entry) ([]statemachine.Entry, error) {

	for i, e := range entries {
		fsmResponse := fsm.apply(e)
		if fsmResponse.err != nil {
			return nil, fmt.Errorf("fsm.Update(): Error in response %v", fsmResponse.err)
		}
		data, err := encodeMsgPack(fsmResponse.snapshots)
		if err != nil {
			return nil, fmt.Errorf("fsm.Update():Error encoding fsmResponse snapshots: %v", err)
		}

		entries[i].Result = statemachine.Result{Data: data.Bytes(), Value: fsmResponse.version}
	}

	return entries, nil
}

func (fsm *BalloonFSM) metaUpdate(leaderId uint64, nodes map[uint64]string) {
	fsm.metaMu.Lock()
	defer fsm.metaMu.Unlock()
	fsm.meta.LeaderId = leaderId
	for k, _ := range fsm.meta.Nodes {
		if nodes[k] == "" {
			delete(fsm.meta.Nodes, k)
		}
	}
}

func (fsm *BalloonFSM) metaMerge(m *Metadata) {
	fsm.metaMu.Lock()
	defer fsm.metaMu.Unlock()
	for k, v := range m.Nodes {
		fsm.meta.Nodes[k] = v
	}
}

func (fsm *BalloonFSM) Info() *Metadata {
	return fsm.meta
}

// Apply applies a Raft log entry to the database.
func (fsm *BalloonFSM) apply(e statemachine.Entry) *fsmResponse {

	cmd := NewCommandFromRaft(e.Cmd)

	switch cmd.id {
	case AddEventCommandType:
		var eventDigest hashing.Digest
		if err := cmd.Decode(&eventDigest); err != nil {
			return &fsmResponse{err: err}
		}

		newState := &fsmState{Index: e.Index, BalloonVersion: fsm.balloon.Version()}
		if fsm.state.shouldApply(newState) {
			return fsm.applyAdd(eventDigest, newState)
		}
		return &fsmResponse{err: fmt.Errorf("state already applied!: %+v -> %+v", fsm.state, newState)}

	case AddEventsBulkCommandType:
		var eventDigests []hashing.Digest
		if err := cmd.Decode(&eventDigests); err != nil {
			return &fsmResponse{err: err}
		}
		// INFO: after applying a bulk there will be a jump in term version due to balloon version mapping.
		newState := &fsmState{e.Index, fsm.balloon.Version() + uint64(len(eventDigests)-1)}
		if fsm.state.shouldApply(newState) {
			return fsm.applyAddBulk(eventDigests, newState)
		}
		return &fsmResponse{err: fmt.Errorf("state already applied!: %+v -> %+v", fsm.state, newState)}

	case MetadataUpdateCommandType:
		var meta *Metadata
		if err := cmd.Decode(&meta); err != nil {
			return &fsmResponse{err: err}
		}

		fsm.metaMerge(meta)

		return &fsmResponse{}

	default:
		return &fsmResponse{err: fmt.Errorf("unknown command: %v", cmd.id)}

	}
}

// Lookup queries the state of the IOnDiskStateMachine instance and
// returns the query result as a byte slice. The input byte slice specifies
// what to query, it is up to the IOnDiskStateMachine implementation to
// interpret the input byte slice. The returned byte slice contains the query
// result provided by the IOnDiskStateMachine implementation.
//
// When an error is returned by the Lookup method, the error will be passed
// to the caller of NodeHost's ReadLocalNode() or SyncRead() methods to be
// handled. A typical scenario for returning an error is that the state
// machine has already been closed or aborted from a RecoverFromSnapshot
// procedure before Lookup is handled.
//
// Concurrent call to the Update and RecoverFromSnapshot method should be
// allowed when call to the Lookup method is being processed.
//
// The IOnDiskStateMachine implementation should not keep a reference of
// the input byte slice after return.
//
// The Lookup method is a read only method, it should never change the state
// of IOnDiskStateMachine.
func (fsm *BalloonFSM) Lookup(key interface{}) (interface{}, error) {
	return fsm.store.Get(storage.FSMStateTable, key.([]byte))
}

// Sync synchronizes all in-core state of the state machine to permanent
// storage so the state machine can continue from its latest state after
// reboot.
//
// Sync is always invoked with mutual exclusion protection from the Update,
// PrepareSnapshot, RecoverFromSnapshot and Close method.
//
// Sync returns an error when there is unrecoverable error for synchronizing
// the in-core state.
func (fsm *BalloonFSM) Sync() error {
	return nil
}

// PrepareSnapshot prepares the snapshot to be concurrently captured and saved.
// PrepareSnapshot is invoked before SaveSnapshot is called and it is invoked
// with mutual exclusion protection from the Update, Sync, RecoverFromSnapshot
// and Close methods.
//
// PrepareSnapshot in general saves a state identifier of the current state,
// such state identifier is usually a version number, a sequence number, a
// change ID or some other in memory small data structure used for describing
// the point in time state of the state machine. The state identifier is
// returned as an interface{} and it is provided to the SaveSnapshot() method
// so a snapshot of the state machine state at the identified point in time
// can be saved when SaveSnapshot is invoked.
//
// PrepareSnapshot returns an error when there is unrecoverable error for
// preparing the snapshot.
type snapshotState struct {
	snapshotId uint64
	state      *fsmState
}

func (fsm *BalloonFSM) PrepareSnapshot() (interface{}, error) {
	snapshotId, err := fsm.store.Snapshot()
	if err != nil {
		return nil, err
	}
	state, err := loadState(fsm.store)

	return &snapshotState{snapshotId, state}, err
}

// SaveSnapshot saves the point in time state of the IOnDiskStateMachine
// instance identified by the input state identifier to the provided
// io.Writer. The io.Writer is a connection to a remote node usually
// significantly behind in terms of its state progress.
//
// It is important to understand that SaveSnapshot should never be implemented
// to save the current latest state of the state machine when it is invoked.
// The latest state is not what suppose to be saved as the state might have
// already been updated by the Update method after the completion of the
// PrepareSnapshot method.
//
// It is application's responsibility to save the complete state to the
// provided io.Writer in a deterministic manner. That is for the same state
// machine state, when SaveSnapshot is invoked multiple times with the same
// input, the content written to the provided io.Writer should always be the
// same. This is a read-only method, it should never change the state of the
// IOnDiskStateMachine.
//
// When there is any connectivity error between the local node and the remote
// node, an ErrSnapshotStreaming will be returned by io.Writer's Write method
// when trying to write data to it. The SaveSnapshot method should return
// ErrSnapshotStreaming to terminate early.
//
// It is SaveSnapshot's responsibility to free the resources owned by the
// input state identifier when it is done.
//
// The provided read-only chan struct{} is provided to notify the SaveSnapshot
// method that the associated Raft node is being closed so the
// IOnDiskStateMachine can choose to abort the SaveSnapshot procedure and
// return ErrSnapshotStopped immediately.
//
// The SaveSnapshot method is allowed to be invoked when there is concurrent
// call to the Update method.
func (fsm *BalloonFSM) SaveSnapshot(state interface{}, w io.Writer, abort <-chan struct{}) error {
	var done chan struct{}
	var err error
	snapshotState := state.(*snapshotState)

	go func() {
		defer func() { close(done) }()
		if err = fsm.store.Backup(w, snapshotState.snapshotId); err != nil {
			log.Infof("fsm.SaveSnapshot(): Error saving snapshot %v", err)
			return
		}
	}()

	for {
		select {
		case <-abort:
			log.Infof("fsm.SaveSnapshot() aborted by raft signal")
			return statemachine.ErrSnapshotStopped
		case <-done:
			log.Debugf("fsm.SaveSnapshot() ended successfully")
			return err
		}
	}
	return err
}

// RecoverFromSnapshot recovers the state of the IOnDiskStateMachine
// instance from a snapshot captured by the SaveSnapshot() method. The saved
// snapshot is provided as an io.Reader backed by a file on disk.
//
// Dragonboat ensures that the Update, Sync, PrepareSnapshot, SaveSnapshot
// and Close methods are not be invoked when RecoverFromSnapshot() is in
// progress.
//
// The provided read-only chan struct{} is provided to notify the
// RecoverFromSnapshot method that the associated Raft node has been closed.
// On receiving such notification, RecoverFromSnapshot() can choose to
// abort recovering from the snapshot and return an ErrSnapshotStopped error
// immediately. Other than ErrSnapshotStopped, IOnDiskStateMachine should
// only return a non-nil error when the system need to be immediately halted
// for non-recoverable error, e.g. disk error preventing you from reading the
// complete saved snapshot.
//
// RecoverFromSnapshot should always synchronize its in-core state with that
// on disk.
//
// RecoverFromSnapshot is invoked when the node's progress is significantly
// behind its leader.
func (fsm *BalloonFSM) RecoverFromSnapshot(r io.Reader, abort <-chan struct{}) error {
	var done chan struct{}
	var err error

	go func() {
		defer func() { close(done) }()
		if err = fsm.store.Load(r); err != nil {
			return
		}
	}()

	for {
		select {
		case <-abort:
			log.Infof("fsm.RecoverFromSnapshot() abort signaled by raft")
			return statemachine.ErrSnapshotStopped
		case <-done:
			return err
		}
	}

	return err
}

// Close closes the IOnDiskStateMachine instance.
//
// Close allows the application to finalize resources to a state easier to
// be re-opened and restarted in the future. It is important to understand
// that Close is not guaranteed to be always called, e.g. node can crash at
// any time without calling Close. IOnDiskStateMachine should be designed
// in a way that the safety and integrity of its on disk data doesn't rely
// on whether Close is called or not.
//
// Other than setting up some internal flags to indicate that the
// IOnDiskStateMachine instance has been closed, the Close method is not
// allowed to update the state of IOnDiskStateMachine visible to the Lookup
// method.
func (fsm *BalloonFSM) Close() error {
	fsm.balloon.Close()
	return nil
}

func (fsm *BalloonFSM) applyAdd(event []byte, state *fsmState) *fsmResponse {

	snapshot, mutations, err := fsm.balloon.Add(event)
	if err != nil {
		return &fsmResponse{err: err}
	}

	stateBuff, err := encodeMsgPack(state)
	if err != nil {
		return &fsmResponse{err: err}
	}

	mutations = append(mutations, storage.NewMutation(storage.FSMStateTable, storage.FSMStateTableKey, stateBuff.Bytes()))
	err = fsm.store.Mutate(mutations)
	if err != nil {
		return &fsmResponse{err: err}
	}
	fsm.state = state

	return &fsmResponse{snapshots: []*balloon.Snapshot{snapshot}}
}

func (fsm *BalloonFSM) applyAddBulk(events []hashing.Digest, state *fsmState) *fsmResponse {

	snapshots, mutations, err := fsm.balloon.AddBulk(events)
	if err != nil {
		return &fsmResponse{err: err}
	}

	stateBuff, err := encodeMsgPack(state)
	if err != nil {
		return &fsmResponse{err: err}
	}

	mutations = append(mutations, storage.NewMutation(storage.FSMStateTable, storage.FSMStateTableKey, stateBuff.Bytes()))
	err = fsm.store.Mutate(mutations)
	if err != nil {
		return &fsmResponse{err: err}
	}
	fsm.state = state

	return &fsmResponse{snapshots: snapshots}
}

// load latest fsmState from storage or returns an error
func loadState(s storage.ManagedStore) (*fsmState, error) {
	var state fsmState
	kvstate, err := s.Get(storage.FSMStateTable, storage.FSMStateTableKey)
	if err == storage.ErrKeyNotFound {
		log.Infof("Unable to find previous state: assuming a clean instance")
		return &fsmState{0, 0}, nil
	}
	if err != nil {
		return nil, err
	}
	err = decodeMsgPack(kvstate.Value, &state)

	return &state, err
}

func (fsm *BalloonFSM) QueryDigestMembershipConsistency(keyDigest hashing.Digest, version uint64) (*balloon.MembershipProof, error) {
	return fsm.balloon.QueryDigestMembershipConsistency(keyDigest, version)
}

func (fsm *BalloonFSM) QueryMembershipConsistency(event []byte, version uint64) (*balloon.MembershipProof, error) {
	return fsm.balloon.QueryMembershipConsistency(event, version)
}

func (fsm *BalloonFSM) QueryDigestMembership(keyDigest hashing.Digest) (*balloon.MembershipProof, error) {
	return fsm.balloon.QueryDigestMembership(keyDigest)
}

func (fsm *BalloonFSM) QueryMembership(event []byte) (*balloon.MembershipProof, error) {
	return fsm.balloon.QueryMembership(event)
}

func (fsm *BalloonFSM) QueryConsistency(start, end uint64) (*balloon.IncrementalProof, error) {
	return fsm.balloon.QueryConsistency(start, end)
}
