package gossip

import (
	"bytes"

	"github.com/hashicorp/go-msgpack/codec"
)

type MsgType uint8

const (
	SnapshotMsgType MsgType = 0
	BatchMsgType    MsgType = 1
	MetaMsgType     MsgType = 2
)

type BatchMsg struct {
	Id    uint64
	Ttl   int
	Snaps []*SnapshotMsg
}

type SnapshotMsg struct {
	HistoryDigest []byte
	HyperDigest   []byte
	Version       uint64
	EventDigest   []byte
}

var msgpackHandle = &codec.MsgpackHandle{}

func Decode(buf []byte, out interface{}) error {
	return codec.NewDecoder(bytes.NewReader(buf), msgpackHandle).Decode(out)
}

func Encode(t MsgType, cmd interface{}) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(uint8(t))
	err := codec.NewEncoder(&buf, msgpackHandle).Encode(cmd)
	return buf.Bytes(), err
}
