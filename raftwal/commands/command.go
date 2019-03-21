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

package commands

import (
	"bytes"

	"github.com/hashicorp/go-msgpack/codec"
)

// CommandType are commands that affect the state of the cluster,
// and must go through raft.
type CommandType uint8

// Commands which modify the database.
const (
	AddEventCommandType CommandType = iota
	MetadataSetCommandType
	MetadataDeleteCommandType
	TamperHyperCommandType
)

type AddEventCommand struct {
	Event []byte
}

type TamperHyperEventCommand struct {
	Event   []byte
	Version uint64
}

type MetadataSetCommand struct {
	Id   string
	Data map[string]string
}

type MetadataDeleteCommand struct {
	Id string
}

// msgpackHandle is a shared handle for encoding/decoding of structs
var msgpackHandle = &codec.MsgpackHandle{}

// Decode is used to encode a MsgPack object with type prefix.
func Decode(buf []byte, out interface{}) error {
	return codec.NewDecoder(bytes.NewReader(buf), msgpackHandle).Decode(out)
}

// Encode is used to encode a MsgPack object with type prefix
func Encode(t CommandType, cmd interface{}) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(uint8(t))
	err := codec.NewEncoder(&buf, msgpackHandle).Encode(cmd)
	return buf.Bytes(), err
}
