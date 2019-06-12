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

	"github.com/hashicorp/go-msgpack/codec"
)

var msgpackHandle = &codec.MsgpackHandle{}

// CommandType are commands that affect the state of the cluster,
// and must go through raft.
type CommandType uint8

const (
	AddEventCommandType CommandType = iota // Commands which modify the database.
	AddEventsBulkCommandType
	MetadataUpdateCommandType
)

type Command struct {
	id   CommandType
	data []byte
}

func (c *Command) Encode(cmd interface{}) error {
	var buf bytes.Buffer
	buf.WriteByte(uint8(c.id))
	err := codec.NewEncoder(&buf, msgpackHandle).Encode(cmd)
	if err != nil {
		return err
	}
	c.data = buf.Bytes()
	return nil
}

func (c *Command) Decode(out interface{}) error {
	if c.data == nil {
		return fmt.Errorf("Command is empty")
	}
	if c.id != CommandType(c.data[0]) {
		return fmt.Errorf("Command type %v is not %v", c.id, CommandType(c.data[0]))
	}
	return codec.NewDecoder(bytes.NewReader(c.data[1:]), msgpackHandle).Decode(out)
}

func NewCommand(t CommandType) *Command {
	return &Command{
		id: t,
	}
}

func NewCommandFromRaft(data []byte) *Command {
	return &Command{
		id:   CommandType(data[0]),
		data: data,
	}
}

// cmd := NewCommand(AddEventCommand)
// cmd.Encode(meta)