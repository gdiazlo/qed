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

type Config struct {
	// Unique name for this node. It identifies itself both in raft and
	// gossip clusters. If not set, fallback to hostname.
	NodeId uint64

	// Unique name for the node's cluster. It identifies this cluster against
	// other clusters which might be present on a given node.
	ClusterId uint64

	// TLS server bind address/port.
	HTTPAddr string

	// Raft communication bind address/port.
	RaftAddr string

	// Raft management server bind address/port. Useful to join the cluster
	// and get cluster information.
	MgmtAddr string

	// Metrics bind address/port.
	MetricsAddr string

	// WAL path
	RaftPath string
}
