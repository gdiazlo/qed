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
package gossip

import (
	"net"
	"time"

	"github.com/hashicorp/memberlist"
)

// This is the default port that we use for the Agent communication
const DefaultBindPort int = 7946

// DefaultConfig contains the defaults for configurations.
func DefaultConfig() *Config {
	return &Config{
		BindAddr:            "",
		AdvertiseAddr:       "",
		LeaveOnTerm:         true,
		EnableCompression:   false,
		BroadcastTimeout:    5 * time.Second,
		LeavePropagateDelay: 0,
		TimeoutQueues:       200 * time.Millisecond,
		ProcessInterval:     1 * time.Second,
		CacheSize:           1 << 20,
		MaxSenders:          10,
	}
}

// Config is the configuration for creating an Agent instance
type Config struct {
	Log string `desc:"Set log level to info, error or debug"`

	// The name of this node. This must be unique in the cluster. If this
	// is not set, Auditor will set it to the hostname of the running machine.
	NodeName string `desc:"Set gossip name for this agent"`

	Role string `desc:"Set gossip role for this agent routing"`

	// BindAddr is the address that the Auditor agent's communication ports
	// will bind to. Auditor will use this address to bind to for both TCP
	// and UDP connections. If no port is present in the address, the default
	// port will be used.
	BindAddr string `desc:"Address ip:port to expose gossip protocol"`

	// AdvertiseAddr is the address agent will advertise to
	// other members of the cluster. Can be used for basic NAT traversal
	// where both the internal ip:port and external ip:port are known.
	AdvertiseAddr string `desc:"Address ip:port to advertise in gossip if our bind addr is not reachable from other agents"`

	// MetricsAddr is the address where the metrics server will expose its
	// API to enable mterics collectors retrieve them
	MetricsAddr string `desc:"Address ip:port to expose metrics"`

	// LeaveOnTerm controls if the agent does a graceful leave when receiving
	// the TERM signal. Defaults false. This can be changed on reload.
	LeaveOnTerm bool `desc:"Controls if the agent does a graceful leave when receiving the TERM signal"`

	// StartJoin is a list of addresses to attempt to join when the
	// agent starts. If the agent is unable to communicate with any of these
	// addresses, then the agent will error and exit.
	StartJoin []string `desc:"Address list ip1:port1,ip2:port2... to join other agents and form a gossip network"`

	// EnableCompression specifies whether message compression is enabled
	// by `github.com/hashicorp/memberlist` when broadcasting events.
	EnableCompression bool `desc:"Specifies whether message compression is enabled when broadcasting events"`

	// BroadcastTimeout is the amount of time to wait for a broadcast
	// message to be sent to the cluster. Broadcast messages are used for
	// things like leave messages and force remove messages. If this is not
	// set, a timeout of 5 seconds will be set.
	BroadcastTimeout time.Duration `desc:"The amount of time to wait for a broadcast message to be sent to the cluster"`

	// LeavePropagateDelay is for our leave (node dead) message to propagate
	// through the cluster. In particular, we want to stay up long enough to
	// service any probes from other nodes before they learn about us
	// leaving and stop probing. Otherwise, we risk getting node failures as
	// we leave.
	LeavePropagateDelay time.Duration `desc:"Time for our leave (node dead) message to propagate through the cluster"`

	// MemberlistConfig is the memberlist configuration that Agent will
	// use to do the underlying membership management and gossip. Some
	// fields in the MemberlistConfig will be overwritten by Auditor no
	// matter what:
	//
	//   * Name - This will always be set to the same as the NodeName
	//     in this configuration.
	//
	//   * Events - Auditor uses a custom event delegate.
	//
	//   * Delegate - Auditor uses a custom delegate.
	//
	MemberlistConfig *memberlist.Config `flag:"-"`

	// Timeout enqueuing elements on a channel
	TimeoutQueues time.Duration `desc:"Timeout enqueuing elements on a channel"`

	// Interval to send out messages to other agents
	ProcessInterval time.Duration `desc:"Interval to send out messages to other agents"`

	// Maximum number of concurrent senders
	MaxSenders int `desc:"Maximum number of concurrent senders"`

	// Cache size in bytes to store agent temporal objects.
	// This cache will evict old objects by default
	CacheSize int `desc:"Cache size in bytes to store agent temporal objects"`
}

// AddrParts returns the parts of the BindAddr that should be
// used to configure the Node.
func (c *Config) AddrParts(address string) (string, int, error) {
	_, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, err
	}

	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return "", 0, err
	}

	return addr.IP.String(), addr.Port, nil
}
