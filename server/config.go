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

package server

import (
	"net"
	"os"
	"path/filepath"
)

type Config struct {
	//Log level
	Log string

	// Unique identifier to allow connections
	APIKey string

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

	// List of raft nodes, through which a cluster can be joined
	// (protocol://host:port).
	RaftJoinAddr []string

	// Path to storage directory.
	DBPath string

	// Path to Raft storage directory.
	RaftPath string

	// Gossip management server bind address/port.
	GossipAddr string

	// List of nodes, through which a gossip cluster can be joined (protocol://host:port).
	GossipJoinAddr []string

	// Path to the private key file used to sign snapshots.
	PrivateKeyPath string

	// Enable TLS service
	EnableTLS bool

	// Enable Pprof prifiling server
	EnableProfiling bool

	// Profiling server address/port
	ProfilingAddr string

	// TLS server cerificate
	SSLCertificate string

	// TLS server cerificate key
	SSLCertificateKey string
}

func DefaultConfig() *Config {
	currentDir := getCurrentDir()

	return &Config{
		Log:               "info",
		APIKey:            "",
		NodeId:            100, // dragonboat does not support 0 as node id
		ClusterId:         100,
		HTTPAddr:          "127.0.0.1:8800",
		RaftAddr:          "127.0.0.1:8500",
		MgmtAddr:          "127.0.0.1:8700",
		MetricsAddr:       "127.0.0.1:8600",
		RaftJoinAddr:      []string{},
		GossipAddr:        "127.0.0.1:8400",
		GossipJoinAddr:    []string{},
		DBPath:            currentDir + "/db",
		RaftPath:          currentDir + "/wal",
		EnableTLS:         false,
		EnableProfiling:   false,
		ProfilingAddr:     "127.0.0.1:6060",
		SSLCertificate:    "",
		SSLCertificateKey: "",
		PrivateKeyPath:    "",
	}
}

func getCurrentDir() string {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)
	return exPath
}

// AddrParts returns the parts of and address/port.
func addrParts(address string) (string, int, error) {
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
