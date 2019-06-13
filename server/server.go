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

// Package server implements the server initialization for the api.apihttp and
// balloon tree structure against a storage engine.
package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bbva/qed/api/apihttp"
	"github.com/bbva/qed/api/mgmthttp"
	"github.com/bbva/qed/consensus"
	"github.com/bbva/qed/gossip"
	"github.com/bbva/qed/log"
	"github.com/bbva/qed/metrics"
	"github.com/bbva/qed/protocol"
	"github.com/bbva/qed/sign"
	"github.com/bbva/qed/storage/rocks"
)

// Server encapsulates the data and login to start/stop a QED server
type Server struct {
	conf      *Config
	bootstrap bool // Set bootstrap to true when bringing up the first node as a master

	httpServer         *http.Server
	mgmtServer         *http.Server
	raftBalloon        *consensus.RaftBalloon
	metrics            *serverMetrics
	metricsServer      *metrics.Server
	prometheusRegistry *prometheus.Registry
	signer             sign.Signer
	sender             *Sender
	agent              *gossip.Agent
	snapshotsCh        chan *protocol.Snapshot
}

func serverInfo(conf *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Make sure we can only be called with an HTTP POST request.
		if r.Method != "GET" {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		out, err := json.Marshal(conf)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(out)
		return

	}
}

// NewServer creates a new Server based on the parameters it receives.
func NewServer(conf *Config) (*Server, error) {

	bootstrap := false
	if len(conf.RaftJoinAddr) <= 0 {
		bootstrap = true
	}

	server := &Server{
		conf:      conf,
		bootstrap: bootstrap,
	}

	log.Infof("ensuring directory at %s exists", conf.DBPath)
	if err := os.MkdirAll(conf.DBPath, 0755); err != nil {
		return nil, err
	}

	log.Infof("ensuring directory at %s exists", conf.RaftPath)
	if err := os.MkdirAll(conf.RaftPath, 0755); err != nil {
		return nil, err
	}

	// Open RocksDB store
	store, err := rocks.NewRocksDBStore(conf.DBPath)
	if err != nil {
		return nil, err
	}

	// Create signer
	server.signer, err = sign.NewEd25519SignerFromFile(conf.PrivateKeyPath)
	if err != nil {
		return nil, err
	}

	// Create metrics server
	server.metricsServer = metrics.NewServer(conf.MetricsAddr)

	// Create profiling server
	if server.conf.EnableProfiling {
		go func() {
			log.Debug("	* Starting QED Profiling server in addr: ", server.conf.ProfilingAddr)
			err := http.ListenAndServe(server.conf.ProfilingAddr, nil)
			if err != http.ErrServerClosed {
				log.Errorf("Can't start QED Profiling Server: %s", err)
			}
		}()
	}

	// Create gossip agent
	config := gossip.DefaultConfig()
	config.BindAddr = conf.GossipAddr
	config.Role = "server"
	config.NodeName = fmt.Sprintf("node-%d", conf.NodeId)

	server.agent, err = gossip.NewAgentFromConfig(config)
	if err != nil {
		return nil, err
	}

	// TODO: add queue size to config
	server.snapshotsCh = make(chan *protocol.Snapshot, 1<<16)

	// Create sender
	server.sender = NewSender(server.agent, server.signer, 500, 2, 3)

	// Create RaftBalloon
	server.raftBalloon, err = consensus.NewRaftBalloon(conf.RaftPath, conf.RaftAddr, conf.ClusterId, conf.NodeId, store, server.snapshotsCh)
	if err != nil {
		return nil, err
	}

	// Create http endpoints
	httpMux := apihttp.NewApiHttp(server.raftBalloon)
	httpMux.HandleFunc("/info", serverInfo(conf))

	if conf.EnableTLS {
		server.httpServer = newTLSServer(conf.HTTPAddr, httpMux)
	} else {
		server.httpServer = newHTTPServer(conf.HTTPAddr, httpMux)
	}

	// Create management endpoints
	mgmtMux := mgmthttp.NewMgmtHttp(server.raftBalloon)
	server.mgmtServer = newHTTPServer(conf.MgmtAddr, mgmtMux)

	// register qed metrics
	server.metrics = newServerMetrics()
	server.RegisterMetrics(server.metricsServer)
	store.RegisterMetrics(server.metricsServer)
	server.raftBalloon.RegisterMetrics(server.metricsServer)
	server.sender.RegisterMetrics(server.metricsServer)

	return server, nil
}

func join(joinAddr, raftAddr string, nodeId, clusterId uint64, meta map[string]string) error {
	body := make(map[string]interface{})
	body["addr"] = raftAddr
	body["nodeId"] = nodeId
	body["clusterId"] = clusterId
	body["meta"] = meta

	b, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(ioutil.Discard, resp.Body)

	return nil
}

// Start will start the server in a non-blockable fashion.
func (s *Server) Start() error {
	s.metrics.Instances.Inc()
	log.Infof("Starting QED server %s\n", s.conf.NodeId)

	metadata := map[string]string{}
	metadata["HTTPAddr"] = s.conf.HTTPAddr

	log.Debugf("	* Starting metrics HTTP server in addr: %s", s.conf.MetricsAddr)
	s.metricsServer.Start()

	if s.conf.EnableTLS {
		go func() {
			log.Debug("	* Starting QED API HTTPS server in addr: ", s.conf.HTTPAddr)
			err := s.httpServer.ListenAndServeTLS(
				s.conf.SSLCertificate,
				s.conf.SSLCertificateKey,
			)
			if err != http.ErrServerClosed {
				log.Errorf("Can't start QED API HTTP Server: %s", err)
			}
		}()
	} else {
		go func() {
			log.Debug("	* Starting QED API HTTP server in addr: ", s.conf.HTTPAddr)
			if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
				log.Errorf("Can't start QED API HTTP Server: %s", err)
			}
		}()
	}

	go func() {
		log.Debug("	* Starting QED MGMT HTTP server in addr: ", s.conf.MgmtAddr)
		if err := s.mgmtServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Errorf("Can't start QED MGMT HTTP Server: %s", err)
		}
	}()

	log.Debugf(" ready on %s and %s\n", s.conf.HTTPAddr, s.conf.MgmtAddr)

	s.sender.Start(s.snapshotsCh)

	s.agent.Start()

	if !s.bootstrap {
		for _, addr := range s.conf.RaftJoinAddr {
			log.Debug("	* Joining existent cluster QED MGMT HTTP server in addr: ", s.conf.MgmtAddr)
			if err := join(addr, s.conf.RaftAddr, s.conf.NodeId, s.conf.ClusterId, metadata); err != nil {
				log.Fatalf("failed to join node at %s: %s", addr, err.Error())
			}
		}
	}

	err := s.raftBalloon.Open(s.bootstrap, metadata)
	if err != nil {
		return err
	}

	return nil
}

// Stop will close all the channels from the mux servers.
func (s *Server) Stop() error {
	s.metrics.Instances.Dec()
	log.Infof("\nShutting down QED server %s", s.conf.NodeId)

	log.Debugf("Metrics enabled: stopping server...")
	s.metricsServer.Shutdown()

	log.Debugf("Stopping MGMT server...")
	if err := s.mgmtServer.Shutdown(context.Background()); err != nil { // TODO include timeout instead nil
		log.Error(err)
		return err
	}

	log.Debugf("Stopping API HTTP server...")
	if err := s.httpServer.Shutdown(context.Background()); err != nil { // TODO include timeout instead nil
		log.Error(err)
		return err
	}

	log.Debugf("Closing QED sender...")
	s.sender.Stop()

	log.Debugf("Stopping QED agent...")
	if err := s.agent.Shutdown(); err != nil {
		log.Error(err)
		return err
	}

	log.Debugf("Stopping RAFT server...")
	err := s.raftBalloon.Close(true)
	if err != nil {
		log.Error(err)
		return err
	}

	close(s.snapshotsCh)

	log.Debugf("Done. Exiting...\n")
	return nil
}

func (s *Server) RegisterMetrics(registry metrics.Registry) {
	if registry != nil {
		registry.MustRegister(s.metrics.collectors()...)
	}
}

func newTLSServer(addr string, mux *http.ServeMux) *http.Server {

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		CurvePreferences: []tls.CurveID{
			tls.CurveP521,
			tls.CurveP384,
			tls.CurveP256,
		},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
	}

	return &http.Server{
		Addr:         addr,
		Handler:      apihttp.LogHandler(mux),
		TLSConfig:    cfg,
		TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler), 0),
	}

}

func newHTTPServer(addr string, mux *http.ServeMux) *http.Server {
	var handler http.Handler
	if mux != nil {
		handler = apihttp.LogHandler(mux)
	} else {
		handler = nil
	}

	return &http.Server{
		Addr:    addr,
		Handler: handler,
	}
}
