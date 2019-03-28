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

package sender

import (
	"fmt"
	"time"

	"github.com/bbva/qed/gossip"
	"github.com/bbva/qed/log"
	"github.com/bbva/qed/metrics"
	"github.com/bbva/qed/protocol"
	"github.com/bbva/qed/sign"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// SENDER

	QedSenderInstancesCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "qed_sender_instances_count",
			Help: "Number of sender agents running",
		},
	)
	QedSenderBatchesSentTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "qed_sender_batches_sent_total",
			Help: "Number of batches sent by the sender.",
		},
	)
)

type Sender struct {
	agent  *gossip.Agent
	config *Config
	signer sign.Signer
	out    chan *protocol.BatchSnapshots
	quit   chan bool
}

type Config struct {
	BatchSize     int
	BatchInterval time.Duration
	NumSenders    int
	TTL           int
	EachN         int
	SendTimer     time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		BatchSize:     100,
		BatchInterval: 1 * time.Second,
		NumSenders:    3,
		TTL:           1,
		EachN:         1,
		SendTimer:     1000 * time.Millisecond,
	}
}

func NewSender(a *gossip.Agent, c *Config, s sign.Signer) *Sender {
	QedSenderInstancesCount.Inc()
	return &Sender{
		agent:  a,
		config: c,
		signer: s,
		out:    make(chan *protocol.BatchSnapshots, 1<<16),
		quit:   make(chan bool),
	}
}

// Start NumSenders concurrent senders and waits for them
// to finish
func (s Sender) Start(ch chan *protocol.Snapshot) {
	for i := 0; i < s.config.NumSenders; i++ {
		log.Debugf("starting sender %d", i)
		go s.batcherSender(i, ch, s.quit)
	}
	<-s.quit
}

func (s Sender) RegisterMetrics(srv *metrics.Server) {
	metrics := []prometheus.Collector{
		QedSenderInstancesCount,
		QedSenderBatchesSentTotal,
	}

	for _, m := range metrics {
		srv.Register(m)
	}
}

func (s Sender) newBatch() *protocol.BatchSnapshots {
	return &protocol.BatchSnapshots{
		TTL:       s.config.TTL,
		From:      s.agent.Self,
		Snapshots: make([]*protocol.SignedSnapshot, 0),
	}
}

// Sign snapshots, build batches of signed snapshots and send those batches
// to other members of the gossip network.
// If the out queue is full,  we drop the current batch and pray other sender will
// send the batches to the gossip network.
func (s Sender) batcherSender(id int, ch chan *protocol.Snapshot, quit chan bool) {
	batch := s.newBatch()

	for {
		select {
		case snap := <-ch:
			if len(batch.Snapshots) == s.config.BatchSize {
				s.agent.ChTimedSend(batch, s.out)
				batch = s.newBatch()
			}
			ss, err := s.doSign(snap)
			if err != nil {
				log.Errorf("Failed signing message: %v", err)
			}
			batch.Snapshots = append(batch.Snapshots, ss)
		case b := <-s.out:
			go s.sender(b)
		case <-time.After(s.config.SendTimer):
			// send whatever we have on each tick, do not wait
			// to have complete batches
			if len(batch.Snapshots) > 0 {
				s.agent.ChTimedSend(batch, s.out)
				batch = s.newBatch()
			}
		case <-quit:
			return
		}
	}
}

// Send a batch to the peers it selects based on the gossip
// network topology.
// Do not retry sending to faulty agents, and pray other
// sender will.
func (s Sender) sender(batch *protocol.BatchSnapshots) {
	msg, _ := batch.Encode()
	peers := s.agent.Topology.Each(s.config.EachN, nil)
	for _, peer := range peers.L {
		QedSenderBatchesSentTotal.Inc()
		dst := peer.Node()

		log.Debugf("Sending batch %+v to node %+v\n", batch, dst.Name)

		err := s.agent.Memberlist().SendReliable(dst, msg)
		if err != nil {
			s.agent.Alerts <- fmt.Sprintf("Sender failed send message to %+v because: %v", peer, err)
			log.Infof("Failed send message to %+v because: %v", peer, err)
		}
	}
	log.Debugf("Sent batch %+v to nodes %+v\n", batch, peers.L)
}

func (s Sender) Stop() {
	QedSenderInstancesCount.Dec()
	close(s.quit)
}

func (s *Sender) doSign(snapshot *protocol.Snapshot) (*protocol.SignedSnapshot, error) {
	signature, err := s.signer.Sign([]byte(fmt.Sprintf("%v", snapshot)))
	if err != nil {
		log.Info("Publisher: error signing snapshot")
		return nil, err
	}
	return &protocol.SignedSnapshot{Snapshot: snapshot, Signature: signature}, nil
}
