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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/bbva/qed/gossip/member"
	"github.com/bbva/qed/hashing"
	"github.com/bbva/qed/log"
	"github.com/bbva/qed/metrics"
	"github.com/bbva/qed/protocol"
	"github.com/coocood/freecache"
	"github.com/hashicorp/memberlist"
)

type hashedBatch struct {
	batch  *protocol.BatchSnapshots
	digest hashing.Digest
}

type Agent struct {
	config *Config
	Self   *member.Peer

	metricsServer *metrics.Server

	memberlist *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue

	Topology *Topology

	stateLock sync.Mutex

	processed  *freecache.Cache
	processors []Processor
	lag        *Lag

	In     chan *hashedBatch
	Out    chan *protocol.BatchSnapshots
	Alerts chan string

	quit chan bool
}

func NewAgent(conf *Config, p []Processor, m *metrics.Server, alertsCh chan string) (agent *Agent, err error) {
	log.Infof("New agent %s\n", conf.NodeName)
	lag := NewLag()
	agent = &Agent{
		config:        conf,
		metricsServer: m,
		Topology:      NewTopology(),
		processors:    append(p, lag),
		processed:     freecache.NewCache(1 << 20),
		lag:           lag,
		In:            make(chan *hashedBatch, 1<<16),
		Out:           make(chan *protocol.BatchSnapshots, 1<<16),
		Alerts:        alertsCh,
		quit:          make(chan bool),
	}

	bindIP, bindPort, err := conf.AddrParts(conf.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("Invalid bind address: %s", err)
	}

	var advertiseIP string
	var advertisePort int
	if conf.AdvertiseAddr != "" {
		advertiseIP, advertisePort, err = conf.AddrParts(conf.AdvertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("Invalid advertise address: %s", err)
		}
	}

	conf.MemberlistConfig = memberlist.DefaultLocalConfig()
	conf.MemberlistConfig.BindAddr = bindIP
	conf.MemberlistConfig.BindPort = bindPort
	conf.MemberlistConfig.AdvertiseAddr = advertiseIP
	conf.MemberlistConfig.AdvertisePort = advertisePort
	conf.MemberlistConfig.Name = conf.NodeName
	conf.MemberlistConfig.Logger = log.GetLogger()
	// Configure delegates
	conf.MemberlistConfig.Delegate = newAgentDelegate(agent)
	conf.MemberlistConfig.Events = &eventDelegate{agent}
	agent.Self = member.NewPeer(conf.NodeName, advertiseIP, uint16(advertisePort), conf.Role)

	agent.memberlist, err = memberlist.Create(conf.MemberlistConfig)
	if err != nil {
		return nil, err
	}

	// Print local member info
	agent.Self = member.ParsePeer(agent.memberlist.LocalNode())
	log.Infof("Local member %+v", agent.Self)

	// Set broadcast queue
	agent.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return agent.memberlist.NumMembers()
		},
		RetransmitMult: 2,
	}

	if p != nil {
		go agent.start()
	}

	return agent, nil
}

// Send a batch into a queue channel with the agent TimeoutQueues timeout.
func (a *Agent) ChTimedSend(batch *protocol.BatchSnapshots, ch chan *protocol.BatchSnapshots) {
	for {
		select {
		case <-time.After(a.config.TimeoutQueues):
			log.Infof("Agent timed out enqueueing batch in out channel")
			return
		case ch <- batch:
			return
		}
	}
}

func (a *Agent) start() {

	for _, p := range a.processors {
		p.RegisterMetrics(a.metricsServer)
	}

	a.metricsServer.Start()

	a.lag.Start(1 * time.Second)

	for {
		select {
		case hashedBatch := <-a.In:
			_, err := a.processed.Get(hashedBatch.digest)
			if err == nil {
				continue
			}

			a.processed.Set(hashedBatch.digest, []byte{0x0}, 0)

			for _, p := range a.processors {
				go p.Process(hashedBatch.batch)
			}

			if a.lag.Get() > 2*a.lag.Snapshots.Rate() {
				log.Infof("We're lagging behind, do not retransmit the batch: %v", a.lag.Lag.Get())
			} else {
				a.ChTimedSend(hashedBatch.batch, a.Out)
			}
		case batch := <-a.Out:
			go a.send(batch)
		case alert := <-a.Alerts:
			a.sendAlert(alert)
		case <-a.quit:
			return
		}
	}
}

func (a *Agent) sendAlert(msg string) {
	alertsUrl := a.config.AlertsUrls[rand.Intn(len(a.config.AlertsUrls))]
	resp, err := http.Post(alertsUrl+"/alert", "application/json", bytes.NewBufferString(msg))
	if err != nil {
		log.Infof("Agent is unable to send an alert because %v. We were trying to alert about %s", err, msg)
		return
	}
	defer resp.Body.Close()
	_, err = io.Copy(ioutil.Discard, resp.Body)
	if err != nil {
		log.Infof("Monitor had an error from alertStore saving a batch: %v", err)
	}
}

func (a *Agent) send(batch *protocol.BatchSnapshots) {

	if batch.TTL <= 0 {
		return
	}

	batch.TTL -= 1
	from := batch.From
	batch.From = a.Self
	msg, _ := batch.Encode()
	for _, dst := range a.route(from) {
		log.Debugf("Sending batch to %+v\n", dst.Name)
		a.memberlist.SendReliable(dst, msg)
	}
}

func (a *Agent) route(src *member.Peer) []*memberlist.Node {
	var excluded PeerList

	dst := make([]*memberlist.Node, 0)

	excluded.L = append(excluded.L, src)
	excluded.L = append(excluded.L, a.Self)

	peers := a.Topology.Each(1, &excluded)
	for _, p := range peers.L {
		dst = append(dst, p.Node())
	}
	return dst
}

// Join asks the Agent instance to join its peers
func (a *Agent) Join(addrs []string) (int, error) {
	if a.State() != member.Alive {
		return 0, fmt.Errorf("Agent can't join after Leave or Shutdown")
	}

	if len(addrs) > 0 {
		return a.memberlist.Join(addrs)
	}

	return 0, nil
}

func (a *Agent) Leave() error {

	// Check the current state
	a.stateLock.Lock()
	switch a.Self.Status {
	case member.Left:
		a.stateLock.Unlock()
		return nil
	case member.Leaving:
		a.stateLock.Unlock()
		return fmt.Errorf("Leave already in progress")
	case member.Shutdown:
		a.stateLock.Unlock()
		return fmt.Errorf("Leave called after Shutdown")
	default:
		a.Self.Status = member.Leaving
		a.stateLock.Unlock()
	}

	// Attempt the memberlist leave
	err := a.memberlist.Leave(a.config.BroadcastTimeout)
	if err != nil {
		return err
	}

	// Wait for the leave to propagate through the cluster. The broadcast
	// timeout is how long we wait for the message to go out from our own
	// queue, but this wait is for that message to propagate through the
	// cluster. In particular, we want to stay up long enough to service
	// any probes from other agents before they learn about us leaving.
	time.Sleep(a.config.LeavePropagateDelay)

	// Transition to Left only if we not already shutdown
	a.stateLock.Lock()
	if a.Self.Status != member.Shutdown {
		a.Self.Status = member.Left
	}
	a.stateLock.Unlock()
	return nil

}

// Shutdown forcefully shuts down the Agent instance, stopping all network
// activity and background maintenance associated with the instance.
//
// This is not a graceful shutdown, and should be preceded by a call
// to Leave. Otherwise, other agents in the cluster will detect this agent's
// exit as a agent failure.
//
// It is safe to call this method multiple times.
func (a *Agent) Shutdown() error {
	log.Infof("Shutting down agent %s", a.config.NodeName)
	a.stateLock.Lock()
	defer a.stateLock.Unlock()

	a.metricsServer.Shutdown()
	a.lag.Stop()

	if a.Self.Status == member.Shutdown {
		return nil
	}

	if a.Self.Status != member.Left {
		log.Info("agent: Shutdown without a Leave")
	}

	a.Self.Status = member.Shutdown
	err := a.memberlist.Shutdown()
	if err != nil {
		return err
	}

	return nil
}

func (a *Agent) Memberlist() *memberlist.Memberlist {
	return a.memberlist
}

func (a *Agent) Broadcasts() *memberlist.TransmitLimitedQueue {
	return a.broadcasts
}

func (a *Agent) GetAddrPort() (net.IP, uint16) {
	n := a.memberlist.LocalNode()
	return n.Addr, n.Port
}

func (a *Agent) State() member.Status {
	a.stateLock.Lock()
	defer a.stateLock.Unlock()
	return a.Self.Status
}
