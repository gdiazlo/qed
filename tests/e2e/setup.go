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

package e2e

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/lni/dragonboat/logger"

	"github.com/bbva/qed/client"
	"github.com/bbva/qed/crypto"
	"github.com/bbva/qed/hashing"
	"github.com/bbva/qed/server"
	"github.com/bbva/qed/testutils/keys"
	"github.com/bbva/qed/testutils/notifierstore"
	"github.com/bbva/qed/testutils/scope"
)

func init() {
	debug.SetGCPercent(10)
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.ERROR)
	logger.GetLogger("transport").SetLevel(logger.ERROR)
	logger.GetLogger("grpc").SetLevel(logger.ERROR)
	logger.GetLogger("dragonboat").SetLevel(logger.ERROR)
	logger.GetLogger("logdb").SetLevel(logger.ERROR)
}

// this function retries the execuntion of fn multiple times
func retry(tries int, delay time.Duration, fn func() error) int {
	var i int
	for i = 0; i < tries; i++ {
		err := fn()
		if err == nil {
			return i
		}
		time.Sleep(delay)
	}
	return i
}

// This function makes an http request
func doReq(method string, url, apiKey string, payload *strings.Reader) (*http.Response, error) {
	var err error
	if payload == nil {
		payload = strings.NewReader("")
	}
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Api-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, err
}

func setupStore(t *testing.T) (scope.TestF, scope.TestF) {
	var s *notifierstore.Service
	before := func(t *testing.T) {
		s = notifierstore.NewService()
		foreground := false
		s.Start(foreground)
	}

	after := func(t *testing.T) {
		s.Shutdown()
	}
	return before, after
}

// This function returns a server config object based on the function parameters:
// 	- nodeId: used to generate diffent listen addrs when starting multiple servers
//	      when id > 0, the node is set up to join the 1 node to its default port
//	- pathDB: path to where the database will store its files
//	- signPath: oath to where the signer key is stored
//	- tlsPath: path to where the tls cer and key are stored
//	- tls: if true, tls is activated
func configQedServer(nodeId, clusterId uint64, pathDB, signPath, tlsPath string, tls bool) *server.Config {
	conf := server.DefaultConfig()
	conf.APIKey = "APIKey"
	conf.NodeId = nodeId
	conf.ClusterId = clusterId
	conf.HTTPAddr = fmt.Sprintf("127.0.0.1:880%d", nodeId)
	conf.MgmtAddr = fmt.Sprintf("127.0.0.1:870%d", nodeId)
	conf.MetricsAddr = fmt.Sprintf("127.0.0.1:860%d", nodeId)
	conf.RaftAddr = fmt.Sprintf("127.0.0.1:850%d", nodeId)
	conf.GossipAddr = fmt.Sprintf("127.0.0.1:840%d", nodeId)
	if nodeId > 1 {
		conf.RaftJoinAddr = []string{"127.0.0.1:8700"}
		conf.GossipJoinAddr = []string{"127.0.0.1:8400"}
	}
	conf.DBPath = pathDB + "data"
	conf.RaftPath = pathDB + "raft"
	conf.PrivateKeyPath = signPath
	if tls {
		conf.SSLCertificate = tlsPath + "/cert.pem"
		conf.SSLCertificateKey = tlsPath + "/key.pem"
	}
	conf.EnableTLS = tls

	return conf
}

// This function returns two functions:
// 	- the first one creates a new server instance
// 	- the second one deletes the server the first one created
// Each server instance is completely new and blank.
// It will also generate all the needed keys for the instance.
func newServerSetup(nodeId uint64, tls bool) (func() error, func() error) {
	var srv *server.Server
	var path string
	var err error

	before := func() error {
		var tlsPath string

		path, err = ioutil.TempDir("", "e2e-qed-")
		if err != nil {
			return err
		}

		_, signKeyPath, err := crypto.NewEd25519SignerKeysFile(path)
		if err != nil {
			return err
		}
		if tls {
			tlsPath, err = keys.GenerateTlsCert(path)
			if err != nil {
				return err
			}
		}
		conf := configQedServer(nodeId, 100, path, signKeyPath, tlsPath, tls)
		srv, err = server.NewServer(conf)
		if err != nil {
			return err
		}
		return srv.Start()
	}

	after := func() error {
		if srv != nil {
			srv.Stop()
		}
		debug.FreeOSMemory()
		os.RemoveAll(path)
		return nil
	}
	return before, after
}

// This function will return a new qed http client.
// Always check for the error.
func newQedClient(nodeId uint64) (*client.HTTPClient, error) {
	// QED client
	transport := http.DefaultTransport.(*http.Transport)
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: false}
	httpClient := http.DefaultClient
	httpClient.Transport = transport
	client, err := client.NewHTTPClient(
		client.SetHttpClient(httpClient),
		client.SetURLs(fmt.Sprintf("http://127.0.0.1:880%d", nodeId)),
		client.SetAPIKey("APIKey"),
		client.SetTopologyDiscovery(true),
		client.SetHealthChecks(false),
		client.SetMaxRetries(5),
		client.SetAttemptToReviveEndpoints(true),
		client.SetHasherFunction(hashing.NewSha256Hasher),
	)
	if err != nil {
		return nil, err
	}
	return client, nil
}
