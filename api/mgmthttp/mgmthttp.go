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

package mgmthttp

import (
	"encoding/json"
	"net/http"

	"github.com/bbva/qed/api/apihttp"
	"github.com/bbva/qed/consensus"
)

// NewMgmtHttp will return a mux server with the endpoint required to
// tamper the server. it's a internal debug implementation. Running a server
// with this enabled will run useless the qed server.
func NewMgmtHttp(raftBalloon consensus.RaftBalloonApi) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/join", joinHandle(raftBalloon))
	return mux
}

func joinHandle(raftBalloon consensus.RaftBalloonApi) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		// Make sure we can only be called with an HTTP POST request.
		w, r, err = apihttp.PostReqSanitizer(w, r)
		if err != nil {
			return
		}

		body := make(map[string]interface{})

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if len(body) != 3 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		remoteAddr, ok := body["addr"].(string)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		nodeId, ok := body["nodeId"].(uint64)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		clusterId, ok := body["clusterId"].(uint64)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		metadata, ok := body["meta"].(map[string]string)
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if err := raftBalloon.Join(nodeId, clusterId, remoteAddr, metadata); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}
