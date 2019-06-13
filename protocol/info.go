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

package protocol

type Scheme string

const (
	Http  Scheme = "http"
	Https Scheme = "https"
)

type ShardDetail struct {
	NodeId   string `json:"nodeId"`
	HTTPAddr string `json:"httpAddr"`
}

type Shards struct {
	NodeId    uint64                 `json:"nodeId"`
	LeaderId  uint64                 `json:"leaderId"`
	URIScheme Scheme                 `json:"uriScheme"`
	Shards    map[string]ShardDetail `json:"shards"`
}
