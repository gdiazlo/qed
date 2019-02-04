/*
   Copyright 2018 Banco Bilbao Vizcaya Argentaria, S.A.

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

package client

type Config struct {
	// Server host:port to consult.
	Endpoint string

	// ApiKey to query the server endpoint.
	APIKey string

	// Enable self-signed certificates, allowing MiTM vector attacks.
	Insecure bool

	// Seconds to wait for an established connection.
	TimeoutSeconds int

	// Seconds to wait for the connection to be established.
	DialTimeoutSeconds int

	// Seconds to wait for a handshake negotiation.
	HandshakeTimeoutSeconds int
}

func DefaultConfig() *Config {
	return &Config{
		Endpoint:                "localhost:8800",
		APIKey:                  "my-key",
		Insecure:                true,
		TimeoutSeconds:          10,
		DialTimeoutSeconds:      5,
		HandshakeTimeoutSeconds: 5,
	}
}