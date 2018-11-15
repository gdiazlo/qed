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

package cmd

import (
	"os"

	"github.com/bbva/qed/gossip"
	"github.com/bbva/qed/log"
	"github.com/spf13/cobra"
)

func newGNodeStartCommand() *cobra.Command {
	var (
		nodeId, bindAddr, joinAddr string
		nodeType                   uint8
	)

	cmd := &cobra.Command{
		Use:   "gossip",
		Short: "Start a gossip node",
		Long:  `Start a gossip node that will process messages following the selected role of auditor, monitor or publisher`,

		Run: func(cmd *cobra.Command, args []string) {
			nodeAddr := gossip.NewNodeAddr(nodeId, bindAddr, joinAddr, gossip.NodeType(nodeType))
			config := gossip.DefaultConfig
			gnode, err := gossip.NewGNode(nodeAddr, &config)

			if err != nil {
				log.Fatalf("Can't start QED gnode: %v", err)
			}

			if err := gnode.Start(); err != nil {
				log.Fatalf("Can't start QED gnode: %v", err)
			}

		},
	}

	hostname, _ := os.Hostname()
	cmd.Flags().StringVarP(&nodeId, "node-id", "", hostname, "Unique name for node. If not set, fallback to hostname")
	cmd.Flags().StringVarP(&bindAddr, "bind-addr", "", "127.0.0.1:9000", "Bind address for TCP/UDP gossip on (host:port)")
	cmd.Flags().StringVarP(&joinAddr, "join-addr", "", "", "Comma-delimited list of nodes ([host]:port), through which a cluster can be joined")
	cmd.Flags().Uint8VarP(&nodeType, "node-kind", "", 0, "1 Auditor, 2 Monitor, 3 Server")
	return cmd

}