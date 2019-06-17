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

package cmd

import (
	"fmt"

	"github.com/bbva/qed/crypto"
	"github.com/spf13/cobra"
)

var generateSelfSignedCert *cobra.Command = &cobra.Command{
	Use:   "self-signed-cert",
	Short: "Generate Self Signed Certificates",
	Run:   runGenerateTlsCerts,
}

func init() {
	generateCmd.AddCommand(generateSelfSignedCert)
}

func runGenerateTlsCerts(cmd *cobra.Command, args []string) {

	conf := generateCtx.Value(k("generate.config")).(*GenerateConfig)

	cert, key, err := crypto.NewSelfSignedCert(conf.Path, conf.Host)
	if err != nil {
		fmt.Errorf("Error: %v\n", err)
	}
	fmt.Printf("New Tls certificates generated at:\n%v\n%v\n", cert, key)

}
