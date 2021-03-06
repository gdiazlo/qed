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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUrlParse(t *testing.T) {

	testCases := []struct {
		endpoints     []string
		expectedError string
	}{
		{
			endpoints:     []string{"http://localhost", "http://localhost:8080", "https://127.0.0.1", "https://127.0.0.1:8080"},
			expectedError: "",
		},
		{
			endpoints:     []string{"localhost", "127.0.0.1", "http//localhost"},
			expectedError: errMissingURLScheme,
		},
		{
			endpoints:     []string{"http://", "https:/localhost", "http://:8080", "localhost:8080"},
			expectedError: errMissingURLHost,
		},
		{
			endpoints:     []string{"127.0.0.1:8080"},
			expectedError: errMalformedURL,
		},
	}

	for _, c := range testCases {
		for _, e := range c.endpoints {
			err := urlParse(e)
			if c.expectedError == "" {
				require.NoError(t, err, "Unexpected error")
				continue
			}
			require.Equal(t, err, fmt.Errorf("%s in %s", c.expectedError, e), "Errors do not match")
		}
	}
}

func TestUrlParseNoSchemaRequired(t *testing.T) {

	testCases := []struct {
		endpoints     []string
		expectedError string
	}{
		{
			endpoints:     []string{"localhost:8080", "127.0.0.1:8080"},
			expectedError: "",
		},
		{
			endpoints:     []string{"localhost", "127.0.0.1"},
			expectedError: errMissingURLPort,
		},
		{
			endpoints:     []string{""},
			expectedError: errMissingURLHost,
		},
		{
			endpoints:     []string{"http://localhost:8080", "http://localhost", "https://127.0.0.1:8080", "https://127.0.0.1"},
			expectedError: errUnexpectedScheme,
		},
	}

	for _, c := range testCases {
		for _, e := range c.endpoints {
			err := urlParseNoSchemaRequired(e)
			if c.expectedError == "" {
				require.NoError(t, err, "Unexpected error")
				continue
			}
			require.Equal(t, err, fmt.Errorf("%s in %s", c.expectedError, e), "Errors do not match")
		}
	}
}

func TestIsValidFQDN(t *testing.T) {

	testCases := []struct {
		fqdn          []string
		expectedError string
	}{
		{
			fqdn: []string{
				"a..bc",
				"xn--d1aacihrobi6i.xn--p1ai",
			},
			expectedError: errFQDNTrailing,
		},
		{
			fqdn: []string{
				"a.b",
			},
			expectedError: errTLDLen,
		},
		{
			fqdn: []string{
				"ec2-35-160-210-253.us-west-2-.compute.amazonaws.com",
				"-ec2-35-160-210-253.us-west-2-.compute.amazonaws.com",
			},
			expectedError: errFQDNHyp,
		},
		{
			fqdn: []string{
				"ec2_35$160%210-253.us-west-2.compute.amazonaws.com",
				"ec235160210-253.us-west-2.compute.@mazonaws.com",
			},
			expectedError: errFQDNInvalid,
		},
		{
			fqdn: []string{
				"ec2-35-160-210-253.us-west-2.compute.amazonaws.com.",
				".ec2-35-160-210-253.us-west-2.compute.amazonaws.com",
				".ec2-35-160-210-253.us-west-2.compute.amazonaws.com.",
				"acme.net.",
			},
			expectedError: errFQDNEmptyDot,
		},
		{
			fqdn: []string{
				"label.name.321",
				"so-me.na-me.567",
			},
			expectedError: errTLDLet,
		},
		{
			fqdn: []string{
				"a23456789-123456789-123456789-123456789-123456789-123456789-1234.b23.com",
				"b23.a23456789-123456789-123456789-123456789-123456789-123456789-1234.com",
			},
			expectedError: errFQDNLabelLen,
		},
		{
			fqdn: []string{
				"a23456789-a23456789-a234567890.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a23456789.a2345678.com.",
			},
			expectedError: errFQDNLen,
		},
	}

	for _, c := range testCases {
		for _, e := range c.fqdn {
			err := isValidFQDN(e)
			if c.expectedError == "" {
				require.NoError(t, err, "Unexpected error")
				continue
			}
			require.Equal(t, err, fmt.Errorf("%s in %s", c.expectedError, e), "Errors do not match")
		}
	}
}
