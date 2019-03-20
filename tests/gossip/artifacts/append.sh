#!/bin/bash
# Copyright 2018-2019 Banco Bilbao Vizcaya Argentaria, S.A.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ -z "$1" ]; then
	echo usage:
	echo $0 entry_file
	exit -1
fi
entry=$(cat $1)

go run $GOPATH/src/github.com/bbva/qed/main.go client add --apikey foo -e http://127.0.0.1:8800 --key "$entry"
