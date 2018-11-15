#!/bin/bash

master="127.0.0.1:9000"
go run $GOPATH/src/github.com/bbva/qed/main.go gossip -k key -l silent --bind-addr $master  --node-id 0 --node-kind 3 &
pids[0]=$!
sleep 1s

for i in `seq 1 $1`;
do
	xterm -hold -e "go run $GOPATH/src/github.com/bbva/qed/main.go gossip -k key -l silent --bind-addr 127.0.0.1:900$i --join-addr $master --node-id $i --node-kind 1" &
	pids[${i}]=$!
done 

for pid in ${pids[*]}; do
	echo waiting for pid $pid
	wait $pid
done
