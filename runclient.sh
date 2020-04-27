#!/bin/bash

if [ "$1" == "" ]; then
    echo "how many clients?"
    exit 1
fi

for i in `seq 1 $1`; do
    sleep 1
    go run gennums.go | nc localhost 8888 &
done
