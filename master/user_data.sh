#!/bin/bash

export MASTER_PORT=8000
export MASTER_ADDRESS=
export OWN_ADDRESS=`wget http://ipecho.net/plain -O - -q ; echo`
export OWN_PORT=8888

/home/ubuntu/go/src/github.com/doriandekoning/IN4392-cloud-computing-lab/worker/bin/worker &