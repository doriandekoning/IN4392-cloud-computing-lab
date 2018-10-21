#!/bin/bash

export MASTER_PORT=8000
export MASTER_ADDRESS=http://in4392.ddns.net
export OWN_ADDRESS=http://`wget http://ipecho.net/plain -O - -q ; echo`
export OWN_PORT=8888
export OWN_INSTANCEID=`ec2metadata --instance-id`

/home/ubuntu/go/src/github.com/doriandekoning/IN4392-cloud-computing-lab/worker/bin/worker &