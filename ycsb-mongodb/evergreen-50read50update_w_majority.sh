#!/bin/bash

SERVER=$1

./bin/ycsb run  mongodb -s -P workloads/workloadEvergreen_50read50update -p mongodb.url="mongodb://$SERVER:27017/ycsb?w=majority" -threads 64 -p maxexecutiontime=600
