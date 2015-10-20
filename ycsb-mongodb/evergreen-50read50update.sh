#!/bin/bash

SERVER=$1

./bin/ycsb run  mongodb -s -P workloads/workloadEvergreen_50read50update -p mongodb.url=$SERVER:27017 -threads 64 
