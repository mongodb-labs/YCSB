#!/bin/bash

SERVER=$1

./bin/ycsb run  mongodb -s -P workloads/workloadEvergreen_95read5update -p mongodb.url=$SERVER:27017 -threads 64 
