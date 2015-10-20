#!/bin/bash

SERVER=$1

./bin/ycsb run  mongodb -s -P workloads/workloadEvergreen -p mongodb.url=$SERVER:27017 -threads 64 
