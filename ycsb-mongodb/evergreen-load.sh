#!/bin/bash

SERVER=$1

./bin/ycsb load mongodb -s -P workloads/workloadEvergreen -p mongodb.url=$SERVER:27017 -threads 32 
