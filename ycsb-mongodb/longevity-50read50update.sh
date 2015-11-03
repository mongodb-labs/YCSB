#!/bin/bash

SERVER=$1
shift

# build YCSB command line with arguments passed in from command line
cmdline="./bin/ycsb run mongodb -s -P workloads/workloadLongevity -p mongodb.url=$SERVER:27017 -threads 64 "
while [[ $# > 0 ]]
do
    cmdline="$cmdline -p $1"
    shift
done

$cmdline


