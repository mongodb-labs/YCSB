#!/bin/bash
# Script to install and run YCSB locally on a Linux machine.

MAVEN_VERSION="3.9.4"
YCSB_VERSION="0.17.0"
LOAD_THREADS=16
RUN_THREADS=16

if command -v yum &> /dev/null; then
sudo yum install java-devel -y
elif command -v apt &> /dev/null; then
sudo apt install default-jdk -y
else
echo "Package manager not supported!"
exit 1
fi

if ! command -v maven &> /dev/null; then
echo "Installing maven ..."
pushd /tmp
wget "https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"
sudo tar -xzvf "apache-maven-${MAVEN_VERSION}-bin.tar.gz" -C /usr/local
popd
pushd /usr/local
sudo ln -s apache-maven-* maven
echo "export M2_HOME=/usr/local/maven" | sudo tee -a /etc/profile.d/maven.sh
echo "export PATH=${M2_HOME}/bin:${PATH}" | sudo tee -a /etc/profile.d/maven.sh
popd
fi

pushd ~
if [ -d "ycsb-${YCSB_VERSION}" ]; then
echo "Found YCSB in home directory!"
else
echo "Installing YCSB ..."
wget "https://github.com/brianfrankcooper/YCSB/releases/download/${YCSB_VERSION}/ycsb-${YCSB_VERSION}.tar.gz"
tar -xzvf "ycsb-${YCSB_VERSION}.tar.gz"

pushd "ycsb-${YCSB_VERSION}"
echo "recordcount=56320000" > workloads/workloadEvergreen_100read                                                                                                          
echo "operationcount=20000000" >> workloads/workloadEvergreen_100read                                                                                                      
echo "maxexecutiontime=360" >> workloads/workloadEvergreen_100read                                                                                                         
echo "workload=site.ycsb.workloads.CoreWorkload" >> workloads/workloadEvergreen_100read                                                                               
echo "readallfields=true" >> workloads/workloadEvergreen_100read                                                                                                           
echo "readproportion=1.0" >> workloads/workloadEvergreen_100read                                                                                                           
echo "updateproportion=0.0" >> workloads/workloadEvergreen_100read                                                                                                         
echo "scanproportion=0" >> workloads/workloadEvergreen_100read                                                                                                             
echo "insertproportion=0.0" >> workloads/workloadEvergreen_100read                                                                                                         
echo "requestdistribution=zipfian" >> workloads/workloadEvergreen_100read
popd
fi

pushd "ycsb-${YCSB_VERSION}"
echo "Running the load phase ..."
./bin/ycsb load mongodb -s -P workloads/workloadEvergreen_100read -threads $LOAD_THREADS

echo "Running the run phase ..."
./bin/ycsb run mongodb -s -P workloads/workloadEvergreen_100read -threads $RUN_THREADS

popd
popd

exit 0