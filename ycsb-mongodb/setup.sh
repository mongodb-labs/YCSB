#!/bin/bash

export JAVA_HOME="/usr/java/jdk1.7.0_71/jre"

source /etc/profile.d/maven.sh

mvn clean package
