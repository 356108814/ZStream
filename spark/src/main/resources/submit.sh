#!/bin/bash

confPath=$1
logPath=$2
jars=$3

export SPARK_HOME=/opt/spark/spark-2.1.1-bin-hadoop2.6
cd /home/bdp/workspace/spark
${SPARK_HOME}/bin/spark-submit \
--class com.ztesoft.zstream.Application \
${jars} \
zstream-spark-1.0.jar \
confPath=${confPath} \
> ${logPath} 2>&1 &
