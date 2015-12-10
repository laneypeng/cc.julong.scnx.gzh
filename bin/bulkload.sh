#!/bin/sh
inputPath=$1
tableName=$2
zkhosts=$3
echo "inputPath  == "$1
echo "tableName  == "$2
echo "zkhosts  == "$3

export HADOOP_CLASSPATH=/usr/hdp/2.2.4.2-2/hbase/lib/hbase-protocol.jar:/usr/hdp/2.2.4.2-2/hbase/conf:./config/config.properties
hadoop jar ../lib/phoenix-4.2.2-client.jar cc.julong.phoenix.bulkload.PhoenixBulkload -z $zkhosts -t $tableName -i $inputPath
