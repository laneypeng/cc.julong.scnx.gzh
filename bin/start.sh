#!/bin/sh


for i in `ls ../lib/*.jar`
do
       LIBJARS=$i:$LIBJARS
	echo $i     
done

export CLASSPATH=$CLASSPATH:$LIBJARS
echo $CLASSPATH
nohup java cc.julong.scnx.gzh.zookeeper.MonitorServer >> ../logs/scnx.gzh.log 2>&1 &


