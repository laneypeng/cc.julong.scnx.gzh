#!/bin/sh
inputPath=$1
outputPath=$2
tableName=$3
echo "inputPath  == "$1
echo "outputPath == "$2
echo "tableName  == "$3

for i in `ls /weblogic/domains/1213/msimport/war/msimport/WEB-INF/lib/*.jar`
do
        LIBJARS=$i,$LIBJARS
        HADOOPCLASSPATH=$i:$HADOOPCLASSPATH
done
export CLASSPATH=$CLASSPATH:$HADOOPCLASSPATH
export JAVA_LIBRARY_PATH=$JAVA_LIBRARY_PATH:/weblogic/tdhcli/base/hadoop/lib/native:$LIBJARS
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/weblogic/tdhcli/base/hadoop/lib/native:$LIBJARS


echo "===========start Driver==========="
java -Djava.security.krb5.conf="/etc/krb5.conf"  com.apache.hbase.bulkimport.Driver  $inputPath $outputPath $tableName $day