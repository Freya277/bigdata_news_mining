#!/bin/bash
mkdir -p build/classes
mkdir -p build/lib

HBASE_HOME=/apps/hbase
SPARK_HOME=/apps/spark
HADOOP_HOME=/apps/hadoop

javac -cp $(echo $HBASE_HOME/lib/*.jar | tr ' ' ':'):$(echo $SPARK_HOME/jars/*.jar | tr ' ' ':'):$(echo $HADOOP_HOME/share/hadoop/common/*.jar | tr ' ' ':') -d build/classes src/com/news/*.java

jar -cvfm hbase-news.jar MANIFEST.MF -C build/classes .

cp hbase-news.jar build/lib/
echo "HBase code build success! Jar file: build/lib/hbase-news.jar"