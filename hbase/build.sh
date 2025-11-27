#!/bin/bash
mkdir -p build/classes
mkdir -p build/lib

HBASE_HOME=/apps/hbase
HADOOP_HOME=/apps/hadoop
JAVA_HOME=/apps/java

$JAVA_HOME/bin/javac -cp $(echo $HBASE_HOME/lib/*.jar | tr ' ' ':'):$(echo $HADOOP_HOME/share/hadoop/common/*.jar | tr ' ' ':'):$(echo $HADOOP_HOME/share/hadoop/hdfs/*.jar | tr ' ' ':') -d build/classes src/com/news/*.java

$JAVA_HOME/bin/jar -cvfm hbase-news.jar MANIFEST.MF -C build/classes .

cp hbase-news.jar build/lib/
echo "Success! Jar file: build/lib/hbase-news.jar"