#!/bin/bash
mkdir -p build/classes
mkdir -p build/lib

SPARK_HOME=/apps/spark
HADOOP_HOME=/apps/hadoop

javac -cp $(echo $SPARK_HOME/jars/*.jar | tr ' ' ':'):$(echo $HADOOP_HOME/share/hadoop/common/*.jar | tr ' ' ':'):$(echo $HADOOP_HOME/share/hadoop/hdfs/*.jar | tr ' ' ':'):$(echo $HADOOP_HOME/share/hadoop/mapreduce/*.jar | tr ' ' ':'):$(echo $HADOOP_HOME/share/hadoop/yarn/*.jar | tr ' ' ':') -d build/classes src/com/news/*.java

jar -cvfm news-spark.jar MANIFEST.MF -C build/classes .

cp news-spark.jar build/lib/
echo "Build success! Jar file: build/lib/news-spark.jar"