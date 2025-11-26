#!/bin/bash
# Reference: MapReduce build.sh in example
mkdir -p build
# Compile Scala code (reference Spark classpath command in example)
scalac -cp $(hadoop classpath):$(spark-shell --classpath 2>/dev/null | grep -o '/.*jar' | tr '\n' ':') -d build src/*.scala
# Package into jar
jar -cvf news-spark.jar -C build .