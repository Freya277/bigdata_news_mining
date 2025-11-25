#!/bin/bash
mkdir -p build

javac -cp $(hadoop classpath):./lib/* -d build src/*.java

jar -cvf categorycount.jar -C build .
