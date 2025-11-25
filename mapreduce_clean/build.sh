#!/bin/bash
mkdir -p build

javac -cp $(hadoop classpath):./lib/* -d build src/*.java

jar -cvf clean.jar -C build .
