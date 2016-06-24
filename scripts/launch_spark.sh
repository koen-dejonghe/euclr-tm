#!/bin/bash

class=$1
shift

if [ -z "$class" ]
then
    echo "Usage: $0 spark-class-name"
    exit
fi

sbt assembly

if [ $? -ne 0 ]
then
    exit
fi

time spark-submit \
--driver-memory 12g \
--driver-class-path target/scala-2.10/*-assembly-1.0.jar \
--class $class \
--master "local[*]" \
target/scala-2.10/*-assembly-1.0.jar \
$*

