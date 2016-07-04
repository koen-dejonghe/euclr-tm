#!/bin/bash

class=$1
shift

if [ -z "$class" ]
then
    echo "Usage: $0 spark-class-name ..."
    echo "For example: "
    echo "$0 euclr.tm.spark.ml.PipelineClassifier labelToClassify=OPS_CURR labeledCsv='../prep/input/100.csv' unlabeledCsv='../prep/input/unlabeled_100.csv' outputPrefix='data/run' w2vLocation=data/meta/w2v/"
    exit
fi

sbt assembly

if [ $? -ne 0 ]
then
    exit
fi

time spark-submit \
--driver-memory 12g \
--driver-class-path target/scala-2.10/euclr-tm-rabbit-assembly-1.0.jar:extLib/stanford-english-corenlp-2016-01-10-models.jar \
--class $class \
--master "local[*]" \
target/scala-2.10/*-assembly-1.0.jar \
$*

# --driver-library-path /Users/koen/projects/euroclear/euclr-tm-rabbit/extLib \
