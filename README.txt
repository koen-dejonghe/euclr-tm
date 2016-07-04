Eurclear Text Mining Hackathon.

Get the Stanford Core NLP English language model
================================================
Create a folder to store the language model:
mkdir ./extLib

Download
http://nlp.stanford.edu/software/stanford-english-corenlp-2016-01-10-models.jar
and store it in
./extLib

Copy data
=========

Data provided by Euroclear consists of the following:

Training data (labeled)
-----------------------
- txt files: folder with OCR'ed text derived from pdf's
- pdf files: folder original pdf's which can be used for own OCR
- docid_train.csv: relation between labeled document id (file name) and ISIN
- docid_int_test.csv: relation between unlabeled document id (file name) and ISIN
- ISIN_train.csv: relation between ISIN and the following labels: ISSUER.NAME,ZCP.FL,MIN.TRAD.AMT,MLT.TRAD.AMT,OPS.CURR
- ROC_train.csv: relation between ISIN and the label for ROC's (required open cities). There can be 0 or more ROCs per ISIN.
- guarantor_train.csv: relation between ISIN and the label GA.NAME

Testing data (unlabeled)
------------------------
- txt files: folder with OCR'ed text derived from pdf's
- pdf files: folder original pdf's which can be used for own OCR

Submission data (unlabeled)
---------------------------
- txt files: folder with OCR'ed text derived from pdf's
- pdf files: folder original pdf's which can be used for own OCR

Preprocess input
================
OCR
---
For each of the txt files check if OCR went ok. If not then remove the alpha layer from the pdf, and OCR again with tesseract.
See the scripts ./scripts/is_bad_ocr.pl and ./scripts/ocr.sh.

CSV
---
Create labeled csv file with labels and text per ISIN.
Format is as follows:
ISIN,ISSUER.NAME,ZCP.FL,MIN.TRAD.AMT,MLT.TRAD.AMT,OPS.CURR,GA.NAME,ROC,TEXT
See the script ./scripts/flatten_input.pl
Store the result in ./data/derived/labels_text.csv

Create unlabeled csv file with labels and text per ISIN.
Format is as follows:
ISIN,TEXT
See the script ./scripts/flatten_input.pl (you may have to make some modifications to get it working)
Store the result in ./data/derived/unlabeled.csv

W2V
---
Create the unlabeled text file, containing all text of training, testing and submission data.
This will be used for Word2Vec processing.
See the script ./scripts/flatten_all_txt.pl
Store the result in ./data/derived/all_text.txt

Note this script does not filter out duplicate sentences.
In order to do so, execute:
sort -u data/derived/all_txt.txt > data/derived/all_text_unique.txt

Lemmatize
=========
Create the labeled lemmas from the texts by executing
./scripts/launch_spark.sh euclr.tm.spark.ml.Lemmatizer input='data/derived/labels_text.csv' saveAs='data/derived/labeled_lemmas'

Create the unlabeled lemmas:
./scripts/launch_spark.sh euclr.tm.spark.ml.Lemmatizer input='data/derived/unlabeled.csv' saveAs='data/derived/unlabeled_lemmas'

Word vectors
============
Create word2vec vectors by executing:
./scripts/launch_spark.sh euclr.tm.spark.ml.Word2Vectorizer input=data/derived/all_text_unique.txt saveAs=data/model/w2v_vector300_window10_mincount30

Train a classifier with Tf-Idf and Naive Bayes
==============================================
To evaluate this classifier, execute:

./scripts/launch_spark.sh euclr.tm.spark.ml.TfIdfNBClassifier \
labelToClassify=ISSUER_NAME \
labeledLemmas='data/derived/labeled_lemmas'

To persist this classifier, execute:

./scripts/launch_spark.sh euclr.tm.spark.ml.TfIdfNBClassifier \
labelToClassify=ISSUER_NAME \
labeledLemmas='data/derived/labeled_lemmas' \
saveAs='data/model/tfidfnb-ISSUER_NAME' \
evaluate=false

Train a classifier with Word2Vec and Random Forest
==================================================
To evaluate this classifier, execute:

./scripts/launch_spark.sh euclr.tm.spark.ml.W2vRfClassifier \
labelToClassify=ISSUER_NAME \
labeledCsv=../prep/input/labels_text.csv \
w2v=data/model/w2v_vector300_window10_mincount30

To persist this classifier, execute:

./scripts/launch_spark.sh euclr.tm.spark.ml.W2vRfClassifier \
labelToClassify=ISSUER_NAME \
labeledCsv=../prep/input/labels_text.csv \
w2v=data/model/w2v_vector300_window10_mincount30 \
saveAs='data/model/w2vrf-ISSUER_NAME' \
evaluate=false

Execute a classifier on an unlabeled set
========================================

./scripts/launch_spark.sh euclr.tm.spark.ml.PipelineClassifier \
pipeline='data/model/tfidfnb-ISSUER_NAME' \
unlabeled='data/derived/unlabeled_lemmas' \
saveAs=data/run/tfidfnb-ISSUER

./scripts/launch_spark.sh euclr.tm.spark.ml.PipelineClassifier \
pipeline='data/model/w2vrf-ISSUER_NAME' \
unlabeled='data/derived/unlabeled.csv' \
saveAs=data/run/w2vrf-ISSUER

Collect classification results
==============================
The results are stored in parquet format, and can easily be retrieved from the file system.
See the script ./scripts/combine_results.pl for a way of how to do this.

Running on EC2
==============
Instructions for setting up a cluster on Amazon can be found here:
http://spark.apache.org/docs/latest/ec2-scripts.html

Make the data available to all nodes:
-------------------------------------
Data must be put/retrieved in/from hdfs.
You can use hadoop commands in the ephemeral storage like so:

./ephemeral-hdfs/bin/hadoop fs -ls /data/
./ephemeral-hdfs/bin/hadoop fs -copyToLocal /data/run ./data

Make the jar available on all nodes:
------------------------------------
You should make the assembled jar file available on all nodes:
On your EC2 master node:
mkdir dist

On your workstation:
sbt assembly
scp euclr-tm-assembly-1.0.jar 52.99.99.99:dist/

On your EC2 master node:
spark-ec2/copy-dir.sh dist

Execute a Spark job on EC2:
---------------------------
For example:

export jar="/root/dist/euclr-tm-assembly-1.0.jar"

time spark/bin/spark-submit \
--conf spark.executor.extraClassPath="$jar" \
--driver-class-path "$jar" \
--master "spark://$(hostname):7077"
--class euclr.tm.spark.ml.PipelineClassifier $jar \
pipeline='data/model/tfidfnb-ISSUER_NAME' \
unlabeled='data/derived/unlabeled_lemmas' \
saveAs=data/run/tfidfnb-ISSUER

Note: it is important to include the fat jar in the class path of the executors, in order to make use of the BLAS libraries.
See http://deeplearning4j.org/spark#openblas-with-spark



