#!/bin/bash

set -euo pipefail

export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export PYSPARK_PYTHON=python3.6
export PYSPARK_DRIVER_PYTHON=python3.6

source ./venv/bin/activate

spark-submit \
    --master yarn \
    --deploy-mode client \
    scripts/model.py

hdfs dfs -get -f project/models/model1 models/model1
hdfs dfs -get -f project/models/model2 models/model2

hdfs dfs -cat project/output/model1_predictions.csv/*.csv > output/model1_predictions.csv
hdfs dfs -cat project/output/model2_predictions.csv/*.csv > output/model2_predictions.csv
hdfs dfs -cat project/output/evaluation.csv/*.csv > output/evaluation.csv

hdfs dfs -cat project/data/train/*.json > data_train_test/train.json
hdfs dfs -cat project/data/test/*.json > data_train_test/test.json
