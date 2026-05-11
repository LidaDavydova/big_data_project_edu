#!/bin/bash

set -euo pipefail

tar -czf venv.tar.gz venv

unset PYSPARK_PYTHON
unset PYSPARK_DRIVER_PYTHON

export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

export PYSPARK_DRIVER_PYTHON=venv/bin/python
export PYSPARK_PYTHON=./venv/venv/bin/python

spark-submit \
    --master yarn \
    --deploy-mode client \
    --archives venv.tar.gz#venv \
    --conf spark.executorEnv.PYSPARK_PYTHON=./venv/venv/bin/python \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./venv/venv/bin/python \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.executor.instances=2 \
    scripts/model.py
    
hdfs dfs -get -f project/models/model1 models/model1
hdfs dfs -get -f project/models/model2 models/model2

hdfs dfs -cat project/output/model1_predictions.csv/*.csv > output/model1_predictions.csv
hdfs dfs -cat project/output/model2_predictions.csv/*.csv > output/model2_predictions.csv
hdfs dfs -cat project/output/evaluation.csv/*.csv > output/evaluation.csv

hdfs dfs -cat project/data/train/*.json > data_train_test/train.json
hdfs dfs -cat project/data/test/*.json > data_train_test/test.json
