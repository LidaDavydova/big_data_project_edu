#!/bin/bash

set -euo pipefail

source ./venv/bin/activate

spark-submit \
    --master yarn \
    --deploy-mode client \
    scripts/model.py

hdfs dfs -get -f project/models/model1 models/model1
hdfs dfs -get -f project/models/model2 models/model2

hdfs dfs -cat project/output/model1_predictions.csv/*.csv > output/model1_predictions.csv 2>/dev/null
hdfs dfs -cat project/output/model2_predictions.csv/*.csv > output/model2_predictions.csv 2>/dev/null
hdfs dfs -cat project/output/evaluation.csv/*.csv > output/evaluation.csv 2>/dev/null

hdfs dfs -cat project/data/train/*.json > data/train.json 2>/dev/null
hdfs dfs -cat project/data/test/*.json > data/test.json 2>/dev/null
