#!/bin/bash
set -euo pipefail

export password=$(head -n 1 secrets/.hive.pass)

export HIVE_URL="jdbc:hive2://hadoop-03.uni.innopolis.ru:10001"
export USER="team12"

beeline \
    -u "$HIVE_URL" \
    -n "$USER" \
    -p "$password" \
    -f sql/model_eval.hql \
    > output/hive_results_model.txt 2> /dev/null