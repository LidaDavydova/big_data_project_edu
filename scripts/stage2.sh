#!/bin/bash
set -euo pipefail

export password=$(head -n 1 secrets/.hive.pass)

export HIVE_URL="jdbc:hive2://hadoop-03.uni.innopolis.ru:10001"
export USER="team12"

mkdir -p output

beeline \
    -u "$HIVE_URL" \
    -n "$USER" \
    -p "$password" \
    -f sql/db.hql \
    > output/hive_results.txt 2> /dev/null

for q in 1 2 3 4 5 6 7; do
    beeline \
        -u "$HIVE_URL" \
        -n "$USER" \
        -p "$password" \
        -f sql/q${q}.hql \
        --hiveconf hive.resultset.use.unique.column.names=false \
        > "output/q${q}.csv"
done
