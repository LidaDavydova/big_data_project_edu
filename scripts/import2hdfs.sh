#!/bin/bash
set -euo pipefail

password=$(head -n 1 secrets/.psql.pass)
WAREHOUSE="/user/team12/project/warehouse"

hdfs dfs -rm -r -f "$WAREHOUSE" >/dev/null 2>&1 || true

rm -f ./output/*.java

sqoop import-all-tables \
    --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team12_projectdb \
    --username team12 \
    --password "$password" \
    --warehouse-dir=project/warehouse \
    --m 1 \
    --compress \
    --compression-codec="gzip" \
    --as-parquetfile \
    --outdir "./output"
    
echo "Data in hdfs:"
hdfs dfs -du -h /user/team12/project/warehouse/*