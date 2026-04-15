#!/bin/bash
set -euo pipefail


# Config
password=$(head -n 1 secrets/.psql.pass)
WAREHOUSE="/user/team12/project/warehouse"
RESULT_FILE="output/comparison_results.csv"
export HADOOP_CONF_DIR=/etc/hadoop/conf

echo "format,codec,write_time_sec,read_time_sec,size_bytes,size_human" > "$RESULT_FILE"

# Define Test Matrix: format:codec
# We test CSV, AVRO, and Parquet
tests=(
    "text:gzip"
    "text:snappy"
    "avro:snappy"
    "avro:deflate"
    "parquet:snappy"
    "parquet:gzip"
)

for test in "${tests[@]}"; do
    fmt="${test%%:*}"
    codec="${test#*:}"
    
    echo "--- Benchmarking $fmt with $codec ---"

    # 1. Clear HDFS
    hdfs dfs -rm -r -f "$WAREHOUSE" >/dev/null 2>&1 || true

    # 2. Set Sqoop Format Flag
    case $fmt in
        "text")    fmt_flag="--as-textfile" ;;
        "avro")    fmt_flag="--as-avrodatafile" ;;
        "parquet") fmt_flag="--as-parquetfile" ;;
    esac

    # 3. Measure Write Time (Ingestion)
    start_write=$(date +%s)
    sqoop import-all-tables \
        --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team12_projectdb \
        --username team12 \
        --password "$password" \
        --warehouse-dir=project/warehouse \
        --m 1 \
        --compress \
        --compression-codec="$codec" \
        $fmt_flag
    end_write=$(date +%s)
    write_time=$((end_write - start_write))
    
    # 4. Measure Storage Size
    size_bytes=$(hdfs dfs -du -s "$WAREHOUSE" | awk '{print $1}')
    size_human=$(hdfs dfs -du -s -h "$WAREHOUSE" | awk '{print $1}')

    # 5. Measure Read Time using Spark (Full Scan of Fact Table)
    output=$(spark-submit \
    --master yarn \
    --packages com.databricks:spark-avro_2.11:4.0.0 \
    scripts/read_benchmark.py "$WAREHOUSE/fact_flights" "$fmt" 2>&1 | tee /dev/tty)
    
    # Now extract the result from the captured output
    read_time=$(echo "$output" | grep "BENCHMARK_RESULT:" | cut -d':' -f2)

    if [ -z "$read_time" ]; then
        echo "Error: Read benchmark failed or result not found."
        read_time="0"
    else
        echo "Read time captured: $read_time"
    fi
    # 6. Log Results
    echo "$fmt,$codec,$write_time,$read_time,$size_bytes,$size_human" >> "$RESULT_FILE"
    echo "Results: Write ${write_time}s | Read ${read_time}s | Size ${size_human}"
done