import sys
import time
from pyspark.sql import SparkSession

if len(sys.argv) < 3:
    sys.exit(1)

path = sys.argv[1]
fmt = sys.argv[2]

spark = SparkSession.builder.appName("BenchmarkRead").getOrCreate()
spark.catalog.clearCache() # Ensure we aren't reading from memory

start = time.time()
if fmt == "text":
    df = spark.read.csv(path)
elif fmt == "avro":
    df = spark.read.format("com.databricks.spark.avro").load(path)
elif fmt == "parquet":
    df = spark.read.parquet(path)

# Perform a count to force a full data scan
df.count()
end = time.time()

# Print only the duration so the bash script can capture it
execution_time = round(end - start, 2)
print(f"BENCHMARK_RESULT:{execution_time}")