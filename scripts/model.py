import math
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Transformer
from pyspark import keyword_only
from pyspark.ml.param.shared import HasInputCol, HasOutputCol

class CyclicalMonthEncoder(Transformer, HasInputCol, HasOutputCol):
    @keyword_only
    def __init__(self, inputCol="month", outputCol="month_enc"):
        super().__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol="month", outputCol="month_enc"):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        input_col = self.getInputCol()
        base_col = self.getOutputCol()
        sin_col = base_col + "_sin"
        cos_col = base_col + "_cos"

        dataset = dataset.withColumn(
            sin_col, F.sin(2.0 * math.pi * F.col(input_col) / 12.0)
        ).withColumn(
            cos_col, F.cos(2.0 * math.pi * F.col(input_col) / 12.0)
        )
        return dataset

# start spark session
team = 'team12'
warehouse = "project/hive/warehouse"
spark = SparkSession.builder\
        .appName("{} - spark ML".format(team))\
        .master("yarn")\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", warehouse)\
        .config("spark.sql.parquet.compression.codec", "gzip")\
        .enableHiveSupport()\
        .getOrCreate()


# read dataset
flights = spark.read.table('team12_projectdb.fact_flights_optimized')

# ------------------
# DATA PREPROCESSING
# ------------------

# filter regular and passenger flights (not identified category is also passenger flights)
flights = flights.filter(F.col("ds_grupo_di") == "REGULAR")
flights = flights.filter(F.col("ds_servico_tipo_linha").isin("PASSAGEIRO", "NÃO IDENTIFICADO"))

# drop nan values from year, month and passangers
flights = flights.na.drop(subset=["nr_ano_partida_real", "nr_mes_partida_real"])
flights = flights.na.drop(subset=["nr_passag_pagos", "nr_passag_gratis"])

# filter year to be <= 2025
flights = flights.filter(F.col("nr_ano_partida_real") <= 2025)

# filter number of passengers to be adequate
flights = flights.filter((F.col("nr_passag_pagos") > 0) & (F.col("nr_passag_pagos") <= 853))

# create target column - total passengers
flights = flights.withColumn("total_passengers", F.col("nr_passag_pagos") + F.col("nr_passag_gratis"))

flights = flights.filter(F.col("total_passengers") <= 900)

# aggregate passangers by months
monthly = (
        flights
        .groupBy("nr_ano_partida_real", "nr_mes_partida_real")
        .agg(F.sum("total_passengers").alias("total_passengers"))
        .orderBy("nr_ano_partida_real", "nr_mes_partida_real")
    )

# rename columns (translate from portuguese to english)
monthly = monthly.withColumnRenamed("nr_ano_partida_real", "year") \
                 .withColumnRenamed("nr_mes_partida_real", "month")

# create time index
min_year = monthly.select(F.min("year")).first()[0]
monthly = monthly.withColumn(
    "time_index",
    (F.col("year") - F.lit(min_year)) * 12 + F.col("month")
)

monthly = monthly.withColumn("label", F.col("total_passengers"))

# ------------------
# FEATURE ENGINEERING
# ------------------

month_encoder = CyclicalMonthEncoder(inputCol="month", outputCol="month_enc")

time_features = ["year", "time_index"]
assembler = VectorAssembler(
    inputCols=["month_enc_sin", "month_enc_cos"] + time_features,
    outputCol="features"
)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)

# time-based train/test split
train_monthly = monthly.filter(F.col("year") <= 2022)
test_monthly = monthly.filter(F.col("year") >= 2023)

# pre-processing pipeline (fit only on training data)
pipeline = Pipeline(stages=[month_encoder, assembler, scaler])
preproc_model = pipeline.fit(train_monthly)

# transform train and test separately
train_data = preproc_model.transform(train_monthly) \
    .select("scaled_features", "label") \
    .cache()

test_data = preproc_model.transform(test_monthly) \
    .select("scaled_features", "label") \
    .cache()

train_data.count()
test_data.count()

# save splits as json on hdfs
train_data.coalesce(1).write.mode("overwrite").format("json").save("project/data/train")
test_data.coalesce(1).write.mode("overwrite").format("json").save("project/data/test")

# --------
# MODELING
# --------

# model 1
rf = RandomForestRegressor(
    featuresCol="scaled_features",
    labelCol="label",
    seed=42
)

param_grid_rf = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [10, 20, 30])
    .addGrid(rf.maxDepth, [5, 10, 15])
    .addGrid(rf.minInstancesPerNode, [1, 2, 5])
    .build()
)

evaluator_rf = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")

cv_rf = CrossValidator(
    estimator=rf,
    estimatorParamMaps=param_grid_rf,
    evaluator=evaluator_rf,
    numFolds=3,
    parallelism=4,
    seed=42
)

# train
cv_model_rf = cv_rf.fit(train_data)

# select best model and save it
best_rf = cv_model_rf.bestModel
best_rf.write().overwrite().save("project/models/model1")

# predict
pred_rf = best_rf.transform(test_data)

# evaluate
rmse_rf = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse").evaluate(pred_rf)
r2_rf = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2").evaluate(pred_rf)

print(f"RandomForestRegressor - RMSE: {rmse_rf:.4f}, R²: {r2_rf:.4f}")

# save predictions
pred_rf.select("label", "prediction") \
       .coalesce(1) \
       .write.mode("overwrite").format("csv") \
       .option("sep", ",").option("header", "true") \
       .save("project/output/model1_predictions.csv")

# model 2
gbt = GBTRegressor(
    featuresCol="scaled_features",
    labelCol="label",
    seed=42
)

param_grid_gbt = (
    ParamGridBuilder()
    .addGrid(gbt.maxDepth, [3, 5, 7])
    .addGrid(gbt.stepSize, [0.05, 0.1, 0.2])
    .addGrid(gbt.subsamplingRate, [0.6, 0.8, 1.0])
    .build()
)

evaluator_gbt = RegressionEvaluator(labelCol="label", predictionCol="prediction",
                                    metricName="rmse")

cv_gbt = CrossValidator(
    estimator=gbt,
    estimatorParamMaps=param_grid_gbt,
    evaluator=evaluator_gbt,
    numFolds=3,
    parallelism=4,
    seed=42
)

# train
cv_model_gbt = cv_gbt.fit(train_data)

# select best model and save it
best_gbt = cv_model_gbt.bestModel
best_gbt.write().overwrite().save("project/models/model2")

# predict
pred_gbt = best_gbt.transform(test_data)

# evaluate
rmse_gbt = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse").evaluate(pred_gbt)
r2_gbt = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2").evaluate(pred_gbt)

print(f"GBTRegressor - RMSE: {rmse_gbt:.4f}, R²: {r2_gbt:.4f}")

# save predictions
pred_gbt.select("label", "prediction") \
        .coalesce(1) \
        .write.mode("overwrite").format("csv") \
        .option("sep", ",").option("header", "true") \
        .save("project/output/model2_predictions.csv")

# -----------------
# MODELS COMPARISON
# -----------------
comparison = spark.createDataFrame(
    [
        ("RandomForestRegressor", float(rmse_rf), float(r2_rf)),
        ("GBTRegressor", float(rmse_gbt), float(r2_gbt)),
    ],
    ["model", "RMSE", "R2"]
)

# save comparison
comparison.coalesce(1) \
          .write.mode("overwrite").format("csv") \
          .option("sep", ",").option("header", "true") \
          .save("project/output/evaluation.csv")

spark.stop()