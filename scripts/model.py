import math
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml import Transformer, Pipeline
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark import keyword_only

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

# -------------
# DATA CLEANING
# -------------

# filter only regular and passenger flights (not identified category is also passenger flights)
flights = flights.filter(F.col("ds_grupo_di") == "REGULAR")
flights = flights.filter(F.col("ds_servico_tipo_linha").isin("PASSAGEIRO", "NÃO IDENTIFICADO"))

# drop nan values from year, month and passangers
flights = flights.na.drop(subset=["nr_ano_partida_real", "nr_mes_partida_real"])
flights = flights.na.drop(subset=["nr_passag_pagos", "nr_passag_gratis"])

# filter year to be <= 2025
flights = flights.filter(F.col("nr_ano_partida_real") <= 2025)

# filter number of revenue passengers to be adequate
flights = flights.filter((F.col("nr_passag_pagos") > 0) & (F.col("nr_passag_pagos") <= 853))

# create target column - total passengers, filter it to be adequate
flights = flights.withColumn("total_passengers", F.col("nr_passag_pagos") + F.col("nr_passag_gratis"))
flights = flights.filter(F.col("total_passengers") <= 900)

# ----------------
# DATA AGGREGATING
# ----------------

# aggregate passangers by months, make column - date
monthly = (
        flights
        .groupBy("nr_ano_partida_real", "nr_mes_partida_real")
        .agg(F.sum("total_passengers").alias("total_passengers"))
        .withColumnRenamed("nr_ano_partida_real", "year")
        .withColumnRenamed("nr_mes_partida_real", "month")
        .withColumn("date",F.to_date(F.format_string("%04d-%02d-01", F.col("year"), F.col("month"))))
        .orderBy("date")
    )

# fill missing months with zeros to make lag features consistent
bounds = monthly.agg(F.min("date").alias("start_date"), F.max("date").alias("end_date")).collect()[0]
start_date = bounds["start_date"]
end_date = bounds["end_date"]

calendar = spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"]).select(
    F.explode(F.sequence(F.col("start_date"), F.col("end_date"), F.expr("interval 1 month"))).alias("date")
)

full_monthly = (
    calendar
    .join(monthly.select("date", "total_passengers"), on="date", how="left")
    .na.fill({"total_passengers": 0.0})
    .withColumn("year", F.year("date").cast("int"))
    .withColumn("month", F.month("date").cast("int"))
    .withColumn("quarter", F.quarter("date").cast("int"))
    )

# create time index
min_year = full_monthly.agg(F.min("year").alias("min_year")).collect()[0]["min_year"]
full_monthly = full_monthly.withColumn(
    "time_index",
    ((F.col("year") - F.lit(min_year)) * F.lit(12) + F.col("month")).cast("int")
)

full_monthly = full_monthly.withColumn("label", F.col("total_passengers").cast("double"))
full_monthly = full_monthly.withColumn("log_label", F.log(F.col("label") + F.lit(1.0)))

full_monthly = full_monthly.orderBy("date")

# -------------------
# FEATURE ENGINEERING
# -------------------

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
        sin_col = f"{base_col}_sin"
        cos_col = f"{base_col}_cos"

        return (
            dataset
            .withColumn(sin_col, F.sin(2.0 * math.pi * F.col(input_col) / F.lit(12.0)))
            .withColumn(cos_col, F.cos(2.0 * math.pi * F.col(input_col) / F.lit(12.0)))
        )

month_encoder = CyclicalMonthEncoder(inputCol="month", outputCol="month_enc")

monthly_encoded = month_encoder.transform(full_monthly)

w = Window.orderBy("date")
w3 = Window.orderBy("date").rowsBetween(-3, -1)
w6 = Window.orderBy("date").rowsBetween(-6, -1)
w12 = Window.orderBy("date").rowsBetween(-12, -1)

monthly_encoded = (
    monthly_encoded
    .withColumn("lag_1", F.lag("label", 1).over(w))
    .withColumn("lag_3", F.lag("label", 3).over(w))
    .withColumn("lag_6", F.lag("label", 6).over(w))
    .withColumn("lag_12", F.lag("label", 12).over(w))
    .withColumn("roll_mean_3", F.avg("label").over(w3))
    .withColumn("roll_mean_6", F.avg("label").over(w6))
    .withColumn("roll_mean_12", F.avg("label").over(w12))
    .withColumn("roll_std_12", F.stddev("label").over(w12))
    .withColumn("yoy_diff", F.col("label") - F.col("lag_12"))
    .withColumn(
        "yoy_ratio",
        F.when((F.col("lag_12").isNotNull()) & (F.col("lag_12") > 0),
            F.col("label") / F.col("lag_12"))
        .otherwise(F.lit(1.0))
)
)

FEATURE_COLUMNS = [
    "year",
    "time_index",
    "quarter",
    "month_enc_sin",
    "month_enc_cos",
    "lag_1",
    "lag_3",
    "lag_6",
    "lag_12",
    "roll_mean_3",
    "roll_mean_6",
    "roll_mean_12",
    "roll_std_12",
    "yoy_diff",
    "yoy_ratio",
]

# keep only rows with complete features and order by date
monthly_encoded = monthly_encoded.na.drop(subset=FEATURE_COLUMNS).orderBy("date")

# ----------
# DATA SPLIT
# ----------

# keep 80% train data and 20% test data
w = Window.orderBy("date")
monthly_encoded = monthly_encoded.withColumn("row_num", F.row_number().over(w))
total_rows = monthly_encoded.count()
split_point = max(1, int(total_rows * 0.8))

train_data = monthly_encoded.filter(F.col("row_num") <= split_point).drop("row_num")
test_data = monthly_encoded.filter(F.col("row_num") > split_point).drop("row_num")

# save splits as json on hdfs
train_data.coalesce(1).write.mode("overwrite").format("json").save("project/data/train")
test_data.coalesce(1).write.mode("overwrite").format("json").save("project/data/test")

# --------
# MODELING
# --------

def fit_cv_model(pipeline, param_grid, train_df, folds=3):
    evaluator = RegressionEvaluator(labelCol="log_label", predictionCol="prediction", metricName="rmse")
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=folds,
        parallelism=4,
        seed=0
    )
    cv_model = cv.fit(train_df)
    return cv_model

def score_on_original_scale(model, df):
    raw_pred = model.transform(df)

    scored = (
        raw_pred
        .withColumn("prediction", F.expm1(F.col("prediction")))
        .select("label", "prediction")
    )

    rmse = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="rmse"
    ).evaluate(scored)

    r2 = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="r2"
    ).evaluate(scored)

    return scored, rmse, r2

# -------
# MODEL 1
# -------

# build pipeline
assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="raw_features", handleInvalid="skip")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
lr = LinearRegression(featuresCol="features", labelCol="log_label")

lr_pipeline = Pipeline(stages=[assembler, scaler, lr])
lr_grid = (
    ParamGridBuilder()
    .addGrid(lr.regParam, [0.01, 0.1, 1.0])
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
    .addGrid(lr.fitIntercept, [True, False])
    .build()
)

#train
lr_cv = fit_cv_model(lr_pipeline, lr_grid, train_data, folds=3)

# select best model and save it
lr_best_model = lr_cv.bestModel
lr_best_model.write().overwrite().save("project/models/model1")

# evaluate and predict
lr_scored, lr_rmse, lr_r2 = score_on_original_scale(lr_best_model, test_data)

# save predictions
lr_scored.coalesce(1) \
    .write.mode("overwrite").format("csv") \
    .option("sep", ",").option("header", "true") \
    .save("project/output/model1_predictions.csv")

# -------
# MODEL 2
# -------

# build pipeline
assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features", handleInvalid="skip")
gbt = GBTRegressor(featuresCol="features", labelCol="log_label", seed=42)
gbt_pipeline = Pipeline(stages=[assembler, gbt])

gbt_grid = (
    ParamGridBuilder()
    .addGrid(gbt.maxDepth, [2, 4, 6])
    .addGrid(gbt.maxBins, [16, 32, 64])
    .addGrid(gbt.stepSize, [0.03, 0.1, 0.2])
    .build()
)

#train
gbt_cv = fit_cv_model(gbt_pipeline, gbt_grid, train_data, folds=3)

# select best model and save it
gbt_best_model = gbt_cv.bestModel
gbt_best_model.write().overwrite().save("project/models/model2")

# evaluate and predict
gbt_scored, gbt_rmse, gbt_r2 = score_on_original_scale(gbt_best_model, test_data)

# save predictions
gbt_scored.coalesce(1) \
    .write.mode("overwrite").format("csv") \
    .option("sep", ",").option("header", "true") \
    .save("project/output/model2_predictions.csv")

# -----------------
# MODELS COMPARISON
# -----------------

comparison = spark.createDataFrame(
    [
        ("LinearRegression", float(lr_rmse), float(lr_r2)),
        ("GBTRegressor", float(gbt_rmse), float(gbt_r2)),
    ],
    ["model", "RMSE", "R2"]
)

# save comparison
comparison.coalesce(1) \
          .write.mode("overwrite").format("csv") \
          .option("sep", ",").option("header", "true") \
          .save("project/output/evaluation.csv")

spark.stop()