import math
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml import Transformer, Pipeline
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder
from xgboost.spark import SparkXGBRegressor
from pyspark import keyword_only

# start spark session
team = 'team12'
warehouse = "project/hive/warehouse"
spark = SparkSession.builder \
        .appName("{} - spark ML".format(team)) \
        .master("yarn") \
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
        .config("spark.sql.warehouse.dir", warehouse) \
        .config("spark.sql.parquet.compression.codec", "gzip") \
        .enableHiveSupport() \
        .getOrCreate()

# read dataset
flights = spark.read.table('team12_projectdb.fact_flights_optimized')
print(f"Number of records in flights: {flights.count()}")

# --------------------
# DATA CLEANING
# --------------------

# filter only regular and passenger flights (not identified category is also passenger flights)
flights = flights.filter(F.col("ds_grupo_di") == "REGULAR")
flights = flights.filter(F.col("ds_servico_tipo_linha").isin("PASSAGEIRO", "NÃO IDENTIFICADO"))

# drop nan values from year, month and passengers
flights = flights.na.drop(subset=["nr_ano_partida_real", "nr_mes_partida_real"])
flights = flights.na.drop(subset=["nr_passag_pagos", "nr_passag_gratis"])

# filter number of revenue passengers to be adequate
flights = flights.filter((F.col("nr_passag_pagos") > 0) & (F.col("nr_passag_pagos") <= 853))

# create target column - total passengers, filter it to be adequate
flights = flights.withColumn("total_passengers", F.col("nr_passag_pagos") + F.col("nr_passag_gratis"))
flights = flights.filter(F.col("total_passengers") <= 900)

# filter year to be <= 2025
flights = flights.filter(F.col("nr_ano_partida_real") <= 2025)

# create column date using original columns
flights = flights.withColumn(
    "date",
    F.to_date(
        F.format_string(
            "%04d-%02d-01",
            F.col("nr_ano_partida_real"),
            F.col("nr_mes_partida_real")
        )
    )
)

# --------------------
# DATA AGGREGATION
# --------------------

monthly = (
    flights
    .groupBy("date")
    .agg(
        F.sum(F.col("total_passengers").cast("double")).alias("label")
    )
    .orderBy("date")
)

# Build a complete monthly calendar
bounds = monthly.agg(
    F.min("date").alias("start_date"),
    F.max("date").alias("end_date")
).first()

calendar = spark.range(1).select(
    F.explode(
        F.sequence(
            F.lit(bounds["start_date"]),
            F.lit(bounds["end_date"]),
            F.expr("interval 1 month")
        )
    ).alias("date")
)

full_monthly = (
    calendar
    .join(monthly, on="date", how="left")
    .withColumn("is_filled", F.col("label").isNull().cast("int"))
    .na.fill(0, subset=["label"])
    .withColumn("year", F.year("date").cast("int"))
    .withColumn("month", F.month("date").cast("int"))
    .withColumn("quarter", F.quarter("date").cast("int"))
    .orderBy("date")
)

min_year = full_monthly.agg(F.min("year").alias("min_year")).first()["min_year"]

full_monthly = full_monthly.withColumn(
    "time_index",
    ((F.col("year") - F.lit(min_year)) * F.lit(12) + F.col("month")).cast("int")
)

# create log-transformed target (use for both training and feature lags)
full_monthly = full_monthly.withColumn("log_label", F.log1p(F.col("label")))

# --------------------
# FEATURE ENGINEERING
# --------------------

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

w = Window.orderBy("date")
w3 = Window.orderBy("date").rowsBetween(-3, -1)
w12 = Window.orderBy("date").rowsBetween(-12, -1)
w24 = Window.orderBy("date").rowsBetween(-24, -1)

for lag_n in [1, 12, 24]:
    full_monthly = full_monthly.withColumn(f"passengers_lag_{lag_n}", F.lag("log_label", lag_n).over(w))

full_monthly = (
    full_monthly
    .withColumn("passengers_roll_mean_3", F.avg("log_label").over(w3))
    .withColumn("passengers_roll_mean_12", F.avg("log_label").over(w12))
    .withColumn("passengers_roll_mean_24", F.avg("log_label").over(w24))
    .withColumn("passengers_roll_std_12", F.stddev("log_label").over(w12))
)

# apply encoder AFTER lag features
monthly_encoded = month_encoder.transform(full_monthly)

FEATURE_COLUMNS = [
    "year",
    "time_index",
    "quarter",
    "is_filled",
    "month_enc_sin",
    "month_enc_cos",
    "passengers_lag_1",
    "passengers_lag_12",
    "passengers_lag_24",
    "passengers_roll_mean_3",
    "passengers_roll_mean_12",
    "passengers_roll_mean_24",
    "passengers_roll_std_12",
]

# Drop rows with nulls (first 24 months will be removed)
monthly_encoded = monthly_encoded.na.drop(subset=FEATURE_COLUMNS + ["log_label", "label"]).orderBy("date")

# --------------------
# TRAIN / TEST SPLIT
# --------------------

monthly_encoded = monthly_encoded.withColumn("row_num", F.row_number().over(Window.orderBy("date")))
total_rows = monthly_encoded.count()
split_point = max(1, int(total_rows * 0.75))

train_data = monthly_encoded.filter(F.col("row_num") <= split_point).drop("row_num")
test_data = monthly_encoded.filter(F.col("row_num") > split_point).drop("row_num")

# Save train/test as Parquet (removed coalesce(1) for better parallelism)
train_data.write.mode("overwrite").format("parquet").save("project/data_train_test/train")
test_data.write.mode("overwrite").format("parquet").save("project/data_train_test/test")

# --------------------
# TIME-SERIES VALIDATION
# --------------------

def make_time_folds(df, n_folds=3, initial_train_frac=0.5):
    ordered = df.withColumn("row_num", F.row_number().over(Window.orderBy("date")))
    total_rows = ordered.count()

    folds = []
    for i in range(n_folds):
        train_end = int(total_rows * (initial_train_frac + i * (1.0 - initial_train_frac) / n_folds))
        val_end = int(total_rows * (initial_train_frac + (i + 1) * (1.0 - initial_train_frac) / n_folds))

        train_end = max(1, min(train_end, total_rows - 1))
        val_end = max(train_end + 1, min(val_end, total_rows))

        train_fold = ordered.filter(F.col("row_num") <= train_end).drop("row_num")
        val_fold = ordered.filter(
            (F.col("row_num") > train_end) & (F.col("row_num") <= val_end)
        ).drop("row_num")

        if val_fold.take(1):
            folds.append((train_fold, val_fold))

    return folds

def evaluate_original_scale(model, df):
    pred = model.transform(df)
    # model predicts on log scale, convert back
    scored = pred.withColumn(
        "prediction_level",
        F.greatest(F.lit(0.0), F.expm1(F.col("prediction")))
    )

    rmse = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction_level",
        metricName="rmse"
    ).evaluate(scored)

    r2 = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction_level",
        metricName="r2"
    ).evaluate(scored)

    return rmse, r2, scored

def fit_time_series_grid_search(pipeline, param_grid, train_df, n_folds=3):
    folds = make_time_folds(train_df, n_folds=n_folds)
    if not folds:
        raise ValueError("Not enough data to create time-series folds.")

    # Extract assembler (first stage) and estimator (last stage)
    assembler = pipeline.getStages()[0]
    estimator = pipeline.getStages()[-1]

    best_params = None
    best_avg_rmse = float("inf")
    best_avg_r2 = None
    all_results = []

    for params in param_grid:
        fold_rmses = []
        fold_r2s = []

        for fold_train, fold_val in folds:
            # Clone estimator and set parameters
            estimator_copy = estimator.copy(params)
            # Build new pipeline with same assembler and cloned estimator
            pipeline_copy = Pipeline(stages=[assembler, estimator_copy])
            fitted = pipeline_copy.fit(fold_train)
            rmse, r2, _ = evaluate_original_scale(fitted, fold_val)
            fold_rmses.append(rmse)
            fold_r2s.append(r2)

        avg_rmse = sum(fold_rmses) / len(fold_rmses)
        avg_r2 = sum(fold_r2s) / len(fold_r2s)
        all_results.append((params, avg_rmse, avg_r2))

        if avg_rmse < best_avg_rmse:
            best_avg_rmse = avg_rmse
            best_avg_r2 = avg_r2
            best_params = params

    # Refit on full training data with best params
    estimator_best = estimator.copy(best_params)
    pipeline_best = Pipeline(stages=[assembler, estimator_best])
    best_model = pipeline_best.fit(train_df)
    return best_model, best_params, all_results, best_avg_rmse, best_avg_r2


# ----------------
# MODEL 1: XGBOOST
# ----------------

assembler = VectorAssembler(
    inputCols=FEATURE_COLUMNS,
    outputCol="features",
    handleInvalid="skip"
)

xgb = SparkXGBRegressor(
    features_col="features",
    label_col="log_label",
    num_workers=2,
    objective="reg:squarederror",
    eval_metric="rmse",
    seed=42
)



xgb_pipeline = Pipeline(stages=[assembler, xgb])

xgb_grid = (
    ParamGridBuilder()
    .addGrid(xgb.max_depth, [3, 5, 7])
    .addGrid(xgb.min_child_weight, [1.0, 3.0, 5.0])
    .addGrid(xgb.subsample, [0.8, 1.0])
    .build()
)

xgb_best_model, xgb_best_params, xgb_results, xgb_cv_rmse, xgb_cv_r2 = fit_time_series_grid_search(
    xgb_pipeline, xgb_grid, train_data, n_folds=3
)

xgb_best_model.write().overwrite().save("project/models/model1")

xgb_rmse, xgb_r2, xgb_scored = evaluate_original_scale(xgb_best_model, test_data)

xgb_scored.select(
    F.col("label"),
    F.col("prediction_level").alias("prediction")
).write.mode("overwrite").format("csv").option("header", "true").save("project/output/model1_predictions")

# ------------
# MODEL 2: GBT
# ------------

gbt = GBTRegressor(featuresCol="features", labelCol="log_label", seed=42)
gbt_pipeline = Pipeline(stages=[assembler, gbt])

gbt_grid = (
    ParamGridBuilder()
    .addGrid(gbt.maxDepth, [3, 5, 7])
    .addGrid(gbt.minInstancesPerNode, [1, 2, 4])
    .addGrid(gbt.subsamplingRate, [0.7, 0.85, 1.0])
    .build()
)

gbt_best_model, gbt_best_params, gbt_results, gbt_cv_rmse, gbt_cv_r2 = fit_time_series_grid_search(
    gbt_pipeline, gbt_grid, train_data, n_folds=3
)

gbt_best_model.write().overwrite().save("project/models/model2")

gbt_rmse, gbt_r2, gbt_scored = evaluate_original_scale(gbt_best_model, test_data)

gbt_scored.select(
    F.col("label"),
    F.col("prediction_level").alias("prediction")
).write.mode("overwrite").format("csv").option("header", "true").save("project/output/model2_predictions")

# --------------------
# COMPARISON
# --------------------

comparison = spark.createDataFrame(
    [
        ("SparkXGBRegressor", float(xgb_rmse), float(xgb_r2)),
        ("GBTRegressor", float(gbt_rmse), float(gbt_r2)),
    ],
    ["model", "RMSE", "R2"]
)

comparison.coalesce(1).write.mode("overwrite").format("csv") \
    .option("sep", ",").option("header", "true") \
    .save("project/output/evaluation.csv")

spark.stop()