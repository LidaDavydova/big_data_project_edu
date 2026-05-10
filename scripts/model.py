import math
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml import Transformer, Pipeline
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder
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

# --------------------
# DATA CLEANING
# --------------------

# drop nan values from year, month and passangers
flights = flights.na.drop(subset=["nr_ano_partida_real", "nr_mes_partida_real"])
flights = flights.na.drop(subset=["nr_passag_pagos", "nr_passag_gratis"])

# filter only regular and passenger flights (not identified category is also passenger flights)
flights = flights.filter(F.col("ds_grupo_di") == "REGULAR")
flights = flights.filter(F.col("ds_servico_tipo_linha").isin("PASSAGEIRO", "NÃO IDENTIFICADO"))

# filter number of revenue passengers to be adequate
flights = flights.filter((F.col("nr_passag_pagos") > 0) & (F.col("nr_passag_pagos") <= 853))

# create target column - total passengers, filter it to be adequate
flights = flights.withColumn("total_passengers", F.col("nr_passag_pagos") + F.col("nr_passag_gratis"))
flights = flights.filter(F.col("total_passengers") <= 900)

# filter year to be <= 2025
flights = flights.filter(F.col("nr_ano_partida_real") <= 2025)

# create column date
flights = flights.withColumn(
    "date",
    F.to_date(
        F.format_string(
            "%04d-%02d-01",
            F.col("nr_ano_partida_real").cast("int"),
            F.col("nr_mes_partida_real").cast("int"),
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
        F.sum(F.col("total_passengers").cast("double")).alias("label"),
        F.sum(F.coalesce(F.col("kg_carga_paga").cast("double"), F.lit(0.0))).alias("cargo_kg"),
        F.count(F.lit(1)).alias("n_flights"),
        F.countDistinct("id_aerodromo_origem").alias("n_origins"),
        F.countDistinct("id_aerodromo_destino").alias("n_destinations"),
        F.countDistinct("nm_pais_origem").alias("n_origin_countries"),
        F.countDistinct("nm_pais_destino").alias("n_destination_countries"),
    )
    .orderBy("date")
)

bounds = monthly.agg(
    F.min("date").alias("start_date"),
    F.max("date").alias("end_date")
).first()

calendar = (
    spark.createDataFrame([(bounds["start_date"], bounds["end_date"])], ["start_date", "end_date"])
    .select(
        F.explode(
            F.sequence(F.col("start_date"), F.col("end_date"), F.expr("interval 1 month"))
        ).alias("date")
    )
)

numeric_fill_cols = [
    "label",
    "cargo_kg",
    "n_flights",
    "n_origins",
    "n_destinations",
    "n_origin_countries",
    "n_destination_countries"
]

full_monthly = (
    calendar
    .join(monthly, on="date", how="left")
    .na.fill(0, subset=numeric_fill_cols)
    .withColumn("year", F.year("date").cast("int"))
    .withColumn("month", F.month("date").cast("int"))
    .withColumn("quarter", F.quarter("date").cast("int"))
)

min_year = full_monthly.agg(F.min("year").alias("min_year")).first()["min_year"]

full_monthly = full_monthly.withColumn(
    "time_index",
    ((F.col("year") - F.lit(min_year)) * F.lit(12) + F.col("month")).cast("int")
)

# log target for more stable training
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
monthly_encoded = month_encoder.transform(full_monthly)

w = Window.orderBy("date")
w3 = Window.orderBy("date").rowsBetween(-3, -1)
w12 = Window.orderBy("date").rowsBetween(-12, -1)

monthly_encoded = (
    monthly_encoded
    # target lags / seasonality
    .withColumn("passengers_lag_1", F.lag("label", 1).over(w))
    .withColumn("passengers_lag_12", F.lag("label", 12).over(w))
    .withColumn("passengers_roll_mean_3", F.avg("label").over(w3))
    .withColumn("passengers_roll_mean_12", F.avg("label").over(w12))
    # cargo lags / seasonality
    .withColumn("cargo_lag_1", F.lag("cargo_kg", 1).over(w))
    .withColumn("cargo_lag_12", F.lag("cargo_kg", 12).over(w))
    .withColumn("cargo_roll_mean_3", F.avg("cargo_kg").over(w3))
    .withColumn("cargo_roll_mean_12", F.avg("cargo_kg").over(w12))
    # traffic volume lags
    .withColumn("flights_lag_1", F.lag("n_flights", 1).over(w))
    .withColumn("flights_lag_12", F.lag("n_flights", 12).over(w))
    .withColumn("flights_roll_mean_3", F.avg("n_flights").over(w3))
    .withColumn("flights_roll_mean_12", F.avg("n_flights").over(w12))
    # year-over-year signals
    .withColumn("yoy_diff", F.col("label") - F.col("passengers_lag_12"))
    .withColumn(
        "yoy_ratio",
        F.when(
            (F.col("passengers_lag_12").isNotNull()) & (F.col("passengers_lag_12") > 0),
            F.col("label") / F.col("passengers_lag_12"),
        ).otherwise(F.lit(1.0))
    )
)

FEATURE_COLUMNS = [
    "year",
    "time_index",
    "quarter",
    "month_enc_sin",
    "month_enc_cos",
    "cargo_kg",
    "n_flights",
    "n_origins",
    "n_destinations",
    "n_origin_countries",
    "n_destination_countries",
    "passengers_lag_1",
    "passengers_lag_12",
    "passengers_roll_mean_3",
    "passengers_roll_mean_12",
    "cargo_lag_1",
    "cargo_lag_12",
    "cargo_roll_mean_3",
    "cargo_roll_mean_12",
    "flights_lag_1",
    "flights_lag_12",
    "flights_roll_mean_3",
    "flights_roll_mean_12",
    "yoy_diff",
    "yoy_ratio",
]

monthly_encoded = monthly_encoded.na.drop(subset=FEATURE_COLUMNS + ["log_label"]).orderBy("date")

# --------------------
# TRAIN / TEST SPLIT
# --------------------
monthly_encoded = monthly_encoded.withColumn("row_num", F.row_number().over(Window.orderBy("date")))
total_rows = monthly_encoded.count()
split_point = max(1, int(total_rows * 0.8))

train_data = monthly_encoded.filter(F.col("row_num") <= split_point).drop("row_num")
test_data = monthly_encoded.filter(F.col("row_num") > split_point).drop("row_num")

train_data.coalesce(1).write.mode("overwrite").format("json").save("project/data_train_test/train")
test_data.coalesce(1).write.mode("overwrite").format("json").save("project/data_train_test/test")

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

        if val_fold.limit(1).count() > 0:
            folds.append((train_fold, val_fold))

    return folds


def evaluate_original_scale(model, df):
    pred = model.transform(df)

    scored = pred.withColumn(
        "prediction_level",
        F.greatest(F.lit(0.0), F.expm1(F.col("prediction")))
    )

    rmse = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction_level",
        metricName="rmse",
    ).evaluate(scored)

    r2 = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction_level",
        metricName="r2",
    ).evaluate(scored)

    return rmse, r2, scored


def fit_time_series_grid_search(pipeline, param_grid, train_df, n_folds=3):
    folds = make_time_folds(train_df, n_folds=n_folds)
    if not folds:
        raise ValueError("Not enough data to create time-series folds.")

    best_params = None
    best_avg_rmse = float("inf")
    best_avg_r2 = None
    all_results = []

    for params in param_grid:
        fold_rmses = []
        fold_r2s = []

        for fold_train, fold_val in folds:
            fitted = pipeline.fit(fold_train, params)
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

    best_model = pipeline.fit(train_df, best_params)
    return best_model, best_params, all_results, best_avg_rmse, best_avg_r2


# --------------------
# MODEL 1: RANDOM FOREST
# --------------------

rf_assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features", handleInvalid="skip")
rf = RandomForestRegressor(featuresCol="features", labelCol="log_label", seed=42)

rf_pipeline = Pipeline(stages=[rf_assembler, rf])

rf_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [50, 100])
    .addGrid(rf.maxDepth, [5, 10, 15])
    .addGrid(rf.minInstancesPerNode, [1, 2, 5])
    .build()
)

rf_best_model, rf_best_params, rf_results, rf_cv_rmse, rf_cv_r2 = fit_time_series_grid_search(
    rf_pipeline, rf_grid, train_data, n_folds=3
)

rf_best_model.write().overwrite().save("project/models/model1")

rf_rmse, rf_r2, rf_scored = evaluate_original_scale(rf_best_model, test_data)

rf_scored.select("label", F.col("prediction_level").alias("prediction")) \
    .coalesce(1) \
    .write.mode("overwrite").format("csv") \
    .option("sep", ",").option("header", "true") \
    .save("project/output/model1_predictions.csv")

# --------------------
# MODEL 2: GBT
# --------------------
gbt_assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features", handleInvalid="skip")
gbt = GBTRegressor(featuresCol="features", labelCol="log_label", seed=42)

gbt_pipeline = Pipeline(stages=[gbt_assembler, gbt])

gbt_grid = (
    ParamGridBuilder()
    .addGrid(gbt.maxDepth, [2, 4, 6])                # model parameter
    .addGrid(gbt.minInstancesPerNode, [1, 2, 4])     # model parameter
    .addGrid(gbt.stepSize, [0.03, 0.1])            # algorithmic parameter
    .build()
)

gbt_best_model, gbt_best_params, gbt_results, gbt_cv_rmse, gbt_cv_r2 = fit_time_series_grid_search(
    gbt_pipeline, gbt_grid, train_data, n_folds=3
)

gbt_best_model.write().overwrite().save("project/models/model2")

gbt_rmse, gbt_r2, gbt_scored = evaluate_original_scale(gbt_best_model, test_data)

gbt_scored.select("label", F.col("prediction_level").alias("prediction")) \
    .coalesce(1) \
    .write.mode("overwrite").format("csv") \
    .option("sep", ",").option("header", "true") \
    .save("project/output/model2_predictions.csv")

# --------------------
# COMPARISON
# --------------------

comparison = spark.createDataFrame(
    [
        ("RandomForestRegressor", float(rf_rmse), float(rf_r2)),
        ("GBTRegressor", float(gbt_rmse), float(gbt_r2)),
    ],
    ["model", "RMSE", "R2"]
)

comparison.coalesce(1) \
    .write.mode("overwrite").format("csv") \
    .option("sep", ",").option("header", "true") \
    .save("project/output/evaluation.csv")

spark.stop()