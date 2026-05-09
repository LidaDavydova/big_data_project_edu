# --------------------
# SOLVING PROJECT TASK
# --------------------

# helper functions
def _next_month(current_year: int, current_month: int) -> Tuple[int, int]:
    if current_month == 12:
        return current_year + 1, 1
    return current_year, current_month + 1

def _build_feature_row(history: List[Dict], next_date, min_year: int):
    """
    Build one forecast row from the observed/predicted history.
    history: list of dicts with keys date, label
    """
    year = next_date.year
    month = next_date.month
    time_index = (year - min_year) * 12 + month
    quarter = (month - 1) // 3 + 1

    labels = [float(x["label"]) for x in history]

    def lag(k):
        return float(labels[-k]) if len(labels) >= k else None

    def rolling_mean(k):
        vals = labels[-k:] if len(labels) >= k else labels[:]
        return float(sum(vals) / len(vals)) if vals else None

    def rolling_std(k):
        vals = labels[-k:] if len(labels) >= k else labels[:]
        if len(vals) < 2:
            return 0.0
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)
        return float(math.sqrt(var))

    lag_12 = lag(12)
    yoy_diff = None if lag_12 is None else float(labels[-1] - lag_12)
    yoy_ratio = None if (lag_12 is None or lag_12 == 0) else float(labels[-1] / lag_12)

    month_sin = math.sin(2.0 * math.pi * month / 12.0)
    month_cos = math.cos(2.0 * math.pi * month / 12.0)

    return {
        "date": next_date,
        "year": int(year),
        "time_index": int(time_index),
        "quarter": int(quarter),
        "month_sin": float(month_sin),
        "month_cos": float(month_cos),
        "lag_1": lag(1),
        "lag_3": lag(3),
        "lag_6": lag(6),
        "lag_12": lag_12,
        "roll_mean_3": rolling_mean(3),
        "roll_mean_6": rolling_mean(6),
        "roll_mean_12": rolling_mean(12),
        "roll_std_12": rolling_std(12),
        "yoy_diff": yoy_diff,
        "yoy_ratio": yoy_ratio,
    }

def recursive_forecast(
    spark: SparkSession,
    fitted_pipeline_model,
    history_df,
    months_ahead: int = 36
):
    """
    Forecasts future months recursively using predicted values as future lags.
    """
    history = (
        history_df
        .select("date", "label")
        .orderBy("date")
        .collect()
    )

    history_list = [{"date": r["date"], "label": float(r["label"])} for r in history]
    min_year = min(r["date"].year for r in history_list)

    last_date = history_list[-1]["date"]
    results = []

    for _ in range(months_ahead):
        year = last_date.year
        month = last_date.month
        next_year, next_month = _next_month(year, month)

        # Construct the next month date.
        next_date = last_date.replace(year=next_year, month=next_month, day=1)
        feature_row = _build_feature_row(history_list, next_date, min_year=min_year)

        one_row = spark.createDataFrame([feature_row])
        pred_row = fitted_pipeline_model.transform(one_row).select(
            "date",
            F.expm1(F.col("prediction")).alias("prediction_level")
        ).collect()[0]

        predicted_value = float(pred_row["prediction_level"])
        results.append((next_date, predicted_value))

        # Append prediction to history for the next recursive step.
        history_list.append({"date": next_date, "label": predicted_value})
        last_date = next_date

    forecast_df = spark.createDataFrame(results, ["date", "prediction_level"])
    
    return forecast_df

# pick best model based on test set
if lr_rmse <= gbt_rmse:
    best_name = "LinearRegression"
    best_model = lr_best_model
else:
    best_name = "GBTRegressor"
    best_model = gbt_best_model

# forecast next 36 months recursively using selected model
forecast = recursive_forecast(
    spark=spark,
    fitted_pipeline_model=best_model,
    history_df=full_monthly,
    months_ahead=36
)

forecast.orderBy("date") \
    .coalesce(1) \
    .write.mode("overwrite").format("csv") \
    .option("sep", ",").option("header", "true") \
    .save("project/output/forecast.csv")