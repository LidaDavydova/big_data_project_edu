USE team12_projectdb;

-- MODEL OUTPUTS (CSV)

-- MODEL 1 PREDICTIONS (XGBOOST)

DROP TABLE IF EXISTS model1_predictions;

CREATE EXTERNAL TABLE model1_predictions (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION 'project/output/model1_predictions'
TBLPROPERTIES ("skip.header.line.count"="1");


-- MODEL 2 PREDICTIONS (GBT)

DROP TABLE IF EXISTS model2_predictions;

CREATE EXTERNAL TABLE model2_predictions (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION 'project/output/model2_predictions'
TBLPROPERTIES ("skip.header.line.count"="1");


-- MODEL EVALUATION

DROP TABLE IF EXISTS model_evaluation;

CREATE EXTERNAL TABLE model_evaluation (
    model STRING,
    rmse DOUBLE,
    r2 DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\""
)
STORED AS TEXTFILE
LOCATION 'project/output/evaluation.csv'
TBLPROPERTIES ("skip.header.line.count"="1");

-- VALIDATION

SELECT COUNT(*) AS total_rows
FROM model1_predictions;

SELECT COUNT(*) AS total_rows
FROM model2_predictions;

SELECT *
FROM model_evaluation;
