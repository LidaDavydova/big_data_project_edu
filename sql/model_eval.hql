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
LOCATION '/user/team12/project/output/model1_predictions'
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
LOCATION '/user/team12/project/output/model2_predictions'
TBLPROPERTIES ("skip.header.line.count"="1");

DROP TABLE IF EXISTS model1_predictions_indexed;

CREATE TABLE model1_predictions_indexed AS
SELECT
    row_number() OVER (ORDER BY label) AS idx,
    label,
    prediction
FROM model1_predictions;


DROP TABLE IF EXISTS model2_predictions_indexed;

CREATE TABLE model2_predictions_indexed AS
SELECT
    row_number() OVER (ORDER BY label) AS idx,
    label,
    prediction
FROM model2_predictions;



-- VALIDATION

SELECT 'MODEL1 COUNT' AS info, COUNT(*) FROM model1_predictions;
SELECT 'MODEL2 COUNT' AS info, COUNT(*) FROM model2_predictions;

SELECT * FROM model1_predictions_indexed LIMIT 5;
SELECT * FROM model2_predictions_indexed LIMIT 5;