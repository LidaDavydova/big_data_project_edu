USE team12_projectdb;


DROP TABLE IF EXISTS model_evaluation;

CREATE TABLE model_evaluation (
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
LOCATION 'project/output/evaluation'
TBLPROPERTIES ("skip.header.line.count"="1");

SELECT * FROM model_evaluation;