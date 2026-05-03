-- Q4: Annual Free-to-Paid Passenger Ratio

USE team12_projectdb;

DROP TABLE IF EXISTS q4_results;

CREATE TABLE q4_results
STORED AS PARQUET
AS
SELECT
    nr_ano_partida_real,
    100.0 * SUM(COALESCE(nr_passag_gratis, 0)) /
    NULLIF(SUM(nr_passag_pagos), 0) AS free_to_paid_ratio
FROM fact_flights_optimized
GROUP BY nr_ano_partida_real;

SELECT * FROM q4_results;

