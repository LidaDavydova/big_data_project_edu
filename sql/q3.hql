-- Q3: Free-to-Paid Passenger Percentage (Year vs Month)

USE team12_projectdb;

DROP TABLE IF EXISTS q3_results;

CREATE TABLE q3_results
STORED AS PARQUET
AS
SELECT
    nr_ano_partida_real AS year,

    LPAD(CAST(nr_mes_partida_real AS STRING), 2, '0') AS month_str,

    100.0 * SUM(COALESCE(nr_passag_gratis, 0))
      / NULLIF(SUM(nr_passag_pagos), 0) AS free_to_paid_ratio_pct

FROM fact_flights_optimized
GROUP BY nr_ano_partida_real, nr_mes_partida_real;

SELECT * FROM q3_results;