-- Q2: Annual Passengers Traffic

USE team12_projectdb;

DROP TABLE IF EXISTS q2_results;

CREATE TABLE q2_results
STORED AS PARQUET
AS
SELECT
    nr_ano_partida_real,
    SUM(nr_passag_pagos + COALESCE(nr_passag_gratis, 0)) AS total_passengers
FROM fact_flights_optimized
GROUP BY nr_ano_partida_real;

SELECT * FROM q2_results;