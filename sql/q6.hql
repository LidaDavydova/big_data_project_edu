-- Q6 Seasonality of passenger demand

USE team12_projectdb;

DROP TABLE IF EXISTS q6_results;

CREATE TABLE q6_results
STORED AS PARQUET
AS
SELECT
    nr_ano_partida_real,
    SUM(nr_passag_pagos + COALESCE(nr_passag_gratis, 0)) AS total_passengers,
    SUM(COALESCE(kg_carga_paga, 0)) AS total_cargo
FROM fact_flights_optimized
GROUP BY nr_ano_partida_real;


SELECT * FROM q6_results;