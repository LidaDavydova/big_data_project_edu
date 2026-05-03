-- Q5: Annual Passenger Traffic by Route Type

USE team12_projectdb;

DROP TABLE IF EXISTS q5_results;

CREATE TABLE q5_results 
STORED AS PARQUET
AS
SELECT
    nr_ano_partida_real,
    ds_natureza_tipo_linha,
    SUM(nr_passag_pagos + COALESCE(nr_passag_gratis, 0)) AS total_passengers
FROM fact_flights_optimized
GROUP BY nr_ano_partida_real, ds_natureza_tipo_linha;

SELECT * FROM q5_results;