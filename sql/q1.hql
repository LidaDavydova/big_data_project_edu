-- Q1: Monthly Passenger Traffic by Year

USE team12_projectdb;

DROP TABLE IF EXISTS q1_results;

CREATE TABLE q1_results
STORED AS PARQUET
AS
SELECT
    nr_ano_partida_real AS year,
    
    CASE nr_mes_partida_real
        WHEN 1 THEN '01'
        WHEN 2 THEN '02'
        WHEN 3 THEN '03'
        WHEN 4 THEN '04'
        WHEN 5 THEN '05'
        WHEN 6 THEN '06'
        WHEN 7 THEN '07'
        WHEN 8 THEN '08'
        WHEN 9 THEN '09'
        WHEN 10 THEN '10'
        WHEN 11 THEN '11'
        WHEN 12 THEN '12'
    END AS month_str,

    SUM(nr_passag_pagos + COALESCE(nr_passag_gratis, 0)) AS total_passengers

FROM fact_flights_optimized
GROUP BY nr_ano_partida_real, nr_mes_partida_real;

SELECT * FROM q1_results;