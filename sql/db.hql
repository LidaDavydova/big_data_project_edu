DROP DATABASE IF EXISTS team12_projectdb CASCADE;

CREATE DATABASE team12_projectdb
LOCATION 'project/hive/warehouse';

USE team12_projectdb;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.enforce.bucketing = true;

-- EXTERNAL TABLES 

DROP TABLE IF EXISTS fact_flight;
CREATE TABLE fact_flight (
    flight_id BIGINT,

    nr_passag_pagos INT,
    nr_passag_gratis INT,
    ds_servico_tipo_linha STRING,
    kg_carga_paga DOUBLE,

    id_aerodromo_origem STRING,
    nm_pais_origem STRING,
    id_aerodromo_destino STRING,
    nm_pais_destino STRING,

    ds_grupo_di STRING,
    ds_natureza_tipo_linha STRING,
    ds_tipo_empresa STRING,

    nr_ano_partida_real INT,
    nr_mes_partida_real INT
)
STORED AS PARQUET
LOCATION 'project/warehouse/fact_flight';

-- Partition + Bucketing

DROP TABLE IF EXISTS fact_flights_optimized;

CREATE TABLE fact_flights_optimized (
    flight_id BIGINT,

    nr_passag_pagos INT,
    nr_passag_gratis INT,
    ds_servico_tipo_linha STRING,
    kg_carga_paga DOUBLE,

    id_aerodromo_origem STRING,
    nm_pais_origem STRING,
    id_aerodromo_destino STRING,
    nm_pais_destino STRING,

    ds_grupo_di STRING,
    ds_natureza_tipo_linha STRING,
    ds_tipo_empresa STRING,

    nr_mes_partida_real INT
)
PARTITIONED BY (nr_ano_partida_real INT)
CLUSTERED BY (ds_natureza_tipo_linha) INTO 8 BUCKETS
STORED AS PARQUET;

-- LOAD DATA INTO PARTITIONED TABLE

INSERT INTO TABLE fact_flights_optimized
PARTITION (nr_ano_partida_real)
SELECT
    flight_id,

    nr_passag_pagos,
    nr_passag_gratis,
    ds_servico_tipo_linha,
    kg_carga_paga,

    id_aerodromo_origem,
    nm_pais_origem,
    id_aerodromo_destino,
    nm_pais_destino,

    ds_grupo_di,
    ds_natureza_tipo_linha,
    ds_tipo_empresa,

    nr_mes_partida_real,
    nr_ano_partida_real
FROM fact_flight;

-- VALIDATION

SHOW PARTITIONS fact_flights_optimized;

SELECT
    nr_ano_partida_real, 
    COUNT(*) AS total_flights
FROM fact_flights_optimized
GROUP BY nr_ano_partida_real
ORDER BY nr_ano_partida_real;

-- Drop unused table

DROP TABLE IF EXISTS fact_flight;