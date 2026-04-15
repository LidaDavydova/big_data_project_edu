DROP DATABASE IF EXISTS team12_projectdb CASCADE;

CREATE DATABASE team12_projectdb LOCATION "project/hive/warehouse";
USE team12_projectdb;

CREATE EXTERNAL TABLE staging_flights (
    id_basica STRING,
    sg_empresa_icao STRING,
    dt_partida_real STRING,
    cd_di STRING,
    ds_grupo_di STRING,
    cd_tipo_linha STRING,

    sg_icao_origem STRING,
    nm_municipio_origem STRING,
    sg_uf_origem STRING,
    nm_pais_origem STRING,
    nm_continente_origem STRING,

    sg_icao_destino STRING,
    nm_municipio_destino STRING,
    sg_uf_destino STRING,
    nm_pais_destino STRING,
    nm_continente_destino STRING,

    km_distancia STRING,
    nr_passag_pagos STRING,
    nr_passag_gratis STRING,
    nr_rpk STRING,
    nr_ask STRING,
    nr_carga_paga_km STRING,
    nr_carga_gratis_km STRING
)
STORED AS PARQUET
LOCATION 'project/warehouse/staging_flights';

CREATE EXTERNAL TABLE dim_airport (
    sg_icao STRING,
    nm_municipio STRING,
    sg_uf STRING,
    nm_pais STRING,
    nm_continente STRING
)
STORED AS PARQUET
LOCATION 'project/warehouse/dim_airport';

CREATE EXTERNAL TABLE dim_date (
    dt DATE,
    nr_ano SMALLINT,
    nr_mes SMALLINT,
    nr_trimestre SMALLINT,
    nr_dia_semana SMALLINT
)
STORED AS PARQUET
LOCATION 'project/warehouse/dim_date';

CREATE EXTERNAL TABLE fact_flights (
    id_basica BIGINT,
    sg_empresa_icao STRING,
    dt_partida_real DATE,
    sg_icao_origem STRING,
    sg_icao_destino STRING,
    route_id STRING,

    cd_di STRING,
    ds_grupo_di STRING,
    cd_tipo_linha STRING,

    km_distancia DOUBLE,
    nr_ask DOUBLE,
    nr_passag_pagos INT,
    nr_passag_gratis INT,
    nr_rpk DOUBLE,
    nr_carga_paga_kg DOUBLE,
    nr_carga_gratis DOUBLE
)
STORED AS PARQUET
LOCATION 'project/warehouse/fact_flights';