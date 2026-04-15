BEGIN;

DROP TABLE IF EXISTS fact_flights;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_airport;
DROP TABLE IF EXISTS staging_flights;

CREATE TABLE dim_airport (
    sg_icao TEXT PRIMARY KEY,
    nm_municipio TEXT,
    sg_uf TEXT,
    nm_pais TEXT,
    nm_continente TEXT
);

CREATE TABLE dim_date (
    dt DATE PRIMARY KEY,
    nr_ano SMALLINT,
    nr_mes SMALLINT,
    nr_trimestre SMALLINT,
    nr_dia_semana SMALLINT
);

CREATE TABLE fact_flights (
    id_basica BIGINT NOT NULL,

    sg_empresa_icao TEXT NOT NULL,
    dt_partida_real DATE NOT NULL,
    sg_icao_origem TEXT NOT NULL,
    sg_icao_destino TEXT NOT NULL,
    route_id TEXT NOT NULL,

    cd_di TEXT,
    ds_grupo_di TEXT,
    cd_tipo_linha TEXT,

    km_distancia DOUBLE PRECISION,
    nr_ask DOUBLE PRECISION,
    nr_passag_pagos INTEGER,
    nr_passag_gratis INTEGER,
    nr_rpk DOUBLE PRECISION,
    nr_carga_paga_kg DOUBLE PRECISION,
    nr_carga_gratis DOUBLE PRECISION,

    PRIMARY KEY (id_basica, sg_icao_origem)
);

CREATE TABLE staging_flights (
    id_basica TEXT,
    sg_empresa_icao TEXT,
    dt_partida_real TEXT,
    cd_di TEXT,
    ds_grupo_di TEXT,
    cd_tipo_linha TEXT,

    sg_icao_origem TEXT,
    nm_municipio_origem TEXT,
    sg_uf_origem TEXT,
    nm_pais_origem TEXT,
    nm_continente_origem TEXT,

    sg_icao_destino TEXT,
    nm_municipio_destino TEXT,
    sg_uf_destino TEXT,
    nm_pais_destino TEXT,
    nm_continente_destino TEXT,

    km_distancia TEXT,
    nr_passag_pagos TEXT,
    nr_passag_gratis TEXT,
    nr_rpk TEXT,
    nr_ask TEXT,
    nr_carga_paga_km TEXT,
    nr_carga_gratis_km TEXT
);


ALTER TABLE fact_flights
ADD CONSTRAINT fk_fact_origin_airport
FOREIGN KEY (sg_icao_origem)
REFERENCES dim_airport (sg_icao);

ALTER TABLE fact_flights
ADD CONSTRAINT fk_fact_dest_airport
FOREIGN KEY (sg_icao_destino)
REFERENCES dim_airport (sg_icao);

ALTER TABLE fact_flights
ADD CONSTRAINT fk_fact_date
FOREIGN KEY (dt_partida_real)
REFERENCES dim_date (dt);


ALTER TABLE fact_flights
DROP CONSTRAINT IF EXISTS fact_flights_pkey;

ALTER TABLE fact_flights
ADD CONSTRAINT fact_flights_pkey PRIMARY KEY (id_basica);

ALTER TABLE fact_flights
ADD CONSTRAINT chk_km_distance
CHECK (km_distancia IS NULL OR km_distancia >= 0);

ALTER TABLE fact_flights
ADD CONSTRAINT chk_passag_pagos
CHECK (nr_passag_pagos IS NULL OR nr_passag_pagos >= 0);

ALTER TABLE fact_flights
ADD CONSTRAINT chk_passag_gratis
CHECK (nr_passag_gratis IS NULL OR nr_passag_gratis >= 0);

COMMIT;