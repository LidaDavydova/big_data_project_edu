DROP TABLE IF EXISTS fact_flight CASCADE;

CREATE TABLE fact_flight (
    flight_id BIGSERIAL PRIMARY KEY,

    nr_passag_pagos INT,
    nr_passag_gratis INT,
    ds_servico_tipo_linha TEXT,
    kg_carga_paga DOUBLE PRECISION,

    id_aerodromo_origem TEXT,
    nm_pais_origem TEXT,
    id_aerodromo_destino TEXT,
    nm_pais_destino TEXT,

    ds_grupo_di TEXT,
    ds_natureza_tipo_linha TEXT,
    ds_tipo_empresa TEXT,

    nr_ano_partida_real INT,
    nr_mes_partida_real INT
);

CREATE INDEX idx_flight_time
    ON fact_flight(nr_ano_partida_real, nr_mes_partida_real);

CREATE INDEX idx_origin
    ON fact_flight(id_aerodromo_origem);

CREATE INDEX idx_dest
    ON fact_flight(id_aerodromo_destino);
    
