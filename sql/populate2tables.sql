INSERT INTO dim_airport (
    sg_icao,
    nm_municipio,
    sg_uf,
    nm_pais,
    nm_continente
)
SELECT DISTINCT
    sg_icao_origem,
    nm_municipio_origem,
    sg_uf_origem,
    nm_pais_origem,
    nm_continente_origem
FROM staging_flights
WHERE sg_icao_origem IS NOT NULL

UNION

SELECT DISTINCT
    sg_icao_destino,
    nm_municipio_destino,
    sg_uf_destino,
    nm_pais_destino,
    nm_continente_destino
FROM staging_flights
WHERE sg_icao_destino IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO dim_date (
    dt,
    nr_ano,
    nr_mes,
    nr_trimestre,
    nr_dia_semana
)
SELECT DISTINCT
    dt_partida_real::DATE,
    EXTRACT(YEAR FROM dt_partida_real::DATE),
    EXTRACT(MONTH FROM dt_partida_real::DATE),
    EXTRACT(QUARTER FROM dt_partida_real::DATE),
    EXTRACT(DOW FROM dt_partida_real::DATE)
FROM staging_flights
WHERE dt_partida_real IS NOT NULL
  AND dt_partida_real <> ''
ON CONFLICT DO NOTHING;

INSERT INTO fact_flights (
    id_basica,
    sg_empresa_icao,
    dt_partida_real,
    sg_icao_origem,
    sg_icao_destino,
    route_id,
    cd_di,
    ds_grupo_di,
    cd_tipo_linha,
    km_distancia,
    nr_ask,
    nr_passag_pagos,
    nr_passag_gratis,
    nr_rpk,
    nr_carga_paga_kg,
    nr_carga_gratis
)
SELECT
CASE
    WHEN NULLIF(id_basica, '') ~ '^[0-9]+(\.0+)?$'
    THEN CAST(NULLIF(id_basica, '') AS DOUBLE PRECISION)::BIGINT
    ELSE NULL
END,

NULLIF(sg_empresa_icao, ''),
NULLIF(dt_partida_real, '')::DATE,
NULLIF(sg_icao_origem, ''),
NULLIF(sg_icao_destino, ''),

sg_icao_origem || '-' || sg_icao_destino,

cd_di,
ds_grupo_di,
cd_tipo_linha,

CASE WHEN NULLIF(km_distancia,'') ~ '^-?[0-9]+(\.[0-9]+)?$'
     THEN km_distancia::DOUBLE PRECISION ELSE NULL END,

CASE WHEN NULLIF(nr_ask,'') ~ '^-?[0-9]+(\.[0-9]+)?$'
     THEN nr_ask::DOUBLE PRECISION ELSE NULL END,

CASE WHEN NULLIF(nr_passag_pagos,'') ~ '^-?[0-9]+(\.[0-9]+)?$'
     THEN nr_passag_pagos::DOUBLE PRECISION::INTEGER ELSE NULL END,
     
CASE WHEN NULLIF(nr_passag_gratis,'') ~ '^-?[0-9]+(\.[0-9]+)?$'
     THEN nr_passag_gratis::DOUBLE PRECISION::INTEGER ELSE NULL END,

CASE WHEN NULLIF(nr_rpk,'') ~ '^-?[0-9]+(\.[0-9]+)?$'
     THEN nr_rpk::DOUBLE PRECISION ELSE NULL END,

CASE WHEN NULLIF(nr_carga_paga_km,'') ~ '^-?[0-9]+(\.[0-9]+)?$'
     THEN nr_carga_paga_km::DOUBLE PRECISION ELSE NULL END,

CASE WHEN NULLIF(nr_carga_gratis_km,'') ~ '^-?[0-9]+(\.[0-9]+)?$'
     THEN nr_carga_gratis_km::DOUBLE PRECISION ELSE NULL END

FROM staging_flights;