COPY fact_flight (
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

    nr_ano_partida_real,
    nr_mes_partida_real
)
FROM STDIN
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    NULL '',
    QUOTE '"',
    ESCAPE '"'
);