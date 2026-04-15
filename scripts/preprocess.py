import pandas as pd
import numpy as np
import os

INPUT_FILE = os.path.join("data", "anac_brazil.csv")
OUTPUT_FILE = os.path.join("data", "anac_sample.csv")

# Define required columns only
COLUMNS = [
    "id_basica",
    "sg_empresa_icao",
    "dt_partida_real",
    "cd_di",
    "ds_grupo_di",
    "cd_tipo_linha",

    "sg_icao_origem",
    "nm_municipio_origem",
    "sg_uf_origem",
    "nm_pais_origem",
    "nm_continente_origem",

    "sg_icao_destino",
    "nm_municipio_destino",
    "sg_uf_destino",
    "nm_pais_destino",
    "nm_continente_destino",

    "km_distancia",
    "nr_passag_pagos",
    "nr_passag_gratis",
    "nr_rpk",
    "nr_ask",
    "nr_carga_paga_km",
    "nr_carga_gratis_km"
]

TARGET_SAMPLE_SIZE = 5_000_000
CHUNK_SIZE = 1_000_000

buffer = []  # temporary sampled rows

rng = np.random.default_rng(42)

print("Starting chunked sampling")

for chunk in pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE, low_memory=False):
    # keep only needed columns (ignore missing safely)
    chunk = chunk[[c for c in COLUMNS if c in chunk.columns]]

    # random subset from chunk (proportional sampling)
    sample_fraction = TARGET_SAMPLE_SIZE / 20_000_000 

    sample_size = max(1, int(len(chunk) * sample_fraction))

    sampled_chunk = chunk.sample(n=min(sample_size, len(chunk)), random_state=42)

    buffer.append(sampled_chunk)
    
print("Creating buffer df_samle")7

df_sample = pd.concat(buffer, ignore_index=True)

if len(df_sample) > TARGET_SAMPLE_SIZE:
    df_sample = df_sample.sample(n=TARGET_SAMPLE_SIZE, random_state=42)

df_sample.to_csv(OUTPUT_FILE, index=False)

print(f"Saved {len(df_sample)} rows to {OUTPUT_FILE}")