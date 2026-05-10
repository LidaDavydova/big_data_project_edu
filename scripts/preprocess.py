"""Preprocess ANAC Brazil dataset and export selected columns."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("ANAC Sampling")
    .master("local[*]")
    .getOrCreate()
)

INPUT_FILE = "data/anac_brazil.csv"
OUTPUT_FILE = "data/anac_brazil_sample.csv"

df = spark.read.csv(INPUT_FILE, header=True, inferSchema=True)

df = (
    df.withColumn("nr_passag_pagos", col("nr_passag_pagos").cast("int"))
    .withColumn("nr_passag_gratis", col("nr_passag_gratis").cast("int"))
    .withColumn("nr_ano_partida_real", col("nr_ano_partida_real").cast("int"))
    .withColumn("nr_mes_partida_real", col("nr_mes_partida_real").cast("int"))
)

COLUMNS = [
    "nr_passag_pagos",
    "nr_passag_gratis",
    "ds_servico_tipo_linha",
    "kg_carga_paga",
    "id_aerodromo_origem",
    "nm_pais_origem",
    "id_aerodromo_destino",
    "nm_pais_destino",
    "ds_grupo_di",
    "ds_natureza_tipo_linha",
    "ds_tipo_empresa",
    "nr_ano_partida_real",
    "nr_mes_partida_real",
]

(
    df.select(*COLUMNS)
    .write
    .option("header", True)
    .mode("overwrite")
    .csv(OUTPUT_FILE)
)
