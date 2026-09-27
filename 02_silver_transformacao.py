# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 - Silver: Limpeza e padronização
# MAGIC
# MAGIC Objetivo desta camada: pegar as tabelas Bronze (dado bruto, sem alteração)
# MAGIC e aplicar tipagem correta, remoção de duplicatas, tratamento de nulos e
# MAGIC padronização de texto.
# MAGIC
# MAGIC Nomes de coluna conforme a documentação oficial do SES ([Documentacao das tabelas](https://www2.susep.gov.br/menuestatistica/ses/principal.aspx))
# MAGIC
# MAGIC Escopo temporal do MVP: **apenas dados de 2022 em diante**.
# MAGIC
# MAGIC Entrada (Bronze) -> Saída (Silver):
# MAGIC - `bronze_ses_seguros`           -> `silver_ses_seguros` (fato principal, nacional)
# MAGIC - `bronze_ses_cias`              -> `silver_ses_cias` (dimensão empresa)
# MAGIC - `bronze_ses_ramos`             -> `silver_ses_ramos` (dimensão ramo)
# MAGIC - `bronze_ses_grupos_economicos` -> `silver_ses_grupos_economicos` (dimensão histórica de grupo econômico)
# MAGIC - `bronze_ses_uf`                -> `silver_ses_uf` (fato secundário, geográfico)
# MAGIC - `bronze_ses_gruposramos`       -> `silver_ses_gruposramos` (dimensão grupo de ramos)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md ### Parâmetros

# COMMAND ----------

CATALOG = "mvp_susep_ses"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
DATA_INICIO_ESCOPO = "2022-01-01"  # escopo do MVP: 2022 em diante

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_SILVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Função auxiliares

# COMMAND ----------

def normalizar_codigo(nome_coluna: str):
    return F.col(nome_coluna).cast("double").cast("long").cast("string")

# COMMAND ----------

def checar_qualidade(df: DataFrame, nome_tabela: str, colunas_chave: list):
    total_linhas = df.count()
    duplicadas = total_linhas - df.dropDuplicates(colunas_chave).count()
    nulos_por_coluna = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    ).collect()[0].asDict()
    nulos_relevantes = {k: v for k, v in nulos_por_coluna.items() if v > 0}

    print(f"\n--- Qualidade: {nome_tabela} ---")
    print(f"Total de linhas: {total_linhas}")
    print(f"Linhas duplicadas (por {colunas_chave}): {duplicadas}")
    print(f"Colunas com nulos: {nulos_relevantes if nulos_relevantes else 'nenhuma'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fato principal: `Ses_seguros` (nacional)
# MAGIC
# MAGIC Colunas de origem: `damesano`, `coenti`, `cogrupo`, `coramo`,
# MAGIC `premio_direto`, `premio_de_seguros`, `premio_retido`, `premio_ganho`,
# MAGIC `sinistro_direto`, `sinistro_retido`, `desp_com`.

# COMMAND ----------

df_bronze_seguros = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_ses_seguros")

checar_qualidade(df_bronze_seguros, "bronze_ses_seguros",
                  colunas_chave=["coenti", "coramo", "damesano"])

df_silver_seguros = (
    df_bronze_seguros
    # tipagem das chaves (string evita problema de join por tipo divergente)
    .withColumn("coenti", normalizar_codigo("coenti"))
    .withColumn("coramo", normalizar_codigo("coramo"))
    .withColumn("cogrupo", normalizar_codigo("cogrupo"))
    # damesano vem como AAAAMM (ex: 202201) -> converte para data (primeiro dia do mês)
    .withColumn("data_referencia", F.to_date(F.col("damesano").cast("string"), "yyyyMM"))
    # tipagem das métricas financeiras
    .withColumn("premio_direto", F.regexp_replace(F.col("premio_direto"), ",", ".").cast("double"))
    .withColumn("premio_de_seguros", F.regexp_replace(F.col("premio_de_seguros"), ",", ".").cast("double"))
    .withColumn("premio_retido", F.regexp_replace(F.col("premio_retido"), ",", ".").cast("double"))
    .withColumn("premio_ganho", F.regexp_replace(F.col("premio_ganho"), ",", ".").cast("double"))
    .withColumn("sinistro_direto", F.regexp_replace(F.col("sinistro_direto"), ",", ".").cast("double"))
    .withColumn("sinistro_retido", F.regexp_replace(F.col("sinistro_retido"), ",", ".").cast("double"))
    .withColumn("desp_com", F.regexp_replace(F.col("desp_com"), ",", ".").cast("double"))
    # escopo temporal do MVP
    .filter(F.col("data_referencia") >= F.lit(DATA_INICIO_ESCOPO))
    # remoção de duplicatas pela chave natural do fato
    .dropDuplicates(["coenti", "coramo", "data_referencia"])
    # completude: sem prêmio ganho não dá pra calcular sinistralidade depois
    .filter(F.col("premio_ganho").isNotNull())
    .filter(F.col("data_referencia").isNotNull())
    # acurácia: descarta prêmio negativo (não faz sentido de negócio)
    .filter(F.col("premio_direto") >= 0)
    .drop("_data_ingestao", "_arquivo_origem")
    .withColumn("_data_processamento", F.current_timestamp())
)

checar_qualidade(df_silver_seguros, "silver_ses_seguros (pós-limpeza)",
                  colunas_chave=["coenti", "coramo", "data_referencia"])

df_silver_seguros.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.silver_ses_seguros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fato secundário: `SES_UF2` (geográfico)
# MAGIC
# MAGIC Colunas de origem: `coenti`, `damesano`, `ramos`, `UF`, `premio_dir`,
# MAGIC `premio_ret`, `sin_dir`, `prem_ret_liq`, `gracodigo`, `salvados`, `recuperacao`.

# COMMAND ----------

df_bronze_uf = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_ses_uf")

checar_qualidade(df_bronze_uf, "bronze_ses_uf",
                  colunas_chave=["coenti", "ramos", "UF", "damesano"])

df_silver_uf = (
    df_bronze_uf
    .withColumnRenamed("ramos", "coramo")
    .withColumnRenamed("UF", "uf")
    .withColumn("coenti", normalizar_codigo("coenti"))
    .withColumn("coramo", normalizar_codigo("coramo"))
    .withColumn("gracodigo", normalizar_codigo("gracodigo"))
    .withColumn("uf", F.trim(F.upper(F.col("uf"))))
    .withColumn("data_referencia", F.to_date(F.col("damesano").cast("string"), "yyyyMM"))
    .withColumn("premio_dir", F.regexp_replace(F.col("premio_dir"), ",", ".").cast("double"))
    .withColumn("premio_ret", F.regexp_replace(F.col("premio_ret"), ",", ".").cast("double"))
    .withColumn("sin_dir", F.regexp_replace(F.col("sin_dir"), ",", ".").cast("double"))
    .withColumn("prem_ret_liq", F.regexp_replace(F.col("prem_ret_liq"), ",", ".").cast("double"))
    .withColumn("salvados", F.regexp_replace(F.col("salvados"), ",", ".").cast("double"))
    .withColumn("recuperacao", F.regexp_replace(F.col("recuperacao"), ",", ".").cast("double"))
    .filter(F.col("data_referencia") >= F.lit(DATA_INICIO_ESCOPO))
    .dropDuplicates(["coenti", "coramo", "uf", "data_referencia"])
    .filter(F.col("uf").isNotNull())
    .filter(F.col("data_referencia").isNotNull())
    .drop("_data_ingestao", "_arquivo_origem")
    .withColumn("_data_processamento", F.current_timestamp())
)

checar_qualidade(df_silver_uf, "silver_ses_uf (pós-limpeza)",
                  colunas_chave=["coenti", "coramo", "uf", "data_referencia"])

df_silver_uf.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.silver_ses_uf")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dimensões simples

# COMMAND ----------

def limpar_dimensao_simples(nome_tabela_bronze: str, nome_tabela_silver: str,
                             mapa_colunas: dict, coluna_chave: str) -> DataFrame:
    """
    Lê uma tabela Bronze de dimensão com chave estática, renomeia colunas
    conforme mapa_colunas, padroniza texto, remove duplicatas pela
    coluna_chave e grava como Silver.
    """
    df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{nome_tabela_bronze}")

    checar_qualidade(df, nome_tabela_bronze, colunas_chave=[coluna_chave])

    for col_origem, col_destino in mapa_colunas.items():
        df = df.withColumnRenamed(col_origem, col_destino)

    colunas_texto = [c for c, t in df.dtypes if t == "string" and c != coluna_chave]
    for c in colunas_texto:
        df = df.withColumn(c, F.trim(F.upper(F.col(c))))

    df_silver = (
        df
        .withColumn(coluna_chave, normalizar_codigo(coluna_chave))
        .dropDuplicates([coluna_chave])
        .filter(F.col(coluna_chave).isNotNull())
        .drop("_data_ingestao", "_arquivo_origem")
        .withColumn("_data_processamento", F.current_timestamp())
    )
 
    checar_qualidade(df_silver, f"{nome_tabela_silver} (pós-limpeza)", colunas_chave=[coluna_chave])
 
    df_silver.write.format("delta").mode("overwrite") \
        .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.{nome_tabela_silver}")
 
    return df_silver

# COMMAND ----------

# 3.1 Ramos: coramo (PK), noramo
df_silver_ramos = limpar_dimensao_simples(
    nome_tabela_bronze="bronze_ses_ramos",
    nome_tabela_silver="silver_ses_ramos",
    mapa_colunas={"coramo": "coramo", "noramo": "noramo"},
    coluna_chave="coramo",
)

# COMMAND ----------

# 3.2 Grupos de ramos: GRAID (PK), GRANOME, GRACODIGO
df_silver_gruposramos = limpar_dimensao_simples(
    nome_tabela_bronze="bronze_ses_gruposramos",
    nome_tabela_silver="silver_ses_gruposramos",
    mapa_colunas={"GRAID": "graid", "GRANOME": "granome", "GRACODIGO": "gracodigo"},
    coluna_chave="graid",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dimensão empresa: `Ses_cias`
# MAGIC
# MAGIC Colunas: `coenti` (PK), `noenti`, `cogrupo`, `nogrupo`.

# COMMAND ----------

df_bronze_cias = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_ses_cias")

checar_qualidade(df_bronze_cias, "bronze_ses_cias", colunas_chave=["coenti"])

df_silver_cias = (
    df_bronze_cias
    .withColumn("coenti", normalizar_codigo("coenti"))
    .withColumn("noenti", F.trim(F.upper(F.col("noenti"))))
    .select("coenti", "noenti")  # descarta cogrupo/nogrupo não confiáveis desta tabela
    .dropDuplicates(["coenti"])
    .filter(F.col("coenti").isNotNull())
    .withColumn("_data_processamento", F.current_timestamp())
)

checar_qualidade(df_silver_cias, "silver_ses_cias (pós-limpeza)", colunas_chave=["coenti"])

df_silver_cias.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.silver_ses_cias")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Dimensão histórica: `Ses_grupos_economicos`
# MAGIC
# MAGIC Colunas: `damesano`, `coenti`, `noenti`, `cogrupo`, `nogrupo`.

# COMMAND ----------

df_bronze_grupos_economicos = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.bronze_ses_grupos_economicos")

checar_qualidade(df_bronze_grupos_economicos, "bronze_ses_grupos_economicos",
                  colunas_chave=["coenti", "damesano"])

df_silver_grupos_economicos = (
    df_bronze_grupos_economicos
    .withColumn("coenti", normalizar_codigo("coenti"))
    .withColumn("cogrupo", normalizar_codigo("cogrupo"))
    .withColumn("noenti", F.trim(F.upper(F.col("noenti"))))
    .withColumn("nogrupo", F.trim(F.upper(F.col("nogrupo"))))
    .withColumn("data_referencia", F.try_to_date(F.col("damesano").cast("string"), "yyyyMM"))
    .filter(F.col("data_referencia") >= F.lit(DATA_INICIO_ESCOPO))
    .dropDuplicates(["coenti", "data_referencia"])
    .filter(F.col("coenti").isNotNull())
    .filter(F.col("cogrupo").isNotNull())
    .drop("_data_ingestao", "_arquivo_origem")
    .withColumn("_data_processamento", F.current_timestamp())
)

checar_qualidade(df_silver_grupos_economicos, "silver_ses_grupos_economicos (pós-limpeza)",
                  colunas_chave=["coenti", "data_referencia"])

df_silver_grupos_economicos.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.silver_ses_grupos_economicos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checagem de integridade referencial

# COMMAND ----------

ramos_sem_match = (
    df_silver_seguros.select("coramo").distinct()
    .join(df_silver_ramos.select("coramo"), "coramo", "left_anti")
)
cias_sem_match = (
    df_silver_seguros.select("coenti").distinct()
    .join(df_silver_cias.select("coenti"), "coenti", "left_anti")
)
gracodigo_sem_match = (
    df_silver_uf.select("gracodigo").distinct()
    .join(df_silver_gruposramos.select("gracodigo"), "gracodigo", "left_anti")
)

print(f"Códigos de ramo no fato sem correspondência na dimensão: {ramos_sem_match.count()}")
print(f"Códigos de empresa no fato sem correspondência na dimensão: {cias_sem_match.count()}")
print(f"Códigos de grupo de ramo (UF) sem correspondência na dimensão: {gracodigo_sem_match.count()}")

if ramos_sem_match.count() > 0:
    ramos_sem_match.show(20, truncate=False)