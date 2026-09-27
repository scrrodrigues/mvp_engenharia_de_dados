# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 - Gold: Modelagem e tabelas analíticas
# MAGIC
# MAGIC Objetivo desta camada: cruzar o fato nacional e o fato geográfico com as
# MAGIC dimensões, calcular a sinistralidade e montar tabelas já prontas para
# MAGIC responder a pergunta principal e as 3 perguntas secundárias do MVP.
# MAGIC
# MAGIC **Pergunta principal:** Como a arrecadação e a sinistralidade dos
# MAGIC diferentes segmentos de seguros evoluíram ao longo do tempo no Brasil?
# MAGIC
# MAGIC **Perguntas secundárias:**
# MAGIC 1. Quais segmentos (ramos) apresentam maior volume de prêmios?
# MAGIC 2. Como a sinistralidade evoluiu ao longo do período analisado?
# MAGIC 3. Quais segmentos tiveram crescimento de prêmios acompanhado de
# MAGIC    aumento ou redução da sinistralidade?
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md ### Parâmetros

# COMMAND ----------

CATALOG = "mvp_susep_ses"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")

silver_seguros = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_ses_seguros")
silver_uf = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_ses_uf")
silver_cias = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_ses_cias")
silver_ramos = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_ses_ramos")
silver_grupos_economicos = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_ses_grupos_economicos")
silver_gruposramos = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.silver_ses_gruposramos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ajuste de Nomenclatura

# COMMAND ----------

# Dicionário de tradução: nome técnico (fonte SES) -> nome amigável
NOMES_AMIGAVEIS = {
    "coenti": "codigo_empresa",
    "noenti": "nome_empresa",
    "coramo": "codigo_ramo",
    "noramo": "nome_ramo",
    "cogrupo": "codigo_grupo_economico",
    "nogrupo": "nome_grupo_economico",
    "gracodigo": "codigo_grupo_ramo",
    "granome": "nome_grupo_ramo",
}

def renomear_colunas_amigaveis(df):
    for nome_antigo, nome_novo in NOMES_AMIGAVEIS.items():
        if nome_antigo in df.columns:
            df = df.withColumnRenamed(nome_antigo, nome_novo)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dimensões

# COMMAND ----------

# MAGIC %md ### 1.1 dim_tempo
# MAGIC Construída a partir das datas presentes nos dois fatos (nacional + UF),
# MAGIC para garantir cobertura completa do período em ambas as análises.

# COMMAND ----------

dim_tempo = (
    silver_seguros.select("data_referencia")
    .union(silver_uf.select("data_referencia"))
    .distinct()
    .withColumn("ano", F.year("data_referencia"))
    .withColumn("mes", F.month("data_referencia"))
    .withColumn("trimestre", F.quarter("data_referencia"))
    .withColumn("id_tempo", F.date_format("data_referencia", "yyyyMM").cast("int"))
    .select("id_tempo", "data_referencia", "ano", "mes", "trimestre")
)

dim_tempo.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_tempo")

# COMMAND ----------

# MAGIC %md ### 1.2 dim_empresa

# COMMAND ----------

dim_empresa = silver_cias.select("coenti", "noenti")

dim_empresa_gold = renomear_colunas_amigaveis(dim_empresa)
dim_empresa_gold.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_empresa")

# COMMAND ----------

# MAGIC %md ### 1.3 dim_ramo (segmento)

# COMMAND ----------

dim_ramo = silver_ramos.select("coramo", "noramo")

dim_ramo_gold = renomear_colunas_amigaveis(dim_ramo)
dim_ramo_gold.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_ramo")

# COMMAND ----------

# MAGIC %md ### 1.4 dim_grupo_ramo (grupamento de ramos — complexidade adicional)

# COMMAND ----------

dim_grupo_ramo = silver_gruposramos.select("gracodigo", "granome").dropDuplicates(["gracodigo"])
 
dim_grupo_ramo_gold = renomear_colunas_amigaveis(dim_grupo_ramo)
dim_grupo_ramo_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_grupo_ramo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fato nacional: `fato_premios_sinistros`
# MAGIC
# MAGIC Grão: empresa x ramo x mês. Junta o fato limpo com a dimensão histórica
# MAGIC de grupo econômico (por `coenti` + `data_referencia` e calcula a sinistralidade.
# MAGIC
# MAGIC Sinistralidade = Sinistro Retido / Prêmio Ganho (visão líquida, depois
# MAGIC de resseguro — é a métrica usada nos relatórios oficiais da SUSEP).

# COMMAND ----------

fato_premios_sinistros = (
    silver_seguros.drop("cogrupo")
    .join(
        silver_grupos_economicos.select("coenti", "data_referencia", "cogrupo", "nogrupo"),
        on=["coenti", "data_referencia"],
        how="left",
    )
    .withColumn("id_tempo", F.date_format("data_referencia", "yyyyMM").cast("int"))
    .withColumn(
        "sinistralidade",
        F.when(F.col("premio_ganho") > 0, F.col("sinistro_retido") / F.col("premio_ganho"))
    )
    .select(
        "id_tempo", "data_referencia", "coenti", "coramo", "cogrupo", "nogrupo",
        "premio_direto", "premio_de_seguros", "premio_retido", "premio_ganho",
        "sinistro_direto", "sinistro_retido", "desp_com", "sinistralidade",
    )
)
 
fato_premios_sinistros_gold = renomear_colunas_amigaveis(fato_premios_sinistros)
fato_premios_sinistros_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.fato_premios_sinistros")
 
print(f"fato_premios_sinistros: {fato_premios_sinistros.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fato geográfico: `fato_premios_sinistros_uf`
# MAGIC
# MAGIC Grão: empresa x ramo x UF x mês. Usado para a extensão de análise
# MAGIC geográfica. Sinistralidade aqui é sobre a base de prêmio retido
# MAGIC (`prem_ret_liq`), que é a métrica líquida disponível nesta tabela.

# COMMAND ----------

fato_premios_sinistros_uf = (
    silver_uf
    .withColumn("id_tempo", F.date_format("data_referencia", "yyyyMM").cast("int"))
    .withColumn(
        "sinistralidade_uf",
        F.when(F.col("prem_ret_liq") > 0, F.col("sin_dir") / F.col("prem_ret_liq"))
    )
    .select(
        "id_tempo", "data_referencia", "coenti", "coramo", "uf", "gracodigo",
        "premio_dir", "premio_ret", "sin_dir", "prem_ret_liq",
        "salvados", "recuperacao", "sinistralidade_uf",
    )
)
 
fato_premios_sinistros_uf_gold = renomear_colunas_amigaveis(fato_premios_sinistros_uf)
fato_premios_sinistros_uf_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.fato_premios_sinistros_uf")
 
print(f"fato_premios_sinistros_uf: {fato_premios_sinistros_uf.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tabelas analíticas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Pergunta 2 — Segmentos com maior volume de prêmios

# COMMAND ----------

gold_premios_por_ramo = (
    fato_premios_sinistros
    .join(dim_ramo, "coramo")
    .groupBy("coramo", "noramo")
    .agg(
        F.sum("premio_direto").alias("premio_direto_total"),
        F.sum("premio_ganho").alias("premio_ganho_total"),
    )
    .orderBy(F.col("premio_direto_total").desc())
)
 
gold_premios_por_ramo_gold = renomear_colunas_amigaveis(gold_premios_por_ramo)
gold_premios_por_ramo_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.gold_premios_por_ramo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Pergunta 3 — Evolução da sinistralidade no período

# COMMAND ----------

gold_sinistralidade_por_periodo = (
    fato_premios_sinistros
    .join(dim_tempo, "id_tempo")
    .groupBy("ano", "mes")
    .agg(
        F.sum("sinistro_retido").alias("sinistro_retido_total"),
        F.sum("premio_ganho").alias("premio_ganho_total"),
    )
    .withColumn(
        "sinistralidade_media",
        F.col("sinistro_retido_total") / F.col("premio_ganho_total"),
    )
    .orderBy("ano", "mes")
)
 
gold_sinistralidade_por_periodo.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.gold_sinistralidade_por_periodo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Pergunta 3 — Cruzamento: crescimento de prêmio x variação de sinistralidade
# MAGIC
# MAGIC Compara o primeiro e o último ano do período escopado (2022+) por ramo,
# MAGIC calculando a variação percentual de prêmio e a variação (em pontos) da
# MAGIC sinistralidade, para classificar cada segmento.

# COMMAND ----------

premios_anuais_por_ramo = (
    fato_premios_sinistros
    .join(dim_tempo, "id_tempo")
    .groupBy("coramo", "ano")
    .agg(
        F.sum("premio_direto").alias("premio_direto_ano"),
        F.sum("sinistro_retido").alias("sinistro_retido_ano"),
        F.sum("premio_ganho").alias("premio_ganho_ano"),
    )
    .withColumn(
        "sinistralidade_ano",
        F.when(F.col("premio_ganho_ano") > 0, F.col("sinistro_retido_ano") / F.col("premio_ganho_ano")),
    )
)
 
janela_ramo = Window.partitionBy("coramo").orderBy("ano")
 
primeiro_ultimo_ano = (
    premios_anuais_por_ramo
    .withColumn("primeiro_ano", F.first("ano").over(janela_ramo))
    .withColumn(
        "ultimo_ano",
        F.last("ano").over(janela_ramo.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
    )
    .filter((F.col("ano") == F.col("primeiro_ano")) | (F.col("ano") == F.col("ultimo_ano")))
)
 
pivot_ramo = (
    primeiro_ultimo_ano
    .withColumn(
        "posicao",
        F.when(F.col("ano") == F.col("primeiro_ano"), F.lit("inicio")).otherwise(F.lit("fim")),
    )
    .groupBy("coramo")
    .pivot("posicao", ["inicio", "fim"])
    .agg(
        F.first("premio_direto_ano").alias("premio"),
        F.first("sinistralidade_ano").alias("sinistralidade"),
    )
)
 
gold_cruzamento_premio_sinistralidade = (
    pivot_ramo
    .join(dim_ramo, "coramo")
    .withColumn(
        "variacao_premio_pct",
        F.when(F.col("inicio_premio") != 0, (F.col("fim_premio") - F.col("inicio_premio")) / F.col("inicio_premio") * 100),
    )
    .withColumn(
        "variacao_sinistralidade_pp",
        (F.col("fim_sinistralidade") - F.col("inicio_sinistralidade")) * 100,
    )
    .withColumn(
        "classificacao",
        F.when((F.col("variacao_premio_pct") > 0) & (F.col("variacao_sinistralidade_pp") > 0),
               F.lit("Cresceu prêmio, piorou sinistralidade"))
        .when((F.col("variacao_premio_pct") > 0) & (F.col("variacao_sinistralidade_pp") <= 0),
              F.lit("Cresceu prêmio, melhorou sinistralidade"))
        .when((F.col("variacao_premio_pct") <= 0) & (F.col("variacao_sinistralidade_pp") > 0),
              F.lit("Caiu prêmio, piorou sinistralidade"))
        .otherwise(F.lit("Caiu prêmio, melhorou sinistralidade")),
    )
    .select(
        "coramo", "noramo",
        "inicio_premio", "fim_premio", "variacao_premio_pct",
        "inicio_sinistralidade", "fim_sinistralidade", "variacao_sinistralidade_pp",
        "classificacao",
    )
    .orderBy(F.col("variacao_premio_pct").desc())
)
 
gold_cruzamento_premio_sinistralidade_gold = renomear_colunas_amigaveis(gold_cruzamento_premio_sinistralidade)
gold_cruzamento_premio_sinistralidade_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.gold_cruzamento_premio_sinistralidade")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Extensão — Concentração por grupo econômico

# COMMAND ----------

gold_concentracao_grupo_economico = (
    fato_premios_sinistros
    .filter(F.col("cogrupo").isNotNull())
    .groupBy("cogrupo", "nogrupo")
    .agg(F.sum("premio_direto").alias("premio_direto_total"))
)
 
total_mercado = gold_concentracao_grupo_economico.agg(
    F.sum("premio_direto_total")
).collect()[0][0]
 
gold_concentracao_grupo_economico = (
    gold_concentracao_grupo_economico
    .withColumn("participacao_pct", F.col("premio_direto_total") / F.lit(total_mercado) * 100)
    .orderBy(F.col("premio_direto_total").desc())
)
 
gold_concentracao_grupo_economico_gold = renomear_colunas_amigaveis(gold_concentracao_grupo_economico)
gold_concentracao_grupo_economico_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.gold_concentracao_grupo_economico")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Extensão — Análise geográfica (UF)

# COMMAND ----------

gold_premios_sinistros_por_uf = (
    fato_premios_sinistros_uf
    .groupBy("uf")
    .agg(
        F.sum("premio_dir").alias("premio_direto_total"),
        F.sum("sin_dir").alias("sinistro_direto_total"),
        F.sum("prem_ret_liq").alias("premio_retido_liquido_total"),
    )
    .withColumn(
        "sinistralidade_uf",
        F.col("sinistro_direto_total") / F.col("premio_retido_liquido_total"),
    )
    .orderBy(F.col("premio_direto_total").desc())
)
 
gold_premios_sinistros_por_uf.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.gold_premios_sinistros_por_uf")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Conferência da Camada Gold

# COMMAND ----------

for tabela in [
    "dim_tempo", "dim_empresa", "dim_ramo", "dim_grupo_ramo",
    "fato_premios_sinistros", "fato_premios_sinistros_uf",
    "gold_premios_por_ramo", "gold_sinistralidade_por_periodo",
    "gold_cruzamento_premio_sinistralidade",
    "gold_concentracao_grupo_economico", "gold_premios_sinistros_por_uf",
]:
    tabela_spark = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.{tabela}")
    print(f"{tabela:45s} | {tabela_spark.count():>8} linhas")