# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: Ingestão dos arquivos brutos do SES/SUSEP
# MAGIC
# MAGIC Objetivo desta camada: trazer cada arquivo exatamente como veio da fonte,
# MAGIC sem limpeza ou transformação de conteúdo. Apenas leitura + metadados de controle.
# MAGIC
# MAGIC Tabelas ingeridas:
# MAGIC - `Ses_seguros.csv`            -> fato principal (prêmios, sinistros, despesas)
# MAGIC - `Ses_cias.csv`               -> dimensão empresa
# MAGIC - `Ses_ramos.csv`              -> dimensão ramo
# MAGIC - `Ses_grupos_economicos.csv`  -> dimensão grupo econômico
# MAGIC - `SES_UF2.csv`                -> dimensão UF (geografia)
# MAGIC - `ses_gruposramos.csv`        -> de-para ramo -> grupo de ramos

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# MAGIC %md ### Parâmetros

# COMMAND ----------

CATALOG = "mvp_susep_ses"
SCHEMA_BRONZE = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/arquivos_raw"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mapa dos arquivos a ingerir
# MAGIC Ajuste `delimiter` e `encoding` conforme o formato real de cada arquivo
# MAGIC baixado do portal SES (confira abrindo o .csv bruto antes de rodar).
# MAGIC `header` e `inferSchema` valem para todos; se algum arquivo não tiver
# MAGIC cabeçalho, ajuste esse dicionário.

# COMMAND ----------

arquivos = [
    {
        "nome_arquivo": "Ses_seguros.csv",
        "tabela_bronze": "bronze_ses_seguros",
        "delimiter": ";",
        "encoding": "latin1",
    },
    {
        "nome_arquivo": "Ses_cias.csv",
        "tabela_bronze": "bronze_ses_cias",
        "delimiter": ";",
        "encoding": "latin1",
    },
    {
        "nome_arquivo": "Ses_ramos.csv",
        "tabela_bronze": "bronze_ses_ramos",
        "delimiter": ";",
        "encoding": "latin1",
    },
    {
        "nome_arquivo": "Ses_grupos_economicos.csv",
        "tabela_bronze": "bronze_ses_grupos_economicos",
        "delimiter": ";",
        "encoding": "latin1",
    },
    {
        "nome_arquivo": "SES_UF2.csv",
        "tabela_bronze": "bronze_ses_uf",
        "delimiter": ";",
        "encoding": "latin1",
    },
    {
        "nome_arquivo": "ses_gruposramos.csv",
        "tabela_bronze": "bronze_ses_gruposramos",
        "delimiter": ";",
        "encoding": "latin1",
    },
]

# COMMAND ----------

# MAGIC %md ### Função genérica de ingestão Bronze

# COMMAND ----------

def ingerir_bronze(nome_arquivo: str, tabela_bronze: str, delimiter: str, encoding: str):
    """
    Lê um arquivo bruto do Volume, adiciona metadados de controle
    (data de ingestão e fonte) e grava como tabela Delta na camada Bronze.
    Nenhuma limpeza, renomeação ou filtro de conteúdo é aplicada aqui.
    """
    caminho_arquivo = f"{VOLUME_PATH}/{nome_arquivo}"

    df_raw = (
        spark.read
        .option("header", True)
        .option("delimiter", delimiter)
        .option("encoding", encoding)
        .option("inferSchema", True)
        .csv(caminho_arquivo)
    )

    df_bronze = (
        df_raw
        .withColumn("_data_ingestao", F.current_timestamp())
        .withColumn("_arquivo_origem", F.lit(nome_arquivo))
    )

    nome_completo = f"{CATALOG}.{SCHEMA_BRONZE}.{tabela_bronze}"
    df_bronze.write.format("delta").mode("overwrite").saveAsTable(nome_completo)

    qtd_linhas = df_bronze.count()
    qtd_colunas = len(df_bronze.columns)
    print(f"OK  | {nome_completo:45s} | {qtd_linhas:>10} linhas | {qtd_colunas:>3} colunas")

    return df_bronze

# COMMAND ----------

# MAGIC %md ### Execução da ingestão para todas as tabelas

# COMMAND ----------

resultados = {}

for arq in arquivos:
    try:
        df = ingerir_bronze(
            nome_arquivo=arq["nome_arquivo"],
            tabela_bronze=arq["tabela_bronze"],
            delimiter=arq["delimiter"],
            encoding=arq["encoding"],
        )
        resultados[arq["tabela_bronze"]] = df
    except AnalysisException as e:
        print(f"ERRO | Não encontrei o arquivo '{arq['nome_arquivo']}' no Volume. "
              f"Confirme se ele foi enviado para {VOLUME_PATH}.")
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checagem rápida (evidência para o documento final)
# MAGIC Rode a célula abaixo e capture o print em screenshot para o item
# MAGIC "Carga dos Dados (Etapa 4.2)" do documento de entrega.

# COMMAND ----------

for tabela in [a["tabela_bronze"] for a in arquivos]:
    print(f"\n--- {tabela} ---")
    spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{tabela}").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Próximo passo
# MAGIC Seguir para `02_silver_transformacao.py`, onde:
# MAGIC - `bronze_ses_seguros` será tipado e limpo (fato principal)
# MAGIC - `bronze_ses_cias`, `bronze_ses_ramos`, `bronze_ses_grupos_economicos`,
# MAGIC   `bronze_ses_uf` e `bronze_ses_gruposramos` viram dimensões padronizadas
# MAGIC   para montar o Esquema Estrela na Gold.