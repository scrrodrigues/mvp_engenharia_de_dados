# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Qualidade de Dados
# MAGIC
# MAGIC Checagem formal e consolidada dos 5 pilares pedidos no MVP:
# MAGIC **completude, consistência, unicidade, acurácia e outliers** — aplicada
# MAGIC às tabelas finais (Silver/Gold), para gerar as evidências que vão no
# MAGIC item "Qualidade de Dados (Etapa 4.5)" do documento de entrega.
# MAGIC
# MAGIC Cada checagem imprime um resumo pronto para print/screenshot.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# COMMAND ----------

CATALOG = "mvp_susep_ses"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

spark.sql(f"USE CATALOG {CATALOG}")

fato_nacional = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_premios_sinistros")
fato_uf = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_premios_sinistros_uf")
dim_ramo = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_ramo")
dim_empresa = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_empresa")

# COMMAND ----------

# MAGIC %md ## 1. Completude
# MAGIC Percentual de nulos por coluna. Colunas de métrica financeira devem
# MAGIC estar em 0% depois da limpeza feita na Silver.

# COMMAND ----------

def checar_completude(df: DataFrame, nome_tabela: str):
    total = df.count()
    print(f"\n=== Completude: {nome_tabela} ({total} linhas) ===")
    resultado = df.select([
        (F.count(F.when(F.col(c).isNull(), c)) / F.lit(total) * 100).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    for coluna, pct_nulo in resultado.items():
        marcador = "OK" if pct_nulo == 0 else "ATENÇÃO"
        print(f"  [{marcador:7s}] {coluna:25s} {pct_nulo:.2f}% nulos")

checar_completude(fato_nacional, "fato_premios_sinistros")
checar_completude(fato_uf, "fato_premios_sinistros_uf")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consistência
# MAGIC Os valores de `uf` devem pertencer ao conjunto oficial de 27 siglas do
# MAGIC Brasil; os valores de `coramo` no fato devem existir na `dim_ramo`.

# COMMAND ----------

UFS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
]

ufs_invalidas = (
    fato_uf.select("uf").distinct()
    .filter(~F.col("uf").isin(UFS_VALIDAS))
)

qtd_ufs_invalidas = ufs_invalidas.count()
print(f"\n=== Consistência: UF ===")
print(f"Siglas de UF fora do padrão oficial (27 estados + DF): {qtd_ufs_invalidas}")
if qtd_ufs_invalidas > 0:
    ufs_invalidas.show(30, truncate=False)

# COMMAND ----------

ramos_no_fato_sem_dim = (
    fato_nacional.select("coramo").distinct()
    .join(dim_ramo.select("coramo"), "coramo", "left_anti")
)

print(f"\n=== Consistência: código de ramo ===")
print(f"Ramos usados no fato sem correspondência na dim_ramo: {ramos_no_fato_sem_dim.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unicidade
# MAGIC O grão do fato nacional é empresa x ramo x mês — não pode haver duas
# MAGIC linhas para a mesma combinação.

# COMMAND ----------

def checar_unicidade(df: DataFrame, nome_tabela: str, colunas_chave: list):
    total = df.count()
    distintas = df.dropDuplicates(colunas_chave).count()
    duplicadas = total - distintas
    print(f"\n=== Unicidade: {nome_tabela} ===")
    print(f"Chave: {colunas_chave}")
    print(f"Linhas totais: {total} | Linhas duplicadas na chave: {duplicadas}")

checar_unicidade(fato_nacional, "fato_premios_sinistros", ["coenti", "coramo", "data_referencia"])
checar_unicidade(fato_uf, "fato_premios_sinistros_uf", ["coenti", "coramo", "uf", "data_referencia"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Acurácia
# MAGIC Valores que não fazem sentido de negócio: prêmio negativo, sinistralidade
# MAGIC fora de uma faixa plausível (ex: negativa, ou acima de 500% — que indicaria
# MAGIC problema de dado, não apenas um mês ruim para a seguradora).

# COMMAND ----------

premios_negativos = fato_nacional.filter(F.col("premio_direto") < 0).count()
sinistralidade_negativa = fato_nacional.filter(F.col("sinistralidade") < 0).count()
sinistralidade_extrema = fato_nacional.filter(F.col("sinistralidade") > 5).count()

print(f"\n=== Acurácia ===")
print(f"Linhas com premio_direto negativo: {premios_negativos}")
print(f"Linhas com sinistralidade negativa: {sinistralidade_negativa}")
print(f"Linhas com sinistralidade > 500% (possível problema de dado): {sinistralidade_extrema}")

if sinistralidade_extrema > 0:
    (fato_nacional
     .filter(F.col("sinistralidade") > 5)
     .join(dim_ramo, "coramo")
     .select("coenti", "noramo", "data_referencia", "premio_ganho", "sinistro_retido", "sinistralidade")
     .orderBy(F.col("sinistralidade").desc())
     .show(20, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Outliers
# MAGIC Detecção de valores extremos de `premio_direto` por ramo usando o método
# MAGIC IQR (intervalo interquartil): valores fora de `[Q1 - 1.5*IQR, Q3 + 1.5*IQR]`
# MAGIC são sinalizados para revisão (não removidos automaticamente — o objetivo
# MAGIC é dar visibilidade, não descartar dado legítimo de grandes seguradoras).

# COMMAND ----------

quantis = (
    fato_nacional
    .filter(F.col("premio_direto") > 0)
    .approxQuantile("premio_direto", [0.25, 0.75], 0.01)
)
q1, q3 = quantis[0], quantis[1]
iqr = q3 - q1
limite_inferior = q1 - 1.5 * iqr
limite_superior = q3 + 1.5 * iqr

outliers = fato_nacional.filter(
    (F.col("premio_direto") < limite_inferior) | (F.col("premio_direto") > limite_superior)
)

print(f"\n=== Outliers: premio_direto ===")
print(f"Q1={q1:,.2f} | Q3={q3:,.2f} | IQR={iqr:,.2f}")
print(f"Faixa aceitável: [{limite_inferior:,.2f}, {limite_superior:,.2f}]")
print(f"Linhas fora da faixa: {outliers.count()} de {fato_nacional.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observação sobre os outliers de prêmio
# MAGIC Numa base como a SES, é esperado ter outliers legítimos: grandes
# MAGIC seguradoras (ex: líderes de mercado em Auto ou Vida) naturalmente têm
# MAGIC prêmio muito acima da mediana. Por isso, aqui os outliers **não são
# MAGIC removidos** — apenas documentados. Descreva no documento final se, ao
# MAGIC inspecionar a lista, os valores fazem sentido de negócio ou indicam
# MAGIC erro de dado.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Próximo passo
# MAGIC Seguir para `05_analise_perguntas.py`, onde as tabelas Gold são
# MAGIC consultadas para responder a pergunta principal e as 3 perguntas
# MAGIC secundárias do objetivo, com gráficos de apoio.