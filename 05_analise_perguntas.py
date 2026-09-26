# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Análise: respondendo as perguntas do objetivo
# MAGIC
# MAGIC **Pergunta principal:** Como a arrecadação e a sinistralidade dos
# MAGIC diferentes segmentos de seguros evoluíram ao longo do tempo no Brasil?
# MAGIC
# MAGIC Este notebook responde a pergunta principal através das 3 perguntas
# MAGIC secundárias, usando as tabelas Gold já prontas, e traz as extensões
# MAGIC de concentração por grupo econômico e análise geográfica.

# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

CATALOG = "mvp_susep_ses"
SCHEMA_GOLD = "gold"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA_GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta secundária 1 — Quais segmentos têm maior volume de prêmios?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT noramo, premio_direto_total
# MAGIC FROM gold_premios_por_ramo
# MAGIC ORDER BY premio_direto_total DESC
# MAGIC LIMIT 15

# COMMAND ----------

pdf_top_ramos = (
    spark.table("gold_premios_por_ramo")
    .orderBy(F.col("premio_direto_total").desc())
    .limit(15)
    .toPandas()
)

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(pdf_top_ramos["noramo"], pdf_top_ramos["premio_direto_total"] / 1e6)
ax.set_xlabel("Prêmio direto total (R$ milhões)")
ax.set_title("Top 15 ramos por volume de prêmios — 2022+")
ax.invert_yaxis()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão (preencher no documento final):** descreva quais ramos
# MAGIC lideram o volume de prêmios e se isso é coerente com o que se sabe do
# MAGIC mercado brasileiro de seguros (ex: Auto e Vida tendem a concentrar boa
# MAGIC parte da arrecadação).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta secundária 2 — Como a sinistralidade evoluiu no período?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano, mes, sinistralidade_media
# MAGIC FROM gold_sinistralidade_por_periodo
# MAGIC ORDER BY ano, mes

# COMMAND ----------

pdf_sinistralidade = spark.table("gold_sinistralidade_por_periodo").orderBy("ano", "mes").toPandas()
pdf_sinistralidade["periodo"] = pdf_sinistralidade["ano"].astype(str) + "-" + pdf_sinistralidade["mes"].astype(str).str.zfill(2)

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(pdf_sinistralidade["periodo"], pdf_sinistralidade["sinistralidade_media"] * 100, marker="o")
ax.set_ylabel("Sinistralidade média (%)")
ax.set_xlabel("Período")
ax.set_title("Evolução da sinistralidade — mercado total, 2022+")
ax.tick_params(axis="x", rotation=90)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão (preencher no documento final):** a sinistralidade está
# MAGIC subindo, caindo ou estável? Há sazonalidade (ex: picos em determinados
# MAGIC meses do ano)?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta secundária 3 — Crescimento de prêmio x variação de sinistralidade

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT noramo, variacao_premio_pct, variacao_sinistralidade_pp, classificacao
# MAGIC FROM gold_cruzamento_premio_sinistralidade
# MAGIC ORDER BY variacao_premio_pct DESC

# COMMAND ----------

pdf_cruzamento = spark.table("gold_cruzamento_premio_sinistralidade").toPandas()

cores_por_classe = {
    "Cresceu prêmio, piorou sinistralidade": "tab:red",
    "Cresceu prêmio, melhorou sinistralidade": "tab:green",
    "Caiu prêmio, piorou sinistralidade": "tab:orange",
    "Caiu prêmio, melhorou sinistralidade": "tab:blue",
}

fig, ax = plt.subplots(figsize=(9, 7))
for classe, grupo in pdf_cruzamento.groupby("classificacao"):
    ax.scatter(
        grupo["variacao_premio_pct"], grupo["variacao_sinistralidade_pp"],
        label=classe, color=cores_por_classe.get(classe, "gray"), s=60,
    )
ax.axhline(0, color="gray", linewidth=0.8)
ax.axvline(0, color="gray", linewidth=0.8)
ax.set_xlabel("Variação do prêmio direto (%)")
ax.set_ylabel("Variação da sinistralidade (pontos percentuais)")
ax.set_title("Crescimento de prêmio x variação de sinistralidade por ramo")
ax.legend(loc="best", fontsize=8)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão (preencher no documento final):** cite exemplos concretos de
# MAGIC ramos em cada quadrante do gráfico. O quadrante mais interessante do
# MAGIC ponto de vista de negócio costuma ser "cresceu prêmio, melhorou
# MAGIC sinistralidade" (crescimento saudável) versus "cresceu prêmio, piorou
# MAGIC sinistralidade" (crescimento que pode estar vindo com mais risco).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extensão — Concentração por grupo econômico

# COMMAND ----------

pdf_grupos = (
    spark.table("gold_concentracao_grupo_economico")
    .orderBy(F.col("premio_direto_total").desc())
    .limit(10)
    .toPandas()
)

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(pdf_grupos["nogrupo"], pdf_grupos["participacao_pct"])
ax.set_xlabel("Participação no total de prêmios (%)")
ax.set_title("Top 10 grupos econômicos por participação de mercado — 2022+")
ax.invert_yaxis()
plt.tight_layout()
plt.show()

participacao_top5 = pdf_grupos.head(5)["participacao_pct"].sum()
print(f"Participação somada dos 5 maiores grupos econômicos: {participacao_top5:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão (preencher no documento final):** o mercado é concentrado em
# MAGIC poucos grupos ou é pulverizado? Isso é esperado para o setor de seguros
# MAGIC no Brasil?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extensão — Análise geográfica (UF)

# COMMAND ----------

pdf_uf = (
    spark.table("gold_premios_sinistros_por_uf")
    .orderBy(F.col("premio_direto_total").desc())
    .toPandas()
)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

ax1.barh(pdf_uf["uf"].head(10), pdf_uf["premio_direto_total"].head(10) / 1e6)
ax1.set_xlabel("Prêmio direto total (R$ milhões)")
ax1.set_title("Top 10 UFs por volume de prêmio")
ax1.invert_yaxis()

pdf_uf_sinistralidade = pdf_uf.sort_values("sinistralidade_uf", ascending=False).head(10)
ax2.barh(pdf_uf_sinistralidade["uf"], pdf_uf_sinistralidade["sinistralidade_uf"] * 100)
ax2.set_xlabel("Sinistralidade (%)")
ax2.set_title("Top 10 UFs por sinistralidade")
ax2.invert_yaxis()

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Discussão (preencher no documento final):** os estados com maior
# MAGIC volume de prêmio são os mesmos com maior sinistralidade? Ou há estados
# MAGIC de alto volume e baixo risco (bom para o mercado) e vice-versa?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão geral — conectando tudo à pergunta principal
# MAGIC
# MAGIC *(Espaço para você escrever, no notebook e no documento final, a
# MAGIC síntese conectando as 3 perguntas secundárias e as extensões à pergunta
# MAGIC principal: como arrecadação e sinistralidade evoluíram juntas por
# MAGIC segmento, no Brasil, entre 2022 e o fim do período analisado.)*
# MAGIC
# MAGIC Pontos a cobrir:
# MAGIC - Quais segmentos dominam o mercado (pergunta 1) e se são os mesmos que
# MAGIC   mais cresceram no período
# MAGIC - Se a sinistralidade do mercado como um todo melhorou, piorou ou ficou
# MAGIC   estável (pergunta 2)
# MAGIC - Se o crescimento observado veio "com risco" ou "com qualidade"
# MAGIC   (pergunta 3 — cruzamento)
# MAGIC - Como a concentração por grupo econômico e a distribuição geográfica
# MAGIC   ajudam a explicar (ou não) os padrões encontrados

# COMMAND ----------

# MAGIC %md
# MAGIC ### Próximo passo
# MAGIC Com as respostas e os gráficos gerados aqui, monte o `README.md` do
# MAGIC repositório seguindo a estrutura do item 5 do enunciado (Contexto e
# MAGIC Perguntas, Carga dos Dados, Modelagem e Catálogo, Pipeline, Qualidade
# MAGIC de Dados, Análise de Dados, Autoavaliação), incluindo os prints/
# MAGIC screenshots gerados por este e pelos notebooks anteriores.