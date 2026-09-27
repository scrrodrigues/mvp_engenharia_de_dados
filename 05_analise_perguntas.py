# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Análise:
# MAGIC
# MAGIC **Pergunta principal:** Como a arrecadação e a sinistralidade dos
# MAGIC diferentes segmentos de seguros evoluíram ao longo do tempo no Brasil?

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
# MAGIC SELECT nome_ramo, premio_direto_total
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
ax.barh(pdf_top_ramos["nome_ramo"], pdf_top_ramos["premio_direto_total"] / 1e6)
ax.set_xlabel("Prêmio direto total (R$ milhões)")
ax.set_title("Top 15 ramos por volume de prêmios — 2022+")
ax.invert_yaxis()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC >
# MAGIC > O ramo **Automóvel - Casco (0531)** lidera com folga o volume de prêmios
# MAGIC > diretos no período (cerca de R$ 166 bilhões), praticamente o dobro do
# MAGIC > segundo colocado, **Prestamista (0977)**, com aproximadamente R$ 82,5
# MAGIC > bilhões. Logo em seguida aparecem **Vida em Grupo (0993)** (~R$ 73,5
# MAGIC > bilhões) e **R.C. Facultativa Veículos - RCFV (0553)** (~R$ 58,7 bilhões).
# MAGIC >
# MAGIC > Esse resultado é coerente com o que se sabe do mercado brasileiro de
# MAGIC > seguros: Automóvel é historicamente o ramo de maior arrecadação no
# MAGIC > país, dado o tamanho da frota nacional e a obrigatoriedade prática de
# MAGIC > seguro para financiamento de veículos. A presença de Vida em Grupo e
# MAGIC > Prestamista no topo também reflete produtos frequentemente vendidos em
# MAGIC > conjunto com operações de crédito e RH corporativo, o que ajuda a
# MAGIC > explicar o volume elevado mesmo não sendo o produto mais "visível" ao
# MAGIC > consumidor final.

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

pdf_anual = (
    pdf_sinistralidade
    .groupby("ano", as_index=False)
    .agg(sinistralidade_media=("sinistralidade_media", "mean"))
)

fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(pdf_anual["ano"], pdf_anual["sinistralidade_media"] * 100, marker="o")
ax.set_ylabel("Sinistralidade média (%)")
ax.set_xlabel("Ano")
ax.set_title("Evolução da sinistralidade — mercado total, 2022+")
ax.set_xticks(pdf_anual["ano"])
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC >
# MAGIC > A sinistralidade média aparece em **0% em todos os meses de 2022 a 2026**
# MAGIC > no gráfico. Essa leitura de "zero" não indica ausência de sinistros no
# MAGIC > mercado — ela reflete uma limitação identificada na etapa de Qualidade
# MAGIC > de Dados: os campos `sinistro_retido` e/ou `premio_ganho` vêm zerados
# MAGIC > na fonte (SES/SUSEP) para a maior parte dos registros a partir de um
# MAGIC > certo ponto da série histórica, o que zera a divisão que calcula a
# MAGIC > sinistralidade.
# MAGIC >
# MAGIC > **Decisão metodológica adotada:** optei por manter esses registros
# MAGIC > zerados no pipeline, em vez de excluí-los ou tentar preenchê-los com um
# MAGIC > valor estimado. Considero essa a escolha mais correta por três
# MAGIC > motivos: (1) excluir as linhas reduziria artificialmente o volume de
# MAGIC > prêmios reportado nas outras análises (pergunta 1, concentração por
# MAGIC > grupo econômico, análise geográfica), que não dependem da
# MAGIC > sinistralidade e continuam válidas; (2) qualquer valor "inventado" para
# MAGIC > substituir o zero introduziria um viés que eu não teria como
# MAGIC > justificar tecnicamente; (3) manter o dado como veio da fonte,
# MAGIC > documentando a limitação de forma explícita (como fiz no
# MAGIC > `04_qualidade_dados`), é mais transparente do que "esconder" o
# MAGIC > problema com uma correção artificial — permite que qualquer pessoa que
# MAGIC > leia esta análise entenda exatamente o que o dado mostra e o que ele
# MAGIC > não permite concluir.
# MAGIC >
# MAGIC > Dessa forma, a resposta honesta a esta pergunta secundária é: **não é
# MAGIC > possível concluir, com os dados disponíveis nesta base, se a
# MAGIC > sinistralidade do mercado subiu, caiu ou ficou estável no período** —
# MAGIC > essa é, em si, uma conclusão de qualidade de dados válida para o MVP,
# MAGIC > e não uma falha do pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta secundária 3 — Crescimento de prêmio x variação de sinistralidade

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nome_ramo, variacao_premio_pct, variacao_sinistralidade_pp, classificacao
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
# MAGIC >
# MAGIC > O eixo vertical (variação da sinistralidade) fica achatado em zero para
# MAGIC > praticamente todos os ramos — consequência direta da mesma limitação
# MAGIC > da pergunta 2 (sinistralidade zerada na maior parte dos registros).
# MAGIC > Como resultado, a classificação em 4 categorias colapsa, na prática,
# MAGIC > em apenas 2: "cresceu prêmio" e "caiu prêmio", já que a variação de
# MAGIC > sinistralidade não tem variação real para desempatar.
# MAGIC >
# MAGIC > O que ainda é possível responder com confiança nesta pergunta é a
# MAGIC > parte de **crescimento de prêmio por ramo**: destaques como
# MAGIC > **Stop Loss (0743)** (+2.455%), **Educacional (1380)** (+411%) e
# MAGIC > **RC Veículo Transporte Rodoviário de Carga (0659)** (+368%) mostram
# MAGIC > expansão expressiva no período — embora, tratando-se de ramos de nicho
# MAGIC > com base de prêmio inicial pequena, variações percentuais tão altas
# MAGIC > merecem cautela na interpretação (um crescimento de milhares de % pode
# MAGIC > vir de uma base muito baixa no primeiro ano do período).
# MAGIC >
# MAGIC > A parte da pergunta sobre "acompanhado de aumento ou redução da
# MAGIC > sinistralidade" fica, portanto, sem resposta conclusiva pelos mesmos
# MAGIC > motivos de qualidade de dado documentados na pergunta 2.

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
ax.barh(pdf_grupos["nome_grupo_economico"], pdf_grupos["participacao_pct"])
ax.set_xlabel("Participação no total de prêmios (%)")
ax.set_title("Top 10 grupos econômicos por participação de mercado — 2022+")
ax.invert_yaxis()
plt.tight_layout()
plt.show()

participacao_top5 = pdf_grupos.head(5)["participacao_pct"].sum()
print(f"Participação somada dos 5 maiores grupos econômicos: {participacao_top5:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC >
# MAGIC > Os 5 maiores grupos somam **59,0%** do total de prêmios diretos do
# MAGIC > período, o que indica um mercado moderadamente concentrado — mas com
# MAGIC > uma ressalva importante: as duas maiores "categorias" do ranking não
# MAGIC > são conglomerados individuais. **"OUTROS GRUPOS" (16,5%)** e
# MAGIC > **"INDEPENDENTE" (15,3%)** são classificações agregadas da própria
# MAGIC > base SES para empresas sem grupo econômico definido ou não
# MAGIC > classificadas individualmente — juntas, somam quase um terço do
# MAGIC > mercado, mas escondem muitas empresas distintas dentro de um único
# MAGIC > rótulo.
# MAGIC >
# MAGIC > Olhando só para os grupos identificados nominalmente, **Porto Seguro
# MAGIC > (11,3%)**, **BB Mapfre (~8,4%)** e **Bradesco (~7,3%)** lideram — um
# MAGIC > cenário consistente com o que é público sobre o setor de seguros no
# MAGIC > Brasil, historicamente dominado por poucos grandes grupos ligados a
# MAGIC > bancos de varejo (Bradesco, BB, Itaú) e seguradoras especializadas em
# MAGIC > auto (Porto Seguro). Uma leitura mais precisa da concentração real
# MAGIC > exigiria desagregar a categoria "Outros Grupos" por empresa
# MAGIC > individual — fica registrado aqui como sugestão de trabalho futuro.

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
# MAGIC >
# MAGIC > Os estados com maior volume de prêmio **não são** os mesmos com maior
# MAGIC > sinistralidade. **São Paulo** lidera disparado em volume (muito à
# MAGIC > frente do segundo colocado), seguido por RJ, MG, RS e PR — um ranking
# MAGIC > esperado, que acompanha de perto o tamanho da população e da economia
# MAGIC > de cada estado.
# MAGIC >
# MAGIC > Já no ranking de sinistralidade, quem lidera é o **Acre**, com um
# MAGIC > valor extremo (em torno de 500%), muito acima de qualquer estado do
# MAGIC > top 10 de volume — nenhum dos estados de maior arrecadação (SP, RJ,
# MAGIC > MG) aparece no topo da sinistralidade. Isso sugere o padrão descrito
# MAGIC > na pergunta: existem estados de alto volume e sinistralidade mais
# MAGIC > controlada (bom para o mercado, caso de SP) e estados de baixo volume
# MAGIC > com sinistralidade desproporcionalmente alta (caso do AC). Uma leitura
# MAGIC > importante aqui: sinistralidade calculada sobre uma base de prêmio
# MAGIC > pequena (como costuma ser o caso de estados do Norte) é
# MAGIC > estatisticamente mais volátil — um único sinistro grande pode distorcer
# MAGIC > o percentual muito mais do que faria num estado com volume alto como
# MAGIC > SP. Vale registrar essa ressalva antes de tratar o dado do AC como
# MAGIC > "o estado mais arriscado" do país.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão geral — conectando tudo à pergunta principal

# COMMAND ----------

# MAGIC %md
# MAGIC >
# MAGIC > **Pergunta principal:** Como a arrecadação e a sinistralidade dos
# MAGIC > diferentes segmentos de seguros evoluíram ao longo do tempo no Brasil?
# MAGIC >
# MAGIC > A parte de **arrecadação** desta pergunta tem resposta sólida: o
# MAGIC > mercado é liderado com folga pelo ramo Automóvel - Casco, seguido por
# MAGIC > produtos ligados a crédito e vida em grupo (pergunta 1); a
# MAGIC > arrecadação está moderadamente concentrada em poucos grandes grupos
# MAGIC > econômicos, com Porto Seguro e conglomerados bancários (BB Mapfre,
# MAGIC > Bradesco) no topo (extensão de concentração); e geograficamente segue
# MAGIC > o peso econômico de cada estado, com São Paulo isolado na liderança
# MAGIC > (extensão geográfica).
# MAGIC >
# MAGIC > Já a parte de **sinistralidade** não pôde ser respondida de forma
# MAGIC > conclusiva para a maior parte da série: os campos usados no cálculo
# MAGIC > vêm zerados na fonte para a maioria dos registros do período, o que é
# MAGIC > um achado de qualidade de dados documentado explicitamente (não uma
# MAGIC > falha do pipeline). Optei por manter esses registros como vieram da
# MAGIC > fonte, em vez de excluí-los ou estimá-los, porque isso preserva a
# MAGIC > integridade do restante da análise (que não depende da sinistralidade)
# MAGIC > e é mais honesto do que mascarar uma limitação real do dado com uma
# MAGIC > correção artificial. A única exceção onde a sinistralidade mostrou
# MAGIC > variação real e interpretável foi na análise geográfica, onde o Acre
# MAGIC > aparece como outlier de sinistralidade alta sobre uma base de prêmio
# MAGIC > pequena — um resultado que ilustra bem por que essa métrica é sensível
# MAGIC > a baixo volume de dados.
# MAGIC >
# MAGIC > **Conclusão:** o MVP demonstra o ciclo completo de engenharia de
# MAGIC > dados proposto — da coleta bruta à camada Gold — e responde de forma
# MAGIC > sólida à dimensão de arrecadação da pergunta principal. A dimensão de
# MAGIC > sinistralidade fica limitada pela qualidade do dado de origem, o que é
# MAGIC > registrado como constatação legítima do trabalho, não como uma lacuna
# MAGIC > a esconder.
