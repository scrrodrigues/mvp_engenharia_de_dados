# MVP: Pipeline de Dados na Nuvem — Mercado de Seguros no Brasil (SES/SUSEP)

## 1. Explicação geral do projeto

Este projeto é o MVP da disciplina de Engenharia de Dados da pós-graduação em Ciência de Dados e Analytics. O objetivo foi construir, de ponta a ponta, um pipeline de dados funcional na nuvem (Databricks), cobrindo todas as etapas que um Engenheiro de Dados enfrenta no dia a dia: definição do problema, coleta, modelagem, carga (ETL), qualidade de dados e análise final.

O pipeline segue a **Arquitetura Medalhão** (Bronze → Silver → Gold):

- **Bronze**: dado bruto, exatamente como veio da fonte, sem nenhuma alteração.
- **Silver**: dado limpo, tipado corretamente e padronizado.
- **Gold**: dado modelado em Esquema Estrela, agregado e pronto para responder às perguntas do objetivo.

### Pergunta principal

> Como a arrecadação e a sinistralidade dos diferentes segmentos de seguros evoluíram ao longo do tempo no Brasil?

### Perguntas secundárias

1. Quais segmentos apresentam maior volume de prêmios?
2. Como a sinistralidade evoluiu ao longo do período analisado?
3. Quais segmentos apresentaram crescimento de prêmios acompanhado de aumento ou redução da sinistralidade?

### Extensões de escopo

- Concentração por grupo econômico
- Análise geográfica (por UF)
- Grupamento de ramos

---

## 2. Fonte dos dados

Os dados vêm do **SES (Sistema de Estatísticas da SUSEP)**, base pública mantida pela Superintendência de Seguros Privados (SUSEP), órgão regulador do mercado de seguros no Brasil.

- **Portal oficial:** http://www2.susep.gov.br/menuestatistica/SES/principal.aspx
- **Licença de uso:** dados públicos governamentais, de uso livre.
- **Escopo temporal do MVP:** dados a partir de **janeiro de 2022**.
- **Arquivos utilizados:**

| Arquivo de origem | Papel no projeto |
|---|---|
| `Ses_seguros.csv` | Fato principal — prêmios, sinistros e despesas por empresa/ramo/mês |
| `Ses_cias.csv` | Dimensão empresa |
| `Ses_ramos.csv` | Dimensão ramo (segmento de seguro) |
| `Ses_grupos_economicos.csv` | Dimensão histórica de grupo econômico (análise de concentração) |
| `SES_UF2.csv` | Fato secundário — prêmios e sinistros por UF (análise geográfica) |
| `ses_gruposramos.csv` | Dimensão de grupamento de ramos (complexidade adicional) |

---

## 3. Estrutura do projeto

```
mvp-susep-ses/
├── README.md                     
├── notebooks/
│   ├── 01_bronze_ingestao.py
│   ├── 02_silver_transformacao.py
│   ├── 03_gold_modelagem.py
│   ├── 04_qualidade_dados.py
│   └── 05_analise_perguntas.py
└── docs/
    ├── catalogo_bronze.csv
    └── catalogo_gold.csv
```

**Catálogo de dados completo (com linhagem Bronze → Gold, tipos e chaves):**
[`docs/catalogo_gold.csv`](./docs/catalogo_gold.csv)

---

## 4. Carga dos Dados (Etapa 4.2)

### `01_bronze_ingestao.py`

**Função:** ler cada um dos 6 arquivos CSV brutos do Volume do Unity Catalog
e gravar como tabela Delta na camada Bronze, sem nenhuma transformação de
conteúdo — apenas adicionando metadados de controle (`_data_ingestao`,
`_arquivo_origem`).

**Como funciona:** o notebook usa um dicionário de configuração (`arquivos`)
listando nome do arquivo, delimitador e encoding, e uma função genérica
`ingerir_bronze()` que aplica a mesma lógica de leitura para as 6 tabelas.

**Resultado real da ingestão:**

| Tabela Bronze | Linhas | Colunas |
|---|---:|---:|
| `bronze_ses_seguros` | 1.807.405 | 23 |
| `bronze_ses_cias` | 769 | 6 |
| `bronze_ses_ramos` | 161 | 4 |
| `bronze_ses_grupos_economicos` | 66.461 | 7 |
| `bronze_ses_uf` | 9.936.380 | 13 |
| `bronze_ses_gruposramos` | 22 | 5 |

📷 **[ingestão bronze]**

---

## 5. Modelagem e Catálogo de Dados (Etapa 4.3)

### `02_silver_transformacao.py`

**Função:** limpar e padronizar as 6 tabelas Bronze — tipagem correta, remoção de duplicatas, tratamento de nulos, padronização de texto e normalização de códigos numéricos (função `normalizar_codigo`, que resolve inconsistências de tipo entre arquivos, como `1061` vs `1061.0`).

**Decisões de modelagem:**
- `Ses_seguros.csv` é o fato nacional (grão: empresa × ramo × mês).
- `SES_UF2.csv` é tratado como um **fato secundário geográfico** (grão: empresa × ramo × UF × mês), não como dimensão.
- `Ses_grupos_economicos.csv` é uma dimensão **histórica** (chave composta empresa + mês), porque uma empresa pode trocar de grupo econômico ao longo do tempo.
- Escopo temporal aplicado via `DATA_INICIO_ESCOPO = "2022-01-01"`.

**Resultado real da limpeza (Bronze → Silver):**

| Tabela | Linhas Bronze | Linhas Silver (pós-limpeza, 2022+) |
|---|---:|---:|
| `silver_ses_seguros` | 1.807.405 | 283.324 |
| `silver_ses_uf` | 9.936.380 | 2.393.685 |
| `silver_ses_ramos` | 161 | 161 |
| `silver_ses_gruposramos` | 22 | 22 |
| `silver_ses_cias` | 769 | 769 |
| `silver_ses_grupos_economicos` | 66.461 | 12.574 |

📷 **[imagens_qualidade]**

### `03_gold_modelagem.py`

**Função:** cruzar os fatos Silver com as dimensões, calcular a sinistralidade e montar o Esquema Estrela final, já com nomes de coluna amigáveis (dicionário `NOMES_AMIGAVEIS`, aplicado só no momento de gravar cada tabela — as transformações internas continuam usando os nomes técnicos de origem).

**Modelo dimensional resultante:**

| Tabela | Tipo | Linhas |
|---|---|---:|
| `dim_tempo` | Dimensão | 55 |
| `dim_empresa` | Dimensão | 769 |
| `dim_ramo` | Dimensão (segmento) | 161 |
| `dim_grupo_ramo` | Dimensão (complexidade adicional) | 22 |
| `fato_premios_sinistros` | Fato nacional | 283.324 |
| `fato_premios_sinistros_uf` | Fato geográfico | 2.393.685 |
| `gold_premios_por_ramo` | Analítica (Pergunta 1) | 145 |
| `gold_sinistralidade_por_periodo` | Analítica (Pergunta 2) | 55 |
| `gold_cruzamento_premio_sinistralidade` | Analítica (Pergunta 3) | 145 |
| `gold_concentracao_grupo_economico` | Analítica (Extensão) | 32 |
| `gold_premios_sinistros_por_uf` | Analítica (Extensão) | 27 |

**Catálogo de dados completo**, com descrição de cada campo, tipo de dado na Bronze, tipo de dado após conversão na Gold, chave (PK/FK) e a linhagem completa (de qual tabela/coluna Bronze cada coluna Gold se origina): [`docs/catalogo_gold.csv`](./docs/catalogo_gold.csv).

---

## 6. Pipeline de Dados (Etapa 4.4)

O pipeline é executado em 5 notebooks sequenciais, cada um lendo a saída do anterior (`01 → 02 → 03 → 04 → 05`), todos no mesmo catálogo `mvp_susep_ses` do Unity Catalog:

```
01_bronze_ingestao  →  02_silver_transformacao  →  03_gold_modelagem  →  04_qualidade_dados  →  05_analise_perguntas
```

Cada notebook grava suas tabelas em modo `overwrite`, então a sequência pode ser reexecutada do zero a qualquer momento para reprocessar o pipeline inteiro.

---

## 7. Qualidade de Dados (Etapa 4.5)

### `04_qualidade_dados.py`

**Função:** checagem formal dos 5 pilares de qualidade — completude, consistência, unicidade, acurácia e outliers — sobre as tabelas finais da Gold.

**Resultado real das checagens:**

| Pilar | Resultado |
|---|---|
| **Completude** | Todas as colunas de métrica financeira em 0% de nulos. `codigo_grupo_economico`/`nome_grupo_economico` com 0,08% de nulos (empresas sem grupo econômico registrado no mês). `sinistralidade` com 68,3% de nulos e `sinistralidade_uf` com 57,8% — ver achado abaixo. |
| **Consistência (UF)** | 0 siglas fora do padrão oficial (27 estados + DF). |
| **Consistência (grupo de ramo)** | 0 códigos de `codigo_grupo_ramo` fora do conjunto válido no fato geográfico (achado do desenvolvimento, já corrigido). |
| **Unicidade** | 0 duplicatas na chave de grão dos dois fatos. |
| **Acurácia** | 0 linhas com prêmio negativo, 0 com sinistralidade negativa, 0 com sinistralidade > 500%. |
| **Outliers (IQR, `premio_direto`)** | 10.986 linhas fora da faixa `[Q1 - 1,5×IQR, Q3 + 1,5×IQR]` de 283.324 (~3,9%) — considerados legítimos (grandes seguradoras), não removidos. |

### Achado de qualidade: sinistralidade zerada/nula na maior parte da série

Durante o desenvolvimento, identificamos que os campos `sinistro_retido` e/ou `premio_ganho` vêm zerados ou ausentes na fonte SES para boa parte dos registros do período (68,3% das linhas do fato nacional ficam sem sinistralidade calculável). Isso não é uma falha do pipeline — foi confirmado inspecionando os dados brutos diretamente.

**Decisão adotada: manter os registros como vieram da fonte**, em vez de excluí-los ou preenchê-los artificialmente. Os motivos:

1. **Preserva a integridade das outras análises.** As perguntas 1 (volume de prêmios), a extensão de concentração por grupo econômico e a extensão geográfica de volume não dependem da sinistralidade — excluir essas linhas reduziria artificialmente o prêmio total reportado em todo o resto do MVP, sem necessidade.
2. **Evita introduzir viés.** Qualquer valor estimado para substituir o zero/nulo seria uma suposição sem base técnica — mantê-lo como está é mais correto do que "inventar" um número.
3. **É mais transparente.** Documentar a limitação abertamente (aqui e no notebook `04`) permite que qualquer pessoa lendo a análise saiba exatamente o que o dado permite e não permite concluir — em vez de uma correção silenciosa que esconderia o problema.

📷 **[qualidade de dados]**

---

## 8. Análise de Dados (Etapa 4.5)

### `05_analise_perguntas.py`

**Função:** consultar as tabelas Gold analíticas e responder cada pergunta do objetivo com apoio de gráficos (SQL + matplotlib).

### Pergunta secundária 1 — Quais segmentos têm maior volume de prêmios?

**Automóvel - Casco (0531)** lidera com folga (~R$ 166 bilhões), quase o dobro do segundo colocado, **Prestamista (0977)** (~R$ 82,5 bilhões),
seguido por **Vida em Grupo (0993)** e **R.C. Facultativa Veículos (0553)**. Resultado coerente com o mercado brasileiro de seguros, historicamente liderado pelo ramo Automóvel.

📷 **[Espaço para imagem: gráfico "Top 15 ramos por volume de prêmios"]**

### Pergunta secundária 2 — Como a sinistralidade evoluiu no período?

A sinistralidade média aparece em 0% em todos os meses de 2022 a 2026 no gráfico — resultado direto da limitação de qualidade de dados descrita na seção 7 (campos zerados/nulos na fonte para a maioria dos registros). A resposta honesta a esta pergunta é que **não é possível concluir, com os dados disponíveis, se a sinistralidade subiu, caiu ou ficou estável** — essa é, em si, uma conclusão válida de qualidade de dados para o MVP.

📷 **[Espaço para imagem: gráfico "Evolução da sinistralidade — mercado
total"]**

### Pergunta secundária 3 — Crescimento de prêmio x variação de sinistralidade

Pela mesma limitação, a variação de sinistralidade fica achatada em zero para praticamente todos os ramos, o que reduz a classificação a duas categorias efetivas (cresceu/caiu prêmio). Do lado do crescimento de prêmio, destacam-se **Stop Loss (0743)** (+2.455%), **Educacional (1380)** (+411%) e **RC Veículo Transporte Rodoviário de Carga (0659)** (+368%) — com a ressalva de que são ramos de nicho, com base de prêmio inicial pequena.

📷 **[Espaço para imagem: gráfico "Crescimento de prêmio x variação de
sinistralidade por ramo"]**

### Extensão — Concentração por grupo econômico

Os 5 maiores grupos somam **59,0%** do total de prêmios. As duas maiores categorias — "Outros Grupos" (16,5%) e "Independente" (15,3%) — são rótulos agregados da própria base, não conglomerados únicos. Entre os grupos nomeados individualmente, **Porto Seguro** (11,3%), **BB Mapfre** (~8,4%) e **Bradesco** (~7,3%) lideram, coerente com o domínio histórico de grandes seguradoras ligadas a bancos de varejo no Brasil.

📷 **[Espaço para imagem: gráfico "Top 10 grupos econômicos por
participação de mercado"]**

### Extensão — Análise geográfica (UF)

**São Paulo** lidera disparado em volume de prêmio, seguido por RJ, MG, RS e PR — acompanhando o peso econômico de cada estado. Já em sinistralidade, quem lidera é o **Acre** (~500%), muito acima de qualquer estado do top 10 de volume — nenhum dos líderes em arrecadação aparece no topo da sinistralidade. Isso confirma que existem estados de alto volume e sinistralidade controlada (caso de SP) e estados de baixo volume com sinistralidade desproporcional (caso do AC) — sendo este último efeito provavelmente amplificado pela base de prêmio pequena, que torna a métrica mais volátil.

📷 **[Espaço para imagem: gráficos "Top 10 UFs por volume de prêmio" e "Top
10 UFs por sinistralidade"]**

### Discussão geral — conectando tudo à pergunta principal

A dimensão de **arrecadação** da pergunta principal tem resposta sólida: o mercado é liderado pelo ramo Automóvel, moderadamente concentrado em poucos grandes grupos econômicos, com São Paulo isolado na liderança geográfica. A dimensão de **sinistralidade** não pôde ser respondida de forma conclusiva para a maior parte da série, por uma limitação de qualidade de dados na fonte (documentada na seção 7 e mantida deliberadamente, em vez de corrigida artificialmente). A única exceção com sinistralidade interpretável foi a análise geográfica, onde o Acre aparece como outlier — resultado que ilustra bem a sensibilidade dessa métrica a baixo volume de dados.

---

## 9. Autoavaliação

Considero que os objetivos traçados no início do projeto foram atingidos parcialmente. A dimensão de arrecadação da pergunta principal foi respondida com solidez — consegui identificar os segmentos de maior volume de prêmios, a concentração do mercado por grupo econômico e a distribuição geográfica da arrecadação. Já a dimensão de sinistralidade não pôde ser respondida de forma conclusiva, por uma limitação de qualidade de dado na própria fonte, que descrevo abaixo.

Dificuldades encontradas

A maior dificuldade técnica do projeto foi a tipagem inconsistente entre os arquivos de origem. O mesmo campo era lido pelo Spark como tipos diferentes dependendo do arquivo — long em um CSV, double em outro, string em um terceiro — mesmo representando a mesma informação. Isso quebrava silenciosamente os joins entre fato e dimensões na camada Gold: as tabelas eram gravadas sem erro, mas o cruzamento retornava zero linhas, porque "1061" e "1061.0" nunca são considerados iguais. Precisei depurar isso comparando valores brutos entre as tabelas Silver até identificar o padrão, e resolvi criando uma função de normalização (normalizar_codigo) aplicada de forma consistente a todos os campos de código antes de qualquer cruzamento. Esse processo me ensinou, na prática, por que a camada Silver existe: o dado bruto raramente está pronto para ser cruzado, mesmo quando parece correto à primeira vista.

Uma segunda dificuldade foi identificar uma inconsistência na própria fonte dos dados a partir de 2014: os campos usados para calcular a sinistralidade (sinistro_retido e premio_ganho) aparecem zerados ou ausentes para boa parte dos registros a partir desse ano. Investiguei essa questão diretamente nos dados brutos (inclusive comparando registros específicos de grandes seguradoras, para descartar erro do meu próprio pipeline) e confirmei que se trata de uma característica real da base SES nesse período, não de um bug na ingestão ou na transformação.

Decisão de escopo tomada

Diante dessa inconsistência, avaliei duas alternativas: (1) manter todo o histórico disponível desde o início da série, ou (2) escopar a análise a um intervalo mais recente e coeso. Optei por manter a análise restrita aos últimos cerca de 5 anos (2022 em diante), em vez de trazer o histórico completo desde períodos anteriores a 2014. A justificativa é que ampliar o range temporal para incluir mais de uma década de dados tornaria o range de tempo muito grande e heterogêneo para os propósitos deste MVP, e não considerei essa uma boa prática de modelagem: misturar períodos com qualidade de dado muito distinta na mesma análise dificultaria a interpretação e poderia mascarar, em vez de esclarecer, o problema real. Um recorte temporal mais recente e mais estreito também é mais alinhado à proposta do MVP como "produto mínimo viável" — um escopo enxuto e bem delimitado, em vez de tentar cobrir todo o histórico disponível de uma vez.

Uma ressalva importante: mesmo dentro do recorte 2022+, uma parte relevante dos registros já apresenta essa limitação (68,3% das linhas do fato nacional ficam sem sinistralidade calculável). Ou seja, reduzir o range temporal não eliminou o problema, apenas evitou agravá-lo trazendo ainda mais anos afetados. Optei por manter esses registros como vieram da fonte (em vez de excluí-los ou estimar um valor), pelos motivos já documentados na seção 7 — principalmente para não introduzir viés e para manter a transparência sobre o que o dado realmente permite concluir.

Trabalhos futuros
Investigar a causa raiz dos campos de sinistro zerados diretamente com a documentação/suporte da SUSEP, ou testar se colunas alternativas do arquivo de origem (como sinistro_ocorrido, identificada durante o desenvolvimento mas fora do escopo documentado oficialmente) produzem uma métrica de sinistralidade mais completa.
Desagregar a categoria "Outros Grupos" da análise de concentração por empresa individual, para uma leitura mais precisa da concentração real do mercado.
Caso a causa da inconsistência seja identificada e corrigida, reavaliar a ampliação do range temporal para um histórico mais longo, já que a estrutura do pipeline (Bronze/Silver/Gold) já está pronta para suportar isso sem mudanças estruturais — bastaria ajustar o parâmetro DATA_INICIO_ESCOPO no 02_silver_transformacao.py.

---

## 10. Como reproduzir o pipeline

1. Criar conta no [Databricks Free Edition](https://www.databricks.com/).
2. Criar o catálogo `mvp_susep_ses` com os schemas `bronze`, `silver` e
   `gold`, e o Volume `mvp_susep_ses.bronze.arquivos_raw`.
3. Baixar os 6 arquivos CSV do [portal SES/SUSEP](http://www2.susep.gov.br/menuestatistica/SES/principal.aspx)
   e fazer upload no Volume.
4. Conectar o repositório via Databricks Repos.
5. Executar os notebooks em sequência: `01 → 02 → 03 → 04 → 05`.
