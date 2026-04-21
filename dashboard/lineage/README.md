# App de Auditoria e Rastreabilidade NDVI

Esta ferramenta adiciona uma camada de inspeção humana ao projeto, sem alterar a lógica analítica principal em `farmlab/`.

Ela existe para responder, pela interface:

1. o que é cada arquivo, coluna, feature, driver, gráfico ou hipótese;
2. de qual dado bruto veio;
3. qual função, filtro, join e agregação participou;
4. em quais tabelas intermediárias e CSVs finais apareceu;
5. quais gráficos e hipóteses dependem disso;
6. quais dados reais sustentam ou limitam a interpretação.

## Arquivo principal

- `dashboard/feature_lineage_app.py`

## Camada de metadados

- `dashboard/lineage/registry.py`: registry manual/auditável de tabelas, CSVs, features, hipóteses e gráficos.
- `dashboard/lineage/runtime.py`: resolução de caminhos, carregamento do pipeline real, outputs e arquivos brutos.
- `dashboard/lineage/docs_registry.py`: documentação consolidada de fontes, colunas e drivers.
- `dashboard/lineage/doc_scraper.py`: extração/cache da documentação pública do FarmLab.
- `dashboard/lineage/column_catalog.py`: catálogo de colunas brutas, features e colunas geradas.
- `dashboard/lineage/data_profiler.py`: perfil estatístico simples de tabelas e colunas.
- `dashboard/lineage/quality_rules.py`: regras de qualidade e plausibilidade dos dados.
- `dashboard/lineage/lineage_graph.py`: grafos Graphviz de pipeline, feature, driver e CSV.
- `dashboard/lineage/ui.py`: componentes visuais, tema, cards e badges usados pela interface de auditoria.
- `dashboard/lineage/cache/farmlab_docs_cache.json`: cache local extraído de `https://farm.labs.unimar.br`.

## Páginas da app

- `Home`: visão geral, mapa do pipeline, métricas, qualidade e caminhos ativos.
- `Auditoria de cobertura`: painel de aceitação para verificar cobertura de lineage, features, brutos, CSVs finais, documentação e regras de qualidade.
- `Catálogo de fontes brutas`: arquivos reais de `data/`, colunas, preview, período e documentação vinculada.
- `Dicionário de colunas brutas`: uma linha por coluna bruta, com exemplos, nulos, uso/ignorado, interpretação e link documental.
- `Rastreabilidade de coluna`: matriz unificada bruto -> feature -> intermediário -> CSV final, com função, filtros, agregações, thresholds, downstream, gráficos e hipóteses.
- `Tabelas intermediárias`: `ndvi_clean`, clima, operação, MIIP, feature store semanal, timeline, eventos, diagnóstico e outlook.
- `Catálogo de features`: definição, origem, código, filtros, transformação, distribuição, onde aparece e dependências.
- `Explorador de drivers`: `solo_exposto`, `risco_de_motor`, `alertas_de_maquina`, `falha_de_telemetria`, `pressao_de_pragas`, `sobreposicao_operacional`, `falha_de_dose_na_adubacao`, `estresse_climatico`, `tempo_parado`.
- `CSVs finais`: todos os CSVs em `notebook_outputs/complete_ndvi`, com foco especial nos CSVs de decisão NDVI.
- `Rastreio por linha / semana / área`: filtro por chaves e inspeção da linha final, timeline, linhas diárias, bruto NDVI e JPG quando existir.
- `Hipóteses e evidências`: H1-H4 por par, regra, métrica, evidência, limitações e gráficos relacionados.
- `Gráficos`: galeria dos gráficos principais e seu dataframe de origem.
- `Qualidade dos dados`: regras explícitas de integridade e plausibilidade com exemplos reais.
- `Documentação FarmLab`: rotas, trechos extraídos do bundle JS e documentação consolidada.

## Como rodar

Linux/macOS/WSL:

```bash
./scripts/start_feature_lineage_app.sh
```

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_feature_lineage_app.ps1
```

URL esperada:

```text
http://127.0.0.1:8502
```

## Modos da app

- `Workspace completo`
  - recompõe o pipeline NDVI em memória;
  - exporta CSVs para `notebook_outputs/complete_ndvi`;
  - habilita auditoria das tabelas intermediárias, rastreio de linha e exemplos reais de drivers.

- `Somente outputs prontos`
  - é o modo inicial para abrir a interface rápido;
  - abre mais rápido;
  - depende de CSVs já gerados em `notebook_outputs/complete_ndvi`;
  - mostra CSVs, documentação, catálogo bruto e parte das evidências, mas não recompõe toda a timeline em memória.

## Documentação externa FarmLab

O portal `https://farm.labs.unimar.br` é uma SPA. As rotas como `/docs/dados/satelite` retornam o shell HTML e o conteúdo fica no bundle JavaScript.

Por isso, a ferramenta faz três coisas:

- acessa as rotas públicas e detecta assets da SPA;
- extrai trechos úteis do bundle JS por termos como `ndvi`, `b1_valid_pixels`, `miip`, `geotiff`, `metos` e `cropman`;
- complementa com documentação manual auditável em `docs_registry.py` quando o texto automático não é suficiente.

O cache fica em:

```text
dashboard/lineage/cache/farmlab_docs_cache.json
```

Ele pode ser atualizado pela própria página `Documentação FarmLab` ou via Python:

```bash
.venv/bin/python -c "from dashboard.lineage.doc_scraper import refresh_documentation_cache; refresh_documentation_cache()"
```

## O que é automatizado e o que é manual

- Automatizado: leitura dos arquivos reais, previews, perfis estatísticos, outputs gerados, exemplos de linhas, gráficos e checks de qualidade.
- Derivado do código: funções geradoras, source code exibido, nomes de tabelas, colunas materializadas, CSVs exportados e parte da linhagem por nome de coluna.
- Manual auditável: semântica de algumas colunas, raw columns resolvidas para features agregadas, thresholds, interpretação dos drivers e ligação feature -> hipótese -> gráfico quando a derivação automática seria frágil.
- Performance: arquivos brutos muito grandes do EKOS têm contagem de linhas estimada por tamanho/amostra para não travar a abertura da app; o preview e as colunas continuam sendo lidos dos arquivos reais.

## Como auditar uma coluna final

1. Abra `Rastreabilidade de coluna`.
2. Busque a coluna, por exemplo `gap_ndvi_mean_week_4_0_minus_convencional`.
3. Abra o registro `csv_final / pair_weekly_gaps.csv / ...`.
4. Verifique:
   - `raw_columns`: coluna bruta de origem, por exemplo `b1_mean`;
   - `generated_by`: função Python responsável;
   - `filters`: filtros herdados, como `b1_valid_pixels > 0`;
   - `aggregations`: agregação semanal ou cálculo de gap;
   - `downstream_csvs`, `charts` e `hypotheses`;
   - aba `Valores reais` para exemplos concretos.

## Como auditar uma feature

1. Abra `Catálogo de features`.
2. Escolha a feature, por exemplo `soil_pct_week`.
3. Leia `colunas_brutas_resolvidas`, `transformacao`, `thresholds_regras` e `filtros_envolvidos`.
4. Use as abas de código, preview real e distribuição.
5. Confira CSVs, gráficos e hipóteses relacionadas.

Status comuns:

- `mapeado_por_feature`: coluna ligada a uma feature registrada, com raw columns resolvidas.
- `mapeado_por_driver_dinamico`: coluna de `event_driver_lift.csv`; a origem exata depende do valor de `driver` em cada linha.
- `parcial_por_dependencia_csv`: coluna final explicada pelas dependências do CSV, mas sem expressão coluna-a-coluna isolada.
- `parcial_por_csv_exportado`: coluna de CSV real exportado pelo fluxo completo que ainda não tem registry manual específico; os valores aparecem e a origem é rastreada até o CSV/tabela, mas a semântica coluna-a-coluna pode ser refinada.

## Limitações atuais

- A lineage por linha é forte para NDVI e tabelas materializadas, mas não reconstrói pixel a pixel nem geometria raster original.
- O projeto local usa `ndvi_metadata.csv` e JPGs de apoio; a documentação FarmLab menciona GeoTIFF, mas os TIFFs não estão presentes localmente.
- Drivers são associações observadas nas semanas-problema, não prova causal fechada.
- A documentação externa cacheada depende do formato atual do bundle da SPA; se o portal mudar, atualize o cache.
