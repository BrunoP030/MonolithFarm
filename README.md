# MonolithFarm

Plataforma analitica para comparacao entre areas de milho com manejo convencional e areas acompanhadas por tecnologias 4.0, com foco em NDVI, produtividade, clima, pragas e contexto operacional.

## Resumo

Este repositorio materializa um MVP de ciencia de dados agricola para responder uma pergunta central:

- por que uma area apresentou melhor desempenho agronomico e produtivo do que outra na mesma safra?

O projeto integra fontes heterogeneas (satelite, operacao, clima, solo e pragas). Hoje ele tem tres fluxos separados:

- fluxo analitico principal: notebook `complete_ndvi_analysis.ipynb` + pipeline NDVI em `farmlab/`;
- fluxo de painel: dashboard Streamlit + workspace legado em `dashboard/`;
- fluxo de auditoria: app Streamlit de rastreabilidade e inspeção de lineage focada em NDVI.

## Objetivo do Projeto

Comparar, com base tecnica, areas de milho:

- manejo convencional;
- manejo apoiado por tecnologias 4.0.

O objetivo e explicar diferencas de vigor vegetativo, produtividade e eficiencia operacional, com base para comparacao economica posterior (custo por hectare e retorno).

## Arquitetura da Solucao

O repositorio ficou separado em tres subsistemas independentes.

### 1. Analise NDVI e Notebook

Este e o fluxo principal do projeto e o unico usado para a analise completa, rastreavel e apresentavel:

- [notebooks/complete_ndvi_analysis.ipynb](notebooks/complete_ndvi_analysis.ipynb)
- [farmlab/io.py](farmlab/io.py)
- [farmlab/pairwise.py](farmlab/pairwise.py)
- [farmlab/ndvi_deepdive.py](farmlab/ndvi_deepdive.py)
- [farmlab/ndvi_crispdm.py](farmlab/ndvi_crispdm.py)
- [farmlab/complete_analysis.py](farmlab/complete_analysis.py)

### 2. Dashboard Streamlit

Este fluxo e separado do notebook e existe apenas para leitura interativa do painel:

- [dashboard/app.py](dashboard/app.py)
- [dashboard/workspace.py](dashboard/workspace.py)
- [dashboard/database.py](dashboard/database.py)
- [scripts/start_dashboard.ps1](scripts/start_dashboard.ps1)
- [scripts/start_dashboard.sh](scripts/start_dashboard.sh)

### 3. Auditoria e Rastreabilidade NDVI

Camada nova de inspeção humana para navegar por arquivos brutos, tabelas intermediárias, CSVs finais, features derivadas, gráficos e hipóteses:

- [dashboard/feature_lineage_app.py](dashboard/feature_lineage_app.py)
- [dashboard/lineage/registry.py](dashboard/lineage/registry.py)
- [dashboard/lineage/runtime.py](dashboard/lineage/runtime.py)
- [dashboard/lineage/docs_registry.py](dashboard/lineage/docs_registry.py)
- [dashboard/lineage/doc_scraper.py](dashboard/lineage/doc_scraper.py)
- [dashboard/lineage/column_catalog.py](dashboard/lineage/column_catalog.py)
- [dashboard/lineage/column_lineage.py](dashboard/lineage/column_lineage.py)
- [dashboard/lineage/data_profiler.py](dashboard/lineage/data_profiler.py)
- [dashboard/lineage/quality_rules.py](dashboard/lineage/quality_rules.py)
- [dashboard/lineage/lineage_graph.py](dashboard/lineage/lineage_graph.py)
- [dashboard/lineage/ui.py](dashboard/lineage/ui.py)
- [dashboard/lineage/README.md](dashboard/lineage/README.md)
- [scripts/start_feature_lineage_app.ps1](scripts/start_feature_lineage_app.ps1)
- [scripts/start_feature_lineage_app.sh](scripts/start_feature_lineage_app.sh)

Essa app de auditoria agora funciona como explorador total: auditoria de cobertura, catálogo bruto, dicionário de colunas, rastreabilidade de coluna final até bruto, catálogo de features, drivers, qualidade dos dados, documentação FarmLab cacheada, rastreio por linha/semana/área, CSVs finais e hipóteses H1-H4.

Wrappers de compatibilidade mantidos:

- [streamlit_app.py](streamlit_app.py)
- [farmlab/analysis.py](farmlab/analysis.py)
- [farmlab/database.py](farmlab/database.py)

## Mapeamento do Ecossistema FarmLab

Documentacao oficial consultada:

- <https://farm.labs.unimar.br>
- <https://farm.labs.unimar.br/docs/dados/miip>
- <https://farm.labs.unimar.br/docs/guias/geotiff>
- <https://farm.labs.unimar.br/docs/guias/shapefile>

Categorias de dados descritas no portal:

- `satelite` (OneSoil): recortes NDVI e metadados estatisticos;
- `meteorologia` (Metos): serie horaria;
- `solo` (Cropman): pontos, zonas e recomendacoes;
- `ekos_camadas`: operacoes geoespaciais de campo;
- `ekos_cadastros`: talhoes, maquinas, operadores e implementos;
- `ekos_ordens`: ordens de servico e regras de alerta;
- `ekos_paradas`: motivos de parada e atividades;
- `miip`: catalogo de pragas e eventos de armadilhas.

## Escopo Atual no Repositorio

No estado atual, o app ja processa bem:

- NDVI metadata + imagens;
- clima horario;
- armadilhas (lista e leituras consolidadas);
- camadas operacionais de plantio e colheita;
- sugestao de vinculacao entre recortes NDVI e areas.

Pontos ainda pendentes para "fluxo completo" do portal:

- ingestao integral de `miip` (incluindo `traps_events` com maior profundidade analitica);
- ingestao de `ekos_cadastros`, `ekos_ordens` e `ekos_paradas`;
- ampliacao para todas as camadas EKOS descritas no portal;
- camada economica robusta (custos por hectare/operacao) para prova de eficiencia financeira.

## Dados Esperados

O projeto resolve o diretorio de dados nesta ordem:

1. variavel de ambiente `MONOLITHFARM_DATA_DIR`;
2. pasta local `./data` (raiz do repositorio);
3. pasta local `./FarmLab` (legado);
4. fallback legado `C:\Users\Morgado\Downloads\FarmLab`.

## Preparacao do Ambiente

A preparacao abaixo usa `uv` para criar o ambiente e instalar dependencias, porque o `.venv` gerado por `uv` pode nao ter `pip`.

### Windows

```powershell
uv venv .venv
uv pip install --python .\.venv\Scripts\python.exe -e .
```

### Linux/macOS

```bash
uv venv .venv
uv pip install --python .venv/bin/python -e .
```

Se `uv` nao estiver disponivel, os scripts de dashboard tentam criar o ambiente automaticamente com `python -m venv`.

## Execucao Rapida do Dashboard

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir .\data
```

### Linux/macOS

```bash
REFRESH=1 ./scripts/start_dashboard.sh data
```

Apos iniciar, acessar:

```text
http://127.0.0.1:8501
```

Para informar explicitamente a pasta `data/` deste repositorio:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -DataDir .\data -Refresh
```

```bash
REFRESH=1 ./scripts/start_dashboard.sh data
```

Se a porta estiver ocupada, troque a porta:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir .\data -Port 8502
```

```bash
REFRESH=1 ./scripts/start_dashboard.sh data storage/monolithfarm.duckdb 8502
```

## Guias de Execucao

- [docs/COMO_EXECUTAR.md](docs/COMO_EXECUTAR.md)
- [docs/PRIMEIRO_USO_FACULDADE.md](docs/PRIMEIRO_USO_FACULDADE.md)
- [docs/COLAB_DRIVE.md](docs/COLAB_DRIVE.md)
- [docs/ARQUITETURA.md](docs/ARQUITETURA.md)
- [dashboard/lineage/README.md](dashboard/lineage/README.md)

## Notebook Unico do Projeto

O notebook oficial e unico para navegacao, Colab e apresentacao do projeto e [complete_ndvi_analysis.ipynb](notebooks/complete_ndvi_analysis.ipynb).

Ele agora e gerado a partir do template didatico em `monolithfarm_notebook_ndvi_foco_total_v2_package/` e executa o pipeline real do repositorio com base na configuracao de `.monolithfarm.paths.json`.

Ele documenta o fluxo analitico completo. O dashboard e a app de auditoria nao participam das decisoes, hipoteses e CSVs finais do pipeline NDVI.

Os notebooks antigos por fases e o notebook `master` foram removidos para evitar duplicacao e manter um unico fluxo de leitura e execucao.

O notebook detecta automaticamente a raiz do projeto e usa `./data` por padrao. Se necessario, sobrescreva:

- `MONOLITHFARM_PROJECT_DIR`
- `MONOLITHFARM_DATA_DIR`
- `MONOLITHFARM_OUTPUT_DIR`
- `MONOLITHFARM_PROFILE`

Para nao commitar caminhos pessoais, use um arquivo local ignorado pelo Git:

- copie [monolithfarm.paths.example.json](monolithfarm.paths.example.json) para `.monolithfarm.paths.json`;
- defina os perfis `local`, `colab_drive` ou outros perfis necessarios;
- selecione o perfil com `MONOLITHFARM_PROFILE`.

## Notebook Completo do Projeto

O notebook completo do projeto esta em [complete_ndvi_analysis.ipynb](notebooks/complete_ndvi_analysis.ipynb).

Cobertura:

- leitura completa do objetivo em `info.md`;
- execucao local;
- execucao no Google Colab com pasta no Drive;
- integracao das fases 1, 2 e 3 em um unico fluxo analitico.

### Windows

```powershell
uv pip install --python .\.venv\Scripts\python.exe jupyterlab ipykernel
.\.venv\Scripts\python.exe .\scripts\generate_complete_ndvi_notebook.py
.\.venv\Scripts\python.exe -m jupyter lab notebooks\complete_ndvi_analysis.ipynb
```

### Linux/macOS

```bash
uv pip install --python .venv/bin/python jupyterlab ipykernel
.venv/bin/python scripts/generate_complete_ndvi_notebook.py
.venv/bin/python -m jupyter lab notebooks/complete_ndvi_analysis.ipynb
```

Para gerar os artefatos finais sem abrir o notebook:

Linux/macOS:

```bash
.venv/bin/python scripts/generate_complete_ndvi_review.py --data-dir data --output-dir notebook_outputs/complete_ndvi
```

Windows:

```powershell
.\.venv\Scripts\python.exe .\scripts\generate_complete_ndvi_review.py --data-dir data --output-dir notebook_outputs/complete_ndvi
```

Saidas principais:

- `notebook_outputs/complete_ndvi/dataset_overview.csv`
- `notebook_outputs/complete_ndvi/numeric_profiles.csv`
- `notebook_outputs/complete_ndvi/ndvi_outliers.csv`
- `notebook_outputs/complete_ndvi/pair_classic_tests.csv`
- `notebook_outputs/complete_ndvi/weekly_correlations.csv`
- `notebook_outputs/complete_ndvi/review/review_summary.md`

## App de Auditoria NDVI

Linux/macOS:

```bash
./scripts/start_feature_lineage_app.sh
```

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_feature_lineage_app.ps1
```

URL esperada:

```text
http://127.0.0.1:8502
```

## Protecao de Dados e Artefatos Locais

Os seguintes caminhos ja estao ignorados no Git e nao devem ser incluidos em commit:

- `data/`
- `storage/`
- `notebook_outputs/`
- `.venv/`

Isso protege os dados privados do projeto e evita versionar saidas geradas localmente.

## Estrutura Principal do Repositorio

### Pipeline analitico

- [notebooks/complete_ndvi_analysis.ipynb](notebooks/complete_ndvi_analysis.ipynb): notebook oficial do projeto
- [farmlab/io.py](farmlab/io.py): descoberta e ingestao dos arquivos brutos
- [farmlab/pairwise.py](farmlab/pairwise.py): tratamento inicial, renomeacoes e integracao semanal
- [farmlab/ndvi_deepdive.py](farmlab/ndvi_deepdive.py): flags, eventos, drivers e outlook
- [farmlab/ndvi_crispdm.py](farmlab/ndvi_crispdm.py): auditoria, hipoteses e decisao
- [farmlab/complete_analysis.py](farmlab/complete_analysis.py): estatistica final e export dos CSVs

### Dashboard

- [dashboard/app.py](dashboard/app.py): implementacao real do painel Streamlit
- [dashboard/workspace.py](dashboard/workspace.py): workspace legado do painel
- [dashboard/database.py](dashboard/database.py): persistencia do painel em DuckDB
- [scripts/start_dashboard.ps1](scripts/start_dashboard.ps1): inicializacao assistida (Windows)
- [scripts/start_dashboard.sh](scripts/start_dashboard.sh): inicializacao assistida (Linux/macOS)

## Arquivos Auxiliares

- [templates/season_mapping.example.csv](templates/season_mapping.example.csv)
- [templates/costs.example.csv](templates/costs.example.csv)

## Limitacoes Atuais

- recortes NDVI sem identificacao oficial de talhao no arquivo fonte;
- pacote atual com JPG + metadados, sem TIFF numerico original no repositorio;
- solo sem chave espacial direta para cada talhao no pipeline atual;
- estacao meteorologica representa contexto macro da fazenda;
- comparacao economica depende de custos externos fornecidos pelo usuario.

## Desenvolvimentos Pendentes

1. integrar todas as tabelas EKOS e MIIP descritas na documentacao oficial;
2. padronizar joins por `Service Order`, `field`, `trap`, `data/hora`;
3. consolidar feature store por area e janela temporal;
4. aplicar modelagem explicativa/preditiva para causalidade tecnica;
5. fechar comparacao economica com custo por hectare e retorno produtivo.
