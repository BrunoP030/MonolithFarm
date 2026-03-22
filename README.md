# MonolithFarm

Plataforma analitica para comparacao entre areas de milho com manejo convencional e areas acompanhadas por tecnologias 4.0, com foco em NDVI, produtividade, clima, pragas e contexto operacional.

## Resumo

Este repositorio materializa um MVP de ciencia de dados agricola para responder uma pergunta central:

- por que uma area apresentou melhor desempenho agronomico e produtivo do que outra na mesma safra?

O projeto integra fontes heterogeneas (satelite, operacao, clima, solo e pragas), persiste em banco local DuckDB e disponibiliza leitura visual em Streamlit.

## Objetivo do Projeto

Comparar, com base tecnica, areas de milho:

- manejo convencional;
- manejo apoiado por tecnologias 4.0.

O objetivo final e sustentar explicacoes sobre vigor vegetativo, produtividade e eficiencia operacional, preparando o caminho para comparacao economica (custo por hectare e retorno).

## Arquitetura da Solucao

O sistema esta organizado em quatro camadas:

1. ingestao: descoberta e leitura automatica dos arquivos do pacote `FarmLab`;
2. tratamento: normalizacao, conversoes de tipos e enriquecimentos;
3. persistencia: materializacao em `DuckDB`;
4. visualizacao: painel analitico em `Streamlit`.

Fluxo tecnico principal:

- [farmlab/io.py](farmlab/io.py)
- [farmlab/analysis.py](farmlab/analysis.py)
- [farmlab/database.py](farmlab/database.py)
- [streamlit_app.py](streamlit_app.py)

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

O fluxo mais confiavel hoje usa `uv` para criar o ambiente e instalar dependencias, porque o `.venv` gerado por `uv` pode nao ter `pip`.

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

Se `uv` nao estiver disponivel, os scripts de dashboard tentam criar o ambiente automaticamente com `python -m venv`, mas o caminho recomendado continua sendo `uv`.

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

Se quiser forcar explicitamente a pasta `data/` deste repositorio:

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

## Notebook Mestre no Jupyter

O notebook unico para navegar por toda a analise atual e [ndvi_master_analysis.ipynb](notebooks/ndvi_master_analysis.ipynb).

Esse notebook consolida:

- fase 1 pareada;
- fase 2 de deep dive em NDVI;
- tabelas analiticas;
- graficos Plotly inline;
- galerias das imagens NDVI;
- outlook pre-colheita;
- gaps atuais da evidencia.

### Windows

```powershell
uv pip install --python .\.venv\Scripts\python.exe jupyterlab ipykernel
.\.venv\Scripts\python.exe .\scripts\generate_ndvi_master_notebook.py
.\.venv\Scripts\python.exe -m jupyter lab notebooks\ndvi_master_analysis.ipynb
```

### Linux/macOS

```bash
uv pip install --python .venv/bin/python jupyterlab ipykernel
.venv/bin/python scripts/generate_ndvi_master_notebook.py
.venv/bin/python -m jupyter lab notebooks/ndvi_master_analysis.ipynb
```

Os notebooks detectam automaticamente a raiz do projeto e usam `./data` por padrao. Se necessario, sobrescreva:

- `MONOLITHFARM_PROJECT_DIR`
- `MONOLITHFARM_DATA_DIR`
- `MONOLITHFARM_OUTPUT_DIR`

## Revisao e Graficos da Fase 1

Para gerar os CSVs finais, o parecer textual e os graficos HTML da revisao:

Linux/macOS:

```bash
.venv/bin/python scripts/generate_phase1_review.py --data-dir data --output-dir notebook_outputs
```

Windows:

```powershell
.\.venv\Scripts\python.exe .\scripts\generate_phase1_review.py --data-dir data --output-dir notebook_outputs
```

Saidas principais:

- `notebook_outputs/review/review_summary.md`
- `notebook_outputs/review/ndvi_weekly_by_pair.html`
- `notebook_outputs/review/harvest_yield_by_area.html`
- `notebook_outputs/review/miip_pressure_by_area.html`
- `notebook_outputs/review/ops_quality_by_area.html`
- `notebook_outputs/review/weather_coverage_timeline.html`

Notebook da fase 1:

Linux/macOS:

```bash
uv pip install --python .venv/bin/python jupyterlab
.venv/bin/python scripts/generate_phase1_notebook.py
.venv/bin/python -m jupyter lab notebooks/phase1_ndvi_pairwise.ipynb
```

Windows:

```powershell
uv pip install --python .\.venv\Scripts\python.exe jupyterlab
.\.venv\Scripts\python.exe .\scripts\generate_phase1_notebook.py
.\.venv\Scripts\python.exe -m jupyter lab notebooks\phase1_ndvi_pairwise.ipynb
```

## Fase 2 - Deep Dive de NDVI

Para gerar a revisao aprofundada de NDVI com eventos, outlook pre-colheita e graficos:

Linux/macOS:

```bash
.venv/bin/python scripts/generate_phase2_ndvi_review.py --data-dir data --output-dir notebook_outputs/phase2_ndvi
```

Windows:

```powershell
.\.venv\Scripts\python.exe .\scripts\generate_phase2_ndvi_review.py --data-dir data --output-dir notebook_outputs/phase2_ndvi
```

Saidas principais:

- `notebook_outputs/phase2_ndvi/ndvi_phase_timeline.csv`
- `notebook_outputs/phase2_ndvi/ndvi_events.csv`
- `notebook_outputs/phase2_ndvi/ndvi_pair_diagnostics.csv`
- `notebook_outputs/phase2_ndvi/ndvi_outlook.csv`
- `notebook_outputs/phase2_ndvi/review/review_summary.md`

Notebook da fase 2:

Linux/macOS:

```bash
.venv/bin/python scripts/generate_phase2_ndvi_notebook.py
.venv/bin/python -m jupyter lab notebooks/phase2_ndvi_deepdive.ipynb
```

Windows:

```powershell
.\.venv\Scripts\python.exe .\scripts\generate_phase2_ndvi_notebook.py
.\.venv\Scripts\python.exe -m jupyter lab notebooks\phase2_ndvi_deepdive.ipynb
```

## Protecao de Dados e Artefatos Locais

Os seguintes caminhos ja estao ignorados no Git e nao devem ser incluidos em commit:

- `data/`
- `storage/`
- `notebook_outputs/`
- `.venv/`

Isso protege os dados privados do projeto e evita versionar saidas geradas localmente.

## Estrutura Principal do Repositorio

- [streamlit_app.py](streamlit_app.py): interface principal do painel
- [farmlab/analysis.py](farmlab/analysis.py): regras analiticas e sintese dos dados
- [farmlab/database.py](farmlab/database.py): persistencia e leitura do banco local
- [farmlab/io.py](farmlab/io.py): descoberta e ingestao dos arquivos brutos
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

## Proximos Passos Recomendados

1. integrar todas as tabelas EKOS e MIIP descritas na documentacao oficial;
2. padronizar joins por `Service Order`, `field`, `trap`, `data/hora`;
3. consolidar feature store por area e janela temporal;
4. aplicar modelagem explicativa/preditiva para causalidade tecnica;
5. fechar comparacao economica com custo por hectare e retorno produtivo.
