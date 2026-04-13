# Arquitetura do Repositorio

O repositorio foi separado em dois subsistemas para evitar confusao entre o projeto analitico e o painel Streamlit.

## 1. Pipeline Analitico NDVI

Este e o fluxo oficial do projeto para estudo, notebook, hipoteses, metricas e CSVs finais.

Entradas principais:

- `data/OneSoil - Imagens de Satelite/CSV/ndvi_metadata.csv`
- `data/Metos - Estacao Meteorologica/...`
- `data/EKOS - Telemetria e Armadilhas Eletronicas/...`
- `data/Cropman - Analise de solo/...`

Implementacao:

- `farmlab/io.py`
- `farmlab/pairwise.py`
- `farmlab/ndvi_deepdive.py`
- `farmlab/ndvi_crispdm.py`
- `farmlab/complete_analysis.py`

Artefato principal:

- `notebooks/complete_ndvi_analysis.ipynb`

Saidas principais:

- `notebook_outputs/complete_ndvi/*.csv`

## 2. Dashboard Streamlit

Este e um subsistema separado para leitura interativa. Ele nao e a fonte de verdade das hipoteses H1-H4 nem dos CSVs finais do pipeline NDVI.

Implementacao:

- `dashboard/app.py`
- `dashboard/workspace.py`
- `dashboard/database.py`

Inicializacao:

- `scripts/start_dashboard.ps1`
- `scripts/start_dashboard.sh`

Persistencia:

- `storage/monolithfarm.duckdb`

## 3. Wrappers de compatibilidade

Os arquivos abaixo foram mantidos apenas para nao quebrar comandos e imports antigos:

- `streamlit_app.py`
- `farmlab/analysis.py`
- `farmlab/database.py`

Eles nao devem mais ser tratados como implementacao principal.

## 4. Regra pratica

- quer entender a analise completa: abra `notebooks/complete_ndvi_analysis.ipynb`;
- quer inspecionar a logica analitica: abra os modulos `farmlab/*` do pipeline NDVI;
- quer mexer no painel: abra `dashboard/`.
