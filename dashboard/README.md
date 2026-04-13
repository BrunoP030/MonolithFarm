# Dashboard Streamlit

Esta pasta concentra a implementacao do dashboard interativo, separada do pipeline analitico usado pelo notebook `complete_ndvi_analysis.ipynb`.

Arquivos principais:

- `app.py`: interface Streamlit.
- `workspace.py`: workspace legado usado apenas pelo dashboard.
- `database.py`: materializacao e leitura do DuckDB do dashboard.

Fluxo do dashboard:

1. `dashboard/workspace.py` le os brutos necessarios para o painel.
2. `dashboard/database.py` materializa ou recarrega o workspace em DuckDB.
3. `dashboard/app.py` renderiza a interface Streamlit.

Observacao:

- o pipeline analitico completo do projeto continua em `farmlab/io.py`, `farmlab/pairwise.py`, `farmlab/ndvi_deepdive.py`, `farmlab/ndvi_crispdm.py` e `farmlab/complete_analysis.py`;
- `streamlit_app.py`, `farmlab/analysis.py` e `farmlab/database.py` foram mantidos apenas como wrappers de compatibilidade.
