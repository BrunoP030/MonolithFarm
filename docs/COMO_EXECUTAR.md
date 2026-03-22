# Como Executar o Projeto

Guia objetivo para subir o MonolithFarm com dashboard, revisoes analiticas e notebooks Jupyter.

## Pre-requisitos

- Python `3.11+`;
- dados privados disponiveis localmente;
- terminal:
  - Windows: PowerShell;
  - Linux/macOS: Bash;
- `uv` instalado.

`uv` e o caminho recomendado porque o `.venv` criado por ele pode nao incluir `pip`, e os scripts do projeto ja tratam isso.

## Resolucao do Diretorio de Dados

O projeto procura os dados nesta ordem:

1. `MONOLITHFARM_DATA_DIR`;
2. `./data`;
3. `./FarmLab`;
4. `C:\Users\Morgado\Downloads\FarmLab`.

Para este repositorio, o caminho esperado e `./data`.

## Preparar o Ambiente

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

## Subir o Dashboard

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir .\data
```

### Linux/macOS

```bash
REFRESH=1 ./scripts/start_dashboard.sh data
```

URL padrao:

```text
http://127.0.0.1:8501
```

## Trocar Porta

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir .\data -Port 8502
```

### Linux/macOS

```bash
REFRESH=1 ./scripts/start_dashboard.sh data storage/monolithfarm.duckdb 8502
```

## Rodar Tudo em Um Unico Notebook

Notebook principal: `notebooks/ndvi_master_analysis.ipynb`.

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

O notebook mestre consolida:

- inventario das areas;
- fase 1 pareada;
- fase 2 de deep dive de NDVI;
- graficos Plotly inline;
- galerias visuais das imagens NDVI;
- outlook pre-colheita;
- export dos artefatos finais.

## Gerar as Revisoes sem Abrir o Notebook

### Fase 1

Linux/macOS:

```bash
.venv/bin/python scripts/generate_phase1_review.py --data-dir data --output-dir notebook_outputs
```

Windows:

```powershell
.\.venv\Scripts\python.exe .\scripts\generate_phase1_review.py --data-dir data --output-dir notebook_outputs
```

### Fase 2

Linux/macOS:

```bash
.venv/bin/python scripts/generate_phase2_ndvi_review.py --data-dir data --output-dir notebook_outputs/phase2_ndvi
```

Windows:

```powershell
.\.venv\Scripts\python.exe .\scripts\generate_phase2_ndvi_review.py --data-dir data --output-dir notebook_outputs/phase2_ndvi
```

## Fluxo Manual

Use este fluxo se quiser controlar cada etapa explicitamente.

### Linux/macOS

```bash
uv venv .venv
uv pip install --python .venv/bin/python -e .
.venv/bin/python -m farmlab.database --data-dir data --db-path storage/monolithfarm.duckdb
.venv/bin/python -m streamlit run streamlit_app.py --server.address 127.0.0.1 --server.port 8501
```

### Windows

```powershell
uv venv .venv
uv pip install --python .\.venv\Scripts\python.exe -e .
.\.venv\Scripts\python.exe -m farmlab.database --data-dir data --db-path storage/monolithfarm.duckdb
.\.venv\Scripts\python.exe -m streamlit run streamlit_app.py --server.address 127.0.0.1 --server.port 8501
```

## Validacao Rapida

1. Confirmar o banco:

Linux/macOS:

```bash
ls -lh storage/monolithfarm.duckdb
```

Windows:

```powershell
Get-Item .\storage\monolithfarm.duckdb
```

2. Verificar tabelas:

Linux/macOS:

```bash
.venv/bin/python - <<'PY'
import duckdb
con = duckdb.connect("storage/monolithfarm.duckdb", read_only=True)
print(con.execute("show tables").fetchall())
PY
```

Windows:

```powershell
.\.venv\Scripts\python.exe -c "import duckdb; con = duckdb.connect('storage/monolithfarm.duckdb', read_only=True); print(con.execute('show tables').fetchall())"
```

3. Confirmar a URL no navegador.

## Problemas Comuns

- `diretorio de dados nao encontrado`:
  - confirme que os dados estao em `./data` ou passe o caminho explicitamente;
  - use `-DataDir` no PowerShell ou o primeiro argumento no script Bash.
- `pip nao encontrado no venv`:
  - use `uv pip install --python ...` em vez de `python -m pip`.
- `modulo jupyter nao encontrado`:
  - instale `jupyterlab` e `ipykernel` no mesmo Python da `.venv`.
- porta ocupada:
  - troque a porta para `8502` ou outra livre.

## Privacidade e Git

Nao inclua em commit:

- `data/`
- `storage/`
- `notebook_outputs/`
- `.venv/`

Esses caminhos ja estao ignorados no `.gitignore`.

## Arquivos Relacionados

- [../README.md](../README.md)
- [../scripts/start_dashboard.ps1](../scripts/start_dashboard.ps1)
- [../scripts/start_dashboard.sh](../scripts/start_dashboard.sh)
- [../scripts/generate_ndvi_master_notebook.py](../scripts/generate_ndvi_master_notebook.py)
- [../streamlit_app.py](../streamlit_app.py)
- [../farmlab/database.py](../farmlab/database.py)
