# Como Executar o Projeto

Guia objetivo para subir o MonolithFarm separando claramente os dois fluxos do repositorio:

- notebook analitico NDVI;
- dashboard Streamlit.

## Pre-requisitos

- Python `3.11+`;
- dados privados disponiveis localmente;
- terminal:
  - Windows: PowerShell;
  - Linux/macOS: Bash;
- `uv` instalado.

`uv` e usado porque o `.venv` criado por ele pode nao incluir `pip`, e os scripts do projeto ja tratam esse caso.

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

O dashboard usa a implementacao em `dashboard/` e nao participa do pipeline analitico completo do notebook.

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

## Notebook Unico do Projeto

Notebook principal: `notebooks/complete_ndvi_analysis.ipynb`.

Esse e o unico notebook oficial do projeto. Os notebooks antigos por fases e o notebook `master` foram removidos para deixar a execucao e a leitura concentradas em um unico fluxo.

O notebook documenta o pipeline analitico em `farmlab/` e nao depende da implementacao do dashboard em `dashboard/`.

## Paths Ajustaveis sem Commit

Para evitar commitar caminhos locais ou do Drive:

1. copie [../monolithfarm.paths.example.json](../monolithfarm.paths.example.json) para `.monolithfarm.paths.json`;
2. defina os perfis no arquivo local;
3. use `MONOLITHFARM_PROFILE=local`, `MONOLITHFARM_PROFILE=colab_drive` ou outro perfil definido no arquivo.

Esse arquivo local esta ignorado no Git.

## Notebook Completo CRISP-DM

Notebook completo: `notebooks/complete_ndvi_analysis.ipynb`.

Ele consolida:

- objetivo do projeto a partir de `info.md`;
- CRISP-DM com foco em NDVI;
- estatistica descritiva com `shape` e `describe`;
- z-score para deteccao de outliers;
- testes classicos com `p-value`;
- correlacoes entre NDVI, clima, operacao e pragas;
- modelagem interpretavel;
- export final em CSV.

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

## Google Colab + Drive

O notebook `complete_ndvi_analysis.ipynb` detecta Colab e permite montar o Drive.

Fluxo:

1. coloque o repositorio no Google Drive;
2. abra o notebook no Colab;
3. ajuste `COLAB_PROJECT_HINTS` se o caminho for diferente;
4. rode a primeira celula para montar o Drive e localizar o projeto;
5. se necessario, defina:
   - `MONOLITHFARM_PROJECT_DIR`
   - `MONOLITHFARM_DATA_DIR`
   - `MONOLITHFARM_OUTPUT_DIR`

Guia dedicado:

- [COLAB_DRIVE.md](./COLAB_DRIVE.md)

## Gerar a Revisao sem Abrir o Notebook

Linux/macOS:

```bash
.venv/bin/python scripts/generate_complete_ndvi_review.py --data-dir data --output-dir notebook_outputs/complete_ndvi
```

Windows:

```powershell
.\.venv\Scripts\python.exe .\scripts\generate_complete_ndvi_review.py --data-dir data --output-dir notebook_outputs/complete_ndvi
```

## Fluxo Manual

Fluxo manual com controle explicito das etapas.

### Linux/macOS

```bash
uv venv .venv
uv pip install --python .venv/bin/python -e .
.venv/bin/python -m dashboard.database --data-dir data --db-path storage/monolithfarm.duckdb
.venv/bin/python -m streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port 8501
```

### Windows

```powershell
uv venv .venv
uv pip install --python .\.venv\Scripts\python.exe -e .
.\.venv\Scripts\python.exe -m dashboard.database --data-dir data --db-path storage/monolithfarm.duckdb
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port 8501
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
- [../scripts/generate_complete_ndvi_notebook.py](../scripts/generate_complete_ndvi_notebook.py)
- [../scripts/generate_complete_ndvi_review.py](../scripts/generate_complete_ndvi_review.py)
- [../dashboard/app.py](../dashboard/app.py)
- [../dashboard/database.py](../dashboard/database.py)
- [../dashboard/workspace.py](../dashboard/workspace.py)
