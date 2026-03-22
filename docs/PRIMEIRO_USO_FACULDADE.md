# Checklist de Primeiro Uso na Faculdade

Checklist rapido para reduzir erro de ambiente em maquina nova e deixar o projeto pronto para dashboard e notebooks.

## 1) Conferencia Inicial

- abra o terminal na raiz do projeto;
- confirme o Python:

Windows:

```powershell
python --version
```

Linux/macOS:

```bash
python3 --version
```

- confirme que os dados privados estao em `./data`.

## 2) Ordem de Busca do Diretorio de Dados

O projeto utiliza:

1. `MONOLITHFARM_DATA_DIR`;
2. `./data`;
3. `./FarmLab`;
4. `C:\Users\Morgado\Downloads\FarmLab`.

## 3) Preparar o Ambiente

Windows:

```powershell
uv venv .venv
uv pip install --python .\.venv\Scripts\python.exe -e .
```

Linux/macOS:

```bash
uv venv .venv
uv pip install --python .venv/bin/python -e .
```

## 4) Subir o Dashboard

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir .\data
```

Linux/macOS:

```bash
REFRESH=1 ./scripts/start_dashboard.sh data
```

URL esperada:

```text
http://127.0.0.1:8501
```

## 5) Abrir o Notebook Mestre

Windows:

```powershell
uv pip install --python .\.venv\Scripts\python.exe jupyterlab ipykernel
.\.venv\Scripts\python.exe .\scripts\generate_ndvi_master_notebook.py
.\.venv\Scripts\python.exe -m jupyter lab notebooks\ndvi_master_analysis.ipynb
```

Linux/macOS:

```bash
uv pip install --python .venv/bin/python jupyterlab ipykernel
.venv/bin/python scripts/generate_ndvi_master_notebook.py
.venv/bin/python -m jupyter lab notebooks/ndvi_master_analysis.ipynb
```

## 6) Se os Dados Estiverem em Outro Caminho

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir "D:\Caminho\FarmLab"
```

Linux/macOS:

```bash
REFRESH=1 ./scripts/start_dashboard.sh "/caminho/FarmLab"
```

## 7) Se a Porta 8501 Estiver Ocupada

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir .\data -Port 8502
```

Linux/macOS:

```bash
REFRESH=1 ./scripts/start_dashboard.sh data storage/monolithfarm.duckdb 8502
```

URL:

```text
http://127.0.0.1:8502
```

## 8) Verificacao Final Esperada

- ambiente virtual criado;
- dependencias instaladas;
- banco DuckDB atualizado;
- painel Streamlit abrindo no navegador;
- notebook mestre abrindo no Jupyter.

## 9) Solucao Rapida de Problemas

- erro de caminho dos dados:
  - confirme `./data` ou use caminho explicito.
- erro de dependencia com `pip`:
  - use `uv pip install --python ...` em vez de `python -m pip`.
- `jupyter` nao abre:
  - instale `jupyterlab` e `ipykernel` na mesma `.venv`.
- painel nao abre:
  - verifique a URL e mantenha o terminal aberto.

## 10) O Que Nao Deve Entrar em Commit

Nao versione:

- `data/`
- `storage/`
- `notebook_outputs/`
- `.venv/`

Esses caminhos ja estao protegidos no `.gitignore`.
