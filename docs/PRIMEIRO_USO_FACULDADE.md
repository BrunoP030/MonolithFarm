# Checklist de Primeiro Uso na Faculdade

Checklist rapido para reduzir erro de ambiente em maquina nova e deixar o projeto pronto para os tres fluxos separados do repositorio:

- dashboard Streamlit;
- notebook analitico NDVI;
- app de auditoria e rastreabilidade NDVI.

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

O dashboard usa a implementacao em `dashboard/`.

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

## 5) Abrir o Notebook Completo

Notebook completo: `notebooks/complete_ndvi_analysis.ipynb`.

Windows:

```powershell
uv pip install --python .\.venv\Scripts\python.exe jupyterlab ipykernel
.\.venv\Scripts\python.exe .\scripts\generate_complete_ndvi_notebook.py
.\.venv\Scripts\python.exe -m jupyter lab notebooks\complete_ndvi_analysis.ipynb
```

Linux/macOS:

```bash
uv pip install --python .venv/bin/python jupyterlab ipykernel
.venv/bin/python scripts/generate_complete_ndvi_notebook.py
.venv/bin/python -m jupyter lab notebooks/complete_ndvi_analysis.ipynb
```

Esse e o unico notebook oficial do projeto. Os notebooks antigos por fases foram removidos para manter um unico fluxo de execucao e apresentacao.

Esse notebook cobre o pipeline analitico em `farmlab/`. O dashboard e a app de auditoria sao subsistemas separados.

## 6) Abrir a App de Auditoria NDVI

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

## 7) Se os Dados Estiverem em Outro Caminho

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir "D:\Caminho\FarmLab"
```

Linux/macOS:

```bash
REFRESH=1 ./scripts/start_dashboard.sh "/caminho/FarmLab"
```

## 8) Se a Porta 8501 Estiver Ocupada

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

## 9) Verificacao Final Esperada

- ambiente virtual criado;
- dependencias instaladas;
- banco DuckDB atualizado;
- painel Streamlit abrindo no navegador;
- notebook mestre abrindo no Jupyter;
- app de auditoria abrindo no navegador.

## 10) Solucao Rapida de Problemas

- erro de caminho dos dados:
  - confirme `./data` ou use caminho explicito.
- erro de dependencia com `pip`:
  - use `uv pip install --python ...` em vez de `python -m pip`.
- `jupyter` nao abre:
  - instale `jupyterlab` e `ipykernel` na mesma `.venv`.
- painel nao abre:
  - verifique a URL e mantenha o terminal aberto.
- app de auditoria nao abre:
  - confirme que a `.venv` tem `streamlit` e que `notebook_outputs/complete_ndvi` pode ser escrito.

## 11) O Que Nao Deve Entrar em Commit

Nao versione:

- `data/`
- `storage/`
- `notebook_outputs/`
- `.venv/`

Esses caminhos ja estao protegidos no `.gitignore`.
