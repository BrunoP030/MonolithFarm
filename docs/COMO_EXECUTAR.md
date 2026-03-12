# Como Executar o Projeto

Guia objetivo para subir o MonolithFarm com banco local DuckDB e dashboard Streamlit.

## Pre-requisitos

- Python `3.11+`;
- pacote de dados `FarmLab` disponivel localmente;
- terminal:
  - Windows: PowerShell;
  - Linux/macOS: Bash.

Opcional (recomendado): `uv` para ambientes onde `pip` nao estiver disponivel no venv.

## Resolucao do Diretorio de Dados

O projeto busca os dados nesta ordem:

1. `MONOLITHFARM_DATA_DIR`;
2. `./FarmLab`;
3. `C:\Users\Morgado\Downloads\FarmLab` (fallback legado).

## Execucao Rapida

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh
```

### Linux/macOS

```bash
REFRESH=1 ./scripts/start_dashboard.sh
```

Apos iniciar:

```text
http://127.0.0.1:8501
```

## Executar em Porta Diferente

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -Port 8502
```

### Linux/macOS

```bash
REFRESH=1 ./scripts/start_dashboard.sh "" "storage/monolithfarm.duckdb" 8502
```

## Diretorio de Dados Personalizado

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir "D:\Dados\FarmLab"
```

### Linux/macOS

```bash
REFRESH=1 ./scripts/start_dashboard.sh "/caminho/FarmLab"
```

## Fluxo Manual (quando necessario)

1. criar ambiente virtual:

```bash
python -m venv .venv
```

2. instalar projeto:

```bash
.venv/bin/python -m pip install -e .
```

3. fallback sem `pip`:

```bash
uv pip install --python .venv/bin/python -e .
```

4. materializar banco:

```bash
.venv/bin/python -m farmlab.database --data-dir "FarmLab" --db-path "storage/monolithfarm.duckdb"
```

5. subir dashboard:

```bash
.venv/bin/python -m streamlit run streamlit_app.py
```

## Validacao Rapida Pos-Subida

1. confirmar existencia do banco:

```bash
ls -lh storage/monolithfarm.duckdb
```

2. validar tabelas:

```bash
.venv/bin/python - <<'PY'
import duckdb
con = duckdb.connect("storage/monolithfarm.duckdb", read_only=True)
print(con.execute("show tables").fetchall())
PY
```

3. validar URL no navegador: `http://127.0.0.1:8501`.

## Problemas Comuns

- "diretorio de dados nao encontrado":
  - conferir caminho do `FarmLab`;
  - usar `-DataDir` (PowerShell) ou argumento de caminho no Bash.
- "pip nao encontrado no venv":
  - instalar com `uv pip install --python ... -e .`.
- porta ocupada:
  - trocar para `8502` (ou outra porta livre).

## Arquivos Relacionados

- [../scripts/start_dashboard.ps1](../scripts/start_dashboard.ps1)
- [../scripts/start_dashboard.sh](../scripts/start_dashboard.sh)
- [../streamlit_app.py](../streamlit_app.py)
- [../farmlab/database.py](../farmlab/database.py)
