# Checklist de Primeiro Uso na Faculdade

Checklist rapido para reduzir erro de ambiente em maquina nova.

## 1) Conferencia Inicial

- abrir terminal na raiz do projeto;
- confirmar Python:

```powershell
python --version
```

- confirmar pasta de dados `FarmLab` disponivel.

## 2) Ordem de Busca do Diretorio de Dados

O projeto utiliza:

1. `MONOLITHFARM_DATA_DIR`;
2. `./FarmLab`;
3. `C:\Users\Morgado\Downloads\FarmLab`.

## 3) Comando Recomendado

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh
```

### Linux/macOS

```bash
REFRESH=1 ./scripts/start_dashboard.sh
```

## 4) Se os Dados Estiverem em Outro Caminho

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir "D:\Caminho\FarmLab"
```

### Linux/macOS

```bash
REFRESH=1 ./scripts/start_dashboard.sh "/caminho/FarmLab"
```

## 5) Se a Porta 8501 Estiver Ocupada

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -Port 8502
```

### Linux/macOS

```bash
REFRESH=1 ./scripts/start_dashboard.sh "" "storage/monolithfarm.duckdb" 8502
```

URL:

```text
http://127.0.0.1:8502
```

## 6) Verificacao Final Esperada

- ambiente virtual criado;
- dependencias instaladas;
- banco DuckDB atualizado;
- painel Streamlit disponivel no navegador.

## 7) Solucao Rapida de Problemas

- erro de caminho dos dados:
  - revisar `FarmLab` e usar caminho explicito.
- erro de dependencia (`pip`):
  - usar `uv pip install --python .venv/bin/python -e .`.
- painel nao abre:
  - verificar URL/porta mostrada no terminal;
  - manter terminal aberto durante o uso.
