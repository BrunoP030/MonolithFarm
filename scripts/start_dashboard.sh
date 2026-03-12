#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${1:-}"
DB_PATH="${2:-storage/monolithfarm.duckdb}"
PORT="${3:-8501}"
REFRESH="${REFRESH:-0}"

if [[ -z "${DATA_DIR}" ]]; then
  if [[ -n "${MONOLITHFARM_DATA_DIR:-}" ]]; then
    DATA_DIR="${MONOLITHFARM_DATA_DIR}"
  elif [[ -d "FarmLab" ]]; then
    DATA_DIR="FarmLab"
  else
    DATA_DIR="C:/Users/Morgado/Downloads/FarmLab"
  fi
fi

if [[ ! -d "${DATA_DIR}" ]]; then
  echo "Erro: diretorio de dados nao encontrado: ${DATA_DIR}" >&2
  echo "Uso: ./scripts/start_dashboard.sh [DATA_DIR] [DB_PATH] [PORT]" >&2
  exit 1
fi

PYTHON_BIN=".venv/bin/python"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  if command -v uv >/dev/null 2>&1; then
    echo "Criando ambiente virtual com uv..."
    uv venv .venv
  else
    echo "Criando ambiente virtual com python3 -m venv..."
    python3 -m venv .venv
  fi
fi

if "${PYTHON_BIN}" -m pip --version >/dev/null 2>&1; then
  echo "Instalando dependencias com pip..."
  "${PYTHON_BIN}" -m pip install -e .
elif command -v uv >/dev/null 2>&1; then
  echo "Instalando dependencias com uv..."
  uv pip install --python "${PYTHON_BIN}" -e .
else
  echo "Erro: pip nao esta disponivel no venv e uv nao foi encontrado." >&2
  exit 1
fi

if [[ "${REFRESH}" == "1" || ! -f "${DB_PATH}" ]]; then
  echo "Atualizando banco DuckDB..."
  "${PYTHON_BIN}" -m farmlab.database --data-dir "${DATA_DIR}" --db-path "${DB_PATH}"
fi

echo "Abrindo dashboard em http://127.0.0.1:${PORT}"
exec "${PYTHON_BIN}" -m streamlit run streamlit_app.py --server.address 127.0.0.1 --server.port "${PORT}"
