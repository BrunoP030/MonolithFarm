#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8502}"

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

echo "Abrindo app de auditoria NDVI em http://127.0.0.1:${PORT}"
exec "${PYTHON_BIN}" -m streamlit run dashboard/feature_lineage_app.py --server.address 127.0.0.1 --server.port "${PORT}"
