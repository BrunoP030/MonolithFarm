#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-5173}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ATLAS_DIR="${ROOT}/lineage_atlas"

echo "start_feature_lineage_app.sh agora redireciona para o MonolithFarm Atlas NDVI em React."

PYTHON_BIN="${ROOT}/.venv/bin/python"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="${ROOT}/.venv_win/Scripts/python.exe"
fi
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python"
fi

if [[ ! -d "${ATLAS_DIR}/node_modules" ]]; then
  (cd "${ATLAS_DIR}" && npm install)
fi

(cd "${ROOT}" && "${PYTHON_BIN}" scripts/export_lineage_atlas_data.py)
echo "Abrindo MonolithFarm Atlas NDVI em http://127.0.0.1:${PORT}"
exec npm --prefix "${ATLAS_DIR}" run dev -- --host 127.0.0.1 --port "${PORT}"
