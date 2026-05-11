#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

python -m pip install -e .
python scripts/bootstrap_data.py
python scripts/export_lineage_atlas_data.py

if [[ -f lineage_atlas/package-lock.json ]]; then
  npm --prefix lineage_atlas ci
else
  npm --prefix lineage_atlas install
fi
npm --prefix lineage_atlas run build
