#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

PORT="${PORT:-4173}"
exec npm --prefix lineage_atlas run start -- --port "${PORT}"
