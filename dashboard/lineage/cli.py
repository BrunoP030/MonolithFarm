from __future__ import annotations

import argparse
import sys
from pathlib import Path
import subprocess

from dashboard.lineage.column_catalog import build_raw_column_catalog
from dashboard.lineage.doc_scraper import load_or_refresh_documentation_cache
from dashboard.lineage.manifest import export_lineage_manifest
from dashboard.lineage.quality_rules import run_quality_rules
from dashboard.lineage.runtime import (
    build_raw_file_catalog,
    build_workspace_and_outputs,
    load_output_csvs,
    load_raw_preview,
    load_resolved_paths,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Executa o MonolithFarm Atlas NDVI em React.")
    parser.add_argument("--port", type=int, default=5173, help="Porta local do Vite.")
    parser.add_argument("--address", default="127.0.0.1", help="Endereco local do Vite.")
    parser.add_argument(
        "--export-manifest",
        action="store_true",
        help="Gera lineage_manifest.* e gates em notebook_outputs/complete_ndvi sem abrir a interface.",
    )
    parser.add_argument(
        "--outputs-only",
        action="store_true",
        help="Ao exportar manifesto, usa CSVs existentes sem recompor o workspace completo.",
    )
    args = parser.parse_args()

    if args.export_manifest:
        _export_manifest(outputs_only=args.outputs_only)
        return

    root = Path(__file__).resolve().parents[2]
    atlas_dir = root / "lineage_atlas"
    if not atlas_dir.exists():
        raise SystemExit(f"Diretorio do Atlas nao encontrado: {atlas_dir}")

    subprocess.check_call([sys.executable, str(root / "scripts" / "export_lineage_atlas_data.py")], cwd=root)
    if not (atlas_dir / "node_modules").exists():
        subprocess.check_call(["npm", "install"], cwd=atlas_dir)

    command = ["npm", "run", "dev", "--", "--host", args.address, "--port", str(args.port)]
    raise SystemExit(subprocess.call(command, cwd=atlas_dir))


def _export_manifest(*, outputs_only: bool) -> None:
    paths = load_resolved_paths()
    _, raw_catalog = build_raw_file_catalog(paths.data_dir)
    docs_cache = load_or_refresh_documentation_cache(force=False)
    raw_columns = build_raw_column_catalog(raw_catalog, load_raw_preview, docs_cache=docs_cache)
    if outputs_only:
        workspace = None
        outputs = load_output_csvs(paths.output_dir)
    else:
        workspace, outputs = build_workspace_and_outputs(paths.data_dir, paths.output_dir, persist_outputs=True)
    quality_summary, _ = run_quality_rules(workspace, outputs)
    files = export_lineage_manifest(paths.output_dir, raw_columns, workspace, outputs, quality_summary)
    print("Manifesto de lineage exportado:")
    for label, path in files.items():
        print(f"- {label}: {path}")


if __name__ == "__main__":
    main()
