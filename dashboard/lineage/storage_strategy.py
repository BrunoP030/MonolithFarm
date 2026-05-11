from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


RAW_DB_THRESHOLD_MB = 250.0
LARGE_FILE_THRESHOLD_MB = 80.0


def build_storage_inventory(raw_catalog: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    """Monta inventario leve dos CSVs sem reler arquivos grandes por completo."""

    rows: list[dict[str, Any]] = []
    if raw_catalog is not None and not raw_catalog.empty:
        for row in raw_catalog.itertuples(index=False):
            path = Path(str(row.path))
            if str(getattr(row, "kind", "")) != "file" or path.suffix.lower() != ".csv":
                continue
            rows.append(
                {
                    "zone": "bruto",
                    "source_key": str(getattr(row, "source_key", "")),
                    "source_group": str(getattr(row, "source_group", "")),
                    "file_name": path.name,
                    "path": str(path),
                    "size_mb": _size_mb(path),
                    "rows": getattr(row, "rows", pd.NA),
                    "columns": getattr(row, "columns", pd.NA),
                    "row_count_method": getattr(row, "row_count_method", "desconhecido"),
                }
            )

    output_dir = Path(output_dir)
    if output_dir.exists():
        for path in sorted(output_dir.glob("*.csv")):
            rows.append(
                {
                    "zone": "output",
                    "source_key": path.stem,
                    "source_group": "notebook_outputs/complete_ndvi",
                    "file_name": path.name,
                    "path": str(path),
                    "size_mb": _size_mb(path),
                    "rows": _safe_line_count(path),
                    "columns": _safe_column_count(path),
                    "row_count_method": "exato",
                }
            )
    columns = ["zone", "source_key", "source_group", "file_name", "path", "size_mb", "rows", "columns", "row_count_method"]
    frame = pd.DataFrame(rows, columns=columns)
    if frame.empty:
        return frame
    return frame.sort_values(["zone", "size_mb"], ascending=[True, False]).reset_index(drop=True)


def build_storage_recommendation(inventory: pd.DataFrame) -> pd.DataFrame:
    """Decide se a auditoria deve operar em CSV cacheado, DuckDB ou modo hibrido."""

    if inventory is None or inventory.empty:
        return pd.DataFrame(
            [
                {
                    "decision": "sem_dados",
                    "recommendation": "Nenhum CSV foi encontrado para avaliar armazenamento.",
                    "reason": "Inventario vazio.",
                    "frontend_mode": "bloqueado",
                }
            ]
        )

    raw = inventory[inventory["zone"].eq("bruto")]
    outputs = inventory[inventory["zone"].eq("output")]
    total_mb = float(inventory["size_mb"].sum())
    raw_mb = float(raw["size_mb"].sum()) if not raw.empty else 0.0
    outputs_mb = float(outputs["size_mb"].sum()) if not outputs.empty else 0.0
    largest_mb = float(inventory["size_mb"].max())
    large_files = int(inventory["size_mb"].ge(LARGE_FILE_THRESHOLD_MB).sum())

    if raw_mb >= RAW_DB_THRESHOLD_MB or largest_mb >= LARGE_FILE_THRESHOLD_MB:
        decision = "hibrido_csv_manifesto_duckdb_opcional"
        frontend_mode = "CSV/manifesto para UI; DuckDB para consulta pesada nos brutos"
        recommendation = (
            "Nao transferir tudo para banco como fonte oficial. Manter CSV bruto como fonte de verdade, "
            "usar outputs/manifesto cacheados no Streamlit e materializar DuckDB apenas como cache consultavel "
            "para arquivos brutos grandes ou filtros ad-hoc."
        )
        reason = (
            f"Os CSVs somam {total_mb:.1f} MB; brutos somam {raw_mb:.1f} MB; "
            f"maior arquivo tem {largest_mb:.1f} MB; {large_files} arquivos passam de {LARGE_FILE_THRESHOLD_MB:.0f} MB."
        )
    else:
        decision = "csv_cacheado_suficiente"
        frontend_mode = "CSV cacheado em pandas/Streamlit"
        recommendation = (
            "CSV cacheado e leitura sob demanda sao suficientes. DuckDB continua util se o uso virar SQL exploratorio, "
            "mas nao e requisito para performance da interface."
        )
        reason = (
            f"Volume total de {total_mb:.1f} MB e maior arquivo com {largest_mb:.1f} MB ficam dentro do limite definido."
        )

    return pd.DataFrame(
        [
            {
                "decision": decision,
                "recommendation": recommendation,
                "reason": reason,
                "frontend_mode": frontend_mode,
                "csv_total_mb": total_mb,
                "raw_total_mb": raw_mb,
                "outputs_total_mb": outputs_mb,
                "largest_file_mb": largest_mb,
                "csv_count": int(len(inventory)),
                "large_file_count": large_files,
            }
        ]
    )


def _size_mb(path: Path) -> float:
    try:
        return round(path.stat().st_size / 1_048_576, 4)
    except OSError:
        return 0.0


def _safe_line_count(path: Path) -> int | pd.NA:
    try:
        with path.open("rb") as handle:
            return max(sum(chunk.count(b"\n") for chunk in iter(lambda: handle.read(1024 * 1024), b"")) - 1, 0)
    except OSError:
        return pd.NA


def _safe_column_count(path: Path) -> int | pd.NA:
    try:
        return len(pd.read_csv(path, nrows=0).columns)
    except Exception:
        try:
            return len(pd.read_csv(path, nrows=0, sep=";").columns)
        except Exception:
            return pd.NA
