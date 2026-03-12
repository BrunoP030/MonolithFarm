from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from farmlab.analysis import build_workspace


DEFAULT_DB_PATH = Path("storage/monolithfarm.duckdb")
PERSISTED_TABLES = [
    "soil",
    "ndvi",
    "seasons",
    "season_mapping",
    "traps_list",
    "traps_data",
    "weather",
    "weather_daily",
    "planting",
    "harvest",
    "ndvi_daily",
    "nearby_traps",
    "pest_daily",
    "harvest_summary",
    "planting_summary",
    "evidence",
]


def ensure_workspace(base_dir: Path, db_path: Path | None = None, *, force_refresh: bool = False) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved_db_path = Path(db_path or DEFAULT_DB_PATH)
    if force_refresh or not resolved_db_path.exists():
        workspace = materialize_workspace(base_dir, resolved_db_path)
        metadata = get_database_status(resolved_db_path)
        metadata["loaded_from"] = "raw_files"
        return workspace, metadata

    workspace = load_workspace_from_db(resolved_db_path)
    metadata = get_database_status(resolved_db_path)
    metadata["loaded_from"] = "duckdb"
    return workspace, metadata


def materialize_workspace(base_dir: Path, db_path: Path) -> dict[str, Any]:
    workspace = build_workspace(base_dir)
    persist_workspace(workspace, base_dir, db_path)
    return workspace


def persist_workspace(workspace: dict[str, Any], base_dir: Path, db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    frames = _workspace_to_frames(workspace, base_dir)

    with duckdb.connect(str(db_path)) as connection:
        for table_name, frame in frames.items():
            connection.register("frame_view", frame)
            connection.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM frame_view")
            connection.unregister("frame_view")


def load_workspace_from_db(db_path: Path) -> dict[str, Any]:
    with duckdb.connect(str(db_path), read_only=True) as connection:
        frames = {table_name: connection.execute(f"SELECT * FROM {table_name}").df() for table_name in PERSISTED_TABLES}
        inventory = connection.execute("SELECT * FROM inventory").df().iloc[0].to_dict()
        crop_window = connection.execute("SELECT * FROM crop_window").df().iloc[0].to_dict()
        gaps = connection.execute("SELECT gap FROM gaps").df()["gap"].tolist()

    workspace = {**frames, "inventory": inventory, "crop_window": crop_window, "gaps": gaps}
    return workspace


def get_database_status(db_path: Path) -> dict[str, Any]:
    status = {
        "db_path": str(db_path),
        "exists": db_path.exists(),
        "size_mb": round(db_path.stat().st_size / 1_048_576, 2) if db_path.exists() else 0.0,
        "refreshed_at": None,
    }
    if not db_path.exists():
        return status

    try:
        with duckdb.connect(str(db_path), read_only=True) as connection:
            metadata = connection.execute("SELECT * FROM ingestion_metadata").df()
            if not metadata.empty:
                status["refreshed_at"] = metadata.iloc[0]["refreshed_at"]
                status["data_dir"] = metadata.iloc[0]["data_dir"]
    except duckdb.Error:
        pass
    return status


def main() -> None:
    parser = argparse.ArgumentParser(description="Materializa o workspace MonolithFarm em DuckDB.")
    parser.add_argument("--data-dir", required=True, help="Pasta raiz com o pacote FarmLab.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Caminho do arquivo DuckDB a ser gerado.")
    args = parser.parse_args()

    base_dir = Path(args.data_dir)
    db_path = Path(args.db_path)
    materialize_workspace(base_dir, db_path)
    status = get_database_status(db_path)
    print(f"DuckDB atualizado em {status['db_path']} ({status['size_mb']} MB)")


def _workspace_to_frames(workspace: dict[str, Any], base_dir: Path) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}

    for table_name in PERSISTED_TABLES:
        frames[table_name] = _prepare_frame_for_duckdb(workspace[table_name])

    frames["inventory"] = pd.DataFrame([workspace["inventory"]])
    frames["crop_window"] = pd.DataFrame([workspace["crop_window"]])
    frames["gaps"] = pd.DataFrame({"gap": workspace["gaps"]})
    frames["ingestion_metadata"] = pd.DataFrame(
        [
            {
                "data_dir": str(base_dir),
                "refreshed_at": pd.Timestamp.utcnow().tz_localize(None),
            }
        ]
    )
    return frames


def _prepare_frame_for_duckdb(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    for column in prepared.columns:
        series = prepared[column]
        if pd.api.types.is_datetime64tz_dtype(series):
            prepared[column] = series.dt.tz_convert("UTC").dt.tz_localize(None)
        if column in {"geometry", "centroid"}:
            prepared[column] = series.map(lambda value: value.wkt if value is not None else None)
    return prepared

