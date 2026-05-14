from __future__ import annotations

import importlib
import inspect
import json
import os
import re
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from dashboard.lineage.registry import CSV_LINEAGE_ORDER, RAW_SOURCE_GROUPS
from farmlab.io import DatasetPaths, discover_dataset_paths, load_ndvi_metadata


FINAL_OUTPUT_TABLES = [name.replace(".csv", "") for name in CSV_LINEAGE_ORDER]


@dataclass(frozen=True)
class ResolvedPaths:
    project_dir: Path
    data_dir: Path
    output_dir: Path
    profile_name: str
    config_path: Path | None


def load_resolved_paths(project_dir: Path | None = None, profile_name: str | None = None) -> ResolvedPaths:
    root = _find_project_dir(project_dir)
    _load_dotenv(root / ".env")
    config_path = _find_config_path(root)
    config = _read_paths_config(config_path) if config_path else {}
    env_profile = os.environ.get("MONOLITHFARM_PROFILE") or None
    forced_profile = profile_name or env_profile
    selected_profile = forced_profile or config.get("default_profile", "local")
    profile = config.get("profiles", {}).get(selected_profile, {})

    project_path = _resolve_path(profile.get("project_dir"), root) or root
    env_data_dir = os.environ.get("MONOLITHFARM_DATA_DIR")
    data_path = _resolve_path(env_data_dir, project_path) or _resolve_path(profile.get("data_dir"), project_path) or (project_path / "data")
    output_root = _resolve_path(profile.get("output_root"), project_path) or (project_path / "notebook_outputs")
    output_dir_candidate = output_root / "complete_ndvi"
    if forced_profile is None and selected_profile != "local" and (not data_path.exists() or not output_dir_candidate.exists()):
        # Fallback util quando o profile default aponta para WSL, mas o app esta rodando no Windows.
        local_profile = config.get("profiles", {}).get("local", {})
        local_project = _resolve_path(local_profile.get("project_dir"), root) or root
        local_data = _resolve_path(local_profile.get("data_dir"), local_project) or (local_project / "data")
        local_output_root = _resolve_path(local_profile.get("output_root"), local_project) or (local_project / "notebook_outputs")
        local_output_dir = local_output_root / "complete_ndvi"
        if local_data.exists() and (not data_path.exists() or local_output_dir.exists()):
            selected_profile = "local"
            project_path = local_project
            data_path = local_data
            output_root = local_output_root
    output_dir = (output_root / "complete_ndvi").resolve()
    return ResolvedPaths(
        project_dir=project_path.resolve(),
        data_dir=data_path.resolve(),
        output_dir=output_dir,
        profile_name=selected_profile,
        config_path=config_path,
    )


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().lstrip("\ufeff")
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def build_workspace_and_outputs(data_dir: Path, output_dir: Path, *, persist_outputs: bool = True) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    from farmlab.complete_analysis import build_complete_ndvi_workspace, save_complete_ndvi_outputs

    workspace = build_complete_ndvi_workspace(data_dir)
    paths = discover_dataset_paths(data_dir)
    workspace["ndvi_raw"] = load_ndvi_metadata(paths)
    if persist_outputs:
        save_complete_ndvi_outputs(workspace, output_dir)
    outputs = load_output_csvs(output_dir)
    return workspace, outputs


def load_output_csvs(output_dir: Path) -> dict[str, pd.DataFrame]:
    outputs: dict[str, pd.DataFrame] = {}
    for csv_name in CSV_LINEAGE_ORDER:
        path = output_dir / csv_name
        if path.exists():
            outputs[csv_name] = pd.read_csv(path)
    for path in sorted(output_dir.glob("*.csv")):
        if path.name not in outputs:
            outputs[path.name] = pd.read_csv(path)
    return outputs


def build_raw_file_catalog(data_dir: Path) -> tuple[DatasetPaths, pd.DataFrame]:
    paths = discover_dataset_paths(data_dir)
    rows: list[dict[str, Any]] = []
    for key, value in vars(paths).items():
        if value is None:
            continue
        path = Path(value)
        group_info = RAW_SOURCE_GROUPS.get(key, {"group": "Outros", "description": key})
        if path.is_dir():
            rows.append(
                {
                    "source_key": key,
                    "source_group": group_info["group"],
                    "description": group_info["description"],
                    "path": str(path),
                    "kind": "directory",
                    "exists": path.exists(),
                    "rows": pd.NA,
                    "columns": pd.NA,
                    "temporal_min": pd.NA,
                    "temporal_max": pd.NA,
                    "column_names": [],
                    "file_count": sum(1 for _ in path.glob("*")),
                    "row_count_method": "diretorio",
                }
            )
            continue
        summary = summarize_raw_file(path)
        rows.append(
            {
                "source_key": key,
                "source_group": group_info["group"],
                "description": group_info["description"],
                "path": str(path),
                "kind": "file",
                "exists": path.exists(),
                "rows": summary["rows"],
                "columns": summary["columns"],
                "temporal_min": summary["temporal_min"],
                "temporal_max": summary["temporal_max"],
                "column_names": summary["column_names"],
                "file_count": pd.NA,
                "row_count_method": summary["row_count_method"],
            }
        )
    catalog = pd.DataFrame(rows).sort_values(["source_group", "source_key"]).reset_index(drop=True)
    return paths, catalog


def summarize_raw_file(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".csv":
        try:
            frame = pd.read_csv(path, dtype=str, encoding="utf-8-sig", sep=_guess_sep(path), nrows=5000)
        except Exception:
            frame = pd.read_csv(path, dtype=str, encoding="latin1", sep=_guess_sep(path), nrows=5000)
        temporal_min, temporal_max = _infer_temporal_bounds(frame)
        return {
            "rows": _count_or_estimate_csv_rows(path),
            "columns": len(frame.columns),
            "temporal_min": temporal_min,
            "temporal_max": temporal_max,
            "column_names": list(frame.columns),
            "row_count_method": _row_count_method(path),
        }
    return {
        "rows": pd.NA,
        "columns": pd.NA,
        "temporal_min": pd.NA,
        "temporal_max": pd.NA,
        "column_names": [],
        "row_count_method": "nao_aplicavel",
    }


def load_raw_preview(path: Path, rows: int = 50) -> pd.DataFrame:
    if path.is_dir():
        return pd.DataFrame({"file_name": sorted(p.name for p in path.iterdir())[:rows]})
    if path.suffix.lower() == ".csv":
        try:
            return pd.read_csv(path, dtype=str, encoding="utf-8-sig", sep=_guess_sep(path), nrows=rows)
        except Exception:
            return pd.read_csv(path, dtype=str, encoding="latin1", sep=_guess_sep(path), nrows=rows)
    return pd.DataFrame()


def get_function_source(module_name: str, function_name: str) -> str:
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)
    return inspect.getsource(function)


def build_data_quality_checks(workspace: dict[str, Any], raw_catalog: pd.DataFrame) -> dict[str, pd.DataFrame]:
    ndvi_raw = workspace.get("ndvi_raw")
    weather_daily = workspace.get("weather_daily")
    ops_area_daily = workspace.get("ops_area_daily")

    ndvi_checks = []
    if isinstance(ndvi_raw, pd.DataFrame) and not ndvi_raw.empty:
        ndvi_checks.append(
            {
                "check": "linhas com b1_valid_pixels <= 0",
                "severity": "info",
                "value": int(pd.to_numeric(ndvi_raw["b1_valid_pixels"], errors="coerce").fillna(0).le(0).sum()),
                "rule": "cenas sem pixel válido são descartadas da análise principal",
            }
        )

    weather_checks = []
    if isinstance(weather_daily, pd.DataFrame) and not weather_daily.empty:
        temp = pd.to_numeric(weather_daily.get("temp_avg_c"), errors="coerce")
        humidity = pd.to_numeric(weather_daily.get("humidity_avg_pct"), errors="coerce")
        wind = pd.to_numeric(weather_daily.get("wind_avg_kmh"), errors="coerce")
        precipitation = pd.to_numeric(weather_daily.get("precipitation_mm"), errors="coerce")
        weather_checks.extend(
            [
                {
                    "check": "temperatura média fora de faixa plausível",
                    "severity": "warning",
                    "value": int(temp[(temp < -10) | (temp > 50)].count()),
                    "rule": "heurística física: temperatura média diária entre -10°C e 50°C",
                },
                {
                    "check": "umidade fora de 0-100%",
                    "severity": "warning",
                    "value": int(humidity[(humidity < 0) | (humidity > 100)].count()),
                    "rule": "umidade relativa deve ficar entre 0 e 100",
                },
                {
                    "check": "vento médio acima de 120 km/h",
                    "severity": "warning",
                    "value": int(wind[wind > 120].count()),
                    "rule": "heurística operacional para vento médio diário",
                },
                {
                    "check": "chuva negativa",
                    "severity": "warning",
                    "value": int(precipitation[precipitation < 0].count()),
                    "rule": "precipitação não deve ser negativa",
                },
            ]
        )

    ops_checks = []
    if isinstance(ops_area_daily, pd.DataFrame) and not ops_area_daily.empty:
        yield_kg = pd.to_numeric(ops_area_daily.get("harvest_yield_mean_kg_ha"), errors="coerce")
        pop = pd.to_numeric(ops_area_daily.get("planting_population_mean_ha"), errors="coerce")
        ops_checks.extend(
            [
                {
                    "check": "produtividade negativa",
                    "severity": "warning",
                    "value": int(yield_kg[yield_kg < 0].count()),
                    "rule": "produtividade por hectare não deve ser negativa",
                },
                {
                    "check": "população de plantio acima de 200 mil plantas/ha",
                    "severity": "info",
                    "value": int(pop[pop > 200_000].count()),
                    "rule": "heurística agronômica para identificar valores extremos",
                },
            ]
        )

    return {
        "ndvi": pd.DataFrame(ndvi_checks),
        "weather": pd.DataFrame(weather_checks),
        "operations": pd.DataFrame(ops_checks),
        "raw_catalog": raw_catalog,
    }


def _find_project_dir(project_dir: Path | None = None) -> Path:
    if project_dir is not None:
        return Path(project_dir).resolve()
    current = Path.cwd().resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("Nao foi possivel localizar pyproject.toml a partir do diretório atual.")


def _find_config_path(project_dir: Path) -> Path | None:
    candidates = [
        project_dir / ".monolithfarm.paths.json",
        project_dir / "monolithfarm.paths.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _read_paths_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_path(value: str | None, base_dir: Path) -> Path | None:
    if not value:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def _guess_sep(path: Path) -> str:
    return ";" if "EKOS" in str(path) or "Cropman" in str(path) or "Metos" in str(path) else ","


def _count_or_estimate_csv_rows(path: Path) -> int:
    if path.stat().st_size > 30 * 1024 * 1024:
        return _estimate_csv_rows(path)
    return _count_csv_rows(path)


def _row_count_method(path: Path) -> str:
    return "estimado_por_tamanho" if path.stat().st_size > 30 * 1024 * 1024 else "exato"


def _count_csv_rows(path: Path) -> int:
    # Arquivos EKOS passam de 100 MB; contar em binário por bloco mantém o
    # catálogo bruto responsivo sem carregar o CSV inteiro em memória.
    line_count = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            line_count += chunk.count(b"\n")
    return max(line_count - 1, 0)


def _estimate_csv_rows(path: Path, sample_bytes: int = 4 * 1024 * 1024) -> int:
    size = path.stat().st_size
    with path.open("rb") as handle:
        sample = handle.read(sample_bytes)
    newlines = max(sample.count(b"\n"), 1)
    average_row_bytes = max(len(sample) / newlines, 1)
    estimated_lines = int(size / average_row_bytes)
    return max(estimated_lines - 1, 0)


def _infer_temporal_bounds(frame: pd.DataFrame) -> tuple[str | None, str | None]:
    filename_dates = _dates_from_filename_column(frame)
    if not filename_dates.empty:
        return filename_dates.min().isoformat(), filename_dates.max().isoformat()

    candidates = [column for column in frame.columns if any(token in column.lower() for token in ["date", "data", "timestamp", "createdat", "week_start"])]
    mins: list[pd.Timestamp] = []
    maxs: list[pd.Timestamp] = []
    for column in candidates:
        lower = column.lower()
        if lower in {"width", "height", "count"} or "pixels" in lower:
            continue
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            if lower == "timestamp":
                parsed = pd.to_datetime(pd.to_numeric(frame[column], errors="coerce"), errors="coerce", unit="s", utc=False)
            else:
                parsed = pd.to_datetime(frame[column], errors="coerce", utc=False, dayfirst=_looks_day_first(frame[column]))
        parsed = parsed.dropna()
        parsed = parsed[(parsed.dt.year >= 2000) & (parsed.dt.year <= 2035)]
        if not parsed.empty:
            mins.append(parsed.min())
            maxs.append(parsed.max())
    if not mins:
        return None, None
    return mins and min(mins).isoformat(), maxs and max(maxs).isoformat()


def _dates_from_filename_column(frame: pd.DataFrame) -> pd.Series:
    if "filename" not in frame.columns:
        return pd.Series(dtype="datetime64[ns]")
    extracted = frame["filename"].astype(str).str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
    parsed = pd.to_datetime(extracted, errors="coerce")
    return parsed.dropna()


def _looks_day_first(series: pd.Series) -> bool:
    sample = series.dropna().astype(str).head(20)
    return bool(sample.str.contains(r"\d{1,2}/\d{1,2}/\d{4}", regex=True).any())
