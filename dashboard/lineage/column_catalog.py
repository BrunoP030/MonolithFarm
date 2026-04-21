from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import pandas as pd

from dashboard.lineage.docs_registry import SOURCE_DOCUMENTATION, column_documentation_for
from dashboard.lineage.column_lineage import raw_columns_for_feature, thresholds_for_feature
from dashboard.lineage.registry import (
    CSV_REGISTRY,
    FEATURE_REGISTRY,
    INTERMEDIATE_TABLE_REGISTRY,
    KEY_COLUMNS,
)


PreviewLoader = Callable[[Path, int], pd.DataFrame]


def build_raw_column_catalog(
    raw_catalog: pd.DataFrame,
    preview_loader: PreviewLoader,
    docs_cache: dict[str, Any] | None = None,
    *,
    sample_rows: int = 500,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    feature_usage = _feature_usage_by_source_column()
    for source in raw_catalog.itertuples(index=False):
        if getattr(source, "kind", None) != "file":
            continue
        path = Path(str(source.path))
        try:
            preview = preview_loader(path, sample_rows)
        except Exception as exc:
            rows.append(
                {
                    "source_key": source.source_key,
                    "source_group": source.source_group,
                    "file_path": str(path),
                    "column": "__erro_leitura__",
                    "dtype": "erro",
                    "null_pct_sample": pd.NA,
                    "examples": str(exc),
                    "documentation_status": "erro_ao_ler",
                    "documentation": "",
                    "practical_interpretation": "",
                    "pipeline_usage": "",
                    "usage_status": "erro",
                }
            )
            continue
        source_doc = documentation_for_source_group(source.source_group, docs_cache or {})
        for column in preview.columns:
            doc = column_documentation_for(column)
            used_by = feature_usage.get(column, [])
            usage_status = _classify_usage(column, doc.usage_status, used_by)
            rows.append(
                {
                    "source_key": source.source_key,
                    "source_group": source.source_group,
                    "file_path": str(path),
                    "column": column,
                    "dtype": str(preview[column].dtype),
                    "null_pct_sample": _null_pct(preview[column]),
                    "non_null_sample": int(preview[column].notna().sum()),
                    "unique_count_sample": int(preview[column].nunique(dropna=True)),
                    "examples": _examples(preview[column]),
                    "temporal_min_source": getattr(source, "temporal_min", pd.NA),
                    "temporal_max_source": getattr(source, "temporal_max", pd.NA),
                    "documentation_status": doc.documentation_status,
                    "documentation": doc.definition,
                    "practical_interpretation": doc.practical_interpretation,
                    "pipeline_usage": _format_pipeline_usage(doc.pipeline_usage, used_by),
                    "usage_status": usage_status,
                    "farm_docs_url": source_doc.get("farm_docs_url", ""),
                    "source_doc_status": source_doc.get("documentation_status", "sem_documentacao_externa_encontrada"),
                }
            )
    return pd.DataFrame(rows)


def build_feature_catalog() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for spec in FEATURE_REGISTRY.values():
        rows.append(
            {
                "feature": spec.name,
                "category": spec.feature_type,
                "definition": spec.definition,
                "born_table": spec.table_where_born,
                "function": f"{spec.module}.{spec.function}",
                "source_columns": ", ".join(spec.source_columns),
                "raw_columns_resolved": ", ".join(raw_columns_for_feature(spec.name)),
                "raw_sources": ", ".join(spec.raw_sources),
                "transformation": spec.transformation,
                "thresholds": " | ".join(thresholds_for_feature(spec.name)),
                "filters": " | ".join(spec.filters_involved),
                "appears_in_tables": ", ".join(spec.appears_in_tables),
                "appears_in_csvs": ", ".join(spec.appears_in_csvs),
                "hypotheses": ", ".join(spec.related_hypotheses),
                "charts": ", ".join(spec.related_charts),
            }
        )
    return pd.DataFrame(rows).sort_values(["category", "feature"]).reset_index(drop=True)


def build_workspace_column_catalog(
    workspace: dict[str, Any] | None,
    outputs: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if workspace:
        for table_name, spec in INTERMEDIATE_TABLE_REGISTRY.items():
            frame = workspace.get(table_name)
            if not isinstance(frame, pd.DataFrame):
                continue
            rows.extend(_rows_for_frame("intermediaria", table_name, frame, spec.created_columns, spec.description))
    for csv_name, frame in outputs.items():
        spec = CSV_REGISTRY.get(csv_name)
        docs = spec.column_docs if spec else {}
        for column in frame.columns:
            doc = docs.get(column)
            generated_doc = column_documentation_for(column)
            rows.append(
                {
                    "kind": "csv_final",
                    "table": csv_name,
                    "column": column,
                    "dtype": str(frame[column].dtype),
                    "null_pct_sample": _null_pct(frame[column].head(5000)),
                    "examples": _examples(frame[column].head(5000)),
                    "created_here": False,
                    "documentation": doc or generated_doc.definition,
                    "usage_status": "csv_final",
                    "description": spec.description if spec else "",
                }
            )
    return pd.DataFrame(rows)


def _rows_for_frame(kind: str, table_name: str, frame: pd.DataFrame, created_columns: list[str], description: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for column in frame.columns:
        doc = column_documentation_for(column)
        rows.append(
            {
                "kind": kind,
                "table": table_name,
                "column": column,
                "dtype": str(frame[column].dtype),
                "null_pct_sample": _null_pct(frame[column].head(5000)),
                "examples": _examples(frame[column].head(5000)),
                "created_here": column in created_columns,
                "documentation": doc.definition,
                "usage_status": _classify_usage(column, doc.usage_status, _feature_usage_by_source_column().get(column, [])),
                "description": description,
            }
        )
    return rows


def _feature_usage_by_source_column() -> dict[str, list[str]]:
    usage: dict[str, list[str]] = {}
    for feature_name, spec in FEATURE_REGISTRY.items():
        for column in spec.source_columns:
            usage.setdefault(column, []).append(feature_name)
    manual_raw_usage = {
        "b1_mean": ["ndvi_mean", "ndvi_delta", "ndvi_auc", "ndvi_mean_week", "ndvi_delta_week", "ndvi_auc_week", "low_vigor_flag", "major_drop_flag"],
        "b1_std": ["ndvi_std"],
        "b1_valid_pixels": ["ndvi_clean_filter"],
        "b1_pct_solo": ["soil_pct", "soil_pct_week", "high_soil_flag"],
        "b1_pct_veg_densa": ["dense_veg_pct", "dense_veg_pct_week"],
        "Data": ["weather_daily", "weather_weekly"],
        "Precipitação (mm)": ["precipitation_mm_week", "weather_stress_flag"],
        "Evapotranspiração (mm)": ["water_balance_mm_week", "weather_stress_flag"],
        "Temp. Média (°C)": ["temp_avg_c_week"],
        "Vel do Vento Média (km/h)": ["wind_avg_kmh_week"],
        "Umidade Rel. Média (%)": ["humidity_avg_pct_week"],
        "pestCount": ["avg_pest_count_week", "pest_risk_flag"],
        "alert": ["alert_hits_week", "pest_risk_flag"],
        "damage": ["damage_hits_week", "pest_risk_flag"],
        "AppliedDos - kg/ha": ["fert_dose_gap_abs_mean_kg_ha_week", "fert_risk_flag"],
        "Configured - kg/ha": ["fert_dose_gap_abs_mean_kg_ha_week", "fert_risk_flag"],
        "OverlapArea - ha": ["overlap_area_pct_bbox_week", "overlap_risk_flag"],
        "Duration": ["stop_duration_h_per_bbox_ha_week", "stop_risk_flag"],
        "Duration - h": ["stop_duration_h_per_bbox_ha_week", "stop_risk_flag"],
        "InvalidCommunication": ["invalid_telemetry_share_week", "telemetry_risk_flag"],
        "Alarm": ["alarm_events_week", "alert_risk_flag"],
        "Alert": ["param_alert_events_week", "alert_risk_flag"],
        "EngineTemperature - ºC": ["engine_temp_max_c_week", "engine_risk_flag"],
        "EngineRotation - rpm": ["engine_idle_share_week", "engine_risk_flag"],
        "FuelConsumption - L/h": ["fuel_zero_share_week", "engine_risk_flag"],
        "Yield - kg/ha": ["harvest_yield_mean_kg_ha"],
        "Population - ha": ["planting_population_mean_ha"],
    }
    for column, features in manual_raw_usage.items():
        usage.setdefault(column, []).extend(features)
    for key in KEY_COLUMNS:
        usage.setdefault(key, []).append("chave_de_rastreio")
    return {column: sorted(set(features)) for column, features in usage.items()}


def documentation_for_source_group(source_group: str, docs_cache: dict[str, Any]) -> dict[str, Any]:
    manual = docs_cache.get("manual_sources", {}).get(source_group)
    if manual:
        return manual
    doc = SOURCE_DOCUMENTATION.get(source_group)
    if doc:
        return {
            "title": doc.title,
            "summary": doc.summary,
            "practical_context": doc.practical_context,
            "farm_docs_url": doc.farm_docs_url,
            "documentation_status": doc.documentation_status,
            "relevant_excerpt": doc.relevant_excerpt,
        }
    return {
        "title": source_group,
        "summary": "Sem documentação externa específica vinculada automaticamente.",
        "practical_context": "",
        "farm_docs_url": "",
        "documentation_status": "sem_documentacao_externa_encontrada",
        "relevant_excerpt": "",
    }


def _classify_usage(column: str, doc_status: str, used_by: list[str]) -> str:
    if used_by:
        return "usada"
    if "ignorada" in doc_status:
        return doc_status
    if column in KEY_COLUMNS:
        return "usada_como_chave"
    return "contexto_ou_nao_usada_diretamente"


def _format_pipeline_usage(base_usage: str, used_by: list[str]) -> str:
    if used_by:
        return f"{base_usage} Usada por: {', '.join(sorted(set(used_by)))}."
    return base_usage


def _examples(series: pd.Series, *, limit: int = 5) -> str:
    values = [str(value) for value in series.dropna().astype(str).unique()[:limit]]
    return " | ".join(values)


def _null_pct(series: pd.Series) -> float:
    return float(series.isna().mean()) if len(series) else 0.0
