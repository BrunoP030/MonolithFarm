from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from dashboard.lineage.docs_registry import DRIVER_DOCUMENTATION, column_documentation_for
from dashboard.lineage.registry import (
    CSV_REGISTRY,
    FEATURE_REGISTRY,
    INTERMEDIATE_TABLE_REGISTRY,
    KEY_COLUMNS,
)


@dataclass(frozen=True)
class ColumnLineage:
    lineage_id: str
    column: str
    layer: str
    table: str
    definition: str
    raw_sources: list[str] = field(default_factory=list)
    raw_columns: list[str] = field(default_factory=list)
    upstream_columns: list[str] = field(default_factory=list)
    transformation: str = ""
    filters: list[str] = field(default_factory=list)
    joins: list[str] = field(default_factory=list)
    aggregations: list[str] = field(default_factory=list)
    thresholds: list[str] = field(default_factory=list)
    generated_by: str = ""
    python_file: str = ""
    downstream_tables: list[str] = field(default_factory=list)
    downstream_csvs: list[str] = field(default_factory=list)
    charts: list[str] = field(default_factory=list)
    hypotheses: list[str] = field(default_factory=list)
    mapping_status: str = "mapeado"
    mapping_confidence: str = "manual_auditavel"
    limitations: list[str] = field(default_factory=list)


def build_column_lineage_index(
    raw_column_catalog: pd.DataFrame,
    workspace: dict[str, Any] | None,
    outputs: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    records: list[ColumnLineage] = []
    records.extend(_raw_column_records(raw_column_catalog))
    records.extend(_feature_records())
    records.extend(_intermediate_records(workspace))
    records.extend(_final_csv_records(outputs))
    frame = pd.DataFrame([_record_to_row(record) for record in records])
    if frame.empty:
        return frame
    frame = frame.drop_duplicates(subset=["lineage_id"]).reset_index(drop=True)
    return frame.sort_values(["layer_order", "table", "column"]).drop(columns=["layer_order"]).reset_index(drop=True)


def lineage_records_for_column(
    lineage_index: pd.DataFrame,
    column: str,
    *,
    table: str | None = None,
    layer: str | None = None,
) -> pd.DataFrame:
    if lineage_index.empty:
        return lineage_index
    mask = lineage_index["column"].astype(str).eq(str(column))
    if table:
        mask &= lineage_index["table"].astype(str).eq(str(table))
    if layer:
        mask &= lineage_index["layer"].astype(str).eq(str(layer))
    return lineage_index[mask]


def lineage_detail_from_row(row: pd.Series) -> ColumnLineage:
    return ColumnLineage(
        lineage_id=str(row["lineage_id"]),
        column=str(row["column"]),
        layer=str(row["layer"]),
        table=str(row["table"]),
        definition=str(row.get("definition", "")),
        raw_sources=_split_cell(row.get("raw_sources")),
        raw_columns=_split_cell(row.get("raw_columns")),
        upstream_columns=_split_cell(row.get("upstream_columns")),
        transformation=str(row.get("transformation", "")),
        filters=_split_cell(row.get("filters")),
        joins=_split_cell(row.get("joins")),
        aggregations=_split_cell(row.get("aggregations")),
        thresholds=_split_cell(row.get("thresholds")),
        generated_by=str(row.get("generated_by", "")),
        python_file=str(row.get("python_file", "")),
        downstream_tables=_split_cell(row.get("downstream_tables")),
        downstream_csvs=_split_cell(row.get("downstream_csvs")),
        charts=_split_cell(row.get("charts")),
        hypotheses=_split_cell(row.get("hypotheses")),
        mapping_status=str(row.get("mapping_status", "")),
        mapping_confidence=str(row.get("mapping_confidence", "")),
        limitations=_split_cell(row.get("limitations")),
    )


def build_lineage_coverage_report(lineage_index: pd.DataFrame, outputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    final = lineage_index[lineage_index["layer"] == "csv_final"] if not lineage_index.empty else pd.DataFrame()
    for csv_name, frame in outputs.items():
        expected = set(frame.columns)
        mapped = final[final["table"] == csv_name] if not final.empty else pd.DataFrame()
        mapped_columns = set(mapped["column"]) if not mapped.empty else set()
        strong = (
            mapped[mapped["mapping_status"].isin(["mapeado", "mapeado_por_feature", "mapeado_por_driver", "mapeado_por_driver_dinamico", "mapeado_por_dependencia"])]
            if not mapped.empty
            else pd.DataFrame()
        )
        rows.append(
            {
                "csv": csv_name,
                "columns_total": len(expected),
                "columns_with_lineage": len(mapped_columns & expected),
                "strong_or_partial_lineage": len(set(strong["column"]) & expected) if not strong.empty else 0,
                "coverage_pct": (len(mapped_columns & expected) / len(expected)) if expected else 1.0,
                "unmapped_columns": ", ".join(sorted(expected - mapped_columns)),
            }
        )
    return pd.DataFrame(rows).sort_values(["coverage_pct", "csv"]).reset_index(drop=True)


def infer_feature_for_column(column: str, table: str = "") -> str | None:
    if column in FEATURE_REGISTRY:
        return column
    candidates = [
        column.replace("gap_", "").replace("_4_0_minus_convencional", ""),
        column.replace("_4_0", "").replace("_convencional", ""),
        column.replace("_mean", ""),
        column.replace("_rate", "_flag"),
    ]
    special = {
        "mean": "ndvi_mean",
        "max": "ndvi_mean",
        "soil_pct_mean": "soil_pct",
        "dense_veg_pct_mean": "dense_veg_pct",
        "ndvi_zscore": "ndvi_mean",
        "ndvi_robust_zscore": "ndvi_mean",
        "outlier_flag": "ndvi_mean",
        "gap_ndvi_mean_week": "ndvi_mean_week",
        "gap_ndvi_auc_week": "ndvi_auc_week",
        "gap_low_vigor_flag": "low_vigor_flag",
        "gap_major_drop_flag": "major_drop_flag",
        "gap_high_soil_flag": "high_soil_flag",
        "gap_ops_risk_flag": "ops_risk_flag",
        "problem_rate": "risk_flag_count",
        "baseline_rate": "risk_flag_count",
        "delta_pp": "risk_flag_count",
        "lift_ratio": "risk_flag_count",
        "trajectory_score": "risk_flag_count",
    }
    candidates.extend(special.get(column, "").split() if column in special else [])
    if table == "event_driver_lift.csv" and column in {"driver", "problem_rate", "baseline_rate", "delta_pp", "lift_ratio", "evidence_level"}:
        return "risk_flag_count"
    for candidate in candidates:
        if candidate in FEATURE_REGISTRY:
            return candidate
    for feature in FEATURE_REGISTRY:
        if feature in column:
            return feature
    return None


def raw_columns_for_feature(feature: str) -> list[str]:
    return _resolve_raw_columns_for_feature(feature)


def thresholds_for_feature(feature: str) -> list[str]:
    return _thresholds_for_feature(feature)


def _raw_column_records(raw_column_catalog: pd.DataFrame) -> list[ColumnLineage]:
    records: list[ColumnLineage] = []
    if raw_column_catalog.empty:
        return records
    for row in raw_column_catalog.itertuples(index=False):
        column = str(row.column)
        doc = column_documentation_for(column)
        used_by = _features_using_raw_column(column)
        records.append(
            ColumnLineage(
                lineage_id=f"raw::{row.source_key}::{column}",
                column=column,
                layer="bruto",
                table=str(row.source_key),
                definition=str(getattr(row, "documentation", "")) or doc.definition,
                raw_sources=[str(row.file_path)],
                raw_columns=[column],
                upstream_columns=[],
                transformation="Coluna lida diretamente do arquivo bruto.",
                filters=[],
                generated_by="leitura_csv",
                downstream_tables=_tables_using_raw_column(column),
                downstream_csvs=_csvs_using_features(used_by),
                charts=_charts_using_features(used_by),
                hypotheses=_hypotheses_using_features(used_by),
                mapping_status=str(getattr(row, "usage_status", "")),
                mapping_confidence="automatico_por_catalogo_bruto",
                limitations=[] if used_by else ["Uso direto nao detectado por feature; pode ser contexto, cadastro ou coluna ainda nao usada no pipeline NDVI."],
            )
        )
    return records


def _feature_records() -> list[ColumnLineage]:
    records: list[ColumnLineage] = []
    for feature_name, spec in FEATURE_REGISTRY.items():
        raw_columns = _resolve_raw_columns_for_feature(feature_name)
        thresholds = _thresholds_for_feature(feature_name)
        records.append(
            ColumnLineage(
                lineage_id=f"feature::{feature_name}",
                column=feature_name,
                layer="feature",
                table=spec.table_where_born,
                definition=spec.definition,
                raw_sources=spec.raw_sources,
                raw_columns=raw_columns,
                upstream_columns=spec.source_columns,
                transformation=spec.transformation,
                filters=spec.filters_involved,
                joins=[],
                aggregations=_aggregation_for_feature(feature_name),
                thresholds=thresholds,
                generated_by=f"{spec.module}.{spec.function}",
                python_file=spec.file_path,
                downstream_tables=spec.appears_in_tables,
                downstream_csvs=spec.appears_in_csvs,
                charts=spec.related_charts,
                hypotheses=spec.related_hypotheses,
                mapping_status="mapeado_por_feature",
                mapping_confidence="registry_manual_validado",
                limitations=_limitations_for_feature(feature_name),
            )
        )
    return records


def _intermediate_records(workspace: dict[str, Any] | None) -> list[ColumnLineage]:
    records: list[ColumnLineage] = []
    if not workspace:
        return records
    for table_name, spec in INTERMEDIATE_TABLE_REGISTRY.items():
        frame = workspace.get(table_name)
        if not isinstance(frame, pd.DataFrame):
            continue
        for column in frame.columns:
            feature = infer_feature_for_column(column, table_name)
            if feature and feature in FEATURE_REGISTRY:
                feature_spec = FEATURE_REGISTRY[feature]
                raw_columns = _resolve_raw_columns_for_feature(feature)
                definition = feature_spec.definition
                transformation = feature_spec.transformation
                thresholds = _thresholds_for_feature(feature)
                downstream_csvs = feature_spec.appears_in_csvs
                charts = feature_spec.related_charts
                hypotheses = feature_spec.related_hypotheses
                upstream = feature_spec.source_columns
                raw_sources = feature_spec.raw_sources
                status = "mapeado_por_feature"
            else:
                doc = column_documentation_for(column)
                raw_columns = [column] if column in _raw_like_columns() else []
                definition = doc.definition
                transformation = "Coluna materializada na tabela intermediaria; lineage especifica nao catalogada por feature."
                thresholds = []
                downstream_csvs = spec.related_csvs
                charts = spec.related_charts
                hypotheses = spec.related_hypotheses
                upstream = []
                raw_sources = spec.inputs
                status = "parcial_por_tabela"
            records.append(
                ColumnLineage(
                    lineage_id=f"intermediate::{table_name}::{column}",
                    column=column,
                    layer="intermediario",
                    table=table_name,
                    definition=definition,
                    raw_sources=raw_sources,
                    raw_columns=raw_columns,
                    upstream_columns=upstream,
                    transformation=transformation,
                    filters=spec.filters,
                    joins=spec.joins,
                    aggregations=spec.aggregations,
                    thresholds=thresholds,
                    generated_by=f"{spec.module}.{spec.function}",
                    python_file=spec.file_path,
                    downstream_tables=spec.downstream_tables,
                    downstream_csvs=downstream_csvs,
                    charts=charts,
                    hypotheses=hypotheses,
                    mapping_status=status,
                    mapping_confidence="feature_registry_ou_table_registry",
                    limitations=[] if feature else ["Lineage por tabela, nao por expressao exata de codigo."],
                )
            )
    return records


def _final_csv_records(outputs: dict[str, pd.DataFrame]) -> list[ColumnLineage]:
    records: list[ColumnLineage] = []
    for csv_name, frame in outputs.items():
        csv_spec = CSV_REGISTRY.get(csv_name)
        for column in frame.columns:
            if csv_name == "event_driver_lift.csv" and column in {"driver", "problem_weeks", "problem_rate", "baseline_rate", "delta_pp", "lift_ratio", "evidence_level"}:
                driver_docs = list(DRIVER_DOCUMENTATION.values())
                records.append(
                    ColumnLineage(
                        lineage_id=f"csv::{csv_name}::{column}",
                        column=column,
                        layer="csv_final",
                        table=csv_name,
                        definition=(
                            "Coluna calculada por driver. A origem exata varia por linha conforme o valor de `driver`; "
                            "cada driver aponta para uma flag real em ndvi_phase_timeline."
                        ),
                        raw_sources=_dedupe([source for doc in driver_docs for source in doc.raw_sources]),
                        raw_columns=_dedupe([doc.flag_feature for doc in driver_docs] + [source for doc in driver_docs for source in doc.source_columns]),
                        upstream_columns=_dedupe(["driver", "event_type", *[doc.flag_feature for doc in driver_docs]]),
                        transformation="build_event_driver_lift compara frequência do driver em semanas-problema contra baseline fora das semanas-problema.",
                        filters=["problem_week = low_vigor_flag ou major_drop_flag ou event_type relevante, conforme função build_event_driver_lift"],
                        joins=[],
                        aggregations=["problem_rate, baseline_rate, delta_pp e lift_ratio por comparison_pair e driver"],
                        thresholds=_dedupe([doc.rule for doc in driver_docs]),
                        generated_by=f"{csv_spec.module}.{csv_spec.function}" if csv_spec else "farmlab.ndvi_crispdm.build_event_driver_lift",
                        python_file=csv_spec.file_path if csv_spec else "farmlab/ndvi_crispdm.py",
                        downstream_tables=[],
                        downstream_csvs=[csv_name, "final_hypothesis_register.csv", "decision_summary.csv"],
                        charts=["drivers_problem_weeks"],
                        hypotheses=["H3"],
                        mapping_status="mapeado_por_driver_dinamico",
                        mapping_confidence="driver_registry_manual_validado",
                        limitations=["A coluna e calculada por linha; para saber a flag exata, filtre pelo valor da coluna driver."],
                    )
                )
                continue
            feature = infer_feature_for_column(column, csv_name)
            driver = _driver_for_column_or_csv(column, csv_name)
            if feature and feature in FEATURE_REGISTRY:
                feature_spec = FEATURE_REGISTRY[feature]
                raw_columns = _resolve_raw_columns_for_feature(feature)
                definition = csv_spec.column_docs.get(column, feature_spec.definition) if csv_spec else feature_spec.definition
                transformation = _final_column_transformation(column, feature_spec.transformation, csv_name)
                raw_sources = feature_spec.raw_sources
                upstream = [feature, *feature_spec.source_columns]
                thresholds = _thresholds_for_feature(feature)
                generated_by = f"{csv_spec.module}.{csv_spec.function}" if csv_spec else "funcao_nao_catalogada"
                python_file = csv_spec.file_path if csv_spec else ""
                downstream_csvs = [csv_name]
                charts = (csv_spec.related_charts if csv_spec else []) + feature_spec.related_charts
                hypotheses = (csv_spec.related_hypotheses if csv_spec else []) + feature_spec.related_hypotheses
                status = "mapeado_por_feature"
                confidence = "feature_registry_com_csv_registry"
                limitations = _limitations_for_feature(feature)
            elif driver:
                doc = DRIVER_DOCUMENTATION[driver]
                raw_columns = _resolve_raw_columns_for_feature(doc.flag_feature)
                definition = f"Coluna associada ao driver {driver}: {doc.definition}"
                transformation = doc.rule
                raw_sources = doc.raw_sources
                upstream = [doc.flag_feature, *doc.source_columns]
                thresholds = [doc.rule]
                generated_by = f"{csv_spec.module}.{csv_spec.function}" if csv_spec else "farmlab.ndvi_crispdm.build_event_driver_lift"
                python_file = csv_spec.file_path if csv_spec else "farmlab/ndvi_crispdm.py"
                downstream_csvs = [csv_name]
                charts = doc.charts
                hypotheses = doc.hypotheses
                status = "mapeado_por_driver"
                confidence = "driver_registry_manual_validado"
                limitations = doc.limitations
            else:
                doc = column_documentation_for(column)
                definition = csv_spec.column_docs.get(column, doc.definition) if csv_spec else doc.definition
                transformation = "Coluna final gerada a partir das dependencias do CSV; lineage exata por expressao nao catalogada."
                raw_sources = csv_spec.dependencies if csv_spec else []
                raw_columns = []
                upstream = csv_spec.dependencies if csv_spec else []
                thresholds = []
                generated_by = f"{csv_spec.module}.{csv_spec.function}" if csv_spec else "csv_exportado_pelo_fluxo_completo"
                python_file = csv_spec.file_path if csv_spec else ""
                downstream_csvs = [csv_name]
                charts = csv_spec.related_charts if csv_spec else []
                hypotheses = csv_spec.related_hypotheses if csv_spec else []
                status = "parcial_por_dependencia_csv" if csv_spec else "parcial_por_csv_exportado"
                confidence = "csv_registry_ou_csv_real_exportado"
                limitations = ["Lineage por dependencia do CSV; nao ha mapeamento coluna-a-coluna automatico para esta coluna."]
            records.append(
                ColumnLineage(
                    lineage_id=f"csv::{csv_name}::{column}",
                    column=column,
                    layer="csv_final",
                    table=csv_name,
                    definition=definition,
                    raw_sources=_dedupe(raw_sources),
                    raw_columns=_dedupe(raw_columns),
                    upstream_columns=_dedupe(upstream),
                    transformation=transformation,
                    filters=[],
                    joins=[],
                    aggregations=_csv_aggregation_hint(csv_name, column),
                    thresholds=_dedupe(thresholds),
                    generated_by=generated_by,
                    python_file=python_file,
                    downstream_tables=[],
                    downstream_csvs=_dedupe(downstream_csvs),
                    charts=_dedupe(charts),
                    hypotheses=_dedupe(hypotheses),
                    mapping_status=status,
                    mapping_confidence=confidence,
                    limitations=_dedupe(limitations),
                )
            )
    return records


def _record_to_row(record: ColumnLineage) -> dict[str, Any]:
    layer_order = {"bruto": 0, "feature": 1, "intermediario": 2, "csv_final": 3}
    return {
        "lineage_id": record.lineage_id,
        "column": record.column,
        "layer": record.layer,
        "layer_order": layer_order.get(record.layer, 99),
        "table": record.table,
        "definition": record.definition,
        "raw_sources": _join(record.raw_sources),
        "raw_columns": _join(record.raw_columns),
        "upstream_columns": _join(record.upstream_columns),
        "transformation": record.transformation,
        "filters": _join(record.filters),
        "joins": _join(record.joins),
        "aggregations": _join(record.aggregations),
        "thresholds": _join(record.thresholds),
        "generated_by": record.generated_by,
        "python_file": record.python_file,
        "downstream_tables": _join(record.downstream_tables),
        "downstream_csvs": _join(record.downstream_csvs),
        "charts": _join(record.charts),
        "hypotheses": _join(record.hypotheses),
        "mapping_status": record.mapping_status,
        "mapping_confidence": record.mapping_confidence,
        "limitations": _join(record.limitations),
    }


def _resolve_raw_columns_for_feature(feature: str) -> list[str]:
    mapping = {
        "ndvi_mean": ["b1_mean"],
        "ndvi_std": ["b1_std"],
        "soil_pct": ["b1_pct_solo"],
        "dense_veg_pct": ["b1_pct_veg_densa"],
        "ndvi_delta": ["b1_mean", "filename/date"],
        "ndvi_auc": ["b1_mean", "filename/date"],
        "ndvi_mean_week": ["b1_mean"],
        "soil_pct_week": ["b1_pct_solo"],
        "dense_veg_pct_week": ["b1_pct_veg_densa"],
        "ndvi_delta_week": ["b1_mean", "filename/date"],
        "ndvi_auc_week": ["b1_mean", "filename/date"],
        "low_vigor_flag": ["b1_mean"],
        "major_drop_flag": ["b1_mean", "filename/date"],
        "high_soil_flag": ["b1_pct_solo"],
        "weather_stress_flag": ["Metos precipitation/temperature/evapotranspiration columns"],
        "pest_risk_flag": ["traps_data counts", "traps_events alert/control/damage"],
        "fert_risk_flag": ["Fertilization dose/rate columns"],
        "overlap_risk_flag": ["Overlap geometry/area columns"],
        "stop_risk_flag": ["Stop reason duration/date columns"],
        "telemetry_risk_flag": ["InvalidCommunication"],
        "alert_risk_flag": ["Alarm/Event columns", "Parameterized alert columns"],
        "engine_risk_flag": ["Engine temperature", "Engine rotation", "Fuel consumption"],
        "ops_risk_flag": ["fert_risk_flag", "overlap_risk_flag", "stop_risk_flag", "telemetry_risk_flag", "alert_risk_flag", "engine_risk_flag"],
        "risk_flag_count": [
            "high_soil_flag",
            "weather_stress_flag",
            "pest_risk_flag",
            "fert_risk_flag",
            "overlap_risk_flag",
            "stop_risk_flag",
            "telemetry_risk_flag",
            "alert_risk_flag",
            "engine_risk_flag",
        ],
    }
    return mapping.get(feature, FEATURE_REGISTRY.get(feature).source_columns if feature in FEATURE_REGISTRY else [])


def _thresholds_for_feature(feature: str) -> list[str]:
    mapping = {
        "major_drop_flag": ["ndvi_delta_week <= -0.08"],
        "low_vigor_flag": ["ndvi_norm_week <= 0.35"],
        "high_soil_flag": ["soil_pct_week >= quantil 75%; fallback 20.0"],
        "weather_stress_flag": ["has_weather_coverage_week e water_balance_mm_week <= quantil 25%; fallback -10.0"],
        "pest_risk_flag": ["avg_pest_count_week >= quantil 75% ou alert_hits_week > 0 ou damage_hits_week > 0"],
        "fert_risk_flag": ["fert_dose_gap_abs_mean_kg_ha_week >= quantil 75%; fallback 150.0"],
        "overlap_risk_flag": ["overlap_area_pct_bbox_week >= quantil 75%; fallback 0.04"],
        "stop_risk_flag": ["stop_duration_h_per_bbox_ha_week >= quantil 75%; fallback 0.02"],
        "telemetry_risk_flag": ["invalid_telemetry_share_week >= quantil 75%; fallback 0.05"],
        "alert_risk_flag": ["alarm_events_week > 0 ou param_alert_events_week > 0"],
        "engine_risk_flag": ["engine_temp_max_c_week >= quantil 90% ou engine_idle_share_week >= quantil 75% ou fuel_zero_share_week >= 0.4"],
        "ops_risk_flag": ["qualquer flag operacional verdadeira"],
        "risk_flag_count": ["soma das 9 flags de risco"],
    }
    return mapping.get(feature, [])


def _aggregation_for_feature(feature: str) -> list[str]:
    if feature.endswith("_week") or feature in {"ndvi_mean_week", "soil_pct_week", "ndvi_auc_week"}:
        return ["agregacao semanal por season_id e week_start"]
    if feature.endswith("_flag") or feature == "risk_flag_count":
        return ["flag/score calculado na timeline semanal"]
    if feature in {"ndvi_delta", "ndvi_auc"}:
        return ["calculo temporal ordenado por season_id e date"]
    return []


def _csv_aggregation_hint(csv_name: str, column: str) -> list[str]:
    if csv_name == "pair_weekly_gaps.csv":
        return ["pivot/merge semanal entre tratamento 4.0 e convencional; gap = 4.0 - convencional"]
    if csv_name == "pair_effect_tests.csv":
        return ["teste pareado por comparison_pair e metric; vantagem_4_0 orientada por higher_is_better"]
    if csv_name == "event_driver_lift.csv":
        return ["comparacao de frequencia do driver em semanas problema versus baseline"]
    if csv_name == "weekly_correlations.csv":
        return ["correlacao Pearson/Spearman entre feature semanal e alvo"]
    if csv_name == "ndvi_stats_by_area.csv":
        return ["resumo estatistico por area_label/season_id"]
    if csv_name == "ndvi_outliers.csv":
        return ["z-score e z-score robusto por serie NDVI da area"]
    return []


def _final_column_transformation(column: str, feature_transformation: str, csv_name: str) -> str:
    if column.startswith("gap_"):
        return f"{feature_transformation}; no CSV final vira gap 4.0 - convencional."
    if column.endswith("_4_0") or column.endswith("_convencional"):
        return f"{feature_transformation}; no CSV final e separado por tratamento."
    return feature_transformation


def _driver_for_column_or_csv(column: str, csv_name: str) -> str | None:
    if column in DRIVER_DOCUMENTATION:
        return column
    if csv_name == "event_driver_lift.csv":
        return "solo_exposto" if column in {"problem_weeks", "problem_rate", "baseline_rate", "delta_pp", "lift_ratio"} else None
    for driver, doc in DRIVER_DOCUMENTATION.items():
        if driver in column or doc.flag_feature in column:
            return driver
    return None


def _features_using_raw_column(column: str) -> list[str]:
    features = []
    for feature_name in FEATURE_REGISTRY:
        if column in _resolve_raw_columns_for_feature(feature_name) or column in FEATURE_REGISTRY[feature_name].source_columns:
            features.append(feature_name)
    return features


def _tables_using_raw_column(column: str) -> list[str]:
    tables = []
    for feature_name in _features_using_raw_column(column):
        tables.extend(FEATURE_REGISTRY[feature_name].appears_in_tables)
    return _dedupe(tables)


def _csvs_using_features(features: list[str]) -> list[str]:
    csvs: list[str] = []
    for feature in features:
        if feature in FEATURE_REGISTRY:
            csvs.extend(FEATURE_REGISTRY[feature].appears_in_csvs)
    return _dedupe(csvs)


def _charts_using_features(features: list[str]) -> list[str]:
    charts: list[str] = []
    for feature in features:
        if feature in FEATURE_REGISTRY:
            charts.extend(FEATURE_REGISTRY[feature].related_charts)
    return _dedupe(charts)


def _hypotheses_using_features(features: list[str]) -> list[str]:
    hypotheses: list[str] = []
    for feature in features:
        if feature in FEATURE_REGISTRY:
            hypotheses.extend(FEATURE_REGISTRY[feature].related_hypotheses)
    return _dedupe(hypotheses)


def _limitations_for_feature(feature: str) -> list[str]:
    if feature.endswith("_flag") or feature == "risk_flag_count":
        return ["Flag usa limiar operacional/quantil do pacote; deve ser interpretada como evidencia associativa."]
    if feature.startswith("ndvi"):
        return ["NDVI depende de cenas validas; linhas com b1_valid_pixels <= 0 sao descartadas antes da analise."]
    return []


def _raw_like_columns() -> set[str]:
    columns = set()
    for feature in FEATURE_REGISTRY:
        columns.update(_resolve_raw_columns_for_feature(feature))
    return columns


def _join(values: list[str]) -> str:
    return " | ".join(_dedupe([str(value) for value in values if str(value).strip()]))


def _split_cell(value: Any) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [part.strip() for part in str(value).split("|") if part.strip()]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result
