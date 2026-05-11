from __future__ import annotations

import json
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from dashboard.lineage.column_lineage import build_column_lineage_index, build_lineage_coverage_report


MANIFEST_VERSION = "lineage_manifest_v1"

STRONG_MAPPING_STATUSES = {
    "mapeado",
    "mapeado_por_feature",
    "mapeado_por_driver",
    "mapeado_por_driver_dinamico",
    "mapeado_por_driver_entidade",
    "mapeado_por_dependencia",
}

AUDITABLE_MAPPING_STATUSES = STRONG_MAPPING_STATUSES | {
    "mapeado_por_tabela",
    "parcial_por_tabela",
    "parcial_por_dependencia_csv",
    "parcial_por_csv_exportado",
}

CRITICAL_LINEAGE_TARGETS = [
    {
        "target": "solo_exposto",
        "lineage_id": "driver::solo_exposto",
        "required_raw_columns": ["b1_pct_solo"],
        "required_upstream_columns": ["soil_pct_week", "high_soil_flag"],
        "required_outputs": ["event_driver_lift.csv"],
        "required_hypotheses": ["H3", "H4"],
        "business_reason": "Explica semanas com solo exposto relevante antes de interpretar vantagem ou perda do 4.0.",
    },
    {
        "target": "ndvi_mean_week",
        "lineage_id": "feature::ndvi_mean_week",
        "required_raw_columns": ["b1_mean"],
        "required_upstream_columns": ["ndvi_mean"],
        "required_outputs": ["pair_weekly_gaps.csv", "pair_effect_tests.csv"],
        "required_hypotheses": ["H1"],
        "business_reason": "Metrica central para comparar vigor temporal entre area 4.0 e convencional.",
    },
    {
        "target": "pest_risk_flag",
        "lineage_id": "feature::pest_risk_flag",
        "required_raw_columns": ["traps_data", "traps_events"],
        "required_upstream_columns": ["avg_pest_count_week"],
        "required_outputs": ["event_driver_lift.csv", "weekly_correlations.csv"],
        "required_hypotheses": ["H2", "H3", "H4"],
        "business_reason": "Mostra se pragas podem explicar semanas-problema e aparente baixa resposta do 4.0.",
    },
    {
        "target": "harvest_yield_mean_kg_ha",
        "lineage_id": "feature::harvest_yield_mean_kg_ha",
        "required_raw_columns": ["Yield - kg/ha"],
        "required_upstream_columns": [],
        "required_outputs": ["pair_weekly_gaps.csv"],
        "required_hypotheses": ["H4"],
        "business_reason": "Conecta produtividade registrada a leitura final de desempenho, quando ha cobertura de colheita.",
    },
    {
        "target": "overlap_area_pct_bbox_week",
        "lineage_id": "feature::overlap_area_pct_bbox_week",
        "required_raw_columns": ["OverlapArea - ha"],
        "required_upstream_columns": ["OverlapArea - ha"],
        "required_outputs": ["event_driver_lift.csv", "weekly_correlations.csv"],
        "required_hypotheses": ["H3", "H4"],
        "business_reason": "Explica risco operacional por sobreposicao relativa a area monitorada.",
    },
    {
        "target": "invalid_telemetry_share_week",
        "lineage_id": "feature::invalid_telemetry_share_week",
        "required_raw_columns": ["InvalidCommunication"],
        "required_upstream_columns": ["InvalidCommunication"],
        "required_outputs": ["event_driver_lift.csv", "weekly_correlations.csv"],
        "required_hypotheses": ["H3", "H4"],
        "business_reason": "Audita falha de telemetria, ponto importante para entender tecnologia 4.0 sem resultado claro.",
    },
    {
        "target": "falha_de_dose_na_adubacao",
        "lineage_id": "driver::falha_de_dose_na_adubacao",
        "required_raw_columns": ["AppliedDos - kg/ha", "Configured - kg/ha"],
        "required_upstream_columns": ["fert_risk_flag", "fert_dose_gap_abs_mean_kg_ha_week"],
        "required_outputs": ["event_driver_lift.csv"],
        "required_hypotheses": ["H3", "H4"],
        "business_reason": "Mostra se desvio de dose operacional entrou como driver das semanas ruins.",
    },
    {
        "target": "estresse_climatico",
        "lineage_id": "driver::estresse_climatico",
        "required_raw_columns": ["precipitation_mm", "evapotranspiration_mm"],
        "required_upstream_columns": ["weather_stress_flag", "water_balance_mm_week"],
        "required_outputs": ["event_driver_lift.csv"],
        "required_hypotheses": ["H3", "H4"],
        "business_reason": "Separa efeito macroclimatico de falha operacional ou baixa efetividade do 4.0.",
    },
    {
        "target": "tempo_parado",
        "lineage_id": "driver::tempo_parado",
        "required_raw_columns": ["Duration - h"],
        "required_upstream_columns": ["stop_risk_flag", "stop_duration_h_per_bbox_ha_week"],
        "required_outputs": ["event_driver_lift.csv"],
        "required_hypotheses": ["H3", "H4"],
        "business_reason": "Ajuda a explicar perdas por eficiencia operacional e paradas.",
    },
]

PROJECT_LIMITATIONS = [
    "Drivers sao associacoes observadas nas semanas-problema; nao sao prova causal fechada.",
    "O pacote local usa ndvi_metadata.csv e JPGs de apoio; GeoTIFF/pixel original nao esta presente.",
    "A comparacao economica depende de custos por hectare/operacao externos e pode ficar inconclusiva.",
    "A meteorologia representa contexto macro e pode nao capturar variacao microespacial entre areas.",
    "Solo nao tem chave espacial direta para cada talhao no pipeline atual.",
]


def build_lineage_manifest(
    raw_column_catalog: pd.DataFrame,
    workspace: dict[str, Any] | None,
    outputs: dict[str, pd.DataFrame],
    quality_summary: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Monta o pacote canonico de auditoria coluna-a-coluna.

    O manifesto e a fonte operacional para UI, CLI e testes. Ele nao muda a
    analise; apenas materializa o que ja pode ser provado pelo registry,
    pelos CSVs finais e pelas tabelas reais carregadas.
    """

    lineage_index = build_column_lineage_index(raw_column_catalog, workspace, outputs)
    coverage = build_lineage_coverage_report(lineage_index, outputs)
    critical = build_critical_lineage_report(lineage_index)
    gates = build_lineage_acceptance_gates(lineage_index, coverage, critical, outputs, quality_summary)
    summary = build_manifest_summary(lineage_index, coverage, critical, gates, raw_column_catalog, outputs)
    return {
        "metadata": {
            "manifest_version": MANIFEST_VERSION,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": "MonolithFarm NDVI lineage",
        },
        "summary": summary,
        "lineage_index": lineage_index,
        "coverage": coverage,
        "critical_targets": critical,
        "acceptance_gates": gates,
        "project_limitations": pd.DataFrame({"limitation": PROJECT_LIMITATIONS}),
    }


def build_manifest_summary(
    lineage_index: pd.DataFrame,
    coverage: pd.DataFrame,
    critical: pd.DataFrame,
    gates: pd.DataFrame,
    raw_column_catalog: pd.DataFrame,
    outputs: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    final_records = _final_records(lineage_index)
    strong_final = _status_share(final_records, STRONG_MAPPING_STATUSES)
    auditable_final = _status_share(final_records, AUDITABLE_MAPPING_STATUSES)
    all_gates_ok = bool(not gates.empty and gates["status"].isin(["ok", "aceito_com_limitacao"]).all())
    return pd.DataFrame(
        [
            {
                "manifest_version": MANIFEST_VERSION,
                "lineage_records": len(lineage_index),
                "raw_columns_cataloged": len(raw_column_catalog),
                "final_csvs_loaded": len(outputs),
                "final_columns_with_records": len(final_records),
                "strong_final_lineage_pct": strong_final,
                "auditable_final_lineage_pct": auditable_final,
                "csv_coverage_min_pct": float(coverage["coverage_pct"].min()) if not coverage.empty else 0.0,
                "critical_targets_ok_pct": float(critical["status"].eq("ok").mean()) if not critical.empty else 0.0,
                "acceptance_gates_ok": all_gates_ok,
            }
        ]
    )


def build_critical_lineage_report(lineage_index: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for target in CRITICAL_LINEAGE_TARGETS:
        row = _record_by_lineage_id(lineage_index, target["lineage_id"])
        checks = {
            "record_found": row is not None,
            "raw_columns_ok": _cell_contains_all(row, "raw_columns", target["required_raw_columns"]),
            "upstream_columns_ok": _cell_contains_all(row, "upstream_columns", target["required_upstream_columns"]),
            "outputs_ok": _lineage_mentions_all(row, lineage_index, target["target"], target["required_outputs"], ["downstream_csvs", "table"]),
            "hypotheses_ok": _lineage_mentions_all(row, lineage_index, target["target"], target["required_hypotheses"], ["hypotheses"]),
            "transformation_ok": bool(row is not None and str(row.get("transformation", "")).strip()),
            "generated_by_ok": bool(row is not None and str(row.get("generated_by", "")).strip()),
        }
        status = "ok" if all(checks.values()) else "falha"
        rows.append(
            {
                "target": target["target"],
                "lineage_id": target["lineage_id"],
                "status": status,
                "business_reason": target["business_reason"],
                "missing_checks": ", ".join(name for name, ok in checks.items() if not ok),
                "required_raw_columns": " | ".join(target["required_raw_columns"]),
                "required_upstream_columns": " | ".join(target["required_upstream_columns"]),
                "required_outputs": " | ".join(target["required_outputs"]),
                "required_hypotheses": " | ".join(target["required_hypotheses"]),
            }
        )
    return pd.DataFrame(rows)


def build_lineage_acceptance_gates(
    lineage_index: pd.DataFrame,
    coverage: pd.DataFrame,
    critical: pd.DataFrame,
    outputs: dict[str, pd.DataFrame],
    quality_summary: pd.DataFrame | None = None,
) -> pd.DataFrame:
    final_records = _final_records(lineage_index)
    min_coverage = float(coverage["coverage_pct"].min()) if not coverage.empty else 0.0
    auditable_final_pct = _status_share(final_records, AUDITABLE_MAPPING_STATUSES)
    strong_final_pct = _status_share(final_records, STRONG_MAPPING_STATUSES)
    critical_ok_pct = float(critical["status"].eq("ok").mean()) if not critical.empty else 0.0
    quality_rules = 0 if quality_summary is None else len(quality_summary)
    quality_attention = 0
    if quality_summary is not None and not quality_summary.empty and "affected_rows" in quality_summary:
        quality_attention = int(pd.to_numeric(quality_summary["affected_rows"], errors="coerce").fillna(0).gt(0).sum())

    gates = [
        {
            "gate_id": "G1_final_csv_coverage",
            "status": "ok" if outputs and min_coverage >= 1.0 else "falha",
            "metric": min_coverage,
            "evidence": f"Cobertura minima por CSV final: {min_coverage:.0%}.",
            "required_action": "Toda coluna de CSV final deve aparecer no indice de lineage.",
        },
        {
            "gate_id": "G2_final_columns_auditable",
            "status": "ok" if len(final_records) and auditable_final_pct >= 1.0 else "falha",
            "metric": auditable_final_pct,
            "evidence": f"Colunas finais auditaveis: {auditable_final_pct:.0%}.",
            "required_action": "Mapear qualquer coluna final sem status auditavel; se nao houver prova, marcar limitacao explicita.",
        },
        {
            "gate_id": "G3_strong_or_documented_lineage",
            "status": "ok" if strong_final_pct >= 0.60 else "aceito_com_limitacao",
            "metric": strong_final_pct,
            "evidence": f"Mapeamento forte por feature/driver: {strong_final_pct:.0%}.",
            "required_action": "Elevar colunas criticas para mapeamento forte; colunas auxiliares podem ficar parciais se documentadas.",
        },
        {
            "gate_id": "G4_critical_targets",
            "status": "ok" if critical_ok_pct >= 1.0 else "falha",
            "metric": critical_ok_pct,
            "evidence": f"Alvos criticos aprovados: {critical_ok_pct:.0%}.",
            "required_action": "Corrigir rastreabilidade dos alvos que sustentam a historia 4.0 vs convencional.",
        },
        {
            "gate_id": "G5_quality_rules_visible",
            "status": "ok" if quality_rules > 0 else "falha",
            "metric": float(quality_rules),
            "evidence": f"{quality_rules} regras de qualidade calculadas; {quality_attention} com linhas afetadas.",
            "required_action": "Expor regras de qualidade e exemplos antes de fechar conclusao executiva.",
        },
        {
            "gate_id": "G6_known_limitations_visible",
            "status": "aceito_com_limitacao",
            "metric": float(len(PROJECT_LIMITATIONS)),
            "evidence": f"{len(PROJECT_LIMITATIONS)} limitacoes estruturais devem aparecer na documentacao e na UI.",
            "required_action": "Nao vender causalidade, economia fechada ou raster pixel-a-pixel sem dados correspondentes.",
        },
    ]
    return pd.DataFrame(gates)


def export_lineage_manifest(
    output_dir: Path,
    raw_column_catalog: pd.DataFrame,
    workspace: dict[str, Any] | None,
    outputs: dict[str, pd.DataFrame],
    quality_summary: pd.DataFrame | None = None,
) -> dict[str, Path]:
    manifest = build_lineage_manifest(raw_column_catalog, workspace, outputs, quality_summary)
    output_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "lineage_manifest_csv": output_dir / "lineage_manifest.csv",
        "lineage_manifest_json": output_dir / "lineage_manifest.json",
        "lineage_coverage": output_dir / "lineage_coverage.csv",
        "lineage_critical_targets": output_dir / "lineage_critical_targets.csv",
        "lineage_acceptance_gates": output_dir / "lineage_acceptance_gates.csv",
        "lineage_summary": output_dir / "lineage_summary.csv",
    }
    manifest["lineage_index"].to_csv(files["lineage_manifest_csv"], index=False, encoding="utf-8-sig")
    manifest["coverage"].to_csv(files["lineage_coverage"], index=False, encoding="utf-8-sig")
    manifest["critical_targets"].to_csv(files["lineage_critical_targets"], index=False, encoding="utf-8-sig")
    manifest["acceptance_gates"].to_csv(files["lineage_acceptance_gates"], index=False, encoding="utf-8-sig")
    manifest["summary"].to_csv(files["lineage_summary"], index=False, encoding="utf-8-sig")
    files["lineage_manifest_json"].write_text(json.dumps(_manifest_to_jsonable(manifest), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return files


def manifest_to_json_bytes(manifest: dict[str, Any]) -> bytes:
    return json.dumps(_manifest_to_jsonable(manifest), ensure_ascii=False, indent=2, default=str).encode("utf-8")


def _manifest_to_jsonable(manifest: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in manifest.items():
        if isinstance(value, pd.DataFrame):
            result[key] = _records(value)
        else:
            result[key] = value
    return result


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    clean = frame.replace({pd.NA: None})
    clean = clean.where(pd.notna(clean), None)
    return clean.to_dict(orient="records")


def _final_records(lineage_index: pd.DataFrame) -> pd.DataFrame:
    if lineage_index.empty or "layer" not in lineage_index:
        return pd.DataFrame()
    return lineage_index[lineage_index["layer"].eq("csv_final")].copy()


def _status_share(frame: pd.DataFrame, statuses: set[str]) -> float:
    if frame.empty or "mapping_status" not in frame:
        return 0.0
    return float(frame["mapping_status"].isin(statuses).mean())


def _record_by_lineage_id(lineage_index: pd.DataFrame, lineage_id: str) -> pd.Series | None:
    if lineage_index.empty or "lineage_id" not in lineage_index:
        return None
    rows = lineage_index[lineage_index["lineage_id"].eq(lineage_id)]
    if rows.empty:
        return None
    return rows.iloc[0]


def _cell_contains_all(row: pd.Series | None, column: str, required: list[str]) -> bool:
    if not required:
        return True
    if row is None or column not in row:
        return False
    text = str(row.get(column, "")).lower()
    return all(_normalise_token(token) in _normalise_token(text) for token in required)


def _lineage_mentions_all(
    row: pd.Series | None,
    lineage_index: pd.DataFrame,
    target: str,
    required: list[str],
    columns: list[str],
) -> bool:
    if not required:
        return True
    haystack_parts: list[str] = []
    if row is not None:
        haystack_parts.extend(str(row.get(column, "")) for column in columns)
    if not lineage_index.empty:
        target_rows = lineage_index[
            lineage_index.astype(str).agg(" ".join, axis=1).str.contains(str(target), case=False, na=False)
        ]
        if not target_rows.empty:
            haystack_parts.extend(target_rows.astype(str).agg(" ".join, axis=1).tolist())
    haystack = _normalise_token(" | ".join(haystack_parts))
    return all(_normalise_token(token) in haystack for token in required)


def _normalise_token(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value).lower())
    return "".join(char for char in text if not unicodedata.combining(char))
