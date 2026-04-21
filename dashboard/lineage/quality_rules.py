from __future__ import annotations

from typing import Any

import pandas as pd


def run_quality_rules(
    workspace: dict[str, Any] | None,
    outputs: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    workspace = workspace or {}
    outputs = outputs or {}
    rows: list[dict[str, Any]] = []
    examples: dict[str, pd.DataFrame] = {}

    def add_rule(
        *,
        rule_id: str,
        domain: str,
        severity: str,
        dataset: str,
        rule: str,
        reason: str,
        impact: str,
        affected: pd.DataFrame,
    ) -> None:
        affected = affected.copy()
        examples[rule_id] = affected.head(50)
        rows.append(
            {
                "rule_id": rule_id,
                "domain": domain,
                "severity": severity,
                "dataset": dataset,
                "affected_rows": int(len(affected)),
                "status": "ok" if affected.empty else "atenção",
                "rule": rule,
                "reason": reason,
                "impact": impact,
            }
        )

    ndvi_raw = workspace.get("ndvi_raw")
    if isinstance(ndvi_raw, pd.DataFrame) and not ndvi_raw.empty:
        add_rule(
            rule_id="ndvi_b1_valid_pixels_zero",
            domain="NDVI",
            severity="info",
            dataset="ndvi_raw",
            rule="b1_valid_pixels <= 0",
            reason="Cenas sem pixel NDVI válido não representam vigor útil.",
            impact="Essas linhas são descartadas antes de ndvi_clean.",
            affected=ndvi_raw[_numeric(ndvi_raw, "b1_valid_pixels").fillna(0).le(0)] if "b1_valid_pixels" in ndvi_raw else pd.DataFrame(),
        )
        add_rule(
            rule_id="ndvi_b1_mean_outside_range",
            domain="NDVI",
            severity="warning",
            dataset="ndvi_raw",
            rule="b1_mean fora de [-1, 1]",
            reason="NDVI físico deve ficar no intervalo [-1, 1].",
            impact="Valores fora da faixa indicam erro de escala ou extração.",
            affected=ndvi_raw[_numeric(ndvi_raw, "b1_mean").lt(-1) | _numeric(ndvi_raw, "b1_mean").gt(1)] if "b1_mean" in ndvi_raw else pd.DataFrame(),
        )
        pct_columns = [column for column in ndvi_raw.columns if column.startswith("b1_pct_")]
        for column in pct_columns:
            values = _numeric(ndvi_raw, column)
            add_rule(
                rule_id=f"ndvi_{column}_outside_pct",
                domain="NDVI",
                severity="warning",
                dataset="ndvi_raw",
                rule=f"{column} fora de 0-100%",
                reason="Percentuais de classes de cobertura devem ficar entre 0 e 100.",
                impact="Afeta leitura de solo exposto e vegetação densa.",
                affected=ndvi_raw[values.lt(0) | values.gt(100)],
            )

    ndvi_clean = workspace.get("ndvi_clean")
    if isinstance(ndvi_clean, pd.DataFrame) and not ndvi_clean.empty:
        add_rule(
            rule_id="ndvi_clean_missing_keys",
            domain="NDVI",
            severity="error",
            dataset="ndvi_clean",
            rule="season_id/date/ndvi_mean nulos",
            reason="Sem essas chaves não há rastreio temporal confiável.",
            impact="Linhas afetadas comprometem agregações semanais.",
            affected=_missing_any(ndvi_clean, ["season_id", "date", "ndvi_mean"]),
        )

    weather_daily = workspace.get("weather_daily")
    if isinstance(weather_daily, pd.DataFrame) and not weather_daily.empty:
        add_rule(
            rule_id="weather_temp_avg_implausible",
            domain="Clima",
            severity="warning",
            dataset="weather_daily",
            rule="temp_avg_c fora de [-10, 50]",
            reason="Faixa física plausível para temperatura média diária na região.",
            impact="Pode distorcer drivers de estresse climático.",
            affected=weather_daily[_numeric(weather_daily, "temp_avg_c").lt(-10) | _numeric(weather_daily, "temp_avg_c").gt(50)] if "temp_avg_c" in weather_daily else pd.DataFrame(),
        )
        add_rule(
            rule_id="weather_humidity_outside_pct",
            domain="Clima",
            severity="warning",
            dataset="weather_daily",
            rule="humidity_avg_pct fora de 0-100%",
            reason="Umidade relativa é percentual.",
            impact="Indica erro de escala ou sensor.",
            affected=weather_daily[_numeric(weather_daily, "humidity_avg_pct").lt(0) | _numeric(weather_daily, "humidity_avg_pct").gt(100)] if "humidity_avg_pct" in weather_daily else pd.DataFrame(),
        )
        add_rule(
            rule_id="weather_wind_implausible",
            domain="Clima",
            severity="warning",
            dataset="weather_daily",
            rule="wind_avg_kmh > 120",
            reason="Vento médio diário acima desse valor é improvável para operação agrícola normal.",
            impact="Pode gerar interpretação incorreta de clima operacional.",
            affected=weather_daily[_numeric(weather_daily, "wind_avg_kmh").gt(120)] if "wind_avg_kmh" in weather_daily else pd.DataFrame(),
        )
        precipitation = _numeric(weather_daily, "precipitation_mm")
        add_rule(
            rule_id="weather_precipitation_negative_or_extreme",
            domain="Clima",
            severity="warning",
            dataset="weather_daily",
            rule="precipitation_mm < 0 ou > 300 mm/dia",
            reason="Chuva negativa é impossível; valores diários extremos precisam ser auditados.",
            impact="Afeta water_balance_mm_week e weather_stress_flag.",
            affected=weather_daily[precipitation.lt(0) | precipitation.gt(300)] if "precipitation_mm" in weather_daily else pd.DataFrame(),
        )

    ops = workspace.get("ops_area_daily")
    if isinstance(ops, pd.DataFrame) and not ops.empty:
        yield_values = _numeric(ops, "harvest_yield_mean_kg_ha")
        add_rule(
            rule_id="ops_yield_negative_or_extreme",
            domain="Operação",
            severity="warning",
            dataset="ops_area_daily",
            rule="harvest_yield_mean_kg_ha < 0 ou > 25000",
            reason="Produtividade negativa é impossível e valores muito altos pedem auditoria.",
            impact="Afeta qualquer tentativa de comparar NDVI com produtividade.",
            affected=ops[yield_values.lt(0) | yield_values.gt(25000)] if "harvest_yield_mean_kg_ha" in ops else pd.DataFrame(),
        )
        population = _numeric(ops, "planting_population_mean_ha")
        add_rule(
            rule_id="ops_population_implausible",
            domain="Operação",
            severity="info",
            dataset="ops_area_daily",
            rule="planting_population_mean_ha <= 0 ou > 200000",
            reason="População por hectare fora dessa faixa pode representar manobra, ausência de operação ou erro de escala.",
            impact="Afeta interpretação de stand e falhas de plantio.",
            affected=ops[population.le(0) | population.gt(200000)] if "planting_population_mean_ha" in ops else pd.DataFrame(),
        )

    for name, frame in {**workspace, **outputs}.items():
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            continue
        empty_columns = [column for column in frame.columns if frame[column].isna().all()]
        if empty_columns:
            affected = pd.DataFrame({"empty_column": empty_columns})
        else:
            affected = pd.DataFrame(columns=["empty_column"])
        add_rule(
            rule_id=f"{name}_empty_columns",
            domain="Estrutura",
            severity="info",
            dataset=str(name),
            rule="colunas completamente vazias",
            reason="Colunas vazias podem ser esperadas por fonte ausente, mas precisam ficar visíveis.",
            impact="Indica lacuna de cobertura ou campo não preenchido.",
            affected=affected,
        )

    summary = pd.DataFrame(rows)
    if not summary.empty:
        severity_order = {"error": 0, "warning": 1, "info": 2}
        summary["_severity_order"] = summary["severity"].map(severity_order).fillna(99)
        summary = summary.sort_values(["_severity_order", "affected_rows"], ascending=[True, False]).drop(columns=["_severity_order"])
    return summary.reset_index(drop=True), examples


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(dtype="float64", index=frame.index)
    return pd.to_numeric(frame[column], errors="coerce")


def _missing_any(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    available = [column for column in columns if column in frame.columns]
    if not available:
        return pd.DataFrame()
    mask = frame[available].isna().any(axis=1)
    return frame[mask]
