from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pandas as pd

from farmlab.io import build_season_geometries, discover_dataset_paths, load_layer_map, load_ndvi_metadata
from farmlab.pairwise import assign_centroid_rows_to_seasons, build_phase1_workspace


NDVI_DEEPDIVE_OUTPUT_TABLES = [
    "ops_support_daily",
    "ops_support_weekly",
    "ndvi_phase_timeline",
    "ndvi_events",
    "ndvi_pair_diagnostics",
    "ndvi_outlook",
]


def build_ndvi_deep_dive_workspace(base_dir: Path, manual_mapping: pd.DataFrame | None = None) -> dict[str, Any]:
    phase1 = build_phase1_workspace(base_dir, manual_mapping)
    paths = discover_dataset_paths(base_dir)
    seasons = build_season_geometries(load_ndvi_metadata(paths))
    area_metadata = phase1["area_metadata"].merge(seasons[["season_id", "bbox_area_ha"]], on="season_id", how="left")

    ops_support_daily = build_ops_support_daily(paths, seasons, area_metadata)
    ops_support_weekly = build_ops_support_weekly(ops_support_daily)
    ndvi_phase_timeline = build_ndvi_phase_timeline(
        pairwise_weekly_features=phase1["pairwise_weekly_features"],
        ops_support_weekly=ops_support_weekly,
    )
    ndvi_events = build_ndvi_events(ndvi_phase_timeline)
    ndvi_pair_diagnostics = build_ndvi_pair_diagnostics(
        ndvi_phase_timeline=ndvi_phase_timeline,
        area_inventory=phase1["area_inventory"],
        hypothesis_matrix=phase1["hypothesis_matrix"],
    )
    ndvi_outlook = build_ndvi_outlook(
        ndvi_phase_timeline=ndvi_phase_timeline,
        ndvi_pair_diagnostics=ndvi_pair_diagnostics,
    )
    deep_dive_gaps = list_ndvi_deepdive_gaps(
        phase1_gaps=phase1.get("gaps", []),
        paths_summary=paths,
        ndvi_phase_timeline=ndvi_phase_timeline,
    )

    return {
        **phase1,
        "ops_support_daily": ops_support_daily,
        "ops_support_weekly": ops_support_weekly,
        "ndvi_phase_timeline": ndvi_phase_timeline,
        "ndvi_events": ndvi_events,
        "ndvi_pair_diagnostics": ndvi_pair_diagnostics,
        "ndvi_outlook": ndvi_outlook,
        "deep_dive_gaps": deep_dive_gaps,
    }


def save_ndvi_deep_dive_outputs(workspace: dict[str, Any], output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for table_name in NDVI_DEEPDIVE_OUTPUT_TABLES:
        frame = workspace.get(table_name)
        if not isinstance(frame, pd.DataFrame):
            continue
        path = output_dir / f"{table_name}.csv"
        frame.to_csv(path, index=False)
        written.append(path)
    return written


def build_ops_support_daily(paths, seasons: pd.DataFrame, area_metadata: pd.DataFrame) -> pd.DataFrame:
    summaries: list[pd.DataFrame] = []
    layer_specs = [
        {
            "path": paths.alarm_layer,
            "numeric_columns": [],
            "builder": _summarize_alarm_daily,
        },
        {
            "path": paths.parameterized_alert_layer,
            "numeric_columns": ["Duration", "Valor"],
            "builder": _summarize_parameterized_alert_daily,
        },
        {
            "path": paths.telemetry_communication_layer,
            "numeric_columns": [],
            "builder": _summarize_telemetry_daily,
        },
        {
            "path": paths.engine_rotation_layer,
            "numeric_columns": ["EngineRotation - rpm"],
            "builder": _summarize_engine_rotation_daily,
        },
        {
            "path": paths.engine_temperature_layer,
            "numeric_columns": ["EngineTemperature - ºC"],
            "builder": _summarize_engine_temperature_daily,
        },
        {
            "path": paths.fuel_consumption_layer,
            "numeric_columns": ["FuelConsumption - L/h"],
            "builder": _summarize_fuel_daily,
        },
    ]

    for spec in layer_specs:
        if spec["path"] is None:
            continue
        frame = load_layer_map(spec["path"], numeric_columns=spec["numeric_columns"])
        frame["date"] = pd.to_datetime(frame["Date Time"], errors="coerce").dt.normalize()
        frame = assign_centroid_rows_to_seasons(frame, seasons)
        frame = frame.merge(area_metadata[["season_id", "area_label", "treatment", "crop_type", "comparison_pair"]], on="season_id", how="left")
        summaries.append(spec["builder"](frame))

    if not summaries:
        return pd.DataFrame(columns=["season_id", "date"])

    merged = summaries[0]
    for summary in summaries[1:]:
        merged = merged.merge(summary, on=["season_id", "date"], how="outer")

    merged = merged.merge(
        area_metadata[["season_id", "area_label", "treatment", "crop_type", "comparison_pair"]],
        on="season_id",
        how="left",
    )
    return merged.sort_values(["comparison_pair", "area_label", "date"]).reset_index(drop=True)


def build_ops_support_weekly(ops_support_daily: pd.DataFrame) -> pd.DataFrame:
    if ops_support_daily.empty:
        return pd.DataFrame(columns=["season_id", "week_start"])

    frame = ops_support_daily.copy()
    frame["week_start"] = _week_start(frame["date"])
    aggregations: dict[str, str | Any] = {
        "alarm_events": "sum",
        "alarm_type_count": "mean",
        "param_alert_events": "sum",
        "param_alert_type_count": "mean",
        "telemetry_points": "sum",
        "invalid_telemetry_share": "mean",
        "external_gps_share": "mean",
        "mobile_comm_share": "mean",
        "engine_rotation_points": "sum",
        "engine_rotation_mean_rpm": "mean",
        "engine_idle_share": "mean",
        "engine_temp_points": "sum",
        "engine_temp_mean_c": "mean",
        "engine_temp_max_c": "max",
        "engine_temp_hot_share": "mean",
        "fuel_points": "sum",
        "fuel_consumption_mean_l_h": "mean",
        "fuel_zero_share": "mean",
    }
    available = {column: agg for column, agg in aggregations.items() if column in frame.columns}
    weekly = frame.groupby(["season_id", "week_start"], as_index=False).agg(available)
    return weekly.rename(
        columns={column: f"{column}_week" for column in weekly.columns if column not in {"season_id", "week_start"}}
    )


def build_ndvi_phase_timeline(
    *,
    pairwise_weekly_features: pd.DataFrame,
    ops_support_weekly: pd.DataFrame,
) -> pd.DataFrame:
    if pairwise_weekly_features.empty:
        return pd.DataFrame(columns=["season_id", "week_start"])

    timeline = pairwise_weekly_features.copy()
    timeline["week_start"] = pd.to_datetime(timeline["week_start"], errors="coerce")
    timeline = timeline.merge(ops_support_weekly, on=["season_id", "week_start"], how="left")
    timeline = timeline.sort_values(["comparison_pair", "area_label", "week_start"]).reset_index(drop=True)

    area_min = timeline.groupby("season_id")["ndvi_mean_week"].transform("min")
    area_max = timeline.groupby("season_id")["ndvi_mean_week"].transform("max")
    timeline["ndvi_norm_week"] = (timeline["ndvi_mean_week"] - area_min) / (area_max - area_min).replace(0, pd.NA)
    timeline["ndvi_norm_week"] = timeline["ndvi_norm_week"].clip(lower=0, upper=1)

    peak_weeks = (
        timeline.dropna(subset=["ndvi_mean_week"])
        .sort_values(["season_id", "ndvi_mean_week", "week_start"], ascending=[True, False, True])
        .groupby("season_id", as_index=False)
        .first()[["season_id", "week_start"]]
        .rename(columns={"week_start": "peak_week_start"})
    )
    timeline = timeline.merge(peak_weeks, on="season_id", how="left")
    timeline["weeks_from_peak"] = (
        (pd.to_datetime(timeline["week_start"], errors="coerce") - pd.to_datetime(timeline["peak_week_start"], errors="coerce"))
        .dt.days.div(7)
    )

    thresholds = _build_ndvi_thresholds(timeline)
    _apply_flag_columns(timeline, thresholds)

    flag_columns = [
        "major_drop_flag",
        "high_soil_flag",
        "weather_stress_flag",
        "pest_risk_flag",
        "fert_risk_flag",
        "overlap_risk_flag",
        "stop_risk_flag",
        "telemetry_risk_flag",
        "alert_risk_flag",
        "engine_risk_flag",
    ]
    for column in flag_columns:
        timeline[column] = _to_bool_series(timeline[column])
        timeline[f"{column}_lag1"] = _to_bool_series(timeline.groupby("season_id")[column].shift(1))

    timeline["ops_risk_flag"] = timeline[
        ["fert_risk_flag", "overlap_risk_flag", "stop_risk_flag", "telemetry_risk_flag", "alert_risk_flag", "engine_risk_flag"]
    ].any(axis=1)
    timeline["risk_flag_count"] = timeline[
        [
            "high_soil_flag",
            "weather_stress_flag",
            "pest_risk_flag",
            "fert_risk_flag",
            "overlap_risk_flag",
            "stop_risk_flag",
            "telemetry_risk_flag",
            "alert_risk_flag",
            "engine_risk_flag",
        ]
    ].sum(axis=1)
    timeline["phase"] = timeline.apply(_classify_ndvi_phase, axis=1)
    timeline["event_type"] = timeline.apply(_classify_event_type, axis=1)
    timeline["driver_candidates"] = timeline.apply(_driver_candidates, axis=1)
    timeline["primary_driver"] = timeline["driver_candidates"].map(lambda value: value[0] if value else "sem_driver_forte")
    timeline["secondary_driver"] = timeline["driver_candidates"].map(lambda value: value[1] if len(value) > 1 else pd.NA)
    timeline["drivers_summary"] = timeline["driver_candidates"].map(
        lambda value: " | ".join(value[:3]) if value else "Sem driver forte observado no pacote atual."
    )
    timeline["pair_position"] = timeline.apply(_classify_pair_position, axis=1)
    timeline["story_sentence"] = timeline.apply(_build_story_sentence, axis=1)
    return timeline


def build_ndvi_events(ndvi_phase_timeline: pd.DataFrame) -> pd.DataFrame:
    if ndvi_phase_timeline.empty:
        return pd.DataFrame(columns=["season_id", "week_start"])

    events = ndvi_phase_timeline[ndvi_phase_timeline["event_type"].notna()].copy()
    if events.empty:
        return pd.DataFrame(columns=["season_id", "week_start"])

    columns = [
        "season_id",
        "week_start",
        "area_label",
        "treatment",
        "crop_type",
        "comparison_pair",
        "phase",
        "event_type",
        "ndvi_mean_week",
        "ndvi_delta_week",
        "ndvi_norm_week",
        "pair_ndvi_gap_4_0_minus_conv",
        "pair_position",
        "precipitation_mm_week",
        "water_balance_mm_week",
        "avg_pest_count_week",
        "alert_hits_week",
        "damage_hits_week",
        "fert_dose_gap_abs_mean_kg_ha_week",
        "overlap_area_pct_bbox_week",
        "stop_duration_h_per_bbox_ha_week",
        "invalid_telemetry_share_week",
        "alarm_events_week",
        "param_alert_events_week",
        "primary_driver",
        "secondary_driver",
        "drivers_summary",
        "story_sentence",
    ]
    available = [column for column in columns if column in events.columns]
    return events[available].sort_values(["comparison_pair", "area_label", "week_start"]).reset_index(drop=True)


def build_ndvi_pair_diagnostics(
    *,
    ndvi_phase_timeline: pd.DataFrame,
    area_inventory: pd.DataFrame,
    hypothesis_matrix: pd.DataFrame,
) -> pd.DataFrame:
    if ndvi_phase_timeline.empty:
        return pd.DataFrame(columns=["pair", "trajectory_evidence_strength", "trajectory_winner", "supports_4_0", "supports_convencional", "ndvi_interpretation", "known_gaps"])

    area_summary = _summarize_ndvi_timeline_by_area(ndvi_phase_timeline)
    rows: list[dict[str, Any]] = []

    for pair, group in area_summary.groupby("comparison_pair", dropna=False):
        pair_key = str(pair)
        tech = group[group["treatment"] == "tecnologia_4_0"].head(1)
        conv = group[group["treatment"] == "convencional"].head(1)
        supports_4_0: list[str] = []
        supports_conv: list[str] = []
        evidence_categories = 0

        if not tech.empty and not conv.empty:
            tech_row = tech.iloc[0]
            conv_row = conv.iloc[0]
            evidence_categories += _append_pair_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label="NDVI medio semanal",
                higher_is_better=True,
                tech_value=tech_row["ndvi_mean_season"],
                conv_value=conv_row["ndvi_mean_season"],
                threshold=0.02,
                unit="",
            )
            evidence_categories += _append_pair_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label="Pico de NDVI",
                higher_is_better=True,
                tech_value=tech_row["ndvi_peak"],
                conv_value=conv_row["ndvi_peak"],
                threshold=0.03,
                unit="",
            )
            evidence_categories += _append_pair_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label="Condicao NDVI nas semanas finais",
                higher_is_better=True,
                tech_value=tech_row["ndvi_norm_last"],
                conv_value=conv_row["ndvi_norm_last"],
                threshold=0.08,
                unit=" norm",
            )
            evidence_categories += _append_pair_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label="Area sob a curva do NDVI",
                higher_is_better=True,
                tech_value=tech_row["ndvi_auc_last"],
                conv_value=conv_row["ndvi_auc_last"],
                threshold=5.0,
                unit=" auc",
            )
            evidence_categories += _append_pair_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label="Semanas de queda relevante",
                higher_is_better=False,
                tech_value=tech_row["major_drop_weeks"],
                conv_value=conv_row["major_drop_weeks"],
                threshold=1.0,
                unit=" sem",
            )
            evidence_categories += _append_pair_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label="Semanas de baixo vigor",
                higher_is_better=False,
                tech_value=tech_row["low_vigor_weeks"],
                conv_value=conv_row["low_vigor_weeks"],
                threshold=1.0,
                unit=" sem",
            )
            evidence_categories += _append_pair_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label="Semanas com risco de praga",
                higher_is_better=False,
                tech_value=tech_row["pest_risk_weeks"],
                conv_value=conv_row["pest_risk_weeks"],
                threshold=1.0,
                unit=" sem",
            )
            evidence_categories += _append_pair_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label="Semanas com risco operacional",
                higher_is_better=False,
                tech_value=tech_row["ops_risk_weeks"],
                conv_value=conv_row["ops_risk_weeks"],
                threshold=1.0,
                unit=" sem",
            )

        known_gaps = _lookup_known_gaps(hypothesis_matrix, pair_key)
        coverage_share = group["weather_coverage_share"].mean()
        evidence_strength = "baixa"
        if evidence_categories >= 6:
            evidence_strength = "alta"
        elif evidence_categories >= 3:
            evidence_strength = "media"
        if pd.notna(coverage_share) and coverage_share < 0.5 and evidence_strength == "alta":
            evidence_strength = "media"
        elif pd.notna(coverage_share) and coverage_share < 0.5 and evidence_strength == "media":
            evidence_strength = "baixa"

        winner = "sem vantagem clara"
        if len(supports_4_0) > len(supports_conv):
            winner = "4.0"
        elif len(supports_conv) > len(supports_4_0):
            winner = "convencional"

        interpretation = _build_pair_interpretation(
            pair=pair_key,
            winner=winner,
            evidence_strength=evidence_strength,
            supports_4_0=supports_4_0,
            supports_conv=supports_conv,
        )
        rows.append(
            {
                "pair": pair_key,
                "trajectory_evidence_strength": evidence_strength,
                "trajectory_winner": winner,
                "supports_4_0": " | ".join(supports_4_0) if supports_4_0 else "Sem vantagem temporal clara do 4.0 no NDVI.",
                "supports_convencional": " | ".join(supports_conv) if supports_conv else "Sem vantagem temporal clara do convencional no NDVI.",
                "ndvi_interpretation": interpretation,
                "known_gaps": known_gaps,
            }
        )

    return pd.DataFrame(rows).sort_values("pair").reset_index(drop=True)


def build_ndvi_outlook(
    *,
    ndvi_phase_timeline: pd.DataFrame,
    ndvi_pair_diagnostics: pd.DataFrame,
) -> pd.DataFrame:
    if ndvi_phase_timeline.empty:
        return pd.DataFrame(columns=["season_id"])

    area_summary = _summarize_ndvi_timeline_by_area(ndvi_phase_timeline)
    rows: list[dict[str, Any]] = []

    for pair, group in area_summary.groupby("comparison_pair", dropna=False):
        pair_key = str(pair)
        pair_scores: dict[str, float] = {}
        for row in group.itertuples(index=False):
            score = _build_outlook_score(row)
            pair_scores[str(row.season_id)] = score

        for row in group.itertuples(index=False):
            score = pair_scores[str(row.season_id)]
            counterpart = group[group["season_id"] != row.season_id]
            counterpart_score = float(counterpart.iloc[0]["trajectory_score"]) if "trajectory_score" in counterpart.columns and not counterpart.empty else math.nan
            if counterpart.empty:
                counterpart_score = math.nan
            else:
                counterpart_score = pair_scores[str(counterpart.iloc[0]["season_id"])]
            pair_gap = score - counterpart_score if pd.notna(counterpart_score) else math.nan
            expected_vs_pair = _expected_vs_pair(pair_gap)
            top_risks = _build_top_risks(row)
            rows.append(
                {
                    "season_id": row.season_id,
                    "area_label": row.area_label,
                    "treatment": row.treatment,
                    "crop_type": row.crop_type,
                    "comparison_pair": row.comparison_pair,
                    "trajectory_score": round(score, 1),
                    "outlook_band": _outlook_band(score),
                    "expected_vs_pair": expected_vs_pair,
                    "latest_phase": row.latest_phase,
                    "latest_event": row.latest_event,
                    "last_ndvi_norm": round(_safe_float(row.ndvi_norm_last), 3),
                    "major_drop_weeks": int(_safe_float(row.major_drop_weeks, default=0)),
                    "low_vigor_weeks": int(_safe_float(row.low_vigor_weeks, default=0)),
                    "pest_risk_weeks": int(_safe_float(row.pest_risk_weeks, default=0)),
                    "ops_risk_weeks": int(_safe_float(row.ops_risk_weeks, default=0)),
                    "weather_stress_weeks": int(_safe_float(row.weather_stress_weeks, default=0)),
                    "top_risks": " | ".join(top_risks) if top_risks else "Sem risco dominante alem dos limites gerais do pacote.",
                    "expected_harvest_note": _build_expected_harvest_note(
                        area_label=row.area_label,
                        expected_vs_pair=expected_vs_pair,
                        latest_phase=row.latest_phase,
                        latest_event=row.latest_event,
                    ),
                    "recommended_actions": _build_recommended_actions(top_risks),
                    "pair_context": _lookup_pair_interpretation(ndvi_pair_diagnostics, pair_key),
                }
            )

    outlook = pd.DataFrame(rows)
    return outlook.sort_values(["comparison_pair", "trajectory_score", "area_label"], ascending=[True, False, True]).reset_index(drop=True)


def list_ndvi_deepdive_gaps(
    *,
    phase1_gaps: list[str],
    paths_summary,
    ndvi_phase_timeline: pd.DataFrame,
) -> list[str]:
    gaps = list(dict.fromkeys(phase1_gaps))
    if paths_summary.alarm_layer is None or paths_summary.parameterized_alert_layer is None:
        gaps.append("Camadas de alarmes operacionais nao vieram completas; a atribuicao de risco operacional fica parcial.")
    if paths_summary.telemetry_communication_layer is None:
        gaps.append("Sem telemetria de comunicacao; nao da para medir perda de qualidade de monitoramento em todas as semanas.")
    if not ndvi_phase_timeline.empty and _to_bool_series(ndvi_phase_timeline["has_weather_coverage_week"]).mean() < 0.5:
        gaps.append("Menos da metade das semanas NDVI tem clima local associado; leituras clima->NDVI continuam parciais.")
    gaps.append("Sem GeoTIFF numerico original, a inspeção visual usa JPG e a prova quantitativa continua limitada aos metadados do NDVI.")
    gaps.append("Sem colheita final consolidada, esta fase entrega expectativa pré-colheita e nao prova produtiva final.")
    return list(dict.fromkeys(gaps))


def _summarize_alarm_daily(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    return (
        frame.groupby(["season_id", "date"], as_index=False)
        .agg(
            alarm_events=("Alarm", "size"),
            alarm_type_count=("Alarm", lambda values: len({str(value) for value in values.dropna() if str(value).strip()})),
        )
    )


def _summarize_parameterized_alert_daily(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    return (
        frame.groupby(["season_id", "date"], as_index=False)
        .agg(
            param_alert_events=("Alert", "size"),
            param_alert_type_count=("Alert", lambda values: len({str(value) for value in values.dropna() if str(value).strip()})),
        )
    )


def _summarize_telemetry_daily(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    normalized = frame.copy()
    normalized["invalid_flag"] = normalized["InvalidCommunication"].astype("string").str.lower().isin(["true", "1", "sim"])
    normalized["external_gps_flag"] = normalized["ExternalGPS"].astype("string").str.lower().isin(["true", "1", "sim"])
    normalized["mobile_comm_flag"] = normalized["TelemetryCommunication"].astype("string").str.contains("m[oó]vel", case=False, na=False)
    return (
        normalized.groupby(["season_id", "date"], as_index=False)
        .agg(
            telemetry_points=("TelemetryCommunication", "size"),
            invalid_telemetry_share=("invalid_flag", "mean"),
            external_gps_share=("external_gps_flag", "mean"),
            mobile_comm_share=("mobile_comm_flag", "mean"),
        )
    )


def _summarize_engine_rotation_daily(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    normalized = frame.copy()
    values = pd.to_numeric(normalized["EngineRotation - rpm"], errors="coerce")
    normalized["engine_idle_flag"] = values.fillna(0).between(1, 900)
    normalized["engine_rotation_value"] = values
    return (
        normalized.groupby(["season_id", "date"], as_index=False)
        .agg(
            engine_rotation_points=("engine_rotation_value", "size"),
            engine_rotation_mean_rpm=("engine_rotation_value", "mean"),
            engine_idle_share=("engine_idle_flag", "mean"),
        )
    )


def _summarize_engine_temperature_daily(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    normalized = frame.copy()
    values = pd.to_numeric(normalized["EngineTemperature - ºC"], errors="coerce")
    hot_threshold = _quantile_or_default(values, 0.9, default=85.0)
    normalized["engine_temp_value"] = values
    normalized["engine_temp_hot_flag"] = values >= hot_threshold
    return (
        normalized.groupby(["season_id", "date"], as_index=False)
        .agg(
            engine_temp_points=("engine_temp_value", "size"),
            engine_temp_mean_c=("engine_temp_value", "mean"),
            engine_temp_max_c=("engine_temp_value", "max"),
            engine_temp_hot_share=("engine_temp_hot_flag", "mean"),
        )
    )


def _summarize_fuel_daily(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    normalized = frame.copy()
    values = pd.to_numeric(normalized["FuelConsumption - L/h"], errors="coerce")
    normalized["fuel_value"] = values
    normalized["fuel_zero_flag"] = values.fillna(0).eq(0)
    return (
        normalized.groupby(["season_id", "date"], as_index=False)
        .agg(
            fuel_points=("fuel_value", "size"),
            fuel_consumption_mean_l_h=("fuel_value", "mean"),
            fuel_zero_share=("fuel_zero_flag", "mean"),
        )
    )


def _build_ndvi_thresholds(timeline: pd.DataFrame) -> dict[str, float]:
    return {
        "high_soil": _quantile_or_default(timeline.get("soil_pct_week"), 0.75, default=20.0),
        "weather_stress": _quantile_or_default(timeline.get("water_balance_mm_week"), 0.25, default=-10.0),
        "pest_risk": _quantile_or_default(timeline.get("avg_pest_count_week"), 0.75, default=20.0),
        "fert_risk": _quantile_or_default(timeline.get("fert_dose_gap_abs_mean_kg_ha_week"), 0.75, default=150.0),
        "overlap_risk": _quantile_or_default(timeline.get("overlap_area_pct_bbox_week"), 0.75, default=0.04),
        "stop_risk": _quantile_or_default(timeline.get("stop_duration_h_per_bbox_ha_week"), 0.75, default=0.02),
        "telemetry_risk": _quantile_or_default(timeline.get("invalid_telemetry_share_week"), 0.75, default=0.05),
        "engine_hot": _quantile_or_default(timeline.get("engine_temp_max_c_week"), 0.9, default=85.0),
        "engine_idle": _quantile_or_default(timeline.get("engine_idle_share_week"), 0.75, default=0.25),
    }


def _apply_flag_columns(timeline: pd.DataFrame, thresholds: dict[str, float]) -> None:
    timeline["major_drop_flag"] = pd.to_numeric(timeline.get("ndvi_delta_week"), errors="coerce") <= -0.08
    timeline["severe_drop_flag"] = pd.to_numeric(timeline.get("ndvi_delta_week"), errors="coerce") <= -0.15
    timeline["recovery_flag"] = pd.to_numeric(timeline.get("ndvi_delta_week"), errors="coerce") >= 0.08
    timeline["low_vigor_flag"] = pd.to_numeric(timeline.get("ndvi_norm_week"), errors="coerce") <= 0.35
    timeline["high_soil_flag"] = pd.to_numeric(timeline.get("soil_pct_week"), errors="coerce") >= thresholds["high_soil"]
    timeline["weather_stress_flag"] = (
        _to_bool_series(timeline.get("has_weather_coverage_week"))
        & (pd.to_numeric(timeline.get("water_balance_mm_week"), errors="coerce") <= thresholds["weather_stress"])
    )
    timeline["weather_relief_flag"] = (
        _to_bool_series(timeline.get("has_weather_coverage_week"))
        & (pd.to_numeric(timeline.get("water_balance_mm_week"), errors="coerce") > 0)
        & (pd.to_numeric(timeline.get("precipitation_mm_week"), errors="coerce") > 20)
    )
    timeline["pest_risk_flag"] = (
        (pd.to_numeric(timeline.get("avg_pest_count_week"), errors="coerce") >= thresholds["pest_risk"])
        | (pd.to_numeric(timeline.get("alert_hits_week"), errors="coerce").fillna(0) > 0)
        | (pd.to_numeric(timeline.get("damage_hits_week"), errors="coerce").fillna(0) > 0)
    )
    timeline["fert_risk_flag"] = pd.to_numeric(timeline.get("fert_dose_gap_abs_mean_kg_ha_week"), errors="coerce") >= thresholds["fert_risk"]
    timeline["overlap_risk_flag"] = pd.to_numeric(timeline.get("overlap_area_pct_bbox_week"), errors="coerce") >= thresholds["overlap_risk"]
    timeline["stop_risk_flag"] = pd.to_numeric(timeline.get("stop_duration_h_per_bbox_ha_week"), errors="coerce") >= thresholds["stop_risk"]
    timeline["telemetry_risk_flag"] = pd.to_numeric(timeline.get("invalid_telemetry_share_week"), errors="coerce") >= thresholds["telemetry_risk"]
    timeline["alert_risk_flag"] = (
        pd.to_numeric(timeline.get("alarm_events_week"), errors="coerce").fillna(0).gt(0)
        | pd.to_numeric(timeline.get("param_alert_events_week"), errors="coerce").fillna(0).gt(0)
    )
    timeline["engine_risk_flag"] = (
        (pd.to_numeric(timeline.get("engine_temp_max_c_week"), errors="coerce") >= thresholds["engine_hot"])
        | (pd.to_numeric(timeline.get("engine_idle_share_week"), errors="coerce") >= thresholds["engine_idle"])
        | (pd.to_numeric(timeline.get("fuel_zero_share_week"), errors="coerce") >= 0.4)
    )


def _classify_ndvi_phase(row: pd.Series) -> str:
    ndvi = row.get("ndvi_mean_week")
    if pd.isna(ndvi):
        return "sem_cena"
    if bool(row.get("severe_drop_flag", False)):
        return "queda_forte"
    if bool(row.get("major_drop_flag", False)):
        return "queda"
    if bool(row.get("recovery_flag", False)) and bool(row.get("major_drop_flag_lag1", False)):
        return "recuperacao"
    if _safe_float(row.get("ndvi_norm_week")) <= 0.35 and _safe_float(row.get("weeks_from_peak")) < 0:
        return "estabelecimento"
    if _safe_float(row.get("ndvi_norm_week")) >= 0.85 and abs(_safe_float(row.get("ndvi_delta_week"))) <= 0.05:
        return "pico"
    if _safe_float(row.get("weeks_from_peak")) > 0 and _safe_float(row.get("ndvi_delta_week")) < -0.02:
        return "senescencia"
    if _safe_float(row.get("ndvi_delta_week")) > 0.05:
        return "expansao"
    return "estavel"


def _classify_event_type(row: pd.Series) -> str | None:
    if pd.isna(row.get("ndvi_mean_week")):
        return None
    if bool(row.get("severe_drop_flag", False)):
        return "queda_forte"
    if bool(row.get("major_drop_flag", False)):
        return "queda"
    if bool(row.get("recovery_flag", False)) and bool(row.get("major_drop_flag_lag1", False)):
        return "recuperacao"
    if _safe_float(row.get("ndvi_norm_week")) >= 0.85 and abs(_safe_float(row.get("ndvi_delta_week"))) <= 0.05:
        return "pico"
    if bool(row.get("low_vigor_flag", False)) and _safe_float(row.get("risk_flag_count")) >= 2:
        return "baixo_vigor"
    return None


def _driver_candidates(row: pd.Series) -> list[str]:
    if pd.isna(row.get("ndvi_mean_week")):
        return []

    candidates: list[tuple[str, float]] = []
    if bool(row.get("pest_risk_flag", False)) or bool(row.get("pest_risk_flag_lag1", False)):
        candidates.append(("pressao_de_pragas", 3.0))
    if bool(row.get("weather_stress_flag", False)) or bool(row.get("weather_stress_flag_lag1", False)):
        candidates.append(("estresse_climatico", 2.8))
    if bool(row.get("high_soil_flag", False)):
        candidates.append(("solo_exposto", 2.2))
    if bool(row.get("fert_risk_flag", False)) or bool(row.get("fert_risk_flag_lag1", False)):
        candidates.append(("falha_de_dose_na_adubacao", 2.0))
    if bool(row.get("overlap_risk_flag", False)) or bool(row.get("overlap_risk_flag_lag1", False)):
        candidates.append(("sobreposicao_operacional", 1.7))
    if bool(row.get("stop_risk_flag", False)) or bool(row.get("stop_risk_flag_lag1", False)):
        candidates.append(("paradas_operacionais", 1.5))
    if bool(row.get("telemetry_risk_flag", False)) or bool(row.get("telemetry_risk_flag_lag1", False)):
        candidates.append(("falha_de_telemetria", 1.3))
    if bool(row.get("alert_risk_flag", False)) or bool(row.get("alert_risk_flag_lag1", False)):
        candidates.append(("alertas_de_maquina", 1.2))
    if bool(row.get("engine_risk_flag", False)) or bool(row.get("engine_risk_flag_lag1", False)):
        candidates.append(("risco_de_maquina", 1.0))

    event_type = row.get("event_type")
    if event_type == "recuperacao":
        if bool(row.get("weather_relief_flag", False)):
            candidates.append(("alivio_climatico", 2.3))
        if not bool(row.get("pest_risk_flag", False)) and not bool(row.get("high_soil_flag", False)):
            candidates.append(("ambiente_mais_estavel", 1.6))
        if not bool(row.get("ops_risk_flag", False)):
            candidates.append(("operacao_sem_desvio_forte", 1.3))

    candidates = sorted(candidates, key=lambda item: (-item[1], item[0]))
    return [label for label, _ in candidates[:3]]


def _classify_pair_position(row: pd.Series) -> str:
    gap = _safe_float(row.get("pair_ndvi_gap_4_0_minus_conv"), default=math.nan)
    if pd.isna(gap):
        return "sem_par_no_periodo"
    if abs(gap) < 0.03:
        return "equilibrado"
    if row.get("treatment") == "tecnologia_4_0":
        return "acima_do_par" if gap > 0 else "abaixo_do_par"
    return "acima_do_par" if gap < 0 else "abaixo_do_par"


def _build_story_sentence(row: pd.Series) -> str:
    week = pd.to_datetime(row.get("week_start"), errors="coerce")
    week_text = week.strftime("%Y-%m-%d") if pd.notna(week) else "sem_data"
    ndvi = _safe_float(row.get("ndvi_mean_week"), default=math.nan)
    delta = _safe_float(row.get("ndvi_delta_week"), default=math.nan)
    phase = row.get("phase")
    event_type = row.get("event_type") or "sem_evento"
    pair_position = row.get("pair_position")
    drivers = row.get("drivers_summary")
    ndvi_text = f"{ndvi:.3f}" if pd.notna(ndvi) else "NA"
    delta_text = f"{delta:+.3f}" if pd.notna(delta) else "NA"
    return (
        f"{week_text}: {row.get('area_label')} ficou em fase {phase}, evento {event_type}, "
        f"NDVI {ndvi_text} com delta {delta_text}; posicao {pair_position}; drivers provaveis: {drivers}"
    )


def _summarize_ndvi_timeline_by_area(ndvi_phase_timeline: pd.DataFrame) -> pd.DataFrame:
    aggregations: dict[str, tuple[str, Any]] = {
        "ndvi_mean_season": ("ndvi_mean_week", "mean"),
        "ndvi_peak": ("ndvi_mean_week", "max"),
        "ndvi_auc_last": ("ndvi_auc_week", "max"),
        "ndvi_norm_last": ("ndvi_norm_week", _last_valid_numeric),
        "major_drop_weeks": ("major_drop_flag", "sum"),
        "severe_drop_weeks": ("severe_drop_flag", "sum"),
        "recovery_weeks": ("recovery_flag", "sum"),
        "low_vigor_weeks": ("low_vigor_flag", "sum"),
        "weather_stress_weeks": ("weather_stress_flag", "sum"),
        "pest_risk_weeks": ("pest_risk_flag", "sum"),
        "ops_risk_weeks": ("ops_risk_flag", "sum"),
        "telemetry_risk_weeks": ("telemetry_risk_flag", "sum"),
        "valid_weeks": ("ndvi_mean_week", lambda values: int(values.notna().sum())),
        "weather_weeks": ("has_weather_coverage_week", lambda values: int(_to_bool_series(values).sum())),
        "latest_phase": ("phase", _last_valid_text),
        "latest_event": ("event_type", _last_valid_text),
        "last4_ndvi_norm_mean": ("ndvi_norm_week", _last_four_mean),
        "recent_ndvi_delta_mean": ("ndvi_delta_week", _last_three_mean),
        "pest_risk_share": ("pest_risk_flag", "mean"),
        "ops_risk_share": ("ops_risk_flag", "mean"),
        "low_vigor_share": ("low_vigor_flag", "mean"),
        "major_drop_share": ("major_drop_flag", "mean"),
        "weather_stress_share": ("weather_stress_flag", "mean"),
        "telemetry_risk_share": ("telemetry_risk_flag", "mean"),
    }
    available = {name: spec for name, spec in aggregations.items() if spec[0] in ndvi_phase_timeline.columns}
    summary = (
        ndvi_phase_timeline.groupby(
            ["season_id", "area_label", "treatment", "crop_type", "comparison_pair"],
            as_index=False,
        )
        .agg(**available)
        .sort_values(["comparison_pair", "area_label"])
        .reset_index(drop=True)
    )
    for expected in aggregations:
        if expected not in summary.columns:
            summary[expected] = pd.NA
    summary["weather_coverage_share"] = summary["weather_weeks"] / summary["valid_weeks"].replace(0, pd.NA)
    summary["trajectory_score"] = summary.apply(_build_outlook_score, axis=1)
    return summary


def _append_pair_evidence(
    *,
    supports_4_0: list[str],
    supports_conv: list[str],
    label: str,
    higher_is_better: bool,
    tech_value: object,
    conv_value: object,
    threshold: float,
    unit: str,
) -> int:
    if tech_value is None or conv_value is None or pd.isna(tech_value) or pd.isna(conv_value):
        return 0
    delta = float(tech_value) - float(conv_value)
    if abs(delta) < threshold:
        return 0
    preferred = supports_4_0 if (delta > 0) == higher_is_better else supports_conv
    label_side = "4.0" if preferred is supports_4_0 else "convencional"
    preferred.append(f"{label}: {label_side} leva vantagem ({delta:+.2f}{unit}).")
    return 1


def _build_pair_interpretation(
    *,
    pair: str,
    winner: str,
    evidence_strength: str,
    supports_4_0: list[str],
    supports_conv: list[str],
) -> str:
    if winner == "4.0":
        return f"No par {pair}, o 4.0 mostra vantagem temporal de NDVI com evidencia {evidence_strength}."
    if winner == "convencional":
        return f"No par {pair}, o convencional mostra vantagem temporal de NDVI com evidencia {evidence_strength}."
    if supports_4_0 or supports_conv:
        return f"No par {pair}, os sinais de NDVI seguem mistos e nao fecham um vencedor unico."
    return f"No par {pair}, ainda nao ha diferenca temporal forte de NDVI."


def _lookup_known_gaps(hypothesis_matrix: pd.DataFrame, pair_key: str) -> str:
    if hypothesis_matrix.empty or "pair" not in hypothesis_matrix.columns:
        return "Sem gaps adicionais registrados."
    row = hypothesis_matrix[hypothesis_matrix["pair"] == pair_key]
    if row.empty:
        return "Sem gaps adicionais registrados."
    return str(row.iloc[0].get("known_gaps") or "Sem gaps adicionais registrados.")


def _build_outlook_score(row: pd.Series | Any) -> float:
    components = [
        (0.30, _component_value(_row_value(row, "last4_ndvi_norm_mean"), neutral=0.5)),
        (0.20, _clip01(_safe_float(_row_value(row, "ndvi_peak"), default=math.nan) / 0.85)),
        (0.15, 1 - _component_value(_row_value(row, "low_vigor_share"), neutral=0.5)),
        (0.10, 1 - _component_value(_row_value(row, "major_drop_share"), neutral=0.4)),
        (0.10, 1 - _component_value(_row_value(row, "pest_risk_share"), neutral=0.4)),
        (0.10, 1 - _component_value(_row_value(row, "ops_risk_share"), neutral=0.4)),
        (0.05, _trend_component(_row_value(row, "recent_ndvi_delta_mean"))),
    ]
    return round(100 * sum(weight * value for weight, value in components), 2)


def _outlook_band(score: float) -> str:
    if score >= 75:
        return "favoravel"
    if score >= 65:
        return "positivo_com_ressalvas"
    if score >= 50:
        return "misto"
    return "atencao"


def _expected_vs_pair(pair_gap: float | object) -> str:
    value = _safe_float(pair_gap, default=math.nan)
    if pd.isna(value):
        return "sem_referencia_no_par"
    if value >= 6:
        return "tende_a_chegar_acima_do_par"
    if value <= -6:
        return "tende_a_chegar_abaixo_do_par"
    return "sem_vantagem_clara_no_par"


def _build_top_risks(row: Any) -> list[str]:
    risks: list[tuple[str, float]] = []
    risk_specs = [
        ("baixo_vigor_recorrente", getattr(row, "low_vigor_share", math.nan)),
        ("quedas_relevantes_de_ndvi", getattr(row, "major_drop_share", math.nan)),
        ("pressao_de_praga", getattr(row, "pest_risk_share", math.nan)),
        ("risco_operacional", getattr(row, "ops_risk_share", math.nan)),
        ("estresse_climatico", getattr(row, "weather_stress_share", math.nan)),
        ("telemetria_instavel", getattr(row, "telemetry_risk_share", math.nan)),
    ]
    for label, value in risk_specs:
        numeric = _safe_float(value, default=math.nan)
        if pd.notna(numeric) and numeric >= 0.20:
            risks.append((label, numeric))
    risks.sort(key=lambda item: (-item[1], item[0]))
    return [label for label, _ in risks[:3]]


def _build_expected_harvest_note(*, area_label: str, expected_vs_pair: str, latest_phase: object, latest_event: object) -> str:
    if expected_vs_pair == "tende_a_chegar_acima_do_par":
        return f"{area_label} tende a chegar a colheita em condicao relativa superior no par, mantendo o perfil recente de NDVI."
    if expected_vs_pair == "tende_a_chegar_abaixo_do_par":
        return f"{area_label} tende a chegar a colheita em condicao relativa inferior no par se o padrao recente de NDVI persistir."
    return f"{area_label} nao mostra vantagem clara no par; a leitura atual e {latest_phase} com evento recente {latest_event or 'sem evento forte'}."


def _build_recommended_actions(top_risks: list[str]) -> str:
    recommendations: list[str] = []
    if "pressao_de_praga" in top_risks:
        recommendations.append("Reforcar validacao agronomica e resposta MIIP nas janelas com limiares.")
    if "risco_operacional" in top_risks or "quedas_relevantes_de_ndvi" in top_risks:
        recommendations.append("Revisar calibracao, sobreposicao, paradas e aderencia operacional nas semanas de queda.")
    if "telemetria_instavel" in top_risks:
        recommendations.append("Tratar falhas de comunicacao e alarmes para nao degradar a capacidade de monitorar.")
    if "baixo_vigor_recorrente" in top_risks:
        recommendations.append("Inspecionar estande, solo exposto e manejo localizado nas faixas de menor vigor.")
    if "estresse_climatico" in top_risks:
        recommendations.append("Separar o efeito do clima do efeito de manejo ao interpretar a diferenca de NDVI.")
    if not recommendations:
        return "Manter monitoramento semanal e validar em campo os sinais de NDVI antes de conclusao forte."
    return " | ".join(recommendations[:3])


def _lookup_pair_interpretation(ndvi_pair_diagnostics: pd.DataFrame, pair_key: str) -> str:
    if ndvi_pair_diagnostics.empty:
        return "Sem leitura comparativa adicional."
    row = ndvi_pair_diagnostics[ndvi_pair_diagnostics["pair"] == pair_key]
    if row.empty:
        return "Sem leitura comparativa adicional."
    return str(row.iloc[0]["ndvi_interpretation"])


def _component_value(value: object, *, neutral: float) -> float:
    numeric = _safe_float(value, default=math.nan)
    if pd.isna(numeric):
        return neutral
    return _clip01(numeric)


def _trend_component(value: object) -> float:
    numeric = _safe_float(value, default=math.nan)
    if pd.isna(numeric):
        return 0.5
    return _clip01((numeric + 0.12) / 0.24)


def _quantile_or_default(series: object, q: float, *, default: float) -> float:
    if series is None:
        return default
    values = pd.to_numeric(series, errors="coerce")
    if not isinstance(values, pd.Series):
        values = pd.Series([values])
    values = values.dropna()
    if values.empty:
        return default
    return float(values.quantile(q))


def _last_valid_numeric(values: pd.Series) -> float | None:
    valid = pd.to_numeric(values, errors="coerce").dropna()
    if valid.empty:
        return None
    return float(valid.iloc[-1])


def _last_valid_text(values: pd.Series) -> str | None:
    valid = values.dropna()
    if valid.empty:
        return None
    return str(valid.iloc[-1])


def _last_four_mean(values: pd.Series) -> float | None:
    valid = pd.to_numeric(values, errors="coerce").dropna()
    if valid.empty:
        return None
    return float(valid.tail(4).mean())


def _last_three_mean(values: pd.Series) -> float | None:
    valid = pd.to_numeric(values, errors="coerce").dropna()
    if valid.empty:
        return None
    return float(valid.tail(3).mean())


def _week_start(series: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(series, errors="coerce")
    return (timestamps - pd.to_timedelta(timestamps.dt.weekday, unit="D")).dt.normalize()


def _clip01(value: float) -> float:
    return float(max(0.0, min(1.0, value)))


def _safe_float(value: object, default: float = 0.0) -> float:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return default
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(converted):
        return default
    return converted


def _row_value(row: pd.Series | Any, key: str) -> object:
    if isinstance(row, pd.Series):
        return row.get(key)
    return getattr(row, key, None)


def _to_bool_series(values: pd.Series | object) -> pd.Series:
    if isinstance(values, pd.Series):
        series = values.copy()
    else:
        series = pd.Series(values)
    return series.map(lambda value: bool(value) if pd.notna(value) else False).astype(bool)
