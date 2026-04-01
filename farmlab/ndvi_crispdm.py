from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from farmlab.ndvi_deepdive import build_ndvi_deep_dive_workspace


CRISPDM_OUTPUT_TABLES = [
    "data_audit",
    "pair_effect_tests",
    "event_driver_lift",
    "transition_model_frame",
    "transition_model_summary",
    "transition_model_coefficients",
    "transition_model_predictions",
    "final_hypothesis_register",
    "decision_summary",
]

PAIR_EFFECT_SPECS = [
    {
        "metric": "ndvi_mean_week",
        "label": "NDVI medio semanal",
        "higher_is_better": True,
        "threshold": 0.015,
    },
    {
        "metric": "ndvi_auc_week",
        "label": "Area sob a curva do NDVI",
        "higher_is_better": True,
        "threshold": 0.25,
    },
    {
        "metric": "low_vigor_flag",
        "label": "Semanas de baixo vigor",
        "higher_is_better": False,
        "threshold": 0.10,
    },
    {
        "metric": "major_drop_flag",
        "label": "Semanas de queda relevante",
        "higher_is_better": False,
        "threshold": 0.08,
    },
    {
        "metric": "pest_risk_flag",
        "label": "Semanas com risco de praga",
        "higher_is_better": False,
        "threshold": 0.08,
    },
    {
        "metric": "ops_risk_flag",
        "label": "Semanas com risco operacional",
        "higher_is_better": False,
        "threshold": 0.08,
    },
    {
        "metric": "high_soil_flag",
        "label": "Semanas com solo exposto relevante",
        "higher_is_better": False,
        "threshold": 0.08,
    },
]

DRIVER_FLAG_SPECS = [
    ("high_soil_flag", "solo_exposto"),
    ("weather_stress_flag", "estresse_climatico"),
    ("pest_risk_flag", "pressao_de_pragas"),
    ("fert_risk_flag", "falha_de_dose_na_adubacao"),
    ("overlap_risk_flag", "sobreposicao_operacional"),
    ("stop_risk_flag", "tempo_parado"),
    ("telemetry_risk_flag", "falha_de_telemetria"),
    ("alert_risk_flag", "alertas_de_maquina"),
    ("engine_risk_flag", "risco_de_motor"),
]

MODEL_NUMERIC_FEATURES = [
    "ndvi_mean_week",
    "ndvi_delta_week",
    "soil_pct_week",
    "dense_veg_pct_week",
    "precipitation_mm_week",
    "water_balance_mm_week",
    "temp_avg_c_week",
    "humidity_avg_pct_week",
    "avg_pest_count_week",
    "fert_dose_gap_abs_mean_kg_ha_week",
    "overlap_area_pct_bbox_week",
    "stop_duration_h_per_bbox_ha_week",
    "invalid_telemetry_share_week",
    "alarm_events_week",
    "engine_temp_hot_share_week",
    "risk_flag_count",
]

MODEL_BOOL_FEATURES = [
    "low_vigor_flag",
    "major_drop_flag",
    "high_soil_flag",
    "weather_stress_flag",
    "pest_risk_flag",
    "ops_risk_flag",
    "fert_risk_flag",
    "overlap_risk_flag",
    "stop_risk_flag",
    "telemetry_risk_flag",
    "alert_risk_flag",
    "engine_risk_flag",
]

MODEL_CATEGORY_FEATURES = [
    "comparison_pair",
    "treatment",
    "phase",
]


def build_ndvi_crispdm_workspace(base_dir: Path, manual_mapping: pd.DataFrame | None = None) -> dict[str, Any]:
    deep = build_ndvi_deep_dive_workspace(base_dir, manual_mapping)
    data_audit = build_data_audit(
        area_inventory=deep["area_inventory"],
        ndvi_clean=deep["ndvi_clean"],
        weather_daily=deep["weather_daily"],
        miip_daily=deep["miip_daily"],
        ops_area_daily=deep["ops_area_daily"],
        ndvi_phase_timeline=deep["ndvi_phase_timeline"],
    )
    pair_effect_tests = build_pair_effect_tests(deep["ndvi_phase_timeline"])
    event_driver_lift = build_event_driver_lift(deep["ndvi_phase_timeline"])
    transition_model_frame = build_transition_model_frame(deep["ndvi_phase_timeline"])
    transition_model = fit_transition_model(transition_model_frame)
    final_hypothesis_register = build_final_hypothesis_register(
        pair_effect_tests=pair_effect_tests,
        event_driver_lift=event_driver_lift,
        ndvi_outlook=deep["ndvi_outlook"],
        ndvi_pair_diagnostics=deep["ndvi_pair_diagnostics"],
        deep_dive_gaps=deep["deep_dive_gaps"],
    )
    decision_summary = build_decision_summary(
        pair_effect_tests=pair_effect_tests,
        event_driver_lift=event_driver_lift,
        final_hypothesis_register=final_hypothesis_register,
        ndvi_outlook=deep["ndvi_outlook"],
        ndvi_pair_diagnostics=deep["ndvi_pair_diagnostics"],
    )
    return {
        **deep,
        "data_audit": data_audit,
        "pair_effect_tests": pair_effect_tests,
        "event_driver_lift": event_driver_lift,
        "transition_model_frame": transition_model_frame,
        "transition_model_summary": transition_model["summary"],
        "transition_model_coefficients": transition_model["coefficients"],
        "transition_model_predictions": transition_model["predictions"],
        "final_hypothesis_register": final_hypothesis_register,
        "decision_summary": decision_summary,
    }


def save_ndvi_crispdm_outputs(workspace: dict[str, Any], output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for table_name in CRISPDM_OUTPUT_TABLES:
        frame = workspace.get(table_name)
        if not isinstance(frame, pd.DataFrame):
            continue
        path = output_dir / f"{table_name}.csv"
        frame.to_csv(path, index=False)
        written.append(path)
    return written


def build_data_audit(
    *,
    area_inventory: pd.DataFrame,
    ndvi_clean: pd.DataFrame,
    weather_daily: pd.DataFrame,
    miip_daily: pd.DataFrame,
    ops_area_daily: pd.DataFrame,
    ndvi_phase_timeline: pd.DataFrame,
) -> pd.DataFrame:
    if area_inventory.empty:
        return pd.DataFrame(columns=["season_id", "area_label"])

    weather_start = pd.to_datetime(weather_daily.get("date"), errors="coerce").min() if not weather_daily.empty else pd.NaT
    weather_end = pd.to_datetime(weather_daily.get("date"), errors="coerce").max() if not weather_daily.empty else pd.NaT

    ndvi_summary = (
        ndvi_clean.groupby("season_id", as_index=False)
        .agg(
            ndvi_dates=("date", "nunique"),
            ndvi_weeks=("week_start", "nunique"),
            ndvi_mean=("ndvi_mean", "mean"),
            ndvi_std=("ndvi_mean", "std"),
            weather_weeks=("has_weather_coverage", "sum"),
        )
        .rename(columns={"weather_weeks": "ndvi_weather_weeks"})
    )
    ops_summary = (
        ops_area_daily.groupby("season_id", as_index=False)
        .agg(
            ops_days=("date", "nunique"),
            harvest_days=("harvest_points", lambda value: int(pd.to_numeric(value, errors="coerce").fillna(0).gt(0).sum())),
            fert_days=("fert_points", lambda value: int(pd.to_numeric(value, errors="coerce").fillna(0).gt(0).sum())),
            overlap_days=("overlap_area_ha", lambda value: int(pd.to_numeric(value, errors="coerce").fillna(0).gt(0).sum())),
            stop_days=("stop_events", lambda value: int(pd.to_numeric(value, errors="coerce").fillna(0).gt(0).sum())),
        )
        if not ops_area_daily.empty
        else pd.DataFrame(columns=["season_id", "ops_days", "harvest_days", "fert_days", "overlap_days", "stop_days"])
    )
    miip_summary = (
        miip_daily.groupby("season_id", as_index=False)
        .agg(
            miip_days=("date", "nunique"),
            miip_weeks=("date", lambda value: pd.to_datetime(value, errors="coerce").dt.to_period("W").nunique()),
            avg_pest_count=("avg_pest_count", "mean"),
            total_pest_count=("total_pest_count", "sum"),
        )
        if not miip_daily.empty
        else pd.DataFrame(columns=["season_id", "miip_days", "miip_weeks", "avg_pest_count", "total_pest_count"])
    )
    phase_summary = (
        ndvi_phase_timeline.groupby("season_id", as_index=False)
        .agg(
            model_weeks=("week_start", "nunique"),
            low_vigor_weeks=("low_vigor_flag", "sum"),
            major_drop_weeks=("major_drop_flag", "sum"),
            ops_risk_weeks=("ops_risk_flag", "sum"),
            pest_risk_weeks=("pest_risk_flag", "sum"),
        )
        if not ndvi_phase_timeline.empty
        else pd.DataFrame(columns=["season_id", "model_weeks", "low_vigor_weeks", "major_drop_weeks", "ops_risk_weeks", "pest_risk_weeks"])
    )

    audit = area_inventory.copy()
    audit = audit.merge(ndvi_summary, on="season_id", how="left")
    audit = audit.merge(ops_summary, on="season_id", how="left")
    audit = audit.merge(miip_summary, on="season_id", how="left")
    audit = audit.merge(phase_summary, on="season_id", how="left")

    audit["ndvi_valid_ratio"] = _safe_div(audit["total_valid_images"], audit["total_images"])
    audit["weather_coverage_ratio"] = _safe_div(audit["ndvi_weather_weeks"], audit["ndvi_weeks"])
    audit["miip_coverage_ratio"] = _safe_div(audit["miip_weeks"], audit["ndvi_weeks"])
    audit["audit_status"] = audit.apply(_classify_audit_status, axis=1)
    audit["weather_window"] = (
        _fmt_ts(weather_start) + " -> " + _fmt_ts(weather_end) if not pd.isna(weather_start) and not pd.isna(weather_end) else "sem_clima_local"
    )
    columns = [
        "season_id",
        "area_label",
        "treatment",
        "crop_type",
        "comparison_pair",
        "total_images",
        "total_valid_images",
        "invalid_images",
        "ndvi_valid_ratio",
        "ndvi_weeks",
        "ndvi_weather_weeks",
        "weather_coverage_ratio",
        "miip_days",
        "miip_weeks",
        "miip_coverage_ratio",
        "ops_days",
        "harvest_days",
        "fert_days",
        "overlap_days",
        "stop_days",
        "low_vigor_weeks",
        "major_drop_weeks",
        "ops_risk_weeks",
        "pest_risk_weeks",
        "audit_status",
        "weather_window",
    ]
    return audit.reindex(columns=columns).sort_values(["comparison_pair", "area_label"]).reset_index(drop=True)


def build_pair_effect_tests(ndvi_phase_timeline: pd.DataFrame) -> pd.DataFrame:
    if ndvi_phase_timeline.empty:
        return pd.DataFrame(columns=["comparison_pair", "metric"])

    rows: list[dict[str, Any]] = []
    timeline = ndvi_phase_timeline.copy()
    timeline["week_start"] = pd.to_datetime(timeline["week_start"], errors="coerce")

    for comparison_pair, pair_frame in timeline.groupby("comparison_pair", sort=True):
        tech = pair_frame[pair_frame["treatment"] == "tecnologia_4_0"].copy()
        conv = pair_frame[pair_frame["treatment"] == "convencional"].copy()
        if tech.empty or conv.empty:
            continue

        for spec in PAIR_EFFECT_SPECS:
            rows.append(_pair_effect_row(comparison_pair, tech, conv, spec))

    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["comparison_pair", "metric"])
    return frame.sort_values(["comparison_pair", "metric_label"]).reset_index(drop=True)


def build_event_driver_lift(ndvi_phase_timeline: pd.DataFrame) -> pd.DataFrame:
    if ndvi_phase_timeline.empty:
        return pd.DataFrame(columns=["comparison_pair", "driver"])

    frame = ndvi_phase_timeline.copy()
    frame["problem_week"] = (
        _to_bool_series(frame.get("major_drop_flag"))
        | _to_bool_series(frame.get("low_vigor_flag"))
        | frame.get("event_type", pd.Series(dtype="object")).isin(["queda", "queda_forte", "baixo_vigor"])
    )

    rows: list[dict[str, Any]] = []
    for comparison_pair, pair_frame in frame.groupby("comparison_pair", sort=True):
        for flag_column, driver_label in DRIVER_FLAG_SPECS:
            if flag_column not in pair_frame.columns:
                continue
            flag = _to_bool_series(pair_frame[flag_column]).astype(float)
            problem_mask = _to_bool_series(pair_frame["problem_week"])
            non_problem_mask = ~problem_mask
            problem_rate = float(flag[problem_mask].mean()) if problem_mask.any() else math.nan
            baseline_rate = float(flag[non_problem_mask].mean()) if non_problem_mask.any() else math.nan
            rows.append(
                {
                    "comparison_pair": comparison_pair,
                    "driver": driver_label,
                    "problem_weeks": int(problem_mask.sum()),
                    "problem_rate": problem_rate,
                    "baseline_rate": baseline_rate,
                    "delta_pp": (problem_rate - baseline_rate) * 100 if not (math.isnan(problem_rate) or math.isnan(baseline_rate)) else math.nan,
                    "lift_ratio": _lift_ratio(problem_rate, baseline_rate),
                    "evidence_level": _driver_lift_level(problem_rate, baseline_rate, int(problem_mask.sum())),
                }
            )

    result = pd.DataFrame(rows)
    if result.empty:
        return pd.DataFrame(columns=["comparison_pair", "driver"])
    return result.sort_values(["comparison_pair", "delta_pp", "lift_ratio"], ascending=[True, False, False]).reset_index(drop=True)


def build_transition_model_frame(ndvi_phase_timeline: pd.DataFrame) -> pd.DataFrame:
    if ndvi_phase_timeline.empty:
        return pd.DataFrame(columns=["season_id", "week_start", "target_next_ndvi_delta"])

    frame = ndvi_phase_timeline.copy()
    frame["week_start"] = pd.to_datetime(frame["week_start"], errors="coerce")
    frame = frame.sort_values(["season_id", "week_start"]).reset_index(drop=True)
    frame["next_ndvi_mean_week"] = frame.groupby("season_id")["ndvi_mean_week"].shift(-1)
    frame["target_next_ndvi_delta"] = frame["next_ndvi_mean_week"] - frame["ndvi_mean_week"]
    frame = frame[frame["target_next_ndvi_delta"].notna()].copy()

    for column in MODEL_BOOL_FEATURES:
        frame[column] = _to_bool_series(frame.get(column)).astype(float)

    for column in MODEL_NUMERIC_FEATURES:
        if column not in frame.columns:
            frame[column] = math.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    keep_columns = [
        "season_id",
        "week_start",
        "area_label",
        "comparison_pair",
        "treatment",
        "phase",
        "target_next_ndvi_delta",
        *MODEL_NUMERIC_FEATURES,
        *MODEL_BOOL_FEATURES,
    ]
    return frame[keep_columns].reset_index(drop=True)


def fit_transition_model(model_frame: pd.DataFrame, *, alpha: float = 1.0) -> dict[str, pd.DataFrame]:
    if model_frame.empty:
        empty = pd.DataFrame()
        return {"summary": empty, "coefficients": empty, "predictions": empty}

    feature_frame = _build_model_design_frame(model_frame)
    x_matrix, feature_names, means, stds = _standardized_matrix(feature_frame)
    y = pd.to_numeric(model_frame["target_next_ndvi_delta"], errors="coerce").fillna(0).to_numpy(dtype=float)
    beta = _ridge_fit(x_matrix, y, alpha=alpha)
    fitted = _ridge_predict(x_matrix, beta)
    in_sample_mae = float(np.mean(np.abs(y - fitted)))
    in_sample_rmse = float(np.sqrt(np.mean((y - fitted) ** 2)))
    in_sample_r2 = _r2_score(y, fitted)

    loo_predictions = np.full(len(model_frame), np.nan, dtype=float)
    for season_id, test_index in model_frame.groupby("season_id").groups.items():
        test_idx = np.array(sorted(test_index))
        train_mask = np.ones(len(model_frame), dtype=bool)
        train_mask[test_idx] = False
        if train_mask.sum() <= len(feature_names):
            continue
        train_x = x_matrix[train_mask]
        train_y = y[train_mask]
        beta_fold = _ridge_fit(train_x, train_y, alpha=alpha)
        loo_predictions[test_idx] = _ridge_predict(x_matrix[test_idx], beta_fold)

    loo_mask = ~np.isnan(loo_predictions)
    loo_mae = float(np.mean(np.abs(y[loo_mask] - loo_predictions[loo_mask]))) if loo_mask.any() else math.nan
    loo_rmse = float(np.sqrt(np.mean((y[loo_mask] - loo_predictions[loo_mask]) ** 2))) if loo_mask.any() else math.nan
    loo_r2 = _r2_score(y[loo_mask], loo_predictions[loo_mask]) if loo_mask.any() else math.nan

    coef_rows = []
    for index, feature_name in enumerate(feature_names, start=1):
        coef_rows.append(
            {
                "feature": feature_name,
                "coefficient": float(beta[index]),
                "abs_coefficient": abs(float(beta[index])),
                "direction": "aumenta_ndvi_futuro" if beta[index] > 0 else "pressiona_ndvi_futuro",
                "feature_mean": means.get(feature_name, math.nan),
                "feature_std": stds.get(feature_name, math.nan),
            }
        )
    coefficients = pd.DataFrame(coef_rows).sort_values("abs_coefficient", ascending=False).reset_index(drop=True)

    predictions = model_frame[["season_id", "week_start", "area_label", "comparison_pair", "treatment", "phase"]].copy()
    predictions["target_next_ndvi_delta"] = y
    predictions["predicted_next_ndvi_delta"] = fitted
    predictions["loo_predicted_next_ndvi_delta"] = loo_predictions
    predictions["residual"] = predictions["target_next_ndvi_delta"] - predictions["predicted_next_ndvi_delta"]
    predictions["loo_residual"] = predictions["target_next_ndvi_delta"] - predictions["loo_predicted_next_ndvi_delta"]

    summary = pd.DataFrame(
        [
            {
                "observations": int(len(model_frame)),
                "features": int(len(feature_names)),
                "alpha": float(alpha),
                "in_sample_mae": in_sample_mae,
                "in_sample_rmse": in_sample_rmse,
                "in_sample_r2": in_sample_r2,
                "loo_mae": loo_mae,
                "loo_rmse": loo_rmse,
                "loo_r2": loo_r2,
                "model_choice": "ridge_linear_interpretavel",
                "model_note": (
                    "Base pequena para yield/ROI final; o modelo foi restrito a transicao semanal do NDVI para manter interpretabilidade."
                ),
            }
        ]
    )
    return {"summary": summary, "coefficients": coefficients, "predictions": predictions}


def build_final_hypothesis_register(
    *,
    pair_effect_tests: pd.DataFrame,
    event_driver_lift: pd.DataFrame,
    ndvi_outlook: pd.DataFrame,
    ndvi_pair_diagnostics: pd.DataFrame,
    deep_dive_gaps: list[str],
) -> pd.DataFrame:
    if ndvi_pair_diagnostics.empty:
        return pd.DataFrame(columns=["comparison_pair", "hypothesis_id"])

    rows: list[dict[str, Any]] = []
    gaps_text = " | ".join(deep_dive_gaps[:3]) if deep_dive_gaps else ""

    pair_column = "comparison_pair" if "comparison_pair" in ndvi_pair_diagnostics.columns else "pair"
    for pair in sorted(ndvi_pair_diagnostics[pair_column].dropna().unique()):
        pair_tests = pair_effect_tests[pair_effect_tests["comparison_pair"] == pair]
        pair_outlook = ndvi_outlook[ndvi_outlook["comparison_pair"] == pair]
        pair_lift = event_driver_lift[event_driver_lift["comparison_pair"] == pair]
        pair_diag = ndvi_pair_diagnostics[ndvi_pair_diagnostics[pair_column] == pair]
        diag_row = pair_diag.iloc[0] if not pair_diag.empty else pd.Series(dtype="object")

        ndvi_metric = _first_metric(pair_tests, "NDVI medio semanal")
        low_vigor_metric = _first_metric(pair_tests, "Semanas de baixo vigor")
        drop_metric = _first_metric(pair_tests, "Semanas de queda relevante")
        driver_row = pair_lift.iloc[0] if not pair_lift.empty else pd.Series(dtype="object")
        outlook_4_0 = pair_outlook[pair_outlook["treatment"] == "tecnologia_4_0"]

        rows.extend(
            [
                _hypothesis_row(
                    comparison_pair=pair,
                    hypothesis_id="H1",
                    hypothesis="4.0 sustenta maior nivel temporal de NDVI no par.",
                    status=_metric_status(ndvi_metric),
                    proof_basis=_metric_basis(ndvi_metric),
                    known_limits=diag_row.get("known_gaps", gaps_text),
                ),
                _hypothesis_row(
                    comparison_pair=pair,
                    hypothesis_id="H2",
                    hypothesis="4.0 reduz semanas de problema no NDVI.",
                    status=_combined_problem_status(low_vigor_metric, drop_metric),
                    proof_basis=" | ".join(filter(None, [_metric_basis(low_vigor_metric), _metric_basis(drop_metric)])),
                    known_limits=diag_row.get("known_gaps", gaps_text),
                ),
                _hypothesis_row(
                    comparison_pair=pair,
                    hypothesis_id="H3",
                    hypothesis="As semanas problema do NDVI apresentam drivers identificaveis no pacote atual.",
                    status=_driver_status(driver_row),
                    proof_basis=_driver_basis(driver_row),
                    known_limits=diag_row.get("known_gaps", gaps_text),
                ),
                _hypothesis_row(
                    comparison_pair=pair,
                    hypothesis_id="H4",
                    hypothesis="O outlook pre-colheita favorece o 4.0 dentro do par.",
                    status=_outlook_status(outlook_4_0),
                    proof_basis=_outlook_basis(outlook_4_0),
                    known_limits=diag_row.get("known_gaps", gaps_text),
                ),
            ]
        )

    return pd.DataFrame(rows).sort_values(["comparison_pair", "hypothesis_id"]).reset_index(drop=True)


def build_decision_summary(
    *,
    pair_effect_tests: pd.DataFrame,
    event_driver_lift: pd.DataFrame,
    final_hypothesis_register: pd.DataFrame,
    ndvi_outlook: pd.DataFrame,
    ndvi_pair_diagnostics: pd.DataFrame,
) -> pd.DataFrame:
    if ndvi_pair_diagnostics.empty:
        return pd.DataFrame(columns=["comparison_pair", "decision_message"])

    rows: list[dict[str, Any]] = []
    pair_column = "comparison_pair" if "comparison_pair" in ndvi_pair_diagnostics.columns else "pair"
    winner_column = "temporal_winner" if "temporal_winner" in ndvi_pair_diagnostics.columns else "trajectory_winner"
    for pair, pair_diag in ndvi_pair_diagnostics.groupby(pair_column, sort=True):
        diag = pair_diag.iloc[0]
        pair_tests = pair_effect_tests[pair_effect_tests["comparison_pair"] == pair]
        pair_lift = event_driver_lift[event_driver_lift["comparison_pair"] == pair]
        pair_hypotheses = final_hypothesis_register[final_hypothesis_register["comparison_pair"] == pair]
        pair_outlook = ndvi_outlook[ndvi_outlook["comparison_pair"] == pair]

        ndvi_metric = _first_metric(pair_tests, "NDVI medio semanal")
        main_driver = pair_lift.iloc[0]["driver"] if not pair_lift.empty else "sem_driver_forte"
        supported = int((pair_hypotheses["status"] == "suportada").sum()) if not pair_hypotheses.empty else 0
        not_supported = int((pair_hypotheses["status"] == "nao_suportada").sum()) if not pair_hypotheses.empty else 0
        outlook_4_0 = pair_outlook[pair_outlook["treatment"] == "tecnologia_4_0"]

        rows.append(
            {
                "comparison_pair": pair,
                "temporal_winner": diag.get(winner_column),
                "ndvi_effect_direction": ndvi_metric.get("winner") if isinstance(ndvi_metric, pd.Series) else pd.NA,
                "ndvi_effect_value": ndvi_metric.get("advantage_4_0") if isinstance(ndvi_metric, pd.Series) else math.nan,
                "top_problem_driver": main_driver,
                "supported_hypotheses": supported,
                "not_supported_hypotheses": not_supported,
                "expected_vs_pair_4_0": _first_value(outlook_4_0, "expected_vs_pair"),
                "decision_message": _decision_message(pair, diag, ndvi_metric, main_driver, outlook_4_0),
                "next_step": _next_step_message(pair, main_driver, outlook_4_0),
            }
        )
    return pd.DataFrame(rows).sort_values("comparison_pair").reset_index(drop=True)


def _pair_effect_row(comparison_pair: str, tech: pd.DataFrame, conv: pd.DataFrame, spec: dict[str, Any]) -> dict[str, Any]:
    metric = spec["metric"]
    tech_frame = tech[["week_start", metric]].copy()
    conv_frame = conv[["week_start", metric]].copy()
    tech_frame[metric] = pd.to_numeric(tech_frame[metric], errors="coerce")
    conv_frame[metric] = pd.to_numeric(conv_frame[metric], errors="coerce")
    paired = tech_frame.merge(conv_frame, on="week_start", how="inner", suffixes=("_4_0", "_convencional")).dropna()
    if paired.empty:
        return {
            "comparison_pair": comparison_pair,
            "metric": metric,
            "metric_label": spec["label"],
            "weeks_compared": 0,
            "mean_4_0": math.nan,
            "mean_convencional": math.nan,
            "advantage_4_0": math.nan,
            "raw_difference_4_0_minus_convencional": math.nan,
            "ci_low": math.nan,
            "ci_high": math.nan,
            "p_value": math.nan,
            "paired_effect_size": math.nan,
            "winner": "inconclusivo",
            "evidence_level": "baixa",
        }

    tech_values = paired[f"{metric}_4_0"].to_numpy(dtype=float)
    conv_values = paired[f"{metric}_convencional"].to_numpy(dtype=float)
    raw_diff = tech_values - conv_values
    adjusted_diff = raw_diff if spec["higher_is_better"] else -raw_diff
    observed = float(np.mean(adjusted_diff))
    ci_low, ci_high = _bootstrap_mean_ci(adjusted_diff)
    p_value = _sign_flip_p_value(adjusted_diff)
    effect_size = _paired_effect_size(adjusted_diff)
    winner = _effect_winner(observed, ci_low, ci_high, spec["threshold"])
    evidence_level = _effect_evidence_level(len(adjusted_diff), p_value, ci_low, ci_high, observed, spec["threshold"])

    return {
        "comparison_pair": comparison_pair,
        "metric": metric,
        "metric_label": spec["label"],
        "weeks_compared": int(len(adjusted_diff)),
        "mean_4_0": float(np.mean(tech_values)),
        "mean_convencional": float(np.mean(conv_values)),
        "advantage_4_0": observed,
        "raw_difference_4_0_minus_convencional": float(np.mean(raw_diff)),
        "ci_low": ci_low,
        "ci_high": ci_high,
        "p_value": p_value,
        "paired_effect_size": effect_size,
        "winner": winner,
        "evidence_level": evidence_level,
    }


def _build_model_design_frame(model_frame: pd.DataFrame) -> pd.DataFrame:
    numeric = model_frame[MODEL_NUMERIC_FEATURES + MODEL_BOOL_FEATURES].copy()
    for column in numeric.columns:
        numeric[column] = pd.to_numeric(numeric[column], errors="coerce")
    numeric = numeric.fillna(numeric.median(numeric_only=True)).fillna(0)
    categories = pd.get_dummies(model_frame[MODEL_CATEGORY_FEATURES].fillna("desconhecido"), prefix=MODEL_CATEGORY_FEATURES, dtype=float)
    return pd.concat([numeric, categories], axis=1)


def _standardized_matrix(feature_frame: pd.DataFrame) -> tuple[np.ndarray, list[str], dict[str, float], dict[str, float]]:
    numeric = feature_frame.apply(pd.to_numeric, errors="coerce").fillna(0)
    means = numeric.mean().to_dict()
    stds_series = numeric.std(ddof=0).replace(0, 1).fillna(1)
    standardized = (numeric - pd.Series(means)) / stds_series
    x = np.column_stack([np.ones(len(standardized)), standardized.to_numpy(dtype=float)])
    return x, standardized.columns.tolist(), means, stds_series.to_dict()


def _ridge_fit(x_matrix: np.ndarray, y: np.ndarray, *, alpha: float) -> np.ndarray:
    identity = np.eye(x_matrix.shape[1], dtype=float)
    identity[0, 0] = 0.0
    return np.linalg.pinv(x_matrix.T @ x_matrix + alpha * identity) @ x_matrix.T @ y


def _ridge_predict(x_matrix: np.ndarray, beta: np.ndarray) -> np.ndarray:
    return x_matrix @ beta


def _bootstrap_mean_ci(values: np.ndarray, *, random_state: int = 42, iterations: int = 2000) -> tuple[float, float]:
    if len(values) == 0:
        return (math.nan, math.nan)
    rng = np.random.default_rng(random_state)
    samples = rng.choice(values, size=(iterations, len(values)), replace=True)
    means = samples.mean(axis=1)
    return (float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975)))


def _sign_flip_p_value(values: np.ndarray, *, random_state: int = 42, iterations: int = 4000) -> float:
    if len(values) == 0:
        return math.nan
    rng = np.random.default_rng(random_state)
    observed = abs(float(np.mean(values)))
    flips = rng.choice([-1.0, 1.0], size=(iterations, len(values)), replace=True)
    permuted = (flips * values).mean(axis=1)
    return float((np.abs(permuted) >= observed).mean())


def _paired_effect_size(values: np.ndarray) -> float:
    if len(values) <= 1:
        return math.nan
    std = float(np.std(values, ddof=1))
    if std == 0:
        return math.nan
    return float(np.mean(values) / std)


def _effect_winner(observed: float, ci_low: float, ci_high: float, threshold: float) -> str:
    if math.isnan(observed):
        return "inconclusivo"
    if observed >= threshold and ci_low > 0:
        return "favorece_4_0"
    if observed <= -threshold and ci_high < 0:
        return "favorece_convencional"
    return "inconclusivo"


def _effect_evidence_level(weeks_compared: int, p_value: float, ci_low: float, ci_high: float, observed: float, threshold: float) -> str:
    if weeks_compared < 4 or math.isnan(observed):
        return "baixa"
    ci_excludes_zero = not (ci_low <= 0 <= ci_high)
    if weeks_compared >= 8 and ci_excludes_zero and p_value <= 0.05 and abs(observed) >= threshold:
        return "alta"
    if weeks_compared >= 6 and ((ci_excludes_zero and abs(observed) >= threshold) or p_value <= 0.10):
        return "media"
    return "baixa"


def _driver_lift_level(problem_rate: float, baseline_rate: float, problem_weeks: int) -> str:
    if problem_weeks < 3 or math.isnan(problem_rate) or math.isnan(baseline_rate):
        return "baixa"
    delta_pp = (problem_rate - baseline_rate) * 100
    if delta_pp >= 20:
        return "alta"
    if delta_pp >= 10:
        return "media"
    return "baixa"


def _metric_status(metric_row: pd.Series | None) -> str:
    if metric_row is None or metric_row.empty:
        return "inconclusiva"
    if metric_row.get("winner") == "favorece_4_0":
        return "suportada"
    if metric_row.get("winner") == "favorece_convencional":
        return "nao_suportada"
    return "inconclusiva"


def _metric_basis(metric_row: pd.Series | None) -> str:
    if metric_row is None or metric_row.empty:
        return "Sem base suficiente."
    return (
        f"{metric_row.get('metric_label')}: vantagem_4_0={_fmt_num(metric_row.get('advantage_4_0'))}, "
        f"IC95% [{_fmt_num(metric_row.get('ci_low'))}, {_fmt_num(metric_row.get('ci_high'))}], "
        f"p={_fmt_num(metric_row.get('p_value'))}, evidencia={metric_row.get('evidence_level')}."
    )


def _combined_problem_status(low_vigor_metric: pd.Series | None, drop_metric: pd.Series | None) -> str:
    statuses = {_metric_status(low_vigor_metric), _metric_status(drop_metric)}
    if "nao_suportada" in statuses and "suportada" not in statuses:
        return "nao_suportada"
    if "suportada" in statuses and "nao_suportada" not in statuses:
        return "suportada"
    if "suportada" in statuses and "nao_suportada" in statuses:
        return "parcialmente_suportada"
    return "inconclusiva"


def _driver_status(driver_row: pd.Series | None) -> str:
    if driver_row is None or driver_row.empty:
        return "inconclusiva"
    if driver_row.get("evidence_level") in {"alta", "media"} and pd.notna(driver_row.get("delta_pp")) and float(driver_row.get("delta_pp")) > 0:
        return "suportada"
    return "inconclusiva"


def _driver_basis(driver_row: pd.Series | None) -> str:
    if driver_row is None or driver_row.empty:
        return "Sem lift relevante de drivers."
    return (
        f"Driver dominante: {driver_row.get('driver')} com delta={_fmt_num(driver_row.get('delta_pp'))} pp, "
        f"lift={_fmt_num(driver_row.get('lift_ratio'))}, evidencia={driver_row.get('evidence_level')}."
    )


def _outlook_status(outlook_4_0: pd.DataFrame) -> str:
    value = _first_value(outlook_4_0, "expected_vs_pair")
    if value == "tende_a_chegar_acima_do_par":
        return "suportada"
    if value == "tende_a_chegar_abaixo_do_par":
        return "nao_suportada"
    if value == "sem_vantagem_clara_no_par":
        return "inconclusiva"
    return "inconclusiva"


def _outlook_basis(outlook_4_0: pd.DataFrame) -> str:
    if outlook_4_0.empty:
        return "Sem outlook 4.0 disponivel."
    row = outlook_4_0.iloc[0]
    return (
        f"Outlook 4.0: score={_fmt_num(row.get('trajectory_score'))}, "
        f"faixa={row.get('outlook_band')}, esperado={row.get('expected_vs_pair')}, riscos={row.get('top_risks')}."
    )


def _hypothesis_row(
    *,
    comparison_pair: str,
    hypothesis_id: str,
    hypothesis: str,
    status: str,
    proof_basis: str,
    known_limits: object,
) -> dict[str, Any]:
    return {
        "comparison_pair": comparison_pair,
        "hypothesis_id": hypothesis_id,
        "hypothesis": hypothesis,
        "status": status,
        "proof_basis": proof_basis,
        "known_limits": known_limits,
    }


def _decision_message(
    comparison_pair: str,
    diag: pd.Series,
    ndvi_metric: pd.Series | None,
    main_driver: str,
    outlook_4_0: pd.DataFrame,
) -> str:
    winner = ndvi_metric.get("winner") if isinstance(ndvi_metric, pd.Series) else "inconclusivo"
    outlook = _first_value(outlook_4_0, "expected_vs_pair")
    if winner == "favorece_convencional":
        return (
            f"No par {comparison_pair}, o convencional segue a referencia temporal de NDVI; o principal bloco de risco associado e {main_driver}; "
            f"o outlook 4.0 atual e {outlook}."
        )
    if winner == "favorece_4_0":
        return (
            f"No par {comparison_pair}, o 4.0 sustenta vantagem temporal de NDVI; o principal bloco de risco associado e {main_driver}; "
            f"o outlook 4.0 atual e {outlook}."
        )
    return (
        f"No par {comparison_pair}, o NDVI ainda nao fecha vencedor estatistico; o principal bloco de risco associado e {main_driver}; "
        f"o outlook 4.0 atual e {outlook}."
    )


def _next_step_message(comparison_pair: str, main_driver: str, outlook_4_0: pd.DataFrame) -> str:
    outlook = _first_value(outlook_4_0, "expected_vs_pair")
    if outlook == "tende_a_chegar_abaixo_do_par":
        return f"Priorizar plano corretivo em {main_driver} e reavaliar a aderencia operacional do 4.0 no par {comparison_pair}."
    if outlook == "tende_a_chegar_acima_do_par":
        return f"Preservar a consistencia operacional e monitorar {main_driver} para confirmar a vantagem pre-colheita no par {comparison_pair}."
    return f"Coletar mais cobertura e revisar {main_driver} antes de fechar conclusao executiva no par {comparison_pair}."


def _first_metric(pair_tests: pd.DataFrame, metric_label: str) -> pd.Series | None:
    if pair_tests.empty:
        return None
    frame = pair_tests[pair_tests["metric_label"] == metric_label]
    if frame.empty:
        return None
    return frame.iloc[0]


def _first_value(frame: pd.DataFrame, column: str) -> object:
    if frame.empty or column not in frame.columns:
        return pd.NA
    return frame.iloc[0][column]


def _lift_ratio(problem_rate: float, baseline_rate: float) -> float:
    if math.isnan(problem_rate) or math.isnan(baseline_rate):
        return math.nan
    if baseline_rate <= 0:
        return math.nan
    return float(problem_rate / baseline_rate)


def _classify_audit_status(row: pd.Series) -> str:
    ndvi_ratio = row.get("ndvi_valid_ratio")
    weather_ratio = row.get("weather_coverage_ratio")
    miip_ratio = row.get("miip_coverage_ratio")
    if pd.notna(ndvi_ratio) and ndvi_ratio < 0.55:
        return "critico"
    if pd.notna(weather_ratio) and weather_ratio < 0.40:
        return "atencao"
    if pd.notna(miip_ratio) and miip_ratio < 0.30:
        return "atencao"
    return "ok"


def _safe_div(numerator: object, denominator: object) -> pd.Series:
    left = pd.to_numeric(numerator, errors="coerce")
    right = pd.to_numeric(denominator, errors="coerce")
    return left.div(right.replace(0, pd.NA))


def _to_bool_series(series: object) -> pd.Series:
    return pd.Series(series, copy=False).fillna(False).astype("boolean")


def _r2_score(actual: np.ndarray, predicted: np.ndarray) -> float:
    if len(actual) == 0:
        return math.nan
    baseline = float(np.mean(actual))
    ss_tot = float(np.sum((actual - baseline) ** 2))
    ss_res = float(np.sum((actual - predicted) ** 2))
    if ss_tot == 0:
        return math.nan
    return 1.0 - (ss_res / ss_tot)


def _fmt_num(value: object) -> str:
    if value is None or pd.isna(value):
        return "NA"
    return f"{float(value):.3f}"


def _fmt_ts(value: object) -> str:
    if value is None or pd.isna(value):
        return "NA"
    return pd.Timestamp(value).strftime("%Y-%m-%d")
