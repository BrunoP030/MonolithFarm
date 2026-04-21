from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from farmlab.ndvi_crispdm import CRISPDM_OUTPUT_TABLES, PAIR_EFFECT_SPECS, build_ndvi_crispdm_workspace
from farmlab.pairwise import build_ndvi_weekly


SUPPORT_OUTPUT_TABLES = [
    "area_inventory",
    "ndvi_clean",
    "weather_daily",
    "weather_weekly",
    "ops_area_daily",
    "miip_daily",
    "pairwise_weekly_features",
    "ops_support_daily",
    "ops_support_weekly",
    "ndvi_phase_timeline",
    "ndvi_events",
    "ndvi_pair_diagnostics",
    "ndvi_outlook",
]

COMPLETE_OUTPUT_TABLES = [
    *SUPPORT_OUTPUT_TABLES,
    *CRISPDM_OUTPUT_TABLES,
    "dataset_overview",
    "numeric_profiles",
    "ndvi_stats_by_area",
    "ndvi_outliers",
    "pair_weekly_gaps",
    "pair_classic_tests",
    "ndvi_trend_tests",
    "weekly_correlations",
]

DATASET_PROFILE_SPECS: dict[str, list[str]] = {
    "ndvi_clean": [
        "ndvi_mean",
        "ndvi_std",
        "soil_pct",
        "dense_veg_pct",
        "ndvi_delta",
        "ndvi_auc",
        "b1_valid_pixels",
    ],
    "weather_daily": [
        "precipitation_mm",
        "evapotranspiration_mm",
        "water_balance_mm",
        "solar_radiation_w_m2",
        "temp_avg_c",
        "temp_max_c",
        "temp_min_c",
        "humidity_avg_pct",
        "wind_avg_kmh",
    ],
    "ops_area_daily": [
        "planting_population_mean_ha",
        "harvest_yield_mean_kg_ha",
        "harvest_humidity_mean_pct",
        "fert_dose_gap_abs_mean_kg_ha",
        "overlap_area_pct_bbox",
        "stop_duration_h_per_bbox_ha",
    ],
    "miip_daily": [
        "avg_pest_count",
        "total_pest_count",
        "alert_hits",
        "control_hits",
        "damage_hits",
        "image_pest_count",
        "detected_boxes",
    ],
    "transition_model_frame": [
        "target_next_ndvi_delta",
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
    ],
}

CLASSIC_TEST_SPECS = [
    *PAIR_EFFECT_SPECS,
    {
        "metric": "avg_pest_count_week",
        "label": "Contagem media de pragas",
        "higher_is_better": False,
        "threshold": 1.0,
    },
    {
        "metric": "fert_dose_gap_abs_mean_kg_ha_week",
        "label": "Gap absoluto de dose na adubacao",
        "higher_is_better": False,
        "threshold": 20.0,
    },
    {
        "metric": "overlap_area_pct_bbox_week",
        "label": "Sobreposicao operacional relativa",
        "higher_is_better": False,
        "threshold": 0.01,
    },
    {
        "metric": "stop_duration_h_per_bbox_ha_week",
        "label": "Horas de parada por area",
        "higher_is_better": False,
        "threshold": 0.05,
    },
    {
        "metric": "invalid_telemetry_share_week",
        "label": "Falha de telemetria",
        "higher_is_better": False,
        "threshold": 0.05,
    },
    {
        "metric": "alarm_events_week",
        "label": "Alertas de maquina",
        "higher_is_better": False,
        "threshold": 5.0,
    },
]

CORRELATION_FEATURES = [
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
    "low_vigor_flag",
    "major_drop_flag",
    "pest_risk_flag",
    "ops_risk_flag",
]


def build_complete_ndvi_workspace(base_dir: Path, manual_mapping: pd.DataFrame | None = None) -> dict[str, Any]:
    crisp = build_ndvi_crispdm_workspace(base_dir, manual_mapping)
    pair_weekly_gaps = build_pair_weekly_gaps(crisp["ndvi_phase_timeline"])
    return {
        **crisp,
        "dataset_overview": build_dataset_overview(crisp),
        "numeric_profiles": build_numeric_profiles(crisp),
        "ndvi_stats_by_area": build_ndvi_stats_by_area(crisp["ndvi_clean"]),
        "ndvi_outliers": build_ndvi_outliers(crisp["ndvi_clean"]),
        "pair_weekly_gaps": pair_weekly_gaps,
        "pair_classic_tests": build_pair_classic_tests(pair_weekly_gaps),
        "ndvi_trend_tests": build_ndvi_trend_tests(crisp["ndvi_clean"]),
        "weekly_correlations": build_weekly_correlations(crisp["transition_model_frame"]),
    }


def save_complete_ndvi_outputs(workspace: dict[str, Any], output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for table_name in COMPLETE_OUTPUT_TABLES:
        frame = workspace.get(table_name)
        if not isinstance(frame, pd.DataFrame):
            continue
        path = output_dir / f"{table_name}.csv"
        frame.to_csv(path, index=False)
        written.append(path)
    return written


def build_dataset_overview(workspace: dict[str, Any]) -> pd.DataFrame:
    frame_names = [
        "ndvi_clean",
        "weather_daily",
        "ops_area_daily",
        "miip_daily",
        "ndvi_phase_timeline",
        "transition_model_frame",
        "transition_model_predictions",
        "soil_context",
    ]
    rows: list[dict[str, Any]] = []
    for frame_name in frame_names:
        frame = workspace.get(frame_name)
        if not isinstance(frame, pd.DataFrame):
            continue
        rows.append(
            {
                "dataset": frame_name,
                "rows": int(len(frame)),
                "columns": int(len(frame.columns)),
                "numeric_columns": int(len(frame.select_dtypes(include=["number", "bool"]).columns)),
                "null_cells": int(frame.isna().sum().sum()),
                "null_ratio": _safe_ratio(frame.isna().sum().sum(), frame.shape[0] * frame.shape[1]),
                "unique_seasons": int(frame["season_id"].nunique()) if "season_id" in frame.columns else math.nan,
                "unique_areas": int(frame["area_label"].nunique()) if "area_label" in frame.columns else math.nan,
                "date_start": _frame_date_start(frame),
                "date_end": _frame_date_end(frame),
                "sample_columns": ", ".join(frame.columns[:8]),
            }
        )
    return pd.DataFrame(rows).sort_values("dataset").reset_index(drop=True)


def build_numeric_profiles(workspace: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for frame_name, columns in DATASET_PROFILE_SPECS.items():
        frame = workspace.get(frame_name)
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            continue
        for column in columns:
            if column not in frame.columns:
                continue
            series = pd.to_numeric(frame[column], errors="coerce")
            rows.append(
                {
                    "dataset": frame_name,
                    "variable": column,
                    **_series_profile(series),
                }
            )
    result = pd.DataFrame(rows)
    if result.empty:
        return pd.DataFrame(columns=["dataset", "variable"])
    return result.sort_values(["dataset", "variable"]).reset_index(drop=True)


def build_ndvi_stats_by_area(ndvi_clean: pd.DataFrame) -> pd.DataFrame:
    if ndvi_clean.empty:
        return pd.DataFrame(columns=["season_id", "area_label"])

    rows: list[dict[str, Any]] = []
    group_columns = ["season_id", "area_label", "treatment", "crop_type", "comparison_pair"]
    for keys, group in ndvi_clean.groupby(group_columns, dropna=False):
        series = pd.to_numeric(group["ndvi_mean"], errors="coerce")
        profile = _series_profile(series)
        rows.append(
            {
                "season_id": keys[0],
                "area_label": keys[1],
                "treatment": keys[2],
                "crop_type": keys[3],
                "comparison_pair": keys[4],
                "images_valid": int(series.notna().sum()),
                "weeks_with_scenes": int(pd.to_datetime(group["week_start"], errors="coerce").nunique()),
                "scene_start": pd.to_datetime(group["date"], errors="coerce").min(),
                "scene_end": pd.to_datetime(group["date"], errors="coerce").max(),
                "soil_pct_mean": float(pd.to_numeric(group["soil_pct"], errors="coerce").mean()),
                "dense_veg_pct_mean": float(pd.to_numeric(group["dense_veg_pct"], errors="coerce").mean()),
                **profile,
            }
        )
    return pd.DataFrame(rows).sort_values(["comparison_pair", "area_label"]).reset_index(drop=True)


def build_ndvi_outliers(ndvi_clean: pd.DataFrame) -> pd.DataFrame:
    if ndvi_clean.empty:
        return pd.DataFrame(columns=["season_id", "date", "area_label"])

    frame = ndvi_clean.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["ndvi_mean"] = pd.to_numeric(frame["ndvi_mean"], errors="coerce")

    rows: list[pd.DataFrame] = []
    for _, group in frame.groupby("season_id", dropna=False):
        series = group["ndvi_mean"]
        mean = float(series.mean())
        std = float(series.std(ddof=0))
        median = float(series.median())
        mad = float(np.median(np.abs(series - median))) if series.notna().any() else math.nan

        group = group.copy()
        group["ndvi_zscore"] = (group["ndvi_mean"] - mean) / std if std > 0 else math.nan
        group["ndvi_robust_zscore"] = 0.6745 * (group["ndvi_mean"] - median) / mad if mad > 0 else math.nan
        group["outlier_zscore_flag"] = group["ndvi_zscore"].abs() >= 2.0
        group["outlier_robust_flag"] = group["ndvi_robust_zscore"].abs() >= 3.5
        group["outlier_flag"] = group["outlier_zscore_flag"] | group["outlier_robust_flag"]
        group["outlier_direction"] = np.where(group["ndvi_zscore"] < 0, "ndvi_abaixo_do_padrao", "ndvi_acima_do_padrao")
        rows.append(group)

    result = pd.concat(rows, ignore_index=True)
    keep_columns = [
        "season_id",
        "date",
        "week_start",
        "area_label",
        "treatment",
        "comparison_pair",
        "ndvi_mean",
        "soil_pct",
        "dense_veg_pct",
        "ndvi_zscore",
        "ndvi_robust_zscore",
        "outlier_zscore_flag",
        "outlier_robust_flag",
        "outlier_flag",
        "outlier_direction",
        "image_path",
    ]
    return result[keep_columns].sort_values(
        ["outlier_flag", "comparison_pair", "area_label", "date", "ndvi_zscore"],
        ascending=[False, True, True, True, True],
    ).reset_index(drop=True)


def build_pair_weekly_gaps(ndvi_phase_timeline: pd.DataFrame) -> pd.DataFrame:
    if ndvi_phase_timeline.empty:
        return pd.DataFrame(columns=["comparison_pair", "week_start"])

    metric_columns = sorted(
        {
            spec["metric"]
            for spec in CLASSIC_TEST_SPECS
            if spec["metric"] in ndvi_phase_timeline.columns
        }
    )
    base_columns = [column for column in ["week_start", "area_label", "season_id"] + metric_columns if column in ndvi_phase_timeline.columns]
    rows: list[pd.DataFrame] = []

    for comparison_pair, pair_frame in ndvi_phase_timeline.groupby("comparison_pair", sort=True):
        tech = pair_frame[pair_frame["treatment"] == "tecnologia_4_0"][base_columns].copy()
        conv = pair_frame[pair_frame["treatment"] == "convencional"][base_columns].copy()
        if tech.empty or conv.empty:
            continue

        merged = tech.merge(conv, on="week_start", how="outer", suffixes=("_4_0", "_convencional"))
        merged["comparison_pair"] = comparison_pair
        merged["tech_area_label"] = merged.get("area_label_4_0")
        merged["conv_area_label"] = merged.get("area_label_convencional")
        merged["paired_week_flag"] = merged["season_id_4_0"].notna() & merged["season_id_convencional"].notna()

        for metric in metric_columns:
            tech_column = f"{metric}_4_0"
            conv_column = f"{metric}_convencional"
            gap_column = f"gap_{metric}_4_0_minus_convencional"
            if tech_column in merged.columns and conv_column in merged.columns:
                merged[gap_column] = pd.to_numeric(merged[tech_column], errors="coerce") - pd.to_numeric(
                    merged[conv_column], errors="coerce"
                )

        rows.append(merged)

    if not rows:
        return pd.DataFrame(columns=["comparison_pair", "week_start"])
    return pd.concat(rows, ignore_index=True).sort_values(["comparison_pair", "week_start"]).reset_index(drop=True)


def build_pair_classic_tests(pair_weekly_gaps: pd.DataFrame) -> pd.DataFrame:
    if pair_weekly_gaps.empty:
        return pd.DataFrame(columns=["comparison_pair", "metric"])

    rows: list[dict[str, Any]] = []
    for comparison_pair, pair_frame in pair_weekly_gaps.groupby("comparison_pair", sort=True):
        for spec in CLASSIC_TEST_SPECS:
            gap_column = f"gap_{spec['metric']}_4_0_minus_convencional"
            tech_column = f"{spec['metric']}_4_0"
            conv_column = f"{spec['metric']}_convencional"
            if gap_column not in pair_frame.columns:
                continue

            subset = pair_frame[[gap_column, tech_column, conv_column]].copy()
            subset[gap_column] = pd.to_numeric(subset[gap_column], errors="coerce")
            subset[tech_column] = pd.to_numeric(subset[tech_column], errors="coerce")
            subset[conv_column] = pd.to_numeric(subset[conv_column], errors="coerce")
            subset = subset.dropna()
            if subset.empty:
                continue

            raw_diff = subset[gap_column].to_numpy(dtype=float)
            favorable_diff = raw_diff if spec["higher_is_better"] else -raw_diff
            mean_favorable_diff = float(np.mean(favorable_diff))
            mean_raw_diff = float(np.mean(raw_diff))
            recommended_p = math.nan

            shapiro_p = _shapiro_pvalue(raw_diff)
            ttest_p = _ttest_1samp_pvalue(raw_diff)
            wilcoxon_p = _wilcoxon_pvalue(raw_diff)
            if not math.isnan(shapiro_p) and shapiro_p >= 0.05 and not math.isnan(ttest_p):
                recommended_p = ttest_p
            elif not math.isnan(wilcoxon_p):
                recommended_p = wilcoxon_p
            else:
                recommended_p = ttest_p

            rows.append(
                {
                    "comparison_pair": comparison_pair,
                    "metric": spec["metric"],
                    "metric_label": spec["label"],
                    "weeks_compared": int(len(subset)),
                    "mean_4_0": float(subset[tech_column].mean()),
                    "mean_convencional": float(subset[conv_column].mean()),
                    "mean_gap_4_0_minus_convencional": mean_raw_diff,
                    "mean_favorable_gap_4_0": mean_favorable_diff,
                    "median_gap_4_0_minus_convencional": float(np.median(raw_diff)),
                    "std_gap_4_0_minus_convencional": float(np.std(raw_diff, ddof=1)) if len(raw_diff) > 1 else math.nan,
                    "gap_zscore_mean": _mean_zscore(raw_diff),
                    "normality_shapiro_p": shapiro_p,
                    "ttest_gap_vs_zero_p": ttest_p,
                    "wilcoxon_gap_vs_zero_p": wilcoxon_p,
                    "recommended_p_value": recommended_p,
                    "recommended_test": _recommended_test_name(shapiro_p, ttest_p, wilcoxon_p),
                    "paired_effect_size_dz": _cohens_dz(raw_diff),
                    "favors": _favors_label(mean_favorable_diff, recommended_p, spec["threshold"]),
                    "significant_0_05": bool(not math.isnan(recommended_p) and recommended_p <= 0.05),
                }
            )

    result = pd.DataFrame(rows)
    if result.empty:
        return pd.DataFrame(columns=["comparison_pair", "metric"])
    return result.sort_values(["comparison_pair", "recommended_p_value", "metric_label"]).reset_index(drop=True)


def build_ndvi_trend_tests(ndvi_clean: pd.DataFrame) -> pd.DataFrame:
    if ndvi_clean.empty:
        return pd.DataFrame(columns=["season_id", "area_label"])

    weekly = build_ndvi_weekly(ndvi_clean)
    weekly = weekly.merge(
        ndvi_clean[["season_id", "area_label", "treatment", "comparison_pair"]].drop_duplicates(),
        on="season_id",
        how="left",
    )

    rows: list[dict[str, Any]] = []
    for keys, group in weekly.groupby(["season_id", "area_label", "treatment", "comparison_pair"], dropna=False):
        series = pd.to_numeric(group["ndvi_mean_week"], errors="coerce").dropna()
        if series.empty:
            continue
        x_values = np.arange(len(series), dtype=float)
        if len(series) >= 2:
            fit = stats.linregress(x_values, series.to_numpy(dtype=float))
            slope = float(fit.slope)
            intercept = float(fit.intercept)
            p_value = float(fit.pvalue)
            r_value = float(fit.rvalue)
            stderr = float(fit.stderr)
        else:
            slope = math.nan
            intercept = math.nan
            p_value = math.nan
            r_value = math.nan
            stderr = math.nan

        rows.append(
            {
                "season_id": keys[0],
                "area_label": keys[1],
                "treatment": keys[2],
                "comparison_pair": keys[3],
                "weeks": int(len(series)),
                "slope_ndvi_per_week": slope,
                "intercept_ndvi": intercept,
                "r_value": r_value,
                "r_squared": r_value**2 if not math.isnan(r_value) else math.nan,
                "p_value": p_value,
                "stderr": stderr,
                "trend_direction": _trend_direction_label(slope, p_value),
            }
        )
    return pd.DataFrame(rows).sort_values(["comparison_pair", "area_label"]).reset_index(drop=True)


def build_weekly_correlations(transition_model_frame: pd.DataFrame) -> pd.DataFrame:
    if transition_model_frame.empty:
        return pd.DataFrame(columns=["analysis_target", "comparison_pair", "feature"])

    frame = transition_model_frame.copy()
    rows: list[dict[str, Any]] = []
    targets = {
        "ndvi_mean_week": "nivel_ndvi",
        "target_next_ndvi_delta": "delta_ndvi_seguinte",
    }

    group_specs = [("geral", frame)]
    group_specs.extend((str(pair), group) for pair, group in frame.groupby("comparison_pair", sort=True))

    for comparison_pair, group in group_specs:
        for target_column, target_label in targets.items():
            if target_column not in group.columns:
                continue
            for feature in CORRELATION_FEATURES:
                if feature not in group.columns or feature == target_column:
                    continue
                subset = group[[target_column, feature]].copy().apply(pd.to_numeric, errors="coerce").dropna()
                if len(subset) < 5:
                    continue
                pearson_r, pearson_p = _safe_pearson(subset[target_column], subset[feature])
                spearman_rho, spearman_p = _safe_spearman(subset[target_column], subset[feature])
                strongest = _strongest_abs_corr(pearson_r, spearman_rho)
                rows.append(
                    {
                        "analysis_target": target_label,
                        "comparison_pair": comparison_pair,
                        "feature": feature,
                        "observations": int(len(subset)),
                        "pearson_r": pearson_r,
                        "pearson_p": pearson_p,
                        "spearman_rho": spearman_rho,
                        "spearman_p": spearman_p,
                        "strongest_abs_correlation": strongest,
                        "direction": _correlation_direction_label(strongest),
                        "strength": _correlation_strength_label(strongest),
                    }
                )

    result = pd.DataFrame(rows)
    if result.empty:
        return pd.DataFrame(columns=["analysis_target", "comparison_pair", "feature"])
    return result.sort_values(
        ["analysis_target", "comparison_pair", "strongest_abs_correlation"],
        ascending=[True, True, False],
    ).reset_index(drop=True)


def _series_profile(series: pd.Series) -> dict[str, Any]:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return {
            "count": 0,
            "missing": int(numeric.isna().sum()),
            "missing_ratio": 1.0 if len(numeric) else math.nan,
            "mean": math.nan,
            "median": math.nan,
            "std": math.nan,
            "min": math.nan,
            "q1": math.nan,
            "q3": math.nan,
            "max": math.nan,
            "cv": math.nan,
            "skew": math.nan,
            "kurtosis": math.nan,
        }

    mean = float(valid.mean())
    std = float(valid.std(ddof=1)) if len(valid) > 1 else math.nan
    return {
        "count": int(valid.count()),
        "missing": int(numeric.isna().sum()),
        "missing_ratio": _safe_ratio(numeric.isna().sum(), len(numeric)),
        "mean": mean,
        "median": float(valid.median()),
        "std": std,
        "min": float(valid.min()),
        "q1": float(valid.quantile(0.25)),
        "q3": float(valid.quantile(0.75)),
        "max": float(valid.max()),
        "cv": abs(std / mean) if not math.isnan(std) and mean not in {0.0, -0.0} else math.nan,
        "skew": float(valid.skew()) if len(valid) > 2 else math.nan,
        "kurtosis": float(valid.kurt()) if len(valid) > 3 else math.nan,
    }


def _frame_date_start(frame: pd.DataFrame) -> pd.Timestamp | pd.NaT:
    for column in ["date", "week_start", "timestamp", "Date Time"]:
        if column in frame.columns:
            return pd.to_datetime(frame[column], errors="coerce").min()
    return pd.NaT


def _frame_date_end(frame: pd.DataFrame) -> pd.Timestamp | pd.NaT:
    for column in ["date", "week_start", "timestamp", "Date Time"]:
        if column in frame.columns:
            return pd.to_datetime(frame[column], errors="coerce").max()
    return pd.NaT


def _safe_ratio(numerator: float | int, denominator: float | int) -> float:
    if denominator in {0, 0.0}:
        return math.nan
    return float(numerator) / float(denominator)


def _shapiro_pvalue(values: np.ndarray) -> float:
    if len(values) < 3 or len(values) > 5000:
        return math.nan
    if np.allclose(values, values[0]):
        return math.nan
    return float(stats.shapiro(values).pvalue)


def _ttest_1samp_pvalue(values: np.ndarray) -> float:
    if len(values) < 2:
        return math.nan
    if np.allclose(values, values[0]):
        return 0.0 if not np.isclose(values[0], 0.0) else 1.0
    return float(stats.ttest_1samp(values, popmean=0.0, nan_policy="omit").pvalue)


def _wilcoxon_pvalue(values: np.ndarray) -> float:
    if len(values) < 2:
        return math.nan
    non_zero = values[~np.isclose(values, 0.0)]
    if len(non_zero) == 0:
        return 1.0
    try:
        return float(stats.wilcoxon(non_zero, alternative="two-sided").pvalue)
    except ValueError:
        return math.nan


def _mean_zscore(values: np.ndarray) -> float:
    if len(values) < 2:
        return math.nan
    std = float(np.std(values, ddof=1))
    if std == 0:
        return math.nan
    return float(np.mean(values) / (std / math.sqrt(len(values))))


def _cohens_dz(values: np.ndarray) -> float:
    if len(values) < 2:
        return math.nan
    std = float(np.std(values, ddof=1))
    if std == 0:
        return math.nan
    return float(np.mean(values) / std)


def _recommended_test_name(shapiro_p: float, ttest_p: float, wilcoxon_p: float) -> str:
    if not math.isnan(shapiro_p) and shapiro_p >= 0.05 and not math.isnan(ttest_p):
        return "ttest_1samp"
    if not math.isnan(wilcoxon_p):
        return "wilcoxon"
    if not math.isnan(ttest_p):
        return "ttest_1samp"
    return "sem_teste"


def _favors_label(mean_favorable_diff: float, p_value: float, threshold: float) -> str:
    if math.isnan(mean_favorable_diff):
        return "inconclusivo"
    if p_value <= 0.05 and mean_favorable_diff >= threshold:
        return "favorece_4_0"
    if p_value <= 0.05 and mean_favorable_diff <= -threshold:
        return "favorece_convencional"
    return "inconclusivo"


def _trend_direction_label(slope: float, p_value: float) -> str:
    if math.isnan(slope):
        return "sem_tendencia_estimada"
    if p_value <= 0.05 and slope > 0:
        return "crescimento_significativo"
    if p_value <= 0.05 and slope < 0:
        return "queda_significativa"
    if slope > 0:
        return "leve_crescimento_sem_significancia"
    if slope < 0:
        return "leve_queda_sem_significancia"
    return "estavel"


def _safe_pearson(x_values: pd.Series, y_values: pd.Series) -> tuple[float, float]:
    if x_values.nunique(dropna=True) <= 1 or y_values.nunique(dropna=True) <= 1:
        return (math.nan, math.nan)
    result = stats.pearsonr(x_values.to_numpy(dtype=float), y_values.to_numpy(dtype=float))
    return (float(result.statistic), float(result.pvalue))


def _safe_spearman(x_values: pd.Series, y_values: pd.Series) -> tuple[float, float]:
    if x_values.nunique(dropna=True) <= 1 or y_values.nunique(dropna=True) <= 1:
        return (math.nan, math.nan)
    result = stats.spearmanr(x_values.to_numpy(dtype=float), y_values.to_numpy(dtype=float), nan_policy="omit")
    return (float(result.statistic), float(result.pvalue))


def _strongest_abs_corr(pearson_r: float, spearman_rho: float) -> float:
    values = [value for value in [pearson_r, spearman_rho] if not math.isnan(value)]
    if not values:
        return math.nan
    return float(max(values, key=lambda value: abs(value)))


def _correlation_direction_label(value: float) -> str:
    if math.isnan(value):
        return "sem_relacao"
    if value > 0:
        return "positiva"
    if value < 0:
        return "negativa"
    return "nula"


def _correlation_strength_label(value: float) -> str:
    if math.isnan(value):
        return "sem_relacao"
    abs_value = abs(value)
    if abs_value >= 0.7:
        return "forte"
    if abs_value >= 0.4:
        return "moderada"
    if abs_value >= 0.2:
        return "fraca"
    return "muito_fraca"
