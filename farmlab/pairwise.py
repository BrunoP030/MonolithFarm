from __future__ import annotations

import json
import math
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd

from farmlab.analysis import build_season_geometries
from farmlab.io import (
    discover_dataset_paths,
    load_layer_map,
    load_ndvi_metadata,
    load_pest_details,
    load_pest_list,
    load_soil_analysis,
    load_traps_data,
    load_traps_events,
    load_traps_list,
    load_weather_hourly,
)


AREA_METADATA_COLUMNS = [
    "season_id",
    "area_label",
    "treatment",
    "crop_type",
    "comparison_pair",
    "mapping_source",
    "notes",
]

OFFICIAL_AREA_MAPPING = [
    {
        "season_id": "0bf86c8b-2779-4a98-ba25-7cf9518ee316",
        "area_label": "Silagem Convencional",
        "treatment": "convencional",
        "crop_type": "silagem",
        "comparison_pair": "silagem",
        "mapping_source": "official_portal",
        "notes": "Mapeamento oficial do portal FarmLab.",
    },
    {
        "season_id": "258e47e0-3bdc-4419-bedd-7b85812c2113",
        "area_label": "Grao 4.0",
        "treatment": "tecnologia_4_0",
        "crop_type": "grao",
        "comparison_pair": "grao",
        "mapping_source": "official_portal",
        "notes": "Mapeamento oficial do portal FarmLab.",
    },
    {
        "season_id": "b7292cb8-72bb-447a-8feb-8ac983afd50b",
        "area_label": "Grao Convencional",
        "treatment": "convencional",
        "crop_type": "grao",
        "comparison_pair": "grao",
        "mapping_source": "official_portal",
        "notes": "Mapeamento oficial do portal FarmLab.",
    },
    {
        "season_id": "f791bf13-1d24-4b4f-88bd-2569162df2b3",
        "area_label": "Silagem 4.0",
        "treatment": "tecnologia_4_0",
        "crop_type": "silagem",
        "comparison_pair": "silagem",
        "mapping_source": "official_portal",
        "notes": "Mapeamento oficial do portal FarmLab.",
    },
]

OUTPUT_TABLES = [
    "area_inventory",
    "ndvi_clean",
    "ops_area_daily",
    "miip_daily",
    "pairwise_weekly_features",
    "hypothesis_matrix",
]


def build_phase1_workspace(base_dir: Path, manual_mapping: pd.DataFrame | None = None) -> dict[str, Any]:
    paths = discover_dataset_paths(base_dir)
    ndvi = load_ndvi_metadata(paths)
    seasons = build_season_geometries(ndvi)
    area_metadata = build_area_metadata(ndvi, manual_mapping).merge(
        seasons[["season_id", "bbox_area_ha"]],
        on="season_id",
        how="left",
    )

    soil = load_soil_analysis(paths.soil_analysis)
    weather = load_weather_hourly(paths.weather_hourly)
    weather_daily = build_weather_daily(weather)
    weather_weekly = build_weather_weekly(weather_daily)
    ndvi_clean = build_ndvi_clean(ndvi, area_metadata, weather_daily)
    ops_area_daily = build_ops_area_daily(paths, seasons, area_metadata)
    miip_daily = build_miip_daily(paths, seasons, area_metadata)
    pairwise_weekly_features = build_pairwise_weekly_features(
        ndvi_clean=ndvi_clean,
        weather_weekly=weather_weekly,
        ops_area_daily=ops_area_daily,
        miip_daily=miip_daily,
        area_metadata=area_metadata,
    )
    area_inventory = build_area_inventory(
        ndvi=ndvi,
        ndvi_clean=ndvi_clean,
        ops_area_daily=ops_area_daily,
        miip_daily=miip_daily,
        area_metadata=area_metadata,
        soil=soil,
        weather_daily=weather_daily,
    )
    hypothesis_matrix = build_hypothesis_matrix(
        area_inventory=area_inventory,
        pairwise_weekly_features=pairwise_weekly_features,
    )
    gaps = list_phase1_gaps(
        ndvi=ndvi,
        weather_daily=weather_daily,
        soil=soil,
        ops_area_daily=ops_area_daily,
        paths_summary=paths,
    )

    return {
        "area_metadata": area_metadata.drop(columns=["bbox_area_ha"], errors="ignore"),
        "area_inventory": area_inventory,
        "ndvi_clean": ndvi_clean,
        "ops_area_daily": ops_area_daily,
        "miip_daily": miip_daily,
        "pairwise_weekly_features": pairwise_weekly_features,
        "hypothesis_matrix": hypothesis_matrix,
        "weather_daily": weather_daily,
        "weather_weekly": weather_weekly,
        "soil_context": soil,
        "gaps": gaps,
    }


def build_area_metadata(ndvi: pd.DataFrame, manual_mapping: pd.DataFrame | None = None) -> pd.DataFrame:
    observed = pd.DataFrame({"season_id": sorted(ndvi["season_id"].dropna().astype(str).unique())})
    base = observed.merge(pd.DataFrame(OFFICIAL_AREA_MAPPING), on="season_id", how="left")

    missing = base["area_label"].isna()
    if missing.any():
        base.loc[missing, "area_label"] = base.loc[missing, "season_id"].map(lambda value: f"Area {value[:8]}")
        base.loc[missing, "treatment"] = "indefinido"
        base.loc[missing, "crop_type"] = "desconhecido"
        base.loc[missing, "comparison_pair"] = "desconhecido"
        base.loc[missing, "mapping_source"] = "auto_missing"
        base.loc[missing, "notes"] = "Season sem mapeamento oficial; validar manualmente."

    if manual_mapping is None or manual_mapping.empty:
        return base[AREA_METADATA_COLUMNS].copy()

    mapping = manual_mapping.copy()
    for column in AREA_METADATA_COLUMNS[1:]:
        if column not in mapping.columns:
            mapping[column] = pd.NA

    merged = base.merge(mapping, on="season_id", how="left", suffixes=("", "_manual"))
    for column in AREA_METADATA_COLUMNS[1:]:
        merged[column] = merged[f"{column}_manual"].combine_first(merged[column])

    drop_columns = [column for column in merged.columns if column.endswith("_manual")]
    return merged.drop(columns=drop_columns)[AREA_METADATA_COLUMNS].copy()


def build_ndvi_clean(
    ndvi: pd.DataFrame,
    area_metadata: pd.DataFrame,
    weather_daily: pd.DataFrame,
) -> pd.DataFrame:
    frame = ndvi.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    frame = frame[frame["b1_valid_pixels"].fillna(0) > 0].copy()
    frame = frame.merge(area_metadata, on="season_id", how="left")
    frame["week_start"] = _week_start(frame["date"])
    frame["ndvi_mean"] = frame["b1_mean"]
    frame["ndvi_std"] = frame["b1_std"]
    frame["soil_pct"] = frame["b1_pct_solo"]
    frame["dense_veg_pct"] = frame["b1_pct_veg_densa"]
    frame = frame.sort_values(["season_id", "date"]).reset_index(drop=True)
    frame["ndvi_delta"] = frame.groupby("season_id")["ndvi_mean"].diff()
    frame["ndvi_auc"] = _cumulative_auc(frame, group_col="season_id", date_col="date", value_col="ndvi_mean")

    weather_start = weather_daily["date"].min() if not weather_daily.empty else pd.NaT
    weather_end = weather_daily["date"].max() if not weather_daily.empty else pd.NaT
    frame["has_weather_coverage"] = frame["date"].between(weather_start, weather_end, inclusive="both")
    return frame


def build_weather_daily(weather: pd.DataFrame) -> pd.DataFrame:
    if weather.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "precipitation_mm",
                "evapotranspiration_mm",
                "water_balance_mm",
                "solar_radiation_w_m2",
                "temp_avg_c",
                "temp_max_c",
                "temp_min_c",
                "humidity_avg_pct",
                "wind_avg_kmh",
                "week_start",
            ]
        )

    daily = (
        weather.groupby("date", as_index=False)
        .agg(
            precipitation_mm=("precipitation_mm", "sum"),
            evapotranspiration_mm=("evapotranspiration_mm", "sum"),
            solar_radiation_w_m2=("solar_radiation_w_m2", "mean"),
            temp_avg_c=("temp_avg_c", "mean"),
            temp_max_c=("temp_max_c", "max"),
            temp_min_c=("temp_min_c", "min"),
            humidity_avg_pct=("humidity_avg_pct", "mean"),
            wind_avg_kmh=("wind_avg_kmh", "mean"),
        )
        .sort_values("date")
        .reset_index(drop=True)
    )
    daily["date"] = pd.to_datetime(daily["date"], errors="coerce").dt.normalize()
    daily["water_balance_mm"] = daily["precipitation_mm"].fillna(0) - daily["evapotranspiration_mm"].fillna(0)
    daily["precipitation_mm_ma7"] = daily["precipitation_mm"].rolling(7, min_periods=1).mean()
    daily["water_balance_mm_ma14"] = daily["water_balance_mm"].rolling(14, min_periods=1).mean()
    daily["week_start"] = _week_start(daily["date"])
    return daily


def build_weather_weekly(weather_daily: pd.DataFrame) -> pd.DataFrame:
    if weather_daily.empty:
        return pd.DataFrame(
            columns=[
                "week_start",
                "weather_days",
                "precipitation_mm_week",
                "evapotranspiration_mm_week",
                "water_balance_mm_week",
                "solar_radiation_w_m2_week",
                "temp_avg_c_week",
                "temp_max_c_week",
                "temp_min_c_week",
                "humidity_avg_pct_week",
                "wind_avg_kmh_week",
            ]
        )

    return (
        weather_daily.groupby("week_start", as_index=False)
        .agg(
            weather_days=("date", "nunique"),
            precipitation_mm_week=("precipitation_mm", "sum"),
            evapotranspiration_mm_week=("evapotranspiration_mm", "sum"),
            water_balance_mm_week=("water_balance_mm", "sum"),
            solar_radiation_w_m2_week=("solar_radiation_w_m2", "mean"),
            temp_avg_c_week=("temp_avg_c", "mean"),
            temp_max_c_week=("temp_max_c", "max"),
            temp_min_c_week=("temp_min_c", "min"),
            humidity_avg_pct_week=("humidity_avg_pct", "mean"),
            wind_avg_kmh_week=("wind_avg_kmh", "mean"),
        )
        .sort_values("week_start")
        .reset_index(drop=True)
    )


def build_ops_area_daily(paths, seasons: pd.DataFrame, area_metadata: pd.DataFrame) -> pd.DataFrame:
    summaries: list[pd.DataFrame] = []

    layer_specs = [
        {
            "path": paths.planting_layer,
            "numeric_columns": ["Timestamp", "Service Order", "Operator Number", "Area - ha", "Population - ha"],
            "rename": {},
            "builder": _summarize_planting_daily,
        },
        {
            "path": paths.harvest_layer,
            "numeric_columns": [
                "Timestamp",
                "Service Order",
                "Operator Number",
                "Area - ha",
                "Yield - kg/ha",
                "Weight - kg",
                "Humidity - %",
            ],
            "rename": {},
            "builder": _summarize_harvest_daily,
        },
        {
            "path": paths.fertilization_layer,
            "numeric_columns": [
                "Timestamp",
                "Service Order",
                "Operator Number",
                "Area - ha",
                "AppliedDos - kg/ha",
                "Configured - kg/ha",
                "Weight - kg",
            ],
            "rename": {"operation": "Operation"},
            "builder": _summarize_fertilization_daily,
        },
        {
            "path": paths.spray_pressure_layer,
            "numeric_columns": ["Timestamp", "Service Order", "Operator Number", "Pressure - psi"],
            "rename": {"MachineName": "Machine Name"},
            "builder": _summarize_spray_daily,
        },
        {
            "path": paths.overlap_layer,
            "numeric_columns": ["Timestamp", "Service Order", "Operator Number", "OverlapArea - ha"],
            "rename": {},
            "builder": _summarize_overlap_daily,
        },
        {
            "path": paths.speed_layer,
            "numeric_columns": ["Timestamp", "Service Order", "Operator Number", "Speed - km/h"],
            "rename": {},
            "builder": _summarize_speed_daily,
        },
        {
            "path": paths.state_layer,
            "numeric_columns": ["Timestamp", "Service Order", "Operator Number"],
            "rename": {},
            "builder": _summarize_state_daily,
        },
        {
            "path": paths.stop_reason_layer,
            "numeric_columns": ["Timestamp", "Service Order", "Operator Number", "Duration - h"],
            "rename": {},
            "builder": _summarize_stop_reason_daily,
        },
    ]

    for spec in layer_specs:
        if spec["path"] is None:
            continue
        frame = load_layer_map(spec["path"], numeric_columns=spec["numeric_columns"])
        if spec["rename"]:
            frame = frame.rename(columns=spec["rename"])
        frame["date"] = pd.to_datetime(frame["Date Time"], errors="coerce").dt.normalize()
        frame = assign_centroid_rows_to_seasons(frame, seasons)
        frame = frame.merge(area_metadata, on="season_id", how="left")
        summaries.append(spec["builder"](frame))

    if not summaries:
        return pd.DataFrame(columns=["season_id", "date"])

    merged = summaries[0]
    for summary in summaries[1:]:
        merged = merged.merge(summary, on=["season_id", "date"], how="outer")

    merged = merged.merge(
        area_metadata[["season_id", "area_label", "treatment", "crop_type", "comparison_pair", "bbox_area_ha"]],
        on="season_id",
        how="left",
    )
    merged["overlap_area_pct_bbox"] = _safe_divide(merged.get("overlap_area_ha"), merged.get("bbox_area_ha"))
    merged["stop_duration_h_per_bbox_ha"] = _safe_divide(merged.get("stop_duration_h"), merged.get("bbox_area_ha"))
    return merged.sort_values(["comparison_pair", "area_label", "date"]).reset_index(drop=True)


def build_miip_daily(paths, seasons: pd.DataFrame, area_metadata: pd.DataFrame) -> pd.DataFrame:
    if not paths.pest_list or not paths.pest_details or not paths.traps_events:
        return pd.DataFrame(columns=["season_id", "date"])

    traps = load_traps_list(paths.traps_list)
    traps_unique = (
        traps.sort_values("installationDate")
        .groupby("code", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )
    traps_unique = assign_point_rows_to_seasons(traps_unique, seasons, lon_col="longitude", lat_col="latitude")
    traps_unique = traps_unique.merge(
        area_metadata[["season_id", "area_label", "treatment", "crop_type", "comparison_pair"]],
        on="season_id",
        how="left",
    )

    pest_lookup = build_pest_lookup(load_pest_list(paths.pest_list), load_pest_details(paths.pest_details))
    trap_data = load_traps_data(paths.traps_data).merge(
        traps_unique[["code", "type", "season_id", "area_label", "treatment", "crop_type", "comparison_pair"]],
        left_on="trapCode",
        right_on="code",
        how="left",
    )
    trap_data["date"] = pd.to_datetime(trap_data["createdAt"], errors="coerce", utc=True).dt.tz_localize(None).dt.normalize()
    trap_data["primary_pest_key"] = trap_data["primaryPest"].map(_normalize_text_key)
    trap_data = trap_data.merge(pest_lookup, on="primary_pest_key", how="left")
    trap_data["alert_hits"] = (
        trap_data["pestCount"].notna()
        & trap_data["alert_threshold"].notna()
        & (trap_data["pestCount"] >= trap_data["alert_threshold"])
    ).astype(int)
    trap_data["control_hits"] = (
        trap_data["pestCount"].notna()
        & trap_data["control_threshold"].notna()
        & (trap_data["pestCount"] >= trap_data["control_threshold"])
    ).astype(int)
    trap_data["damage_hits"] = (
        trap_data["pestCount"].notna()
        & trap_data["damage_threshold"].notna()
        & (trap_data["pestCount"] >= trap_data["damage_threshold"])
    ).astype(int)

    trap_daily = (
        trap_data.groupby(["season_id", "date"], as_index=False)
        .agg(
            trap_readings=("trapCode", "size"),
            traps_reporting=("trapCode", "nunique"),
            electronic_readings=("trapType", lambda values: int((values == "ELECTRONIC").sum())),
            conventional_readings=("trapType", lambda values: int((values == "CONVENTIONAL").sum())),
            avg_pest_count=("pestCount", "mean"),
            total_pest_count=("pestCount", "sum"),
            alert_hits=("alert_hits", "sum"),
            control_hits=("control_hits", "sum"),
            damage_hits=("damage_hits", "sum"),
            avg_action_ray_m=("action_ray_m", "mean"),
            primary_pests=("primaryPest", lambda values: ", ".join(sorted({str(value) for value in values.dropna() if str(value).strip()}))),
        )
    )

    events = load_traps_events(paths.traps_events).merge(
        traps_unique[["code", "season_id", "area_label", "treatment", "crop_type", "comparison_pair"]],
        left_on="trapCode",
        right_on="code",
        how="left",
    )
    events["date"] = pd.to_datetime(events["createdAt"], errors="coerce", utc=True).dt.tz_localize(None).dt.normalize()
    events["detection_records"] = events["detection"].map(extract_detection_records)
    events["detected_boxes"] = events["detection_records"].map(len)
    events["detected_species"] = events["detection_records"].map(
        lambda records: ", ".join(sorted({str(record.get("name")) for record in records if record.get("name")}))
    )

    event_daily = (
        events.groupby(["season_id", "date"], as_index=False)
        .agg(
            image_events=("type", lambda values: int((values == "IMAGE").sum())),
            ping_events=("type", lambda values: int((values == "PING").sum())),
            battery_events=("type", lambda values: int((values == "CLIENT_BATTERY_VOLTAGE").sum())),
            image_pest_count=("pestCount", "sum"),
            detected_boxes=("detected_boxes", "sum"),
            detected_species=("detected_species", lambda values: ", ".join(sorted({value for value in values if value}))),
        )
    )

    merged = trap_daily.merge(event_daily, on=["season_id", "date"], how="outer")
    merged = merged.merge(
        area_metadata[["season_id", "area_label", "treatment", "crop_type", "comparison_pair"]],
        on="season_id",
        how="left",
    )
    return merged.sort_values(["comparison_pair", "area_label", "date"]).reset_index(drop=True)


def build_pairwise_weekly_features(
    *,
    ndvi_clean: pd.DataFrame,
    weather_weekly: pd.DataFrame,
    ops_area_daily: pd.DataFrame,
    miip_daily: pd.DataFrame,
    area_metadata: pd.DataFrame,
) -> pd.DataFrame:
    ndvi_weekly = build_ndvi_weekly(ndvi_clean)
    ops_weekly = _build_ops_weekly(ops_area_daily)
    miip_weekly = _build_miip_weekly(miip_daily)

    frames = [frame for frame in [ndvi_weekly, ops_weekly, miip_weekly] if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=["season_id", "week_start"])

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on=["season_id", "week_start"], how="outer")

    merged = merged.merge(
        area_metadata[["season_id", "area_label", "treatment", "crop_type", "comparison_pair", "mapping_source"]],
        on="season_id",
        how="left",
    )
    merged = merged.merge(weather_weekly, on="week_start", how="left")
    merged = merged.sort_values(["comparison_pair", "area_label", "week_start"]).reset_index(drop=True)

    merged["pair_ndvi_gap_4_0_minus_conv"] = _pair_gap(merged, "ndvi_mean_week")
    merged["pair_harvest_gap_4_0_minus_conv"] = _pair_gap(merged, "harvest_yield_mean_kg_ha_week")
    merged["pair_pest_gap_4_0_minus_conv"] = _pair_gap(merged, "avg_pest_count_week")
    return merged


def build_ndvi_weekly(ndvi_clean: pd.DataFrame) -> pd.DataFrame:
    if ndvi_clean.empty:
        return pd.DataFrame(columns=["season_id", "week_start"])

    weekly = (
        ndvi_clean.groupby(["season_id", "week_start"], as_index=False)
        .agg(
            ndvi_mean_week=("ndvi_mean", "mean"),
            ndvi_peak_week=("ndvi_mean", "max"),
            ndvi_std_week=("ndvi_mean", "std"),
            ndvi_auc_week=("ndvi_auc", "max"),
            soil_pct_week=("soil_pct", "mean"),
            dense_veg_pct_week=("dense_veg_pct", "mean"),
            valid_images_week=("filename", "size"),
            has_weather_coverage_week=("has_weather_coverage", "max"),
        )
        .sort_values(["season_id", "week_start"])
        .reset_index(drop=True)
    )
    weekly["ndvi_std_week"] = weekly["ndvi_std_week"].fillna(0.0)
    weekly["ndvi_delta_week"] = weekly.groupby("season_id")["ndvi_mean_week"].diff()
    return weekly


def build_area_inventory(
    *,
    ndvi: pd.DataFrame,
    ndvi_clean: pd.DataFrame,
    ops_area_daily: pd.DataFrame,
    miip_daily: pd.DataFrame,
    area_metadata: pd.DataFrame,
    soil: pd.DataFrame,
    weather_daily: pd.DataFrame,
) -> pd.DataFrame:
    raw_summary = (
        ndvi.groupby("season_id", as_index=False)
        .agg(
            total_images=("filename", "size"),
            total_valid_images=("b1_valid_pixels", lambda values: int((values.fillna(0) > 0).sum())),
            first_scene=("date", "min"),
            last_scene=("date", "max"),
        )
        .sort_values("season_id")
    )
    clean_summary = (
        ndvi_clean.groupby("season_id", as_index=False)
        .agg(
            ndvi_mean=("ndvi_mean", "mean"),
            ndvi_peak=("ndvi_mean", "max"),
            ndvi_auc=("ndvi_auc", "max"),
            soil_pct_mean=("soil_pct", "mean"),
            dense_veg_pct_mean=("dense_veg_pct", "mean"),
        )
        .sort_values("season_id")
    )

    ops_summary = _summarize_area_ops(ops_area_daily)
    miip_summary = _summarize_area_miip(miip_daily)

    inventory = area_metadata.merge(raw_summary, on="season_id", how="left")
    inventory = inventory.merge(clean_summary, on="season_id", how="left")
    inventory = inventory.merge(ops_summary, on="season_id", how="left")
    inventory = inventory.merge(miip_summary, on="season_id", how="left")
    inventory["invalid_images"] = inventory["total_images"].fillna(0) - inventory["total_valid_images"].fillna(0)
    inventory["soil_samples_available"] = int(len(soil))
    inventory["soil_context_only"] = True
    inventory["weather_start"] = weather_daily["date"].min() if not weather_daily.empty else pd.NaT
    inventory["weather_end"] = weather_daily["date"].max() if not weather_daily.empty else pd.NaT
    return inventory.sort_values(["comparison_pair", "area_label"]).reset_index(drop=True)


def build_hypothesis_matrix(
    *,
    area_inventory: pd.DataFrame,
    pairwise_weekly_features: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if area_inventory.empty:
        return pd.DataFrame(columns=["pair", "evidence_strength", "supports_4_0", "supports_convencional", "known_gaps"])

    for comparison_pair, group in area_inventory.groupby("comparison_pair", dropna=False):
        pair_key = str(comparison_pair)
        tech = group[group["treatment"] == "tecnologia_4_0"].head(1)
        conv = group[group["treatment"] == "convencional"].head(1)
        supports_4_0: list[str] = []
        supports_conv: list[str] = []
        known_gaps: list[str] = []
        evidence_categories = 0
        critical_gaps = 0

        if not tech.empty and not conv.empty:
            tech_row = tech.iloc[0]
            conv_row = conv.iloc[0]
            evidence_categories += _append_directional_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label_better="NDVI medio da safra",
                positive_if_higher=True,
                tech_value=tech_row.get("ndvi_mean"),
                conv_value=conv_row.get("ndvi_mean"),
                unit="",
                threshold=0.02,
            )
            evidence_categories += _append_directional_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label_better="Produtividade de colheita",
                positive_if_higher=True,
                tech_value=tech_row.get("harvest_yield_mean_kg_ha"),
                conv_value=conv_row.get("harvest_yield_mean_kg_ha"),
                unit=" kg/ha",
                threshold=100.0,
            )
            evidence_categories += _append_directional_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label_better="Pressao media de pragas",
                positive_if_higher=False,
                tech_value=tech_row.get("avg_pest_count"),
                conv_value=conv_row.get("avg_pest_count"),
                unit=" insetos",
                threshold=2.0,
            )
            evidence_categories += _append_directional_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label_better="Sobreposicao operacional por area",
                positive_if_higher=False,
                tech_value=tech_row.get("overlap_area_pct_bbox"),
                conv_value=conv_row.get("overlap_area_pct_bbox"),
                unit=" ha/ha",
                threshold=0.01,
            )
            evidence_categories += _append_directional_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label_better="Horas de parada por area",
                positive_if_higher=False,
                tech_value=tech_row.get("stop_duration_h_per_bbox_ha"),
                conv_value=conv_row.get("stop_duration_h_per_bbox_ha"),
                unit=" h/ha",
                threshold=0.05,
            )
            evidence_categories += _append_directional_evidence(
                supports_4_0=supports_4_0,
                supports_conv=supports_conv,
                label_better="Conformidade media de dose em adubacao",
                positive_if_higher=False,
                tech_value=tech_row.get("fert_dose_gap_abs_mean_kg_ha"),
                conv_value=conv_row.get("fert_dose_gap_abs_mean_kg_ha"),
                unit=" kg/ha",
                threshold=10.0,
            )

            if _is_partial_metric_pair(tech_row.get("harvest_yield_mean_kg_ha"), conv_row.get("harvest_yield_mean_kg_ha")):
                known_gaps.append("Produtividade consolidada ausente em um dos lados do par.")
                critical_gaps += 1

            if _is_partial_metric_pair(tech_row.get("avg_pest_count"), conv_row.get("avg_pest_count")):
                known_gaps.append("MIIP sem cobertura nos dois lados do par; comparacao de pragas fica parcial.")
                critical_gaps += 1

            tech_miip_days = tech_row.get("miip_days")
            conv_miip_days = conv_row.get("miip_days")
            if _is_coverage_ratio_high(tech_miip_days, conv_miip_days, ratio_threshold=2.0):
                known_gaps.append("MIIP com cobertura muito desigual entre as areas do par.")

        pair_weeks = pairwise_weekly_features[pairwise_weekly_features["comparison_pair"] == comparison_pair]
        if not pair_weeks.empty and not pair_weeks["has_weather_coverage_week"].astype("boolean").fillna(False).all():
            known_gaps.append("Parte da serie NDVI deste par nao tem cobertura local de clima.")

        if group["soil_samples_available"].fillna(0).max() <= 8:
            known_gaps.append("Solo disponivel apenas como contexto; sem chave espacial completa por talhao.")
        if group["mapping_source"].astype(str).str.contains("auto_missing").any():
            known_gaps.append("Existe area sem mapeamento oficial consolidado.")
        if group["harvest_yield_mean_kg_ha"].isna().all():
            known_gaps.append("Nao ha produtividade consolidada suficiente para este par.")

        evidence_strength = "baixa"
        if evidence_categories >= 4:
            evidence_strength = "alta"
        elif evidence_categories >= 2:
            evidence_strength = "media"
        evidence_strength = _downgrade_evidence_strength(evidence_strength, critical_gaps=critical_gaps)

        rows.append(
            {
                "pair": pair_key,
                "evidence_strength": evidence_strength,
                "supports_4_0": " | ".join(supports_4_0) if supports_4_0 else "Sem evidencia clara favoravel ao 4.0.",
                "supports_convencional": " | ".join(supports_conv) if supports_conv else "Sem evidencia clara favoravel ao convencional.",
                "known_gaps": " | ".join(dict.fromkeys(known_gaps)) if known_gaps else "Sem lacunas adicionais alem das gerais da fase 1.",
            }
        )

    return pd.DataFrame(rows).sort_values("pair").reset_index(drop=True)


def save_phase1_outputs(workspace: dict[str, Any], output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for table_name in OUTPUT_TABLES:
        frame = workspace.get(table_name)
        if not isinstance(frame, pd.DataFrame):
            continue
        path = output_dir / f"{table_name}.csv"
        frame.to_csv(path, index=False)
        written.append(path)
    return written


def extract_detection_records(value: object) -> list[dict[str, Any]]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    return _extract_detection_records_recursive(value)


def build_pest_lookup(pest_list: pd.DataFrame, pest_details: pd.DataFrame) -> pd.DataFrame:
    summary = pest_list.rename(
        columns={
            "MIIP_PEST_NAME_POPULAR": "popular_name",
            "MIIP_PEST_NAME_SCIENTIFIC": "scientific_name",
            "MIIP_PEST_ALERT": "alert_threshold",
            "MIIP_PEST_CONTROL": "control_threshold",
            "MIIP_PEST_DAMAGE": "damage_threshold",
        }
    )[
        ["popular_name", "scientific_name", "alert_threshold", "control_threshold", "damage_threshold"]
    ].copy()
    summary["primary_pest_key"] = summary["popular_name"].map(_normalize_text_key)

    detail_lookup = pest_details.rename(
        columns={
            "namePopular": "popular_name_details",
            "nameScientific": "scientific_name_details",
            "alert": "alert_threshold_details",
            "control": "control_threshold_details",
            "damage": "damage_threshold_details",
            "actionRay": "action_ray_m",
        }
    )[
        [
            "popular_name_details",
            "scientific_name_details",
            "alert_threshold_details",
            "control_threshold_details",
            "damage_threshold_details",
            "action_ray_m",
        ]
    ].copy()
    detail_lookup["primary_pest_key"] = detail_lookup["popular_name_details"].map(_normalize_text_key)

    merged = summary.merge(detail_lookup, on="primary_pest_key", how="outer")
    merged["popular_name"] = merged["popular_name"].combine_first(merged["popular_name_details"])
    merged["scientific_name"] = merged["scientific_name"].combine_first(merged["scientific_name_details"])
    merged["alert_threshold"] = merged["alert_threshold"].combine_first(merged["alert_threshold_details"])
    merged["control_threshold"] = merged["control_threshold"].combine_first(merged["control_threshold_details"])
    merged["damage_threshold"] = merged["damage_threshold"].combine_first(merged["damage_threshold_details"])
    return merged[
        [
            "primary_pest_key",
            "popular_name",
            "scientific_name",
            "alert_threshold",
            "control_threshold",
            "damage_threshold",
            "action_ray_m",
        ]
    ].drop_duplicates(subset=["primary_pest_key"])


def assign_centroid_rows_to_seasons(frame: pd.DataFrame, seasons: pd.DataFrame) -> pd.DataFrame:
    return _assign_points_to_seasons(frame, seasons, lon_col="centroid_lon", lat_col="centroid_lat")


def assign_point_rows_to_seasons(frame: pd.DataFrame, seasons: pd.DataFrame, *, lon_col: str, lat_col: str) -> pd.DataFrame:
    return _assign_points_to_seasons(frame, seasons, lon_col=lon_col, lat_col=lat_col)


def list_phase1_gaps(
    *,
    ndvi: pd.DataFrame,
    weather_daily: pd.DataFrame,
    soil: pd.DataFrame,
    ops_area_daily: pd.DataFrame,
    paths_summary,
) -> list[str]:
    gaps: list[str] = []
    ndvi_start = pd.to_datetime(ndvi["date"], errors="coerce").min()
    weather_start = weather_daily["date"].min() if not weather_daily.empty else pd.NaT
    if pd.notna(ndvi_start) and pd.notna(weather_start) and ndvi_start < weather_start:
        gaps.append(
            f"Clima local cobre apenas de {weather_start:%Y-%m-%d} em diante; agosto a outubro de 2025 ficaram sem meteo local."
        )
    if len(soil) <= 8:
        gaps.append("Solo local tem apenas 8 amostras e nao oferece cobertura espacial completa por talhao.")
    if paths_summary.pest_list is None or paths_summary.pest_details is None or paths_summary.traps_events is None:
        gaps.append("Pacote local de MIIP esta incompleto para uma leitura biologica completa.")
    if ops_area_daily.get("spray_pressure_active_share") is not None and ops_area_daily["spray_pressure_active_share"].fillna(0).eq(0).all():
        gaps.append("A camada de pressao de pulverizacao veio zerada no pacote atual; use apenas como indicio de indisponibilidade.")
    gaps.append("A vinculacao espacial com os talhoes continua aproximada pelo bbox do NDVI, nao por limite vetorial oficial do talhao.")
    return gaps


def _summarize_planting_daily(frame: pd.DataFrame) -> pd.DataFrame:
    filtered = frame[frame["Operation"].str.contains("PLANTIO", case=False, na=False)].copy()
    filtered = filtered[filtered["Population - ha"].fillna(0) > 0].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    return (
        filtered.groupby(["season_id", "date"], as_index=False)
        .agg(
            planting_points=("Population - ha", "size"),
            planting_area_ha=("Area - ha", "sum"),
            planting_population_mean_ha=("Population - ha", "mean"),
            planting_population_p90_ha=("Population - ha", lambda values: values.quantile(0.9)),
        )
    )


def _summarize_harvest_daily(frame: pd.DataFrame) -> pd.DataFrame:
    filtered = frame[frame["Operation"].str.contains("COLHEITA", case=False, na=False)].copy()
    filtered = filtered[filtered["Yield - kg/ha"].fillna(0) > 0].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    return (
        filtered.groupby(["season_id", "date"], as_index=False)
        .agg(
            harvest_points=("Yield - kg/ha", "size"),
            harvest_area_ha=("Area - ha", "sum"),
            harvest_yield_mean_kg_ha=("Yield - kg/ha", "mean"),
            harvest_yield_p90_kg_ha=("Yield - kg/ha", lambda values: values.quantile(0.9)),
            harvest_weight_kg=("Weight - kg", "sum"),
            harvest_humidity_mean_pct=("Humidity - %", "mean"),
        )
    )


def _summarize_fertilization_daily(frame: pd.DataFrame) -> pd.DataFrame:
    filtered = frame.copy()
    filtered["fert_dose_gap_kg_ha"] = filtered["AppliedDos - kg/ha"] - filtered["Configured - kg/ha"]
    filtered["fert_dose_gap_abs_kg_ha"] = filtered["fert_dose_gap_kg_ha"].abs()
    filtered["fert_dose_ratio"] = _safe_divide(filtered["AppliedDos - kg/ha"], filtered["Configured - kg/ha"])
    filtered = filtered[filtered["AppliedDos - kg/ha"].notna() | filtered["Configured - kg/ha"].notna()].copy()
    if filtered.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    return (
        filtered.groupby(["season_id", "date"], as_index=False)
        .agg(
            fert_points=("Operation", "size"),
            fert_area_ha=("Area - ha", "sum"),
            fert_applied_mean_kg_ha=("AppliedDos - kg/ha", "mean"),
            fert_configured_mean_kg_ha=("Configured - kg/ha", "mean"),
            fert_dose_gap_mean_kg_ha=("fert_dose_gap_kg_ha", "mean"),
            fert_dose_gap_abs_mean_kg_ha=("fert_dose_gap_abs_kg_ha", "mean"),
            fert_dose_ratio_mean=("fert_dose_ratio", "mean"),
            fert_weight_kg=("Weight - kg", "sum"),
        )
    )


def _summarize_spray_daily(frame: pd.DataFrame) -> pd.DataFrame:
    filtered = frame.copy()
    if filtered.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    filtered["spray_pressure_active"] = (filtered["Pressure - psi"].fillna(0) > 0).astype(int)
    return (
        filtered.groupby(["season_id", "date"], as_index=False)
        .agg(
            spray_points=("Pressure - psi", "size"),
            spray_pressure_mean_psi=("Pressure - psi", "mean"),
            spray_pressure_active_share=("spray_pressure_active", "mean"),
        )
    )


def _summarize_overlap_daily(frame: pd.DataFrame) -> pd.DataFrame:
    filtered = frame.copy()
    if filtered.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    return (
        filtered.groupby(["season_id", "date"], as_index=False)
        .agg(
            overlap_points=("OverlapArea - ha", "size"),
            overlap_area_ha=("OverlapArea - ha", "sum"),
        )
    )


def _summarize_speed_daily(frame: pd.DataFrame) -> pd.DataFrame:
    filtered = frame.copy()
    if filtered.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    filtered["speed_zero_flag"] = (filtered["Speed - km/h"].fillna(0) <= 0.1).astype(int)
    return (
        filtered.groupby(["season_id", "date"], as_index=False)
        .agg(
            speed_points=("Speed - km/h", "size"),
            speed_mean_kmh=("Speed - km/h", "mean"),
            speed_p90_kmh=("Speed - km/h", lambda values: values.quantile(0.9)),
            speed_zero_share=("speed_zero_flag", "mean"),
        )
    )


def _summarize_state_daily(frame: pd.DataFrame) -> pd.DataFrame:
    filtered = frame.copy()
    if filtered.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    filtered["machine_state_group"] = filtered["MachineState"].map(_normalize_machine_state)
    counts = (
        pd.crosstab(
            index=[filtered["season_id"], filtered["date"]],
            columns=filtered["machine_state_group"],
        )
        .reset_index()
    )
    for column in ["operation", "maneuver", "transit", "supply", "stop", "other"]:
        if column not in counts.columns:
            counts[column] = 0
    total = counts[["operation", "maneuver", "transit", "supply", "stop", "other"]].sum(axis=1).replace(0, pd.NA)
    counts["state_points"] = total
    counts["state_operation_share"] = counts["operation"] / total
    counts["state_maneuver_share"] = counts["maneuver"] / total
    counts["state_transit_share"] = counts["transit"] / total
    counts["state_supply_share"] = counts["supply"] / total
    counts["state_stop_share"] = counts["stop"] / total
    return counts[
        [
            "season_id",
            "date",
            "state_points",
            "state_operation_share",
            "state_maneuver_share",
            "state_transit_share",
            "state_supply_share",
            "state_stop_share",
        ]
    ]


def _summarize_stop_reason_daily(frame: pd.DataFrame) -> pd.DataFrame:
    filtered = frame.copy()
    if filtered.empty:
        return pd.DataFrame(columns=["season_id", "date"])
    filtered["stop_known_reason_flag"] = filtered["event"].fillna("").ne("Sem evento").astype(int)
    return (
        filtered.groupby(["season_id", "date"], as_index=False)
        .agg(
            stop_events=("event", "size"),
            stop_duration_h=("Duration - h", "sum"),
            stop_known_reason_share=("stop_known_reason_flag", "mean"),
        )
    )


def _build_ops_weekly(ops_area_daily: pd.DataFrame) -> pd.DataFrame:
    if ops_area_daily.empty:
        return pd.DataFrame(columns=["season_id", "week_start"])
    frame = ops_area_daily.copy()
    frame["week_start"] = _week_start(frame["date"])

    aggregations = {
        "planting_points": "sum",
        "planting_area_ha": "sum",
        "planting_population_mean_ha": "mean",
        "planting_population_p90_ha": "mean",
        "harvest_points": "sum",
        "harvest_area_ha": "sum",
        "harvest_yield_mean_kg_ha": "mean",
        "harvest_yield_p90_kg_ha": "mean",
        "harvest_weight_kg": "sum",
        "harvest_humidity_mean_pct": "mean",
        "fert_points": "sum",
        "fert_area_ha": "sum",
        "fert_applied_mean_kg_ha": "mean",
        "fert_configured_mean_kg_ha": "mean",
        "fert_dose_gap_mean_kg_ha": "mean",
        "fert_dose_gap_abs_mean_kg_ha": "mean",
        "fert_dose_ratio_mean": "mean",
        "spray_points": "sum",
        "spray_pressure_mean_psi": "mean",
        "spray_pressure_active_share": "mean",
        "overlap_points": "sum",
        "overlap_area_ha": "sum",
        "overlap_area_pct_bbox": "mean",
        "speed_points": "sum",
        "speed_mean_kmh": "mean",
        "speed_p90_kmh": "mean",
        "speed_zero_share": "mean",
        "state_points": "sum",
        "state_operation_share": "mean",
        "state_maneuver_share": "mean",
        "state_transit_share": "mean",
        "state_supply_share": "mean",
        "state_stop_share": "mean",
        "stop_events": "sum",
        "stop_duration_h": "sum",
        "stop_known_reason_share": "mean",
        "stop_duration_h_per_bbox_ha": "mean",
    }
    available_aggregations = {column: agg for column, agg in aggregations.items() if column in frame.columns}
    weekly = frame.groupby(["season_id", "week_start"], as_index=False).agg(available_aggregations)
    return weekly.rename(
        columns={column: f"{column}_week" for column in weekly.columns if column not in {"season_id", "week_start"}}
    )


def _build_miip_weekly(miip_daily: pd.DataFrame) -> pd.DataFrame:
    if miip_daily.empty:
        return pd.DataFrame(columns=["season_id", "week_start"])
    frame = miip_daily.copy()
    frame["week_start"] = _week_start(frame["date"])
    aggregations = {
        "trap_readings": "sum",
        "traps_reporting": "max",
        "electronic_readings": "sum",
        "conventional_readings": "sum",
        "avg_pest_count": "mean",
        "total_pest_count": "sum",
        "alert_hits": "sum",
        "control_hits": "sum",
        "damage_hits": "sum",
        "image_events": "sum",
        "ping_events": "sum",
        "battery_events": "sum",
        "image_pest_count": "sum",
        "detected_boxes": "sum",
        "avg_action_ray_m": "mean",
    }
    available_aggregations = {column: agg for column, agg in aggregations.items() if column in frame.columns}
    weekly = frame.groupby(["season_id", "week_start"], as_index=False).agg(available_aggregations)
    return weekly.rename(
        columns={column: f"{column}_week" for column in weekly.columns if column not in {"season_id", "week_start"}}
    )


def _summarize_area_ops(ops_area_daily: pd.DataFrame) -> pd.DataFrame:
    if ops_area_daily.empty:
        return pd.DataFrame(columns=["season_id"])

    rows: list[dict[str, Any]] = []
    for season_id, group in ops_area_daily.groupby("season_id", dropna=False):
        rows.append(
            {
                "season_id": season_id,
                "ops_days": int(group["date"].nunique()) if "date" in group.columns else 0,
                "harvest_days": int(group.get("harvest_points", pd.Series(dtype="float64")).fillna(0).gt(0).sum()),
                "fert_days": int(group.get("fert_points", pd.Series(dtype="float64")).fillna(0).gt(0).sum()),
                "overlap_days": int(group.get("overlap_area_ha", pd.Series(dtype="float64")).fillna(0).gt(0).sum()),
                "speed_days": int(group.get("speed_points", pd.Series(dtype="float64")).fillna(0).gt(0).sum()),
                "state_days": int(group.get("state_points", pd.Series(dtype="float64")).fillna(0).gt(0).sum()),
                "stop_days": int(group.get("stop_events", pd.Series(dtype="float64")).fillna(0).gt(0).sum()),
                "planting_population_mean_ha": _weighted_mean(group, "planting_population_mean_ha", "planting_points"),
                "harvest_yield_mean_kg_ha": _weighted_mean(group, "harvest_yield_mean_kg_ha", "harvest_points"),
                "harvest_humidity_mean_pct": _weighted_mean(group, "harvest_humidity_mean_pct", "harvest_points"),
                "fert_dose_gap_abs_mean_kg_ha": _weighted_mean(group, "fert_dose_gap_abs_mean_kg_ha", "fert_points"),
                "overlap_area_ha": group.get("overlap_area_ha", pd.Series(dtype="float64")).sum(min_count=1),
                "overlap_area_pct_bbox": group.get("overlap_area_pct_bbox", pd.Series(dtype="float64")).mean(),
                "stop_duration_h": group.get("stop_duration_h", pd.Series(dtype="float64")).sum(min_count=1),
                "stop_duration_h_per_bbox_ha": group.get("stop_duration_h_per_bbox_ha", pd.Series(dtype="float64")).mean(),
            }
        )
    return pd.DataFrame(rows)


def _summarize_area_miip(miip_daily: pd.DataFrame) -> pd.DataFrame:
    if miip_daily.empty:
        return pd.DataFrame(columns=["season_id"])

    return (
        miip_daily.groupby("season_id", as_index=False)
        .agg(
            miip_days=("date", "nunique"),
            trap_readings_total=("trap_readings", "sum"),
            traps_reporting_max=("traps_reporting", "max"),
            avg_pest_count=("avg_pest_count", "mean"),
            total_pest_count=("total_pest_count", "sum"),
            alert_hits=("alert_hits", "sum"),
            control_hits=("control_hits", "sum"),
            damage_hits=("damage_hits", "sum"),
            image_events=("image_events", "sum"),
        )
        .sort_values("season_id")
        .reset_index(drop=True)
    )


def _assign_points_to_seasons(frame: pd.DataFrame, seasons: pd.DataFrame, *, lon_col: str, lat_col: str) -> pd.DataFrame:
    assigned = frame.copy()
    assigned["season_id"] = pd.NA
    assigned["assignment_method"] = pd.NA

    valid = assigned[lon_col].notna() & assigned[lat_col].notna()
    if not valid.any():
        assigned.loc[:, "assignment_method"] = "missing_coordinates"
        return assigned

    season_rows = []
    for row in seasons.itertuples(index=False):
        minx, miny, maxx, maxy = row.geometry.bounds
        season_rows.append(
            {
                "season_id": row.season_id,
                "minx": minx,
                "miny": miny,
                "maxx": maxx,
                "maxy": maxy,
                "center_lon": row.center_lon,
                "center_lat": row.center_lat,
            }
        )

    for season in season_rows:
        mask = (
            valid
            & assigned["season_id"].isna()
            & assigned[lon_col].between(season["minx"], season["maxx"])
            & assigned[lat_col].between(season["miny"], season["maxy"])
        )
        assigned.loc[mask, "season_id"] = season["season_id"]
        assigned.loc[mask, "assignment_method"] = "bbox_contains"

    remaining = valid & assigned["season_id"].isna()
    if remaining.any():
        distances = {}
        for season in season_rows:
            distances[season["season_id"]] = (
                (assigned.loc[remaining, lon_col] - season["center_lon"]) ** 2
                + (assigned.loc[remaining, lat_col] - season["center_lat"]) ** 2
            )
        nearest = pd.DataFrame(distances).idxmin(axis=1)
        assigned.loc[remaining, "season_id"] = nearest.values
        assigned.loc[remaining, "assignment_method"] = "nearest_center"

    assigned.loc[~valid, "assignment_method"] = "missing_coordinates"
    return assigned


def _pair_gap(frame: pd.DataFrame, metric: str) -> pd.Series:
    if metric not in frame.columns or frame.empty:
        return pd.Series(index=frame.index, dtype="float64")

    indexed = frame.reset_index().rename(columns={"index": "__row_id"})
    tech = (
        indexed[indexed["treatment"] == "tecnologia_4_0"][["comparison_pair", "week_start", metric]]
        .rename(columns={metric: "__metric_4_0"})
        .drop_duplicates(subset=["comparison_pair", "week_start"])
    )
    conv = (
        indexed[indexed["treatment"] == "convencional"][["comparison_pair", "week_start", metric]]
        .rename(columns={metric: "__metric_conv"})
        .drop_duplicates(subset=["comparison_pair", "week_start"])
    )
    gap = tech.merge(conv, on=["comparison_pair", "week_start"], how="outer")
    gap["__gap"] = gap["__metric_4_0"] - gap["__metric_conv"]
    merged = indexed.merge(gap[["comparison_pair", "week_start", "__gap"]], on=["comparison_pair", "week_start"], how="left")
    return merged.set_index("__row_id").reindex(frame.index)["__gap"]


def _week_start(series: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(series, errors="coerce")
    return (timestamps - pd.to_timedelta(timestamps.dt.weekday, unit="D")).dt.normalize()


def _cumulative_auc(frame: pd.DataFrame, *, group_col: str, date_col: str, value_col: str) -> pd.Series:
    result = pd.Series(index=frame.index, dtype="float64")
    for _, group in frame.groupby(group_col, sort=False):
        auc = 0.0
        previous_date = None
        previous_value = None
        for index, row in group.iterrows():
            current_date = row[date_col]
            current_value = row[value_col]
            if previous_date is not None and pd.notna(current_date) and pd.notna(current_value):
                delta_days = max((current_date - previous_date).days, 0)
                auc += delta_days * ((previous_value + current_value) / 2)
            result.loc[index] = auc
            previous_date = current_date
            previous_value = current_value
    return result


def _append_directional_evidence(
    *,
    supports_4_0: list[str],
    supports_conv: list[str],
    label_better: str,
    positive_if_higher: bool,
    tech_value: object,
    conv_value: object,
    unit: str,
    threshold: float,
) -> int:
    if tech_value is None or conv_value is None or pd.isna(tech_value) or pd.isna(conv_value):
        return 0

    delta = float(tech_value) - float(conv_value)
    if abs(delta) < threshold:
        return 0

    preferred = supports_4_0 if (delta > 0) == positive_if_higher else supports_conv
    label = "4.0" if preferred is supports_4_0 else "convencional"
    preferred.append(f"{label_better}: {label} leva vantagem ({delta:+.2f}{unit}).")
    return 1


def _is_partial_metric_pair(left: object, right: object) -> bool:
    return (pd.isna(left) and not pd.isna(right)) or (pd.isna(right) and not pd.isna(left))


def _is_coverage_ratio_high(left: object, right: object, *, ratio_threshold: float) -> bool:
    if left is None or right is None or pd.isna(left) or pd.isna(right):
        return False
    low = min(float(left), float(right))
    high = max(float(left), float(right))
    if low <= 0:
        return False
    return (high / low) >= ratio_threshold


def _downgrade_evidence_strength(evidence_strength: str, *, critical_gaps: int) -> str:
    if critical_gaps <= 0:
        return evidence_strength
    if evidence_strength == "alta":
        return "media" if critical_gaps == 1 else "baixa"
    if evidence_strength == "media":
        return "baixa"
    return evidence_strength


def _safe_divide(numerator: pd.Series | object, denominator: pd.Series | object) -> pd.Series:
    left = pd.to_numeric(numerator, errors="coerce")
    right = pd.to_numeric(denominator, errors="coerce")
    return left / right.replace(0, pd.NA)


def _weighted_mean(frame: pd.DataFrame, value_col: str, weight_col: str) -> float | None:
    if value_col not in frame.columns or weight_col not in frame.columns:
        return None
    values = pd.to_numeric(frame[value_col], errors="coerce")
    weights = pd.to_numeric(frame[weight_col], errors="coerce")
    valid = values.notna() & weights.notna() & (weights > 0)
    if not valid.any():
        return None
    return float((values[valid] * weights[valid]).sum() / weights[valid].sum())


def _normalize_machine_state(value: object) -> str:
    text = _normalize_text_key(value)
    if "oper" in text or "trabalh" in text:
        return "operation"
    if "manobra" in text:
        return "maneuver"
    if "desloc" in text or "transit" in text:
        return "transit"
    if "abastec" in text:
        return "supply"
    if "parad" in text:
        return "stop"
    return "other"


def _normalize_text_key(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(character for character in text if not unicodedata.combining(character))
    return " ".join(text.split())


def _extract_detection_records_recursive(value: object) -> list[dict[str, Any]]:
    if isinstance(value, list):
        dict_records = [item for item in value if isinstance(item, dict)]
        if dict_records:
            return dict_records
        for item in value:
            nested = _extract_detection_records_recursive(item)
            if nested:
                return nested
        return []

    if isinstance(value, dict):
        return [value]

    if not isinstance(value, str):
        return []

    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return []
    return _extract_detection_records_recursive(decoded)
