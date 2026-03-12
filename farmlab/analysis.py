from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pyproj import Geod

from farmlab.io import (
    build_season_geometries,
    discover_dataset_paths,
    load_layer_map,
    load_ndvi_metadata,
    load_soil_analysis,
    load_traps_data,
    load_traps_list,
    load_weather_hourly,
)


_GEOD = Geod(ellps="WGS84")


def build_workspace(base_dir: Path) -> dict[str, Any]:
    paths = discover_dataset_paths(base_dir)
    soil = load_soil_analysis(paths.soil_analysis)
    ndvi = load_ndvi_metadata(paths)
    seasons = build_season_geometries(ndvi)
    traps_list = load_traps_list(paths.traps_list)
    traps_data = load_traps_data(paths.traps_data)
    weather = load_weather_hourly(paths.weather_hourly)
    planting = load_layer_map(
        paths.planting_layer,
        numeric_columns=["Timestamp", "Service Order", "Operator Number", "Area - ha", "Population - ha"],
    )
    harvest = load_layer_map(
        paths.harvest_layer,
        numeric_columns=[
            "Timestamp",
            "Service Order",
            "Operator Number",
            "Area - ha",
            "Yield - kg/ha",
            "Weight - kg",
            "Humidity - %",
        ],
    )

    crop_window = infer_crop_window(planting, harvest)
    season_mapping = suggest_season_mapping(seasons, traps_list)
    ndvi_daily = summarize_ndvi(ndvi, crop_window)
    nearby_traps = summarize_nearby_traps(seasons, traps_list, traps_data)
    harvest_assigned = assign_rows_to_seasons(harvest, seasons)
    planting_assigned = assign_rows_to_seasons(planting, seasons)
    harvest_summary = summarize_harvest(harvest_assigned)
    planting_summary = summarize_planting(planting_assigned)
    weather_daily = summarize_weather_daily(weather)
    pest_daily = summarize_pest_daily(traps_data)
    evidence = build_evidence_table(
        season_mapping=season_mapping,
        ndvi_daily=ndvi_daily,
        harvest_summary=harvest_summary,
        planting_summary=planting_summary,
        nearby_traps=nearby_traps,
    )

    inventory = {
        "ndvi_images": int(len(ndvi)),
        "ndvi_seasons": int(ndvi["season_id"].nunique()),
        "soil_samples": int(len(soil)),
        "trap_records": int(len(traps_data)),
        "unique_traps": int(traps_list["code"].nunique()),
        "weather_rows": int(len(weather)),
        "harvest_rows": int(len(harvest)),
        "planting_rows": int(len(planting)),
    }

    return {
        "paths": paths,
        "soil": soil,
        "ndvi": ndvi,
        "seasons": seasons,
        "season_mapping": season_mapping,
        "traps_list": traps_list,
        "traps_data": traps_data,
        "weather": weather,
        "weather_daily": weather_daily,
        "planting": planting_assigned,
        "harvest": harvest_assigned,
        "crop_window": crop_window,
        "ndvi_daily": ndvi_daily,
        "nearby_traps": nearby_traps,
        "pest_daily": pest_daily,
        "harvest_summary": harvest_summary,
        "planting_summary": planting_summary,
        "evidence": evidence,
        "inventory": inventory,
        "gaps": list_data_gaps(),
    }


def infer_crop_window(planting: pd.DataFrame, harvest: pd.DataFrame) -> dict[str, pd.Timestamp | None]:
    planting_rows = planting[planting["Operation"].str.contains("PLANTIO", case=False, na=False)]
    harvest_rows = harvest[harvest["Operation"].str.contains("COLHEITA", case=False, na=False)]
    return {
        "planting_start": planting_rows["Date Time"].min(),
        "planting_end": planting_rows["Date Time"].max(),
        "harvest_start": harvest_rows["Date Time"].min(),
        "harvest_end": harvest_rows["Date Time"].max(),
    }


def summarize_ndvi(ndvi: pd.DataFrame, crop_window: dict[str, pd.Timestamp | None]) -> pd.DataFrame:
    frame = ndvi.copy()
    frame["usable_image"] = frame["b1_valid_pixels"].fillna(0) > 0
    frame = frame[frame["usable_image"]].copy()

    planting_start = crop_window["planting_start"]
    harvest_end = crop_window["harvest_end"]
    if pd.notna(planting_start):
        frame = frame[frame["date"] >= planting_start.normalize()]
    if pd.notna(harvest_end):
        frame = frame[frame["date"] <= harvest_end.normalize()]

    return frame.sort_values(["season_id", "date"]).reset_index(drop=True)


def suggest_season_mapping(seasons: pd.DataFrame, traps_list: pd.DataFrame) -> pd.DataFrame:
    trap_points = traps_list.dropna(subset=["latitude", "longitude"]).copy()
    rows = []
    for season in seasons.itertuples(index=False):
        distances = []
        for trap in trap_points.itertuples(index=False):
            distance_m = _distance_m(season.center_lon, season.center_lat, trap.longitude, trap.latitude)
            distances.append(
                {
                    "trap_code": trap.code,
                    "trap_type": trap.type,
                    "plot": trap.plot,
                    "distance_m": round(distance_m, 1),
                }
            )
        nearest = pd.DataFrame(distances).sort_values("distance_m").head(3)
        top = nearest.iloc[0]
        with_plot = nearest[nearest["plot"].notna()]
        if not with_plot.empty and with_plot.iloc[0]["distance_m"] <= top["distance_m"] * 1.25:
            top = with_plot.iloc[0]
        rows.append(
            {
                "season_id": season.season_id,
                "suggested_plot": top["plot"] if pd.notna(top["plot"]) else "sem_talhão",
                "suggested_treatment": _normalize_treatment(top["trap_type"]),
                "closest_trap": top["trap_code"],
                "distance_to_closest_trap_m": top["distance_m"],
                "suggested_area_label": f"Área {season.season_id[:8]}",
                "supporting_traps": ", ".join(
                    f"{row.trap_code} ({int(row.distance_m)} m)" for row in nearest.itertuples(index=False)
                ),
            }
        )
    return pd.DataFrame(rows)


def summarize_nearby_traps(
    seasons: pd.DataFrame,
    traps_list: pd.DataFrame,
    traps_data: pd.DataFrame,
    *,
    radius_m: float = 350,
) -> pd.DataFrame:
    unique_traps = traps_list.dropna(subset=["latitude", "longitude"]).copy()
    latest_pest = (
        traps_data.sort_values("createdAt")
        .groupby("trapCode", as_index=False)
        .tail(1)[["trapCode", "pestCount", "primaryPest", "trapInfestationLevel"]]
    )
    unique_traps = unique_traps.merge(latest_pest, left_on="code", right_on="trapCode", how="left")

    rows = []
    for season in seasons.itertuples(index=False):
        matches = unique_traps[
            unique_traps.apply(
                lambda row: _distance_m(season.center_lon, season.center_lat, row["longitude"], row["latitude"]) <= radius_m,
                axis=1,
            )
        ]
        rows.append(
            {
                "season_id": season.season_id,
                "electronic_traps": int((matches["type"] == "ELECTRONIC").sum()),
                "conventional_traps": int((matches["type"] == "CONVENTIONAL").sum()),
                "avg_pest_count": matches["pestCount"].dropna().mean(),
                "primary_pests": ", ".join(sorted(matches["primaryPest"].dropna().astype(str).unique())),
            }
        )
    return pd.DataFrame(rows)


def assign_rows_to_seasons(layer: pd.DataFrame, seasons: pd.DataFrame) -> pd.DataFrame:
    season_rows = list(seasons.itertuples(index=False))
    assigned = []
    for row in layer.itertuples(index=False):
        centroid = row.centroid
        if centroid is None:
            assigned.append({"season_id": None, "assignment_method": "missing_geometry"})
            continue

        found = None
        for season in season_rows:
            if season.geometry.contains(centroid) or season.geometry.touches(centroid):
                found = {"season_id": season.season_id, "assignment_method": "contains"}
                break

        if found is None:
            nearest = min(
                season_rows,
                key=lambda season: _distance_m(season.center_lon, season.center_lat, row.centroid_lon, row.centroid_lat),
            )
            found = {"season_id": nearest.season_id, "assignment_method": "nearest_bbox"}

        assigned.append(found)

    return pd.concat([layer.reset_index(drop=True), pd.DataFrame(assigned)], axis=1)


def summarize_harvest(harvest: pd.DataFrame) -> pd.DataFrame:
    frame = harvest[harvest["Operation"].str.contains("COLHEITA", case=False, na=False)].copy()
    frame = frame[frame["Yield - kg/ha"].fillna(0) > 0].copy()
    if frame.empty:
        return pd.DataFrame(columns=["season_id", "yield_mean_kg_ha", "yield_p90_kg_ha", "harvest_points"])
    return (
        frame.groupby("season_id", as_index=False)
        .agg(
            yield_mean_kg_ha=("Yield - kg/ha", "mean"),
            yield_p90_kg_ha=("Yield - kg/ha", lambda values: values.quantile(0.9)),
            harvest_points=("Yield - kg/ha", "size"),
        )
        .sort_values("yield_mean_kg_ha", ascending=False)
    )


def summarize_planting(planting: pd.DataFrame) -> pd.DataFrame:
    frame = planting[planting["Operation"].str.contains("PLANTIO", case=False, na=False)].copy()
    if frame.empty:
        return pd.DataFrame(columns=["season_id", "population_mean_ha", "population_p90_ha", "planting_points"])
    return (
        frame.groupby("season_id", as_index=False)
        .agg(
            population_mean_ha=("Population - ha", "mean"),
            population_p90_ha=("Population - ha", lambda values: values.quantile(0.9)),
            planting_points=("Population - ha", "size"),
        )
        .sort_values("population_mean_ha", ascending=False)
    )


def summarize_weather_daily(weather: pd.DataFrame) -> pd.DataFrame:
    aggregations = {
        "precipitation_mm": "sum",
        "solar_radiation_w_m2": "mean",
        "temp_avg_c": "mean",
        "temp_max_c": "max",
        "temp_min_c": "min",
        "humidity_avg_pct": "mean",
        "wind_avg_kmh": "mean",
    }
    return weather.groupby("date", as_index=False).agg(aggregations)


def summarize_pest_daily(traps_data: pd.DataFrame) -> pd.DataFrame:
    frame = traps_data.dropna(subset=["event_date"]).copy()
    if frame.empty:
        return pd.DataFrame(columns=["event_date", "trapType", "avg_pest_count", "observations"])
    return (
        frame.groupby(["event_date", "trapType"], as_index=False)
        .agg(avg_pest_count=("pestCount", "mean"), observations=("trapId", "size"))
        .sort_values(["event_date", "trapType"])
    )


def build_evidence_table(
    *,
    season_mapping: pd.DataFrame,
    ndvi_daily: pd.DataFrame,
    harvest_summary: pd.DataFrame,
    planting_summary: pd.DataFrame,
    nearby_traps: pd.DataFrame,
) -> pd.DataFrame:
    ndvi_summary = (
        ndvi_daily.groupby("season_id", as_index=False)
        .agg(
            ndvi_mean=("b1_mean", "mean"),
            ndvi_peak=("b1_mean", "max"),
            ndvi_last=("b1_mean", "last"),
            dense_veg_pct_mean=("b1_pct_veg_densa", "mean"),
            valid_images=("date", "size"),
        )
    )

    frame = season_mapping.merge(ndvi_summary, on="season_id", how="left")
    frame = frame.merge(harvest_summary, on="season_id", how="left")
    frame = frame.merge(planting_summary, on="season_id", how="left")
    frame = frame.merge(nearby_traps, on="season_id", how="left")

    yield_reference = frame["yield_mean_kg_ha"].median(skipna=True)
    ndvi_reference = frame["ndvi_mean"].median(skipna=True)

    frame["diagnostic"] = frame.apply(
        lambda row: _build_diagnostic(row, yield_reference=yield_reference, ndvi_reference=ndvi_reference),
        axis=1,
    )
    return frame.sort_values(["yield_mean_kg_ha", "ndvi_mean"], ascending=False, na_position="last").reset_index(drop=True)


def apply_manual_mapping(season_mapping: pd.DataFrame, manual_mapping: pd.DataFrame | None) -> pd.DataFrame:
    base = season_mapping.copy()
    base["area_label"] = base["suggested_area_label"]
    base["treatment"] = base["suggested_treatment"]
    base["notes"] = pd.Series(index=base.index, dtype="string")
    if manual_mapping is None or manual_mapping.empty:
        return base

    mapping = manual_mapping.copy()
    for column in ["area_label", "treatment", "notes"]:
        if column not in mapping.columns:
            mapping[column] = pd.NA
    merged = base.merge(mapping, on="season_id", how="left", suffixes=("", "_manual"))
    merged["area_label"] = merged["area_label_manual"].combine_first(merged["area_label"])
    merged["treatment"] = merged["treatment_manual"].combine_first(merged["treatment"])
    if "notes_manual" in merged:
        merged["notes"] = merged["notes_manual"].combine_first(merged["notes"])
    drop_columns = [column for column in merged.columns if column.endswith("_manual")]
    return merged.drop(columns=drop_columns)


def summarize_costs(costs: pd.DataFrame | None, evidence: pd.DataFrame) -> pd.DataFrame | None:
    if costs is None or costs.empty:
        return None
    frame = costs.copy()
    if "season_id" not in frame.columns or "cost_per_ha_brl" not in frame.columns:
        return None
    if "cost_per_ha_brl" in frame:
        frame["cost_per_ha_brl"] = pd.to_numeric(frame["cost_per_ha_brl"], errors="coerce")
    summary = frame.groupby("season_id", as_index=False).agg(total_cost_per_ha_brl=("cost_per_ha_brl", "sum"))
    summary = summary.merge(evidence[["season_id", "yield_mean_kg_ha"]], on="season_id", how="left")
    summary["kg_per_brl"] = summary["yield_mean_kg_ha"] / summary["total_cost_per_ha_brl"]
    return summary.sort_values("kg_per_brl", ascending=False)


def list_data_gaps() -> list[str]:
    return [
        "Os recortes de NDVI não trazem o nome oficial do talhão; o app sugere a área pela proximidade com armadilhas, mas isso ainda precisa de validação humana.",
        "O pacote atual tem JPGs e metadados do NDVI, mas não traz os TIFFs numéricos originais; isso limita a análise espacial fina por pixel.",
        "A análise de solo tem 8 amostras, porém sem coordenadas ou chave de talhão; hoje ela serve como contexto, não como explicação espacial definitiva.",
        "A estação meteorológica é única para toda a fazenda, então o clima explica o contexto geral do ciclo, não a diferença local entre áreas.",
        "Não há planilha de custo no pacote atual; para provar que a área 4.0 foi mais barata, precisamos incluir custo por hectare ou por operação.",
        "Os layers de operação são aproximados ao bbox do NDVI; sem limite vetorial oficial do talhão, esse vínculo ainda é uma aproximação técnica.",
    ]


def _normalize_treatment(trap_type: str | None) -> str:
    if trap_type == "ELECTRONIC":
        return "tecnologia_4_0"
    if trap_type == "CONVENTIONAL":
        return "convencional"
    return "indefinido"


def _distance_m(lon_a: float, lat_a: float, lon_b: float, lat_b: float) -> float:
    _, _, distance = _GEOD.inv(lon_a, lat_a, lon_b, lat_b)
    return distance


def _build_diagnostic(row: pd.Series, *, yield_reference: float | None, ndvi_reference: float | None) -> str:
    points: list[str] = []

    if pd.notna(row.get("yield_mean_kg_ha")) and pd.notna(yield_reference):
        if row["yield_mean_kg_ha"] >= yield_reference:
            points.append("Produtividade acima da mediana entre os recortes monitorados.")
        else:
            points.append("Produtividade abaixo da mediana entre os recortes monitorados.")

    if pd.notna(row.get("ndvi_mean")) and pd.notna(ndvi_reference):
        if row["ndvi_mean"] >= ndvi_reference:
            points.append("O vigor vegetativo médio no período útil ficou acima da referência interna.")
        else:
            points.append("O vigor vegetativo médio no período útil ficou abaixo da referência interna.")

    electronic_traps = row.get("electronic_traps")
    conventional_traps = row.get("conventional_traps")
    if pd.notna(electronic_traps) and electronic_traps > 0:
        points.append(f"Existe cobertura de armadilhas eletrônicas no entorno imediato ({int(electronic_traps)}).")
    elif pd.notna(conventional_traps) and conventional_traps > 0:
        points.append("O monitoramento no entorno detectado é majoritariamente convencional.")

    if pd.notna(row.get("population_mean_ha")):
        points.append(f"População média de plantio observada: {row['population_mean_ha']:.0f} plantas/ha.")

    if pd.notna(row.get("avg_pest_count")):
        points.append(f"Média recente de contagem de pragas nas armadilhas próximas: {row['avg_pest_count']:.1f}.")

    if not points:
        return "Ainda não há evidências suficientes para explicar esta área com segurança."
    return " ".join(points)
