from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import Point, box
from shapely.ops import transform


@dataclass(frozen=True)
class DatasetPaths:
    root: Path
    soil_analysis: Path
    ndvi_metadata: Path
    ndvi_images_dir: Path
    traps_data: Path
    traps_list: Path
    weather_hourly: Path
    planting_layer: Path
    harvest_layer: Path
    pest_list: Path | None = None
    pest_details: Path | None = None
    traps_events: Path | None = None
    fertilization_layer: Path | None = None
    spray_pressure_layer: Path | None = None
    overlap_layer: Path | None = None
    speed_layer: Path | None = None
    state_layer: Path | None = None
    stop_reason_layer: Path | None = None
    alarm_layer: Path | None = None
    engine_rotation_layer: Path | None = None
    engine_temperature_layer: Path | None = None
    fuel_consumption_layer: Path | None = None
    parameterized_alert_layer: Path | None = None
    schedule_layer: Path | None = None
    telemetry_communication_layer: Path | None = None


_TO_WGS84 = Transformer.from_crs(3857, 4326, always_xy=True)
_FILENAME_DATE_PATTERN = re.compile(r"_(\d{4}-\d{2}-\d{2})\.(?:tiff|jpg)$")
_WEATHER_COLUMN_ALIASES = {
    "Data": "data_raw",
    "Estação": "station",
    "Estação": "station",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Radiação Solar (W/m2)": "solar_radiation_w_m2",
    "RadiaÃ§Ã£o Solar (W/m2)": "solar_radiation_w_m2",
    "Precipitação (mm)": "precipitation_mm",
    "PrecipitaÃ§Ã£o (mm)": "precipitation_mm",
    "Vel do Vento Média (km/h)": "wind_avg_kmh",
    "Vel do Vento MÃ©dia (km/h)": "wind_avg_kmh",
    "Temp. Mínima (°C)": "temp_min_c",
    "Temp. MÃ­nima (Â°C)": "temp_min_c",
    "Temp. Média (°C)": "temp_avg_c",
    "Temp. MÃ©dia (Â°C)": "temp_avg_c",
    "Temp. Máxima (°C)": "temp_max_c",
    "Temp. MÃ¡xima (Â°C)": "temp_max_c",
    "Umidade Rel. Mín. (%)": "humidity_min_pct",
    "Umidade Rel. MÃ­n. (%)": "humidity_min_pct",
    "Umidade Rel. Média (%)": "humidity_avg_pct",
    "Umidade Rel. MÃ©dia (%)": "humidity_avg_pct",
    "Umidade Rel. Máxima (%)": "humidity_max_pct",
    "Umidade Rel. MÃ¡xima (%)": "humidity_max_pct",
    "Rajada de Vento (km/h)": "wind_gust_kmh",
    "Evapotranspiração (mm)": "evapotranspiration_mm",
    "EvapotranspiraÃ§Ã£o (mm)": "evapotranspiration_mm",
}


def discover_dataset_paths(base_dir: Path) -> DatasetPaths:
    root = Path(base_dir).expanduser()
    return DatasetPaths(
        root=root,
        soil_analysis=_pick_one(root, "Cropman*/CSV/soil_analysis.csv"),
        ndvi_metadata=_pick_one(root, "OneSoil*/CSV/ndvi_metadata.csv"),
        ndvi_images_dir=_pick_one(root, "OneSoil*/JPG"),
        traps_data=_pick_one(root, "EKOS*/CSV/Pest/traps_data.csv"),
        traps_list=_pick_one(root, "EKOS*/CSV/Pest/traps_list.csv"),
        weather_hourly=_pick_one(root, "Metos*/CSV/*.csv"),
        planting_layer=_pick_one(root, "EKOS*/CSV/Layers/LAYER_MAP_PLANTING.csv"),
        harvest_layer=_pick_one(root, "EKOS*/CSV/Layers/LAYER_MAP_GRAIN_HARVESTING.csv"),
        pest_list=_pick_optional_one(root, "EKOS*/CSV/Pest/pest_list.csv"),
        pest_details=_pick_optional_one(root, "EKOS*/CSV/Pest/pest_details.csv"),
        traps_events=_pick_optional_one(root, "EKOS*/CSV/Pest/traps_events.csv"),
        fertilization_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_FERTILIZATION.csv"),
        spray_pressure_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_SPRAY_PRESSURE.csv"),
        overlap_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_OVERLAP.csv"),
        speed_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_SPEED.csv"),
        state_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_STATE.csv"),
        stop_reason_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_STOP_REASON.csv"),
        alarm_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_ALARM.csv"),
        engine_rotation_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_ENGINE_ROTATION.csv"),
        engine_temperature_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_ENGINE_TEMPERATURE.csv"),
        fuel_consumption_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_FUEL_CONSUMPTION.csv"),
        parameterized_alert_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_PARAMETERIZED_ALERT.csv"),
        schedule_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_SCHEDULE.csv"),
        telemetry_communication_layer=_pick_optional_one(root, "EKOS*/CSV/Layers/LAYER_MAP_TELEMETRY_COMMUNICATION.csv"),
    )


def load_soil_analysis(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig")
    for column in [column for column in frame.columns if column != "AMOSTRA"]:
        frame[column] = frame[column].map(_parse_number)
    frame["AMOSTRA"] = frame["AMOSTRA"].astype("string")
    return frame


def load_ndvi_metadata(paths: DatasetPaths) -> pd.DataFrame:
    frame = pd.read_csv(paths.ndvi_metadata, dtype=str, encoding="utf-8-sig")
    numeric_columns = [
        column
        for column in frame.columns
        if column not in {"filename", "season_id", "driver", "dtype", "nodata", "crs"}
    ]
    for column in numeric_columns:
        frame[column] = frame[column].map(_parse_number)
    frame["date"] = frame["filename"].map(_extract_date_from_filename)
    frame["image_path"] = frame["filename"].map(
        lambda filename: str(paths.ndvi_images_dir / filename.replace(".tiff", ".jpg"))
    )
    return frame.sort_values(["season_id", "date"]).reset_index(drop=True)


def load_traps_list(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig")
    for column in ["latitude", "longitude", "businessUnit", "farm", "id", "radius"]:
        frame[column] = frame[column].map(_parse_number)
    frame["installationDate"] = pd.to_datetime(frame["installationDate"], errors="coerce", utc=True)
    frame["plot"] = frame["plot"].replace({"": pd.NA, "nan": pd.NA}).astype("string")
    frame["geometry"] = frame.apply(
        lambda row: Point(row["longitude"], row["latitude"])
        if pd.notna(row["longitude"]) and pd.notna(row["latitude"])
        else None,
        axis=1,
    )
    return frame.drop_duplicates(subset=["code", "type", "plot", "latitude", "longitude"]).reset_index(drop=True)


def load_traps_data(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig")
    for column in ["doneMissionCount", "idBusinessUnit", "lateMissionCount", "pendingMissionCount", "pestCount", "trapId"]:
        frame[column] = frame[column].map(_parse_number)
    frame["createdAt"] = pd.to_datetime(frame["createdAt"], errors="coerce", utc=True)
    frame["event_date"] = frame["createdAt"].dt.date
    return frame


def load_pest_list(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig")
    for column in ["MIIP_PEST_ID", "MIIP_PEST_ALERT", "MIIP_PEST_CONTROL", "MIIP_PEST_DAMAGE"]:
        frame[column] = frame[column].map(_parse_number)
    return frame


def load_pest_details(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig")
    for column in ["actionRay", "alert", "control", "damage", "daysAdhesiveFloor", "daysPheromone", "id"]:
        frame[column] = frame[column].map(_parse_number)
    return frame


def load_traps_events(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig")
    for column in ["farm", "id", "pestCount", "trapId"]:
        frame[column] = frame[column].map(_parse_number)
    frame["createdAt"] = pd.to_datetime(frame["createdAt"], errors="coerce", utc=True)
    frame["event_date"] = frame["createdAt"].dt.date
    return frame


def load_weather_hourly(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig")
    frame = frame.rename(columns={column: _WEATHER_COLUMN_ALIASES.get(column, column) for column in frame.columns})
    for column in [column for column in frame.columns if column not in {"data_raw", "station"}]:
        frame[column] = frame[column].map(_parse_number)
    frame["timestamp"] = pd.to_datetime(
        frame["data_raw"].str.replace(r"\s*/ GMT.*$", "", regex=True),
        format="%d/%m/%Y - %H:%M",
        errors="coerce",
        dayfirst=True,
    )
    frame["date"] = frame["timestamp"].dt.date
    return frame.sort_values("timestamp").reset_index(drop=True)


def load_layer_map(path: Path, *, numeric_columns: list[str]) -> pd.DataFrame:
    frame = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig")
    for column in numeric_columns:
        frame[column] = frame[column].map(_parse_number)
    frame["Date Time"] = pd.to_datetime(
        frame["Date Time"],
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce",
        dayfirst=True,
    )
    frame["geometry"] = frame["geometry"].map(_safe_wkt_loads)
    frame["centroid"] = frame["geometry"].map(lambda geometry: geometry.centroid if geometry is not None else None)
    frame["centroid_lon"] = frame["centroid"].map(lambda geometry: geometry.x if geometry is not None else None)
    frame["centroid_lat"] = frame["centroid"].map(lambda geometry: geometry.y if geometry is not None else None)
    return frame


def build_season_geometries(ndvi_metadata: pd.DataFrame) -> pd.DataFrame:
    rows = []
    grouped = ndvi_metadata.groupby("season_id", as_index=False).first()
    for season in grouped.itertuples(index=False):
        polygon_3857 = box(season.bounds_left, season.bounds_bottom, season.bounds_right, season.bounds_top)
        polygon_wgs84 = transform(_TO_WGS84.transform, polygon_3857)
        centroid = polygon_wgs84.centroid
        rows.append(
            {
                "season_id": season.season_id,
                "geometry": polygon_wgs84,
                "center_lon": centroid.x,
                "center_lat": centroid.y,
                "bbox_area_ha": polygon_3857.area / 10_000,
            }
        )
    return pd.DataFrame(rows)


def _pick_one(root: Path, pattern: str) -> Path:
    matches = sorted(root.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para o padrao: {pattern}")
    return matches[0]


def _pick_optional_one(root: Path, pattern: str) -> Path | None:
    matches = sorted(root.glob(pattern))
    if not matches:
        return None
    return matches[0]


def _extract_date_from_filename(filename: str) -> pd.Timestamp:
    match = _FILENAME_DATE_PATTERN.search(filename)
    if not match:
        return pd.NaT
    return pd.to_datetime(match.group(1), errors="coerce")


def _parse_number(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if text in {"", "-", "nan", "None"}:
        return None
    if re.fullmatch(r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", text):
        return float(text)
    if "," in text:
        candidate = text.replace(".", "").replace(",", ".")
        if re.fullmatch(r"-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?", candidate):
            return float(candidate)
    return None


def _safe_wkt_loads(text: str | None):
    if not text:
        return None
    try:
        return wkt.loads(text)
    except Exception:
        return None
