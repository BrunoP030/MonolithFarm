from __future__ import annotations

import os
from pathlib import Path


def _resolve_default_data_dir() -> Path:
    env_path = os.environ.get("MONOLITHFARM_DATA_DIR")
    if env_path:
        return Path(env_path).expanduser()

    project_dir = Path(__file__).resolve().parent.parent
    local_data_dir = project_dir / "FarmLab"
    if local_data_dir.exists():
        return local_data_dir

    return Path(r"C:\Users\Morgado\Downloads\FarmLab")


DEFAULT_DATA_DIR = _resolve_default_data_dir()

SEASON_MAPPING_COLUMNS = ["season_id", "area_label", "treatment", "notes"]
COST_INPUT_COLUMNS = ["season_id", "cost_category", "cost_per_ha_brl", "notes"]
