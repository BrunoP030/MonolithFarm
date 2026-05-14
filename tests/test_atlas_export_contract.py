from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from dashboard.lineage.project_objectives import project_objectives
from dashboard.lineage.runtime import load_resolved_paths
from scripts.export_lineage_atlas_data import _sanitize_public_payload


@dataclass(frozen=True)
class DummyPaths:
    project_dir: Path
    data_dir: Path
    output_dir: Path


class AtlasExportContractTests(unittest.TestCase):
    def test_project_objectives_cover_info_md_sections(self) -> None:
        objectives = project_objectives()
        required = {
            "centralProblem",
            "businessAcademicGoals",
            "priorityTopics",
            "mainHypothesis",
            "currentDataSituation",
            "technicalQuestions",
            "interpretationRisks",
            "recommendedExecutionPlan",
            "domainSupport",
            "referenceLinks",
            "evidenceLegend",
        }

        self.assertTrue(required.issubset(objectives))
        self.assertGreaterEqual(len(objectives["priorityTopics"]), 7)
        self.assertGreaterEqual(len(objectives["technicalQuestions"]), 5)
        self.assertTrue(any(item["currentAnswerStatus"] == "bloqueada_sem_custos_reais" for item in objectives["technicalQuestions"]))

    def test_public_payload_redacts_absolute_paths(self) -> None:
        paths = DummyPaths(
            project_dir=Path("C:/Users/Ana/MonolithFarm"),
            data_dir=Path("C:/Users/Ana/MonolithFarm/data"),
            output_dir=Path("C:/Users/Ana/MonolithFarm/notebook_outputs/complete_ndvi"),
        )
        payload = {
            "meta": {
                "projectDir": "C:/Users/Ana/MonolithFarm",
                "dataDir": "C:/Users/Ana/MonolithFarm/data",
                "outputDir": "C:/Users/Ana/MonolithFarm/notebook_outputs/complete_ndvi",
            },
            "rawFiles": [
                {
                    "path": "C:/Users/Ana/MonolithFarm/data/Cropman/soil.csv",
                    "columnsDetailed": [{"file_path": "C:/Users/Ana/MonolithFarm/data/Cropman/soil.csv"}],
                }
            ],
            "graph": {
                "nodes": [
                    {"search": '{"path": "C:/Users/Ana/MonolithFarm/data/Cropman/soil.csv"} https://farm.labs.unimar.br/docs'},
                ]
            },
        }

        sanitized = _sanitize_public_payload(payload, paths)
        text = str(sanitized)

        self.assertNotIn("C:/Users", text)
        self.assertNotIn("Ana", text)
        self.assertIn("data/Cropman/soil.csv", text)
        self.assertIn("https://farm.labs.unimar.br/docs", text)

    def test_checked_in_public_atlas_has_no_absolute_path_fields(self) -> None:
        path = Path("lineage_atlas/public/atlas-data.json")
        self.assertTrue(path.exists(), "atlas-data.json must exist for public contract checks")
        payload = json.loads(path.read_text(encoding="utf-8"))
        private_markers = ("\\Users\\", "/Users/", "/@fs/")

        for key, value in _walk_public_path_fields(payload):
            with self.subTest(key=key, value=value[:120]):
                self.assertFalse(re.match(r"^[A-Z]:[\\/]", value, flags=re.IGNORECASE))
                self.assertFalse(any(marker in value for marker in private_markers))

    def test_resolved_paths_fall_back_to_local_when_default_output_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "pyproject.toml").write_text("[project]\nname = 'fixture'\n", encoding="utf-8")
            (root / "data").mkdir()
            (root / "notebook_outputs" / "complete_ndvi").mkdir(parents=True)
            (root / ".monolithfarm.paths.json").write_text(
                json.dumps(
                    {
                        "default_profile": "local_wsl",
                        "profiles": {
                            "local_wsl": {
                                "project_dir": "/mnt/c/missing/MonolithFarm",
                                "data_dir": "data",
                                "output_root": "notebook_outputs",
                            },
                            "local": {
                                "project_dir": ".",
                                "data_dir": "data",
                                "output_root": "notebook_outputs",
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"MONOLITHFARM_PROFILE": ""}, clear=False):
                paths = load_resolved_paths(root)

        self.assertEqual(paths.profile_name, "local")
        self.assertEqual(paths.data_dir, (root / "data").resolve())
        self.assertEqual(paths.output_dir, (root / "notebook_outputs" / "complete_ndvi").resolve())


def _walk_public_path_fields(value: object):
    path_keys = {"path", "file_path", "projectDir", "dataDir", "outputDir", "image_path"}
    if isinstance(value, dict):
        for key, item in value.items():
            if key in path_keys and isinstance(item, str):
                yield key, item
            yield from _walk_public_path_fields(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_public_path_fields(item)


if __name__ == "__main__":
    unittest.main()
