from __future__ import annotations

import unittest

import pandas as pd

from dashboard.lineage.column_lineage import build_column_lineage_index
from dashboard.lineage.quality_rules import run_quality_rules


class LineageAuditTests(unittest.TestCase):
    def test_driver_is_searchable_as_first_class_lineage_record(self) -> None:
        lineage = build_column_lineage_index(pd.DataFrame(), None, {})

        driver = lineage[lineage["lineage_id"] == "driver::solo_exposto"].iloc[0]
        self.assertEqual(driver["layer"], "driver")
        self.assertEqual(driver["column"], "solo_exposto")
        self.assertIn("b1_pct_solo", driver["raw_columns"])
        self.assertIn("soil_pct_week", driver["upstream_columns"])
        self.assertIn("high_soil_flag", driver["upstream_columns"])
        self.assertEqual(driver["mapping_status"], "mapeado_por_driver_entidade")

    def test_quality_rules_expose_dimension_action_and_columns(self) -> None:
        workspace = {
            "ndvi_raw": pd.DataFrame(
                [
                    {
                        "season_id": "s1",
                        "filename": "ndvi_raw_s1_2025-08-04.tiff",
                        "date": pd.Timestamp("2025-08-04"),
                        "b1_valid_pixels": 0,
                        "b1_mean": 0.5,
                        "b1_pct_solo": 10.0,
                        "b1_pct_veg_densa": 20.0,
                    }
                ]
            ),
            "ndvi_clean": pd.DataFrame(
                [
                    {
                        "season_id": "s1",
                        "date": pd.Timestamp("2025-08-04"),
                        "ndvi_mean": 0.5,
                    }
                ]
            ),
        }

        summary, examples = run_quality_rules(workspace, {})
        row = summary[summary["rule_id"] == "ndvi_b1_valid_pixels_zero"].iloc[0]

        self.assertEqual(row["quality_dimension"], "completude")
        self.assertIn("b1_valid_pixels", row["affected_columns"])
        self.assertEqual(row["action"], "descartado_da_base_analitica")
        self.assertIn("ndvi_b1_valid_pixels_zero", examples)


if __name__ == "__main__":
    unittest.main()
