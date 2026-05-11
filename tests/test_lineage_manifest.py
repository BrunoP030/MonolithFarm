from __future__ import annotations

import unittest

import pandas as pd

from dashboard.lineage.manifest import build_critical_lineage_report, build_lineage_manifest


class LineageManifestTests(unittest.TestCase):
    def test_manifest_exposes_canonical_tables_and_gates(self) -> None:
        outputs = {
            "event_driver_lift.csv": pd.DataFrame(
                columns=["comparison_pair", "driver", "problem_rate", "baseline_rate", "delta_pp", "lift_ratio", "evidence_level"]
            ),
            "pair_weekly_gaps.csv": pd.DataFrame(columns=["comparison_pair", "week_start", "gap_ndvi_mean_week_4_0_minus_convencional"]),
        }

        manifest = build_lineage_manifest(pd.DataFrame(), None, outputs, pd.DataFrame({"affected_rows": [0]}))

        self.assertIn("lineage_index", manifest)
        self.assertIn("coverage", manifest)
        self.assertIn("critical_targets", manifest)
        self.assertIn("acceptance_gates", manifest)
        self.assertFalse(manifest["lineage_index"].empty)
        self.assertFalse(manifest["acceptance_gates"].empty)

    def test_solo_exposto_critical_target_is_end_to_end(self) -> None:
        manifest = build_lineage_manifest(pd.DataFrame(), None, {}, pd.DataFrame({"affected_rows": [0]}))
        critical = build_critical_lineage_report(manifest["lineage_index"])

        row = critical[critical["target"] == "solo_exposto"].iloc[0]
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["missing_checks"], "")

    def test_manifest_marks_final_columns_as_auditable(self) -> None:
        outputs = {
            "pair_weekly_gaps.csv": pd.DataFrame(
                columns=[
                    "comparison_pair",
                    "week_start",
                    "gap_ndvi_mean_week_4_0_minus_convencional",
                    "gap_high_soil_flag_4_0_minus_convencional",
                ]
            )
        }

        manifest = build_lineage_manifest(pd.DataFrame(), None, outputs, pd.DataFrame({"affected_rows": [0]}))
        gates = manifest["acceptance_gates"]
        auditable_gate = gates[gates["gate_id"] == "G2_final_columns_auditable"].iloc[0]

        self.assertEqual(auditable_gate["status"], "ok")


if __name__ == "__main__":
    unittest.main()
