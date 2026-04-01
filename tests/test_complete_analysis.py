from __future__ import annotations

import unittest

import pandas as pd

from farmlab.complete_analysis import build_ndvi_outliers, build_pair_classic_tests


class CompleteAnalysisTests(unittest.TestCase):
    def test_build_ndvi_outliers_flags_extreme_scene(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "season_id": "s1",
                    "date": pd.Timestamp("2025-08-04"),
                    "week_start": pd.Timestamp("2025-08-04"),
                    "area_label": "Area A",
                    "treatment": "tecnologia_4_0",
                    "comparison_pair": "grao",
                    "ndvi_mean": 0.50,
                    "soil_pct": 2.0,
                    "dense_veg_pct": 40.0,
                    "image_path": "a.jpg",
                },
                {
                    "season_id": "s1",
                    "date": pd.Timestamp("2025-08-11"),
                    "week_start": pd.Timestamp("2025-08-11"),
                    "area_label": "Area A",
                    "treatment": "tecnologia_4_0",
                    "comparison_pair": "grao",
                    "ndvi_mean": 0.51,
                    "soil_pct": 2.0,
                    "dense_veg_pct": 42.0,
                    "image_path": "b.jpg",
                },
                {
                    "season_id": "s1",
                    "date": pd.Timestamp("2025-08-18"),
                    "week_start": pd.Timestamp("2025-08-18"),
                    "area_label": "Area A",
                    "treatment": "tecnologia_4_0",
                    "comparison_pair": "grao",
                    "ndvi_mean": 0.18,
                    "soil_pct": 8.0,
                    "dense_veg_pct": 5.0,
                    "image_path": "c.jpg",
                },
                {
                    "season_id": "s1",
                    "date": pd.Timestamp("2025-08-25"),
                    "week_start": pd.Timestamp("2025-08-25"),
                    "area_label": "Area A",
                    "treatment": "tecnologia_4_0",
                    "comparison_pair": "grao",
                    "ndvi_mean": 0.49,
                    "soil_pct": 3.0,
                    "dense_veg_pct": 41.0,
                    "image_path": "d.jpg",
                },
            ]
        )

        result = build_ndvi_outliers(frame)
        outliers = result[result["outlier_flag"]]
        self.assertEqual(len(outliers), 1)
        self.assertEqual(pd.Timestamp(outliers.iloc[0]["date"]), pd.Timestamp("2025-08-18"))
        self.assertEqual(outliers.iloc[0]["outlier_direction"], "ndvi_abaixo_do_padrao")

    def test_build_pair_classic_tests_detects_positive_gap(self) -> None:
        pair_weekly_gaps = pd.DataFrame(
            [
                {
                    "comparison_pair": "grao",
                    "week_start": pd.Timestamp("2025-08-04"),
                    "ndvi_mean_week_4_0": 0.62,
                    "ndvi_mean_week_convencional": 0.58,
                    "gap_ndvi_mean_week_4_0_minus_convencional": 0.04,
                },
                {
                    "comparison_pair": "grao",
                    "week_start": pd.Timestamp("2025-08-11"),
                    "ndvi_mean_week_4_0": 0.64,
                    "ndvi_mean_week_convencional": 0.59,
                    "gap_ndvi_mean_week_4_0_minus_convencional": 0.05,
                },
                {
                    "comparison_pair": "grao",
                    "week_start": pd.Timestamp("2025-08-18"),
                    "ndvi_mean_week_4_0": 0.66,
                    "ndvi_mean_week_convencional": 0.60,
                    "gap_ndvi_mean_week_4_0_minus_convencional": 0.06,
                },
                {
                    "comparison_pair": "grao",
                    "week_start": pd.Timestamp("2025-08-25"),
                    "ndvi_mean_week_4_0": 0.68,
                    "ndvi_mean_week_convencional": 0.61,
                    "gap_ndvi_mean_week_4_0_minus_convencional": 0.07,
                },
            ]
        )

        result = build_pair_classic_tests(pair_weekly_gaps)
        ndvi_row = result[result["metric"] == "ndvi_mean_week"].iloc[0]
        self.assertEqual(ndvi_row["favors"], "favorece_4_0")
        self.assertTrue(ndvi_row["significant_0_05"])
        self.assertGreater(ndvi_row["mean_gap_4_0_minus_convencional"], 0)


if __name__ == "__main__":
    unittest.main()
