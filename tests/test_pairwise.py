from __future__ import annotations

import unittest

import pandas as pd

from farmlab.pairwise import (
    build_area_metadata,
    build_ndvi_clean,
    build_pairwise_weekly_features,
    extract_detection_records,
    _summarize_harvest_daily,
)


class PairwiseAnalysisTests(unittest.TestCase):
    def test_build_area_metadata_uses_official_mapping(self) -> None:
        ndvi = pd.DataFrame(
            {
                "season_id": [
                    "0bf86c8b-2779-4a98-ba25-7cf9518ee316",
                    "258e47e0-3bdc-4419-bedd-7b85812c2113",
                    "b7292cb8-72bb-447a-8feb-8ac983afd50b",
                    "f791bf13-1d24-4b4f-88bd-2569162df2b3",
                ]
            }
        )

        metadata = build_area_metadata(ndvi).set_index("season_id")

        self.assertEqual(metadata.loc["0bf86c8b-2779-4a98-ba25-7cf9518ee316", "comparison_pair"], "silagem")
        self.assertEqual(metadata.loc["258e47e0-3bdc-4419-bedd-7b85812c2113", "crop_type"], "grao")
        self.assertEqual(metadata.loc["b7292cb8-72bb-447a-8feb-8ac983afd50b", "treatment"], "convencional")
        self.assertEqual(metadata.loc["f791bf13-1d24-4b4f-88bd-2569162df2b3", "mapping_source"], "official_portal")

    def test_build_area_metadata_keeps_old_manual_mapping_compatible(self) -> None:
        ndvi = pd.DataFrame({"season_id": ["b7292cb8-72bb-447a-8feb-8ac983afd50b"]})
        manual = pd.DataFrame(
            {
                "season_id": ["b7292cb8-72bb-447a-8feb-8ac983afd50b"],
                "area_label": ["Grao Convencional Revisado"],
                "treatment": ["convencional"],
                "notes": ["Ajuste manual"],
            }
        )

        metadata = build_area_metadata(ndvi, manual)

        self.assertEqual(metadata.loc[0, "area_label"], "Grao Convencional Revisado")
        self.assertEqual(metadata.loc[0, "crop_type"], "grao")
        self.assertEqual(metadata.loc[0, "comparison_pair"], "grao")

    def test_build_ndvi_clean_filters_invalid_images_and_preserves_pairs(self) -> None:
        ndvi = pd.DataFrame(
            {
                "season_id": [
                    "258e47e0-3bdc-4419-bedd-7b85812c2113",
                    "258e47e0-3bdc-4419-bedd-7b85812c2113",
                ],
                "filename": ["img_a.tiff", "img_b.tiff"],
                "date": pd.to_datetime(["2025-11-01", "2025-11-06"]),
                "b1_valid_pixels": [120, 0],
                "b1_mean": [0.61, 0.10],
                "b1_std": [0.03, 0.02],
                "b1_pct_solo": [10.0, 80.0],
                "b1_pct_veg_densa": [65.0, 0.0],
            }
        )
        metadata = build_area_metadata(ndvi)
        weather_daily = pd.DataFrame({"date": pd.to_datetime(["2025-11-01", "2025-11-02"])})

        clean = build_ndvi_clean(ndvi, metadata, weather_daily)

        self.assertEqual(len(clean), 1)
        self.assertEqual(clean.iloc[0]["comparison_pair"], "grao")
        self.assertEqual(clean.iloc[0]["treatment"], "tecnologia_4_0")

    def test_extract_detection_records_parses_nested_traps_events_payload(self) -> None:
        payload = (
            '["1 Helicoverpa spp., 35 Spodoptera spp.", '
            '"[{\\"xmin\\": 1, \\"ymin\\": 2, \\"xmax\\": 3, \\"ymax\\": 4, '
            '\\"confidence\\": 0.93, \\"class\\": 3, \\"name\\": \\"Spodoptera spp.\\"}]"]'
        )

        records = extract_detection_records(payload)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["name"], "Spodoptera spp.")

    def test_harvest_summary_filters_non_colheita_rows(self) -> None:
        frame = pd.DataFrame(
            {
                "season_id": ["grao", "grao", "grao"],
                "date": pd.to_datetime(["2026-02-01", "2026-02-01", "2026-02-01"]),
                "Operation": ["CALAGEM", "COLHEITA SILAGEM", "COLHEITA SILAGEM"],
                "Yield - kg/ha": [999999.0, 125000.0, 0.0],
                "Area - ha": [0.1, 0.2, 0.1],
                "Weight - kg": [99.0, 42.0, 0.0],
                "Humidity - %": [10.0, 12.0, 14.0],
            }
        )

        summary = _summarize_harvest_daily(frame)

        self.assertEqual(int(summary.iloc[0]["harvest_points"]), 1)
        self.assertEqual(float(summary.iloc[0]["harvest_yield_mean_kg_ha"]), 125000.0)

    def test_pairwise_weekly_features_do_not_mix_grao_and_silagem(self) -> None:
        ndvi = pd.DataFrame(
            {
                "season_id": [
                    "258e47e0-3bdc-4419-bedd-7b85812c2113",
                    "b7292cb8-72bb-447a-8feb-8ac983afd50b",
                    "f791bf13-1d24-4b4f-88bd-2569162df2b3",
                    "0bf86c8b-2779-4a98-ba25-7cf9518ee316",
                ]
            }
        )
        metadata = build_area_metadata(ndvi)
        week_start = pd.Timestamp("2025-11-03")
        ndvi_values = pd.DataFrame(
            {
                "season_id": [
                    "258e47e0-3bdc-4419-bedd-7b85812c2113",
                    "b7292cb8-72bb-447a-8feb-8ac983afd50b",
                    "f791bf13-1d24-4b4f-88bd-2569162df2b3",
                    "0bf86c8b-2779-4a98-ba25-7cf9518ee316",
                ],
                "filename": ["a.tiff", "b.tiff", "c.tiff", "d.tiff"],
                "date": pd.to_datetime(["2025-11-03"] * 4),
                "week_start": [week_start] * 4,
                "ndvi_mean": [0.60, 0.40, 0.50, 0.70],
                "ndvi_auc": [0.60, 0.40, 0.50, 0.70],
                "soil_pct": [10.0, 15.0, 20.0, 8.0],
                "dense_veg_pct": [60.0, 45.0, 50.0, 72.0],
                "has_weather_coverage": [True, True, True, True],
            }
        )
        ndvi_clean = metadata.merge(
            ndvi_values,
            on="season_id",
            how="left",
        )
        weather_weekly = pd.DataFrame({"week_start": [week_start], "precipitation_mm_week": [12.0]})

        features = build_pairwise_weekly_features(
            ndvi_clean=ndvi_clean,
            weather_weekly=weather_weekly,
            ops_area_daily=pd.DataFrame(),
            miip_daily=pd.DataFrame(),
            area_metadata=metadata,
        ).set_index("season_id")

        self.assertAlmostEqual(features.loc["258e47e0-3bdc-4419-bedd-7b85812c2113", "pair_ndvi_gap_4_0_minus_conv"], 0.20)
        self.assertAlmostEqual(features.loc["b7292cb8-72bb-447a-8feb-8ac983afd50b", "pair_ndvi_gap_4_0_minus_conv"], 0.20)
        self.assertAlmostEqual(features.loc["f791bf13-1d24-4b4f-88bd-2569162df2b3", "pair_ndvi_gap_4_0_minus_conv"], -0.20)
        self.assertAlmostEqual(features.loc["0bf86c8b-2779-4a98-ba25-7cf9518ee316", "pair_ndvi_gap_4_0_minus_conv"], -0.20)


if __name__ == "__main__":
    unittest.main()
