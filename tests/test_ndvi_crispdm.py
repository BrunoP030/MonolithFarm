from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from farmlab.ndvi_crispdm import (
    build_event_driver_lift,
    build_final_hypothesis_register,
    build_pair_effect_tests,
    fit_transition_model,
)


class NdviCrispDmTests(unittest.TestCase):
    def test_build_pair_effect_tests_detects_4_0_advantage(self) -> None:
        weeks = pd.date_range("2025-08-04", periods=8, freq="7D")
        rows = []
        for week_index, week_start in enumerate(weeks):
            rows.append(
                {
                    "season_id": "tech",
                    "week_start": week_start,
                    "area_label": "Area 4.0",
                    "treatment": "tecnologia_4_0",
                    "comparison_pair": "grao",
                    "ndvi_mean_week": 0.62 + week_index * 0.01,
                    "ndvi_auc_week": 2.0 + week_index * 0.3,
                    "low_vigor_flag": 0,
                    "major_drop_flag": 0,
                    "pest_risk_flag": 0,
                    "ops_risk_flag": 0,
                    "high_soil_flag": 0,
                }
            )
            rows.append(
                {
                    "season_id": "conv",
                    "week_start": week_start,
                    "area_label": "Area Convencional",
                    "treatment": "convencional",
                    "comparison_pair": "grao",
                    "ndvi_mean_week": 0.55 + week_index * 0.01,
                    "ndvi_auc_week": 1.4 + week_index * 0.2,
                    "low_vigor_flag": 1 if week_index < 6 else 0,
                    "major_drop_flag": 1 if week_index in {1, 2, 5, 6} else 0,
                    "pest_risk_flag": 0,
                    "ops_risk_flag": 1 if week_index in {1, 4} else 0,
                    "high_soil_flag": 1 if week_index < 2 else 0,
                }
            )

        result = build_pair_effect_tests(pd.DataFrame(rows))
        ndvi_row = result[result["metric_label"] == "NDVI medio semanal"].iloc[0]
        self.assertEqual(ndvi_row["winner"], "favorece_4_0")
        self.assertGreater(ndvi_row["advantage_4_0"], 0)

        low_vigor_row = result[result["metric_label"] == "Semanas de baixo vigor"].iloc[0]
        self.assertEqual(low_vigor_row["winner"], "favorece_4_0")

    def test_build_event_driver_lift_surfaces_overrepresented_driver(self) -> None:
        weeks = pd.date_range("2025-09-01", periods=6, freq="7D")
        rows = []
        for week_index, week_start in enumerate(weeks):
            rows.append(
                {
                    "comparison_pair": "silagem",
                    "week_start": week_start,
                    "event_type": "queda" if week_index < 3 else None,
                    "major_drop_flag": week_index < 3,
                    "low_vigor_flag": False,
                    "high_soil_flag": False,
                    "weather_stress_flag": False,
                    "pest_risk_flag": False,
                    "fert_risk_flag": False,
                    "overlap_risk_flag": False,
                    "stop_risk_flag": False,
                    "telemetry_risk_flag": week_index < 3,
                    "alert_risk_flag": week_index in {1, 4},
                    "engine_risk_flag": False,
                }
            )

        result = build_event_driver_lift(pd.DataFrame(rows))
        top_row = result[result["comparison_pair"] == "silagem"].iloc[0]
        self.assertEqual(top_row["driver"], "falha_de_telemetria")
        self.assertGreater(top_row["delta_pp"], 0)

    def test_fit_transition_model_returns_predictions_and_coefficients(self) -> None:
        rows = []
        for area_index, season_id in enumerate(["a", "b", "c", "d"]):
            for week_index in range(5):
                risk = 1.0 if week_index % 2 == 0 else 0.0
                ndvi = 0.5 + area_index * 0.03 + week_index * 0.02
                rows.append(
                    {
                        "season_id": season_id,
                        "week_start": pd.Timestamp("2025-08-04") + pd.Timedelta(days=7 * week_index),
                        "area_label": f"Area {season_id}",
                        "comparison_pair": "grao" if area_index < 2 else "silagem",
                        "treatment": "tecnologia_4_0" if area_index % 2 == 0 else "convencional",
                        "phase": "expansao",
                        "target_next_ndvi_delta": 0.03 - 0.015 * risk + 0.02 * ndvi,
                        "ndvi_mean_week": ndvi,
                        "ndvi_delta_week": 0.02,
                        "soil_pct_week": 4.0,
                        "dense_veg_pct_week": 60.0,
                        "precipitation_mm_week": 15.0,
                        "water_balance_mm_week": 3.0,
                        "temp_avg_c_week": 27.0,
                        "humidity_avg_pct_week": 70.0,
                        "avg_pest_count_week": 4.0,
                        "fert_dose_gap_abs_mean_kg_ha_week": 10.0,
                        "overlap_area_pct_bbox_week": 0.02,
                        "stop_duration_h_per_bbox_ha_week": 0.1,
                        "invalid_telemetry_share_week": risk,
                        "alarm_events_week": risk,
                        "engine_temp_hot_share_week": 0.0,
                        "risk_flag_count": risk,
                        "low_vigor_flag": 0.0,
                        "major_drop_flag": 0.0,
                        "high_soil_flag": 0.0,
                        "weather_stress_flag": 0.0,
                        "pest_risk_flag": 0.0,
                        "ops_risk_flag": risk,
                        "fert_risk_flag": 0.0,
                        "overlap_risk_flag": 0.0,
                        "stop_risk_flag": 0.0,
                        "telemetry_risk_flag": risk,
                        "alert_risk_flag": risk,
                        "engine_risk_flag": 0.0,
                    }
                )

        result = fit_transition_model(pd.DataFrame(rows), alpha=1.0)
        self.assertFalse(result["summary"].empty)
        self.assertFalse(result["coefficients"].empty)
        self.assertFalse(result["predictions"].empty)
        self.assertIn("loo_mae", result["summary"].columns)

    def test_build_final_hypothesis_register_accepts_phase2_pair_column(self) -> None:
        pair_effect_tests = pd.DataFrame(
            [
                {
                    "comparison_pair": "grao",
                    "metric_label": "NDVI medio semanal",
                    "advantage_4_0": -0.02,
                    "ci_low": -0.03,
                    "ci_high": -0.01,
                    "p_value": 0.01,
                    "winner": "favorece_convencional",
                    "evidence_level": "alta",
                },
                {
                    "comparison_pair": "grao",
                    "metric_label": "Semanas de baixo vigor",
                    "advantage_4_0": 0.05,
                    "ci_low": -0.02,
                    "ci_high": 0.12,
                    "p_value": 0.25,
                    "winner": "inconclusivo",
                    "evidence_level": "baixa",
                },
                {
                    "comparison_pair": "grao",
                    "metric_label": "Semanas de queda relevante",
                    "advantage_4_0": 0.10,
                    "ci_low": 0.02,
                    "ci_high": 0.18,
                    "p_value": 0.04,
                    "winner": "favorece_4_0",
                    "evidence_level": "media",
                },
            ]
        )
        event_driver_lift = pd.DataFrame(
            [
                {
                    "comparison_pair": "grao",
                    "driver": "falha_de_telemetria",
                    "delta_pp": 25.0,
                    "lift_ratio": 2.0,
                    "evidence_level": "alta",
                }
            ]
        )
        ndvi_outlook = pd.DataFrame(
            [
                {
                    "comparison_pair": "grao",
                    "treatment": "tecnologia_4_0",
                    "trajectory_score": 70.0,
                    "outlook_band": "positivo_com_ressalvas",
                    "expected_vs_pair": "sem_vantagem_clara_no_par",
                    "top_risks": "telemetria_instavel",
                }
            ]
        )
        ndvi_pair_diagnostics = pd.DataFrame(
            [
                {
                    "pair": "grao",
                    "trajectory_winner": "convencional",
                    "known_gaps": "gap importante",
                }
            ]
        )

        result = build_final_hypothesis_register(
            pair_effect_tests=pair_effect_tests,
            event_driver_lift=event_driver_lift,
            ndvi_outlook=ndvi_outlook,
            ndvi_pair_diagnostics=ndvi_pair_diagnostics,
            deep_dive_gaps=["gap 1", "gap 2"],
        )
        self.assertEqual(len(result), 4)
        self.assertIn("H1", result["hypothesis_id"].tolist())
        self.assertIn("grao", result["comparison_pair"].tolist())


if __name__ == "__main__":
    unittest.main()
