from __future__ import annotations

import unittest

import pandas as pd

from farmlab.ndvi_deepdive import (
    build_ndvi_outlook,
    build_ndvi_pair_diagnostics,
    build_ndvi_phase_timeline,
)


class NdviDeepDiveTests(unittest.TestCase):
    def test_build_ndvi_phase_timeline_flags_drop_and_driver(self) -> None:
        pairwise_weekly_features = pd.DataFrame(
            {
                "season_id": ["tech", "tech", "conv", "conv"],
                "week_start": pd.to_datetime(["2025-11-03", "2025-11-10", "2025-11-03", "2025-11-10"]),
                "ndvi_mean_week": [0.60, 0.42, 0.55, 0.56],
                "ndvi_auc_week": [10.0, 18.0, 9.0, 19.0],
                "soil_pct_week": [12.0, 32.0, 10.0, 9.0],
                "dense_veg_pct_week": [40.0, 18.0, 35.0, 36.0],
                "has_weather_coverage_week": [True, True, True, True],
                "ndvi_delta_week": [pd.NA, -0.18, pd.NA, 0.01],
                "avg_pest_count_week": [5.0, 35.0, 4.0, 4.0],
                "alert_hits_week": [0.0, 2.0, 0.0, 0.0],
                "damage_hits_week": [0.0, 1.0, 0.0, 0.0],
                "fert_dose_gap_abs_mean_kg_ha_week": [40.0, 280.0, 35.0, 30.0],
                "overlap_area_pct_bbox_week": [0.01, 0.08, 0.01, 0.01],
                "stop_duration_h_per_bbox_ha_week": [0.01, 0.04, 0.01, 0.01],
                "pair_ndvi_gap_4_0_minus_conv": [0.05, -0.14, 0.05, -0.14],
                "area_label": ["Grao 4.0", "Grao 4.0", "Grao Convencional", "Grao Convencional"],
                "treatment": ["tecnologia_4_0", "tecnologia_4_0", "convencional", "convencional"],
                "crop_type": ["grao", "grao", "grao", "grao"],
                "comparison_pair": ["grao", "grao", "grao", "grao"],
            }
        )
        ops_support_weekly = pd.DataFrame(
            {
                "season_id": ["tech", "conv"],
                "week_start": pd.to_datetime(["2025-11-10", "2025-11-10"]),
                "invalid_telemetry_share_week": [0.25, 0.0],
                "alarm_events_week": [1.0, 0.0],
                "param_alert_events_week": [2.0, 0.0],
                "engine_idle_share_week": [0.30, 0.05],
                "engine_temp_max_c_week": [90.0, 60.0],
                "fuel_zero_share_week": [0.50, 0.05],
            }
        )

        timeline = build_ndvi_phase_timeline(
            pairwise_weekly_features=pairwise_weekly_features,
            ops_support_weekly=ops_support_weekly,
        )

        drop_row = timeline[(timeline["season_id"] == "tech") & (timeline["week_start"] == pd.Timestamp("2025-11-10"))].iloc[0]
        self.assertEqual(drop_row["event_type"], "queda_forte")
        self.assertEqual(drop_row["primary_driver"], "pressao_de_pragas")
        self.assertEqual(drop_row["pair_position"], "abaixo_do_par")

    def test_build_ndvi_pair_diagnostics_respects_pair_scope(self) -> None:
        phase_timeline = pd.DataFrame(
            {
                "season_id": ["tech_g", "conv_g", "tech_s", "conv_s"],
                "week_start": pd.to_datetime(["2025-11-10"] * 4),
                "ndvi_mean_week": [0.40, 0.55, 0.70, 0.60],
                "ndvi_auc_week": [20.0, 24.0, 28.0, 25.0],
                "ndvi_norm_week": [0.40, 0.75, 0.80, 0.65],
                "major_drop_flag": [True, False, False, True],
                "severe_drop_flag": [True, False, False, False],
                "recovery_flag": [False, False, False, False],
                "low_vigor_flag": [True, False, False, True],
                "weather_stress_flag": [False, False, False, False],
                "pest_risk_flag": [True, False, False, True],
                "ops_risk_flag": [True, False, False, True],
                "telemetry_risk_flag": [True, False, False, False],
                "has_weather_coverage_week": [True, True, True, True],
                "phase": ["queda_forte", "estavel", "pico", "queda"],
                "event_type": ["queda_forte", None, "pico", "queda"],
                "area_label": ["Grao 4.0", "Grao Convencional", "Silagem 4.0", "Silagem Convencional"],
                "treatment": ["tecnologia_4_0", "convencional", "tecnologia_4_0", "convencional"],
                "crop_type": ["grao", "grao", "silagem", "silagem"],
                "comparison_pair": ["grao", "grao", "silagem", "silagem"],
            }
        )
        area_inventory = pd.DataFrame()
        hypothesis_matrix = pd.DataFrame(
            {
                "pair": ["grao", "silagem"],
                "known_gaps": ["gap grao", "gap silagem"],
            }
        )

        diagnostics = build_ndvi_pair_diagnostics(
            ndvi_phase_timeline=phase_timeline,
            area_inventory=area_inventory,
            hypothesis_matrix=hypothesis_matrix,
        )

        self.assertEqual(sorted(diagnostics["pair"].tolist()), ["grao", "silagem"])
        self.assertEqual(
            diagnostics.loc[diagnostics["pair"] == "grao", "trajectory_winner"].iloc[0],
            "convencional",
        )
        self.assertEqual(
            diagnostics.loc[diagnostics["pair"] == "silagem", "trajectory_winner"].iloc[0],
            "4.0",
        )

    def test_build_ndvi_outlook_generates_relative_expectation(self) -> None:
        phase_timeline = pd.DataFrame(
            {
                "season_id": ["tech", "tech", "conv", "conv"],
                "week_start": pd.to_datetime(["2025-11-03", "2025-11-10", "2025-11-03", "2025-11-10"]),
                "ndvi_mean_week": [0.62, 0.66, 0.48, 0.49],
                "ndvi_auc_week": [12.0, 24.0, 9.0, 18.0],
                "ndvi_norm_week": [0.75, 0.82, 0.42, 0.44],
                "major_drop_flag": [False, False, False, False],
                "severe_drop_flag": [False, False, False, False],
                "recovery_flag": [False, False, False, False],
                "low_vigor_flag": [False, False, True, True],
                "weather_stress_flag": [False, False, False, False],
                "pest_risk_flag": [False, False, True, True],
                "ops_risk_flag": [False, False, True, True],
                "telemetry_risk_flag": [False, False, False, False],
                "has_weather_coverage_week": [True, True, True, True],
                "phase": ["expansao", "pico", "estabelecimento", "estavel"],
                "event_type": [None, "pico", "baixo_vigor", "baixo_vigor"],
                "ndvi_delta_week": [0.05, 0.04, 0.01, 0.01],
                "area_label": ["Grao 4.0", "Grao 4.0", "Grao Convencional", "Grao Convencional"],
                "treatment": ["tecnologia_4_0", "tecnologia_4_0", "convencional", "convencional"],
                "crop_type": ["grao", "grao", "grao", "grao"],
                "comparison_pair": ["grao", "grao", "grao", "grao"],
            }
        )
        pair_diagnostics = pd.DataFrame(
            {
                "pair": ["grao"],
                "ndvi_interpretation": ["No par grao, o 4.0 mostra vantagem temporal de NDVI com evidencia media."],
            }
        )

        outlook = build_ndvi_outlook(
            ndvi_phase_timeline=phase_timeline,
            ndvi_pair_diagnostics=pair_diagnostics,
        ).set_index("season_id")

        self.assertEqual(outlook.loc["tech", "expected_vs_pair"], "tende_a_chegar_acima_do_par")
        self.assertEqual(outlook.loc["conv", "expected_vs_pair"], "tende_a_chegar_abaixo_do_par")
        self.assertGreater(outlook.loc["tech", "trajectory_score"], outlook.loc["conv", "trajectory_score"])


if __name__ == "__main__":
    unittest.main()
