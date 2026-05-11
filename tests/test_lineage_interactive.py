from __future__ import annotations

import unittest

import pandas as pd

from dashboard.lineage.column_catalog import build_raw_column_catalog
from dashboard.lineage.interactive_network import build_lineage_network, lineage_network_html
from dashboard.lineage.storage_strategy import build_storage_recommendation


class LineageInteractiveTests(unittest.TestCase):
    def test_storage_recommendation_prefers_hybrid_for_large_raw_csvs(self) -> None:
        inventory = pd.DataFrame(
            [
                {"zone": "bruto", "size_mb": 118.0},
                {"zone": "bruto", "size_mb": 90.0},
                {"zone": "output", "size_mb": 0.5},
            ]
        )

        recommendation = build_storage_recommendation(inventory).iloc[0]

        self.assertEqual(recommendation["decision"], "hibrido_csv_manifesto_duckdb_opcional")
        self.assertIn("CSV bruto como fonte de verdade", recommendation["recommendation"])

    def test_interactive_network_contains_focus_and_connections(self) -> None:
        lineage = pd.DataFrame(
            [
                {
                    "lineage_id": "driver::solo_exposto",
                    "layer": "driver",
                    "table": "ndvi_phase_timeline",
                    "column": "solo_exposto",
                    "definition": "Driver de solo exposto.",
                    "plain_language": "Mostra solo exposto alto.",
                    "raw_columns": "b1_pct_solo",
                    "upstream_columns": "soil_pct_week | high_soil_flag",
                    "downstream_csvs": "event_driver_lift.csv",
                    "hypotheses": "H3 | H4",
                    "charts": "drivers_problem_weeks",
                    "generated_by": "farmlab.ndvi_deepdive._driver_candidates",
                    "mapping_status": "mapeado_por_driver_entidade",
                    "limitations": "associacao_nao_causal",
                }
            ]
        )

        nodes, edges = build_lineage_network(lineage, focus_query="solo_exposto")
        html = lineage_network_html(lineage, focus_query="solo_exposto")

        labels = {node["label"] for node in nodes}
        self.assertIn("solo_exposto", labels)
        self.assertIn("b1_pct_solo", labels)
        self.assertTrue(edges)
        self.assertIn("solo_exposto", html)
        self.assertIn("event_driver_lift.csv", html)
        self.assertIn("Relações do nó", html)
        self.assertIn("Resetar nós", html)
        self.assertIn("startNodeDrag", html)

    def test_raw_column_catalog_uses_farmlab_schema_descriptions(self) -> None:
        raw_catalog = pd.DataFrame(
            [
                {
                    "source_key": "ndvi_metadata",
                    "source_group": "OneSoil",
                    "path": "data/OneSoil/CSV/ndvi_metadata.csv",
                    "kind": "file",
                    "temporal_min": pd.NA,
                    "temporal_max": pd.NA,
                }
            ]
        )
        docs_cache = {
            "dataset_schemas": [
                {
                    "id": "satelite",
                    "title": "Imagens NDVI (OneSoil)",
                    "url": "https://farm.labs.unimar.br/docs/dados/satelite",
                    "cols": [
                        {
                            "col": "b1_pct_solo",
                            "tipo": "float",
                            "desc": "[b1-NDVI] Proporção de pixels solo exposto (%)",
                        }
                    ],
                }
            ]
        }

        catalog = build_raw_column_catalog(
            raw_catalog,
            lambda _path, _rows: pd.DataFrame({"b1_pct_solo": [10.0]}),
            docs_cache,
        )

        row = catalog.iloc[0]
        self.assertEqual(row["documentation_status"], "farm_lab_schema_extraido")
        self.assertIn("solo exposto", row["documentation"])
        self.assertEqual(row["farm_docs_dataset"], "Imagens NDVI (OneSoil)")


if __name__ == "__main__":
    unittest.main()
