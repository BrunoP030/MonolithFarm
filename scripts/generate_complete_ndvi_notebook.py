from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

from notebook_bootstrap import build_runtime_bootstrap_source


ROOT_DIR = Path(__file__).resolve().parents[1]
TEMPLATE_CANDIDATES = [
    ROOT_DIR
    / "monolithfarm_notebook_ndvi_foco_total_v2_package"
    / "monolithfarm_notebook_ndvi_foco_total_v2.ipynb",
    ROOT_DIR
    / "monolithfarm.egg-info"
    / "monolithfarm_notebook_ndvi_foco_total_v2_package"
    / "monolithfarm_notebook_ndvi_foco_total_v2.ipynb",
    ROOT_DIR / "notebooks" / "complete_ndvi_analysis.ipynb",
]
OUTPUT_NOTEBOOK = ROOT_DIR / "notebooks" / "complete_ndvi_analysis.ipynb"


def _lines(source: str) -> list[str]:
    text = dedent(source).strip("\n")
    if not text:
        return []
    return [f"{line}\n" for line in text.splitlines()]


def _bootstrap_cell() -> list[str]:
    bootstrap = build_runtime_bootstrap_source(
        output_subdir="complete_ndvi",
        required_packages=["duckdb", "matplotlib", "numpy", "pandas", "pyproj", "scipy", "shapely"],
    )
    extra = dedent(
        """
        import ast
        import math
        from pathlib import Path

        import matplotlib.pyplot as plt
        import numpy as np
        import pandas as pd
        from IPython.display import HTML, Markdown, display

        from dashboard.lineage.column_catalog import build_feature_catalog, build_workspace_column_catalog
        from dashboard.lineage.column_lineage import raw_columns_for_feature, thresholds_for_feature
        from dashboard.lineage.docs_registry import DRIVER_DOCUMENTATION, column_documentation_for
        from dashboard.lineage.registry import CSV_REGISTRY, FEATURE_REGISTRY, INTERMEDIATE_TABLE_REGISTRY
        from farmlab.complete_analysis import build_complete_ndvi_workspace, save_complete_ndvi_outputs
        from farmlab.io import discover_dataset_paths, load_ndvi_metadata

        pd.set_option("display.max_colwidth", None)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", None)
        pd.set_option("display.max_rows", 2000)

        plt.rcParams["figure.figsize"] = (11, 5)
        plt.rcParams["axes.grid"] = True

        SOURCE_ROOT = PROJECT_DIR
        OUTPUTS_DIR = OUTPUT_DIR

        workspace = build_complete_ndvi_workspace(DATA_DIR)
        save_complete_ndvi_outputs(workspace, OUTPUTS_DIR)
        paths = discover_dataset_paths(DATA_DIR)
        ndvi_raw = load_ndvi_metadata(paths)

        print("PROJECT_DIR =", PROJECT_DIR)
        print("DATA_DIR =", DATA_DIR)
        print("OUTPUTS_DIR =", OUTPUTS_DIR)
        print("Perfil ativo =", os.environ.get("MONOLITHFARM_PROFILE", "<default>"))
        """
    ).strip()
    return _lines(f"{bootstrap}\n\n{extra}")


def _helpers_cell() -> list[str]:
    return _lines(
        """
        def read_text(path: Path) -> str:
            return path.read_text(encoding="utf-8", errors="ignore")


        def resolve_source_file(file_name: str) -> Path:
            raw_path = Path(file_name)
            candidates = [
                SOURCE_ROOT / raw_path,
                SOURCE_ROOT / "farmlab" / raw_path.name,
                SOURCE_ROOT / "dashboard" / raw_path.name,
                SOURCE_ROOT / "docs" / raw_path.name,
                SOURCE_ROOT / "scripts" / raw_path.name,
            ]
            for candidate in candidates:
                if candidate.exists():
                    return candidate
            raise FileNotFoundError(f"Arquivo nao encontrado para exibicao de codigo: {file_name}")


        def extract_function_source(file_path: Path, function_name: str) -> str:
            source = read_text(file_path)
            module = ast.parse(source)
            lines = source.splitlines()
            for node in ast.walk(module):
                if isinstance(node, ast.FunctionDef) and node.name == function_name:
                    start = node.lineno - 1
                    end = node.end_lineno
                    snippet = lines[start:end]
                    numbered = [f"{i + start + 1:04d}: {line}" for i, line in enumerate(snippet)]
                    return "\\n".join(numbered)
            raise ValueError(f"Funcao {function_name} nao encontrada em {file_path}")


        def show_function(file_name: str, function_name: str):
            file_path = resolve_source_file(file_name)
            title = f"### Codigo real — `{function_name}` em `{file_path.relative_to(SOURCE_ROOT)}`"
            display(Markdown(title))
            print(extract_function_source(file_path, function_name))


        def load_csv(name: str) -> pd.DataFrame:
            path = OUTPUTS_DIR / name
            if not path.exists():
                raise FileNotFoundError(f"CSV nao encontrado em notebook_outputs/complete_ndvi: {path.name}")
            return pd.read_csv(path)


        def show_df(title: str, df: pd.DataFrame, n: int = 5):
            display(Markdown(f"### {title}"))
            display(df.head(n))
            print("shape =", df.shape)


        def show_scroll_df(title: str, df: pd.DataFrame, max_height: int = 520):
            display(Markdown(f"### {title}"))
            html = df.to_html(index=False, escape=False)
            display(
                HTML(
                    f"<div style='max-height:{max_height}px; overflow:auto; border:1px solid #d0d7de; "
                    "border-radius:8px; padding:6px; background:white'>"
                    f"{html}</div>"
                )
            )
            print("shape =", df.shape)


        def md(text: str):
            display(Markdown(text))
        """
    )


def _raw_inventory_cell() -> list[str]:
    return _lines(
        """
        raw_inventory = []
        for path in sorted(DATA_DIR.rglob("*")):
            if not path.is_file():
                continue
            raw_inventory.append(
                {
                    "relativo_ao_data": str(path.relative_to(DATA_DIR)),
                    "sufixo": path.suffix.lower(),
                    "tamanho_kb": round(path.stat().st_size / 1024, 2),
                    "pasta_pai": str(path.parent.relative_to(DATA_DIR)),
                }
            )

        raw_inventory = pd.DataFrame(raw_inventory)
        show_df("Inventario bruto real da pasta data/ (primeiras linhas)", raw_inventory, 30)
        print("total_de_arquivos_brutos =", len(raw_inventory))
        """
    )


def _source_catalog_cell() -> list[str]:
    return _lines(
        """
        def infer_source_group(source_key: str) -> str:
            if source_key.startswith("ndvi_"):
                return "OneSoil"
            if source_key.startswith("weather_"):
                return "Metos"
            if source_key.startswith("traps_") or source_key.startswith("pest_"):
                return "EKOS Pest"
            if source_key.startswith("soil_"):
                return "Cropman"
            return "EKOS Layers / apoio"


        source_rows = []
        for source_key, value in vars(paths).items():
            if value is None:
                continue
            path = Path(value)
            source_rows.append(
                {
                    "fonte": infer_source_group(source_key),
                    "source_key": source_key,
                    "caminho_completo": str(path),
                    "relativo_ao_data": str(path.relative_to(DATA_DIR)) if path.exists() and DATA_DIR in path.parents else str(path),
                    "tipo": "diretorio" if path.is_dir() else "arquivo",
                    "existe": path.exists(),
                    "entra_no_fluxo_ndvi": source_key
                    in {
                        "ndvi_metadata",
                        "weather_hourly",
                        "traps_data",
                        "traps_events",
                        "pest_list",
                        "pest_details",
                        "soil_analysis",
                        "layers_dir",
                        "shapes_dir",
                    },
                }
            )

        source_rows = pd.DataFrame(source_rows).sort_values(["fonte", "source_key"]).reset_index(drop=True)
        show_df("Mapa das fontes reais detectadas no ambiente atual", source_rows, 30)
        """
    )


def _ndvi_raw_cell() -> list[str]:
    return _lines(
        """
        ndvi_raw_path = Path(paths.ndvi_metadata)
        show_df("Bruto principal do NDVI: ndvi_metadata.csv", ndvi_raw, 8)
        print("\\nArquivo bruto lido =", ndvi_raw_path)
        print("\\nColunas do bruto NDVI:")
        print(ndvi_raw.columns.tolist())
        """
    )


def _load_outputs_cell() -> list[str]:
    return _lines(
        """
        ndvi_clean = load_csv("ndvi_clean.csv")
        weather_daily = load_csv("weather_daily.csv")
        weather_weekly = load_csv("weather_weekly.csv")
        ops_area_daily = load_csv("ops_area_daily.csv")
        miip_daily = load_csv("miip_daily.csv")
        pairwise_weekly_features = load_csv("pairwise_weekly_features.csv")
        ndvi_phase_timeline = load_csv("ndvi_phase_timeline.csv")
        ndvi_stats_by_area = load_csv("ndvi_stats_by_area.csv")
        pair_weekly_gaps = load_csv("pair_weekly_gaps.csv")
        ndvi_outliers = load_csv("ndvi_outliers.csv")
        pair_effect_tests = load_csv("pair_effect_tests.csv")
        pair_classic_tests = load_csv("pair_classic_tests.csv")
        weekly_correlations = load_csv("weekly_correlations.csv")
        event_driver_lift = load_csv("event_driver_lift.csv")
        final_hypothesis_register = load_csv("final_hypothesis_register.csv")
        decision_summary = load_csv("decision_summary.csv")
        area_inventory = load_csv("area_inventory.csv")

        csv_inventory = pd.DataFrame(
            {
                "csv": [path.name for path in sorted(OUTPUTS_DIR.glob("*.csv"))],
                "tamanho_kb": [round(path.stat().st_size / 1024, 2) for path in sorted(OUTPUTS_DIR.glob("*.csv"))],
            }
        )
        show_df("CSVs atualmente disponiveis em notebook_outputs/complete_ndvi", csv_inventory, 40)
        """
    )


def _feature_driver_catalog_markdown_cell() -> list[str]:
    return _lines(
        """
        ## 3.0 Catálogo completo de features, drivers e colunas geradas

        Esta seção existe para deixar explícito o que antes ficava espalhado entre código, CSVs e gráficos.

        Aqui o notebook documenta:

        - **features brutas/derivadas/agregadas/flags/scores**;
        - **drivers** usados para explicar semanas-problema do NDVI;
        - **colunas dos CSVs finais e intermediários**;
        - **origem bruta** de cada feature principal;
        - **função geradora**;
        - **filtros, agregações e thresholds**;
        - **onde a informação aparece depois**;
        - **quais hipóteses e gráficos dependem dela**.

        Termos importantes:

        - `feature`: variável criada ou usada no pipeline.
        - `driver`: fator explicativo candidato, por exemplo `solo_exposto` ou `falha_de_dose_na_adubacao`.
        - `flag`: feature booleana/indicadora, como `high_soil_flag`.
        - `score`: valor sintético, como `risk_flag_count`.
        - `raw_columns_resolved`: coluna(s) bruta(s) que alimentam a feature.

        **Importante:** driver não significa causa comprovada. Driver significa fator que apareceu associado a semanas problemáticas e que merece investigação.
        """
    )


def _feature_catalog_cell() -> list[str]:
    return _lines(
        """
        feature_catalog = build_feature_catalog()
        show_scroll_df(
            "Catálogo de features principais rastreadas",
            feature_catalog[
                [
                    "feature",
                    "category",
                    "definition",
                    "born_table",
                    "function",
                    "source_columns",
                    "raw_columns_resolved",
                    "raw_sources",
                    "transformation",
                    "thresholds",
                    "filters",
                    "appears_in_tables",
                    "appears_in_csvs",
                    "hypotheses",
                    "charts",
                ]
            ],
            max_height=680,
        )

        missing_raw = feature_catalog[feature_catalog["raw_columns_resolved"].astype(str).str.len().eq(0)]
        print("features_sem_origem_bruta_resolvida =", missing_raw["feature"].tolist())
        """
    )


def _driver_catalog_cell() -> list[str]:
    return _lines(
        """
        driver_catalog = pd.DataFrame(
            [
                {
                    "driver": name,
                    "nome_interpretavel": doc.title,
                    "flag_feature_real": doc.flag_feature,
                    "definicao": doc.definition,
                    "tabela_onde_nasce": doc.born_table,
                    "colunas_que_alimentam": " | ".join(doc.source_columns),
                    "fontes_brutas": " | ".join(doc.raw_sources),
                    "regra_logica": doc.rule,
                    "interpretacao": doc.interpretation,
                    "limitacoes": " | ".join(doc.limitations),
                    "hipoteses": " | ".join(doc.hypotheses),
                    "graficos": " | ".join(doc.charts),
                    "csvs_finais": " | ".join(doc.final_csvs),
                }
                for name, doc in DRIVER_DOCUMENTATION.items()
            ]
        )

        show_scroll_df("Catálogo de drivers das semanas-problema", driver_catalog, max_height=680)

        md(
            '''
            #### Como ler exemplos importantes

            - `solo_exposto` nasce da flag `high_soil_flag`, que usa `soil_pct_week`, derivado de `b1_pct_solo` no `ndvi_metadata.csv`.
            - `falha_de_dose_na_adubacao` nasce da flag `fert_risk_flag`, que compara dose aplicada e dose configurada nas camadas EKOS de adubação.
            - `risco_de_motor` nasce da flag `engine_risk_flag`, alimentada por temperatura do motor, rotação do motor e consumo de combustível.
            - `pressao_de_pragas` nasce da flag `pest_risk_flag`, alimentada por contagens e eventos do MIIP/EKOS Pest.
            - `estresse_climatico` nasce da flag `weather_stress_flag`, alimentada por chuva, evapotranspiração e balanço hídrico.
            '''
        )

        if "driver" in event_driver_lift.columns:
            driver_evidence = event_driver_lift.merge(driver_catalog, on="driver", how="left")
            show_scroll_df(
                "Evidência real dos drivers em event_driver_lift.csv",
                driver_evidence,
                max_height=620,
            )
        """
    )


def _csv_column_catalog_cell() -> list[str]:
    return _lines(
        """
        all_output_frames = {
            path.name: pd.read_csv(path)
            for path in sorted(OUTPUTS_DIR.glob("*.csv"))
        }

        csv_catalog = pd.DataFrame(
            [
                {
                    "csv": name,
                    "linhas": len(frame),
                    "colunas": len(frame.columns),
                    "funcao_geradora": (
                        f"{CSV_REGISTRY[name].module}.{CSV_REGISTRY[name].function}"
                        if name in CSV_REGISTRY
                        else "csv_exportado_pelo_fluxo_completo"
                    ),
                    "dependencias": (
                        " | ".join(CSV_REGISTRY[name].dependencies)
                        if name in CSV_REGISTRY
                        else "workspace/output exportado"
                    ),
                    "hipoteses": (
                        " | ".join(CSV_REGISTRY[name].related_hypotheses)
                        if name in CSV_REGISTRY
                        else ""
                    ),
                    "graficos": (
                        " | ".join(CSV_REGISTRY[name].related_charts)
                        if name in CSV_REGISTRY
                        else ""
                    ),
                    "descricao": (
                        CSV_REGISTRY[name].description
                        if name in CSV_REGISTRY
                        else "CSV real exportado pelo pipeline completo; documentação coluna-a-coluna inferida abaixo."
                    ),
                }
                for name, frame in all_output_frames.items()
            ]
        )

        show_scroll_df("Catálogo dos CSVs gerados em notebook_outputs/complete_ndvi", csv_catalog, max_height=560)

        workspace_column_catalog = build_workspace_column_catalog(workspace, all_output_frames)
        workspace_column_catalog = workspace_column_catalog.sort_values(["kind", "table", "column"]).reset_index(drop=True)
        show_scroll_df(
            "Dicionário de colunas intermediárias e finais geradas",
            workspace_column_catalog[
                [
                    "kind",
                    "table",
                    "column",
                    "dtype",
                    "created_here",
                    "documentation",
                    "usage_status",
                    "examples",
                ]
            ],
            max_height=760,
        )

        print("total_colunas_documentadas_no_notebook =", len(workspace_column_catalog))
        print("csvs_documentados_no_notebook =", len(csv_catalog))
        """
    )


def _ndvi_band_policy_cell() -> list[str]:
    return _lines(
        """
        ndvi_band_policy = pd.DataFrame(
            [
                {
                    "campo": "b1_*",
                    "papel": "banda NDVI usada no pipeline",
                    "uso": "b1_mean, b1_std, b1_pct_solo, b1_pct_veg_densa e b1_valid_pixels alimentam features.",
                    "motivo": "No pacote local, b1 representa a banda analítica de NDVI.",
                },
                {
                    "campo": "b2_* / b2_valid_pixels",
                    "papel": "banda/máscara auxiliar documentada",
                    "uso": "não entra no cálculo principal de NDVI",
                    "motivo": "Não representa a métrica principal de vigor usada no projeto; fica disponível para auditoria.",
                },
                {
                    "campo": "b3_* / b3_valid_pixels",
                    "papel": "banda/máscara auxiliar documentada",
                    "uso": "não entra no cálculo principal de NDVI",
                    "motivo": "Não representa a métrica principal de vigor usada no projeto; fica disponível para auditoria.",
                },
                {
                    "campo": "b1_valid_pixels",
                    "papel": "filtro de qualidade",
                    "uso": "linhas com b1_valid_pixels <= 0 são removidas antes da análise",
                    "motivo": "Sem pixel válido, a cena não tem NDVI utilizável.",
                },
            ]
        )
        show_scroll_df("Política de uso das bandas b1, b2 e b3", ndvi_band_policy, max_height=360)
        """
    )


def patch_notebook(template: dict) -> dict:
    cells = template["cells"]
    cells[1]["source"] = _bootstrap_cell()
    cells[3]["source"] = _helpers_cell()
    cells[5]["source"] = _raw_inventory_cell()
    cells[6]["source"] = _source_catalog_cell()
    cells[8]["source"] = _ndvi_raw_cell()
    cells[12]["source"] = _load_outputs_cell()
    marker = "## 3.0 Catálogo completo de features, drivers e colunas geradas"
    cells = [
        cell
        for cell in cells
        if marker not in "".join(cell.get("source", []))
        and "feature_catalog = build_feature_catalog()" not in "".join(cell.get("source", []))
        and "driver_catalog = pd.DataFrame" not in "".join(cell.get("source", []))
        and "all_output_frames =" not in "".join(cell.get("source", []))
        and "ndvi_band_policy = pd.DataFrame" not in "".join(cell.get("source", []))
    ]
    insert_at = 13
    new_cells = [
        {"cell_type": "markdown", "id": "feature-driver-catalog", "metadata": {}, "source": _feature_driver_catalog_markdown_cell()},
        {"cell_type": "code", "id": "feature-catalog-code", "execution_count": None, "metadata": {}, "outputs": [], "source": _feature_catalog_cell()},
        {"cell_type": "code", "id": "driver-catalog-code", "execution_count": None, "metadata": {}, "outputs": [], "source": _driver_catalog_cell()},
        {"cell_type": "code", "id": "csv-column-catalog-code", "execution_count": None, "metadata": {}, "outputs": [], "source": _csv_column_catalog_cell()},
        {"cell_type": "code", "id": "ndvi-band-policy-code", "execution_count": None, "metadata": {}, "outputs": [], "source": _ndvi_band_policy_cell()},
    ]
    cells[insert_at:insert_at] = new_cells
    template["cells"] = cells
    return template


def build_notebook() -> dict:
    for candidate in TEMPLATE_CANDIDATES:
        if candidate.exists():
            template = json.loads(candidate.read_text(encoding="utf-8"))
            break
    else:
        raise FileNotFoundError("Nenhum notebook template foi encontrado.")
    return patch_notebook(template)


def main() -> None:
    notebook = build_notebook()
    OUTPUT_NOTEBOOK.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_NOTEBOOK.write_text(json.dumps(notebook, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Notebook gerado em: {OUTPUT_NOTEBOOK}")


if __name__ == "__main__":
    main()
