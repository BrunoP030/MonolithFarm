from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

from notebook_bootstrap import build_runtime_bootstrap_source


def markdown_cell(source: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": _lines(source)}


def code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": _lines(source),
    }


def _lines(source: str) -> list[str]:
    text = dedent(source).strip("\n")
    if not text:
        return []
    return [f"{line}\n" for line in text.splitlines()]


def build_notebook() -> dict:
    cells = [
        markdown_cell(
            """
            # Projeto Completo NDVI - CRISP-DM

            Notebook principal do projeto, organizado em CRISP-DM e alinhado ao objetivo descrito em `info.md`.

            Estrutura adotada:
            - Business Understanding
            - Data Understanding
            - Data Preparation
            - Statistical Analysis
            - Modeling
            - Evaluation
            - Export

            Ambientes suportados:
            - localmente;
            - no Jupyter tradicional;
            - no Google Colab com pasta no Drive.
            """
        ),
        markdown_cell(
            """
            ## Uso no Colab

            1. Coloque o repositorio no Drive, se a execucao for feita a partir dele.
            2. Para nao versionar caminhos locais, use um arquivo `.monolithfarm.paths.json`.
            3. Selecione o perfil com `MONOLITHFARM_PROFILE`.
            4. Para definir o modo explicitamente, use `MONOLITHFARM_NOTEBOOK_MODE=colab`.
            5. Rode a primeira celula para montar o Drive e localizar o projeto.
            6. Variaveis opcionais:
               - `MONOLITHFARM_PROJECT_DIR`
               - `MONOLITHFARM_DATA_DIR`
               - `MONOLITHFARM_OUTPUT_DIR`
            """
        ),
        code_cell(
            build_runtime_bootstrap_source(
                output_subdir="notebook_outputs/complete_ndvi",
                required_packages=["duckdb", "pandas", "plotly", "pyproj", "shapely", "scipy"],
            )
        ),
        code_cell(
            """
            import base64
            import math

            import pandas as pd
            import plotly.express as px
            import plotly.graph_objects as go
            from IPython.display import HTML, Markdown, display

            from farmlab.complete_analysis import build_complete_ndvi_workspace, save_complete_ndvi_outputs


            workspace = build_complete_ndvi_workspace(DATA_DIR)
            dataset_overview = workspace["dataset_overview"]
            numeric_profiles = workspace["numeric_profiles"]
            ndvi_stats_by_area = workspace["ndvi_stats_by_area"]
            ndvi_outliers = workspace["ndvi_outliers"]
            pair_weekly_gaps = workspace["pair_weekly_gaps"]
            pair_classic_tests = workspace["pair_classic_tests"]
            ndvi_trend_tests = workspace["ndvi_trend_tests"]
            weekly_correlations = workspace["weekly_correlations"]

            data_audit = workspace["data_audit"]
            pair_effect_tests = workspace["pair_effect_tests"]
            event_driver_lift = workspace["event_driver_lift"]
            transition_model_frame = workspace["transition_model_frame"]
            transition_model_summary = workspace["transition_model_summary"]
            transition_model_coefficients = workspace["transition_model_coefficients"]
            transition_model_predictions = workspace["transition_model_predictions"]
            final_hypothesis_register = workspace["final_hypothesis_register"]
            decision_summary = workspace["decision_summary"]
            ndvi_phase_timeline = workspace["ndvi_phase_timeline"]
            ndvi_events = workspace["ndvi_events"]
            ndvi_clean = workspace["ndvi_clean"]
            weather_daily = workspace["weather_daily"]
            ops_area_daily = workspace["ops_area_daily"]
            miip_daily = workspace["miip_daily"]
            soil_context = workspace["soil_context"]

            print("Workspace carregado.")
            """
        ),
        code_cell(
            """
            def sample_rows_per_area(frame: pd.DataFrame, max_images_per_area: int = 2) -> pd.DataFrame:
                if frame.empty:
                    return frame.copy()
                groups = []
                for _, group in frame.sort_values("date").groupby("season_id", sort=False):
                    if len(group) <= max_images_per_area:
                        groups.append(group)
                        continue
                    positions = sorted({0, len(group) // 2, len(group) - 1})
                    groups.append(group.iloc[positions[:max_images_per_area]])
                return pd.concat(groups, ignore_index=True)


            def _image_to_base64(path: str) -> str | None:
                file_path = Path(path)
                if not file_path.exists():
                    return None
                return base64.b64encode(file_path.read_bytes()).decode("ascii")


            def gallery_html(frame: pd.DataFrame, *, title: str, subtitle_columns: list[str]) -> HTML:
                if frame.empty:
                    return HTML(f"<h2>{title}</h2><p>Sem imagens disponiveis.</p>")

                cards = [f"<h2>{title}</h2>"]
                for row in frame.itertuples(index=False):
                    image_b64 = _image_to_base64(str(row.image_path)) if pd.notna(row.image_path) else None
                    subtitle = " | ".join(
                        f"{column}={getattr(row, column)}" for column in subtitle_columns if hasattr(row, column)
                    )
                    cards.append(f"<h3>{row.area_label} | {pd.Timestamp(row.date).date()}</h3>")
                    cards.append(f"<p>{subtitle}</p>")
                    if image_b64 is None:
                        cards.append("<p>Imagem nao encontrada.</p>")
                        continue
                    cards.append(
                        f'<img src="data:image/jpeg;base64,{image_b64}" '
                        'style="max-width: 560px; border-radius: 8px; margin-bottom: 16px;" />'
                    )
                return HTML("\\n".join(cards))


            def event_gallery_html(events: pd.DataFrame, ndvi_clean: pd.DataFrame, max_events: int = 10) -> HTML:
                if events.empty or ndvi_clean.empty:
                    return HTML("<h2>Galeria de eventos</h2><p>Sem eventos ou imagens disponiveis.</p>")

                priority = {"queda_forte": 0, "queda": 1, "baixo_vigor": 2, "recuperacao": 3, "pico": 4}
                selected = events.copy()
                selected["priority"] = selected["event_type"].map(priority).fillna(9)
                selected = selected.sort_values(["priority", "week_start", "area_label"]).head(max_events)

                cards = ["<h2>Eventos NDVI mais relevantes</h2>"]
                for event in selected.itertuples(index=False):
                    candidates = ndvi_clean[ndvi_clean["season_id"] == event.season_id].copy()
                    if candidates.empty:
                        continue
                    candidates["date"] = pd.to_datetime(candidates["date"], errors="coerce")
                    event_date = pd.to_datetime(event.week_start, errors="coerce")
                    match = candidates.loc[(candidates["date"] - event_date).abs().idxmin()]
                    image_b64 = _image_to_base64(str(match["image_path"])) if pd.notna(match["image_path"]) else None
                    cards.append(f"<h3>{event.area_label} | {pd.Timestamp(event.week_start).date()} | {event.event_type}</h3>")
                    cards.append(f"<p><strong>Drivers:</strong> {event.drivers_summary}</p>")
                    cards.append(f"<p>{event.story_sentence}</p>")
                    if image_b64 is not None:
                        cards.append(
                            f'<img src="data:image/jpeg;base64,{image_b64}" '
                            'style="max-width: 580px; border-radius: 8px; margin-bottom: 18px;" />'
                        )
                return HTML("\\n".join(cards))
            """
        ),
        markdown_cell(
            """
            ## 1. Business Understanding

            O objetivo desta analise e explicar, com base estatistica e agronomica, por que em alguns momentos a area convencional supera a area com tecnologias 4.0 e em outros momentos ocorre o inverso.

            O material da aula de ciclo de vida do dado foi usado como referencia metodologica. O notebook segue a estrutura CRISP-DM com foco em problema, dados, preparo, modelagem e avaliacao.
            """
        ),
        code_cell(
            """
            info_path = PROJECT_DIR / "info.md"
            if info_path.exists():
                display(Markdown(info_path.read_text(encoding="utf-8")))
            else:
                print("info.md nao encontrado em", info_path)
            """
        ),
        markdown_cell(
            """
            ## 2. Data Understanding

            Primeiro validamos o inventario das fontes, o tamanho de cada base e a cobertura temporal.
            """
        ),
        code_cell(
            """
            raw_frames = {
                "ndvi_clean": ndvi_clean,
                "weather_daily": weather_daily,
                "ops_area_daily": ops_area_daily,
                "miip_daily": miip_daily,
                "soil_context": soil_context,
                "ndvi_phase_timeline": ndvi_phase_timeline,
                "transition_model_frame": transition_model_frame,
            }

            shape_rows = []
            for name, frame in raw_frames.items():
                shape_rows.append(
                    {
                        "dataset": name,
                        "shape": frame.shape,
                        "columns": ", ".join(frame.columns[:10]),
                    }
                )
            pd.DataFrame(shape_rows)
            """
        ),
        code_cell("dataset_overview"),
        code_cell(
            """
            describe_targets = {
                "ndvi_clean": ndvi_clean[["ndvi_mean", "ndvi_std", "soil_pct", "dense_veg_pct", "ndvi_delta", "ndvi_auc"]],
                "weather_daily": weather_daily[
                    [
                        "precipitation_mm",
                        "evapotranspiration_mm",
                        "water_balance_mm",
                        "temp_avg_c",
                        "humidity_avg_pct",
                        "wind_avg_kmh",
                    ]
                ],
                "ops_area_daily": ops_area_daily[
                    [
                        column
                        for column in [
                            "harvest_yield_mean_kg_ha",
                            "fert_dose_gap_abs_mean_kg_ha",
                            "overlap_area_pct_bbox",
                            "stop_duration_h_per_bbox_ha",
                        ]
                        if column in ops_area_daily.columns
                    ]
                ],
                "miip_daily": miip_daily[
                    [
                        column
                        for column in [
                            "avg_pest_count",
                            "total_pest_count",
                            "alert_hits",
                            "control_hits",
                            "damage_hits",
                        ]
                        if column in miip_daily.columns
                    ]
                ],
            }

            for name, frame in describe_targets.items():
                display(Markdown(f"### `.describe()` de `{name}`"))
                if frame.empty:
                    print("Base vazia.")
                    continue
                display(frame.describe().T)
            """
        ),
        code_cell(
            """
            fig = px.bar(
                data_audit,
                x="area_label",
                y=["ndvi_valid_ratio", "weather_coverage_ratio", "miip_coverage_ratio"],
                barmode="group",
                title="Cobertura por area: NDVI valido, clima e MIIP",
            )
            fig.update_layout(yaxis_title="Razao de cobertura", legend_title="")
            fig
            """
        ),
        markdown_cell(
            """
            ## 3. Data Preparation

            Nesta etapa os dados foram integrados por area e por semana, que e a granularidade mais defensavel para comparar dinamica de NDVI, risco operacional, clima e pragas.
            """
        ),
        code_cell("numeric_profiles"),
        code_cell("transition_model_frame.head(20)"),
        markdown_cell(
            """
            ## 4. Exploratory Statistical Analysis

            Aqui entram estatistica descritiva, identificacao de outliers por z-score, trajetoria temporal e tendencia por area.
            """
        ),
        code_cell("ndvi_stats_by_area"),
        code_cell("ndvi_trend_tests"),
        code_cell(
            """
            fig = px.line(
                ndvi_phase_timeline.sort_values("week_start"),
                x="week_start",
                y="ndvi_mean_week",
                color="area_label",
                facet_row="comparison_pair",
                markers=True,
                title="NDVI medio semanal por area e por par",
            )
            fig.update_layout(height=850)
            fig
            """
        ),
        code_cell(
            """
            flagged = ndvi_outliers.copy()
            flagged["date"] = pd.to_datetime(flagged["date"], errors="coerce")

            fig = px.scatter(
                flagged,
                x="date",
                y="ndvi_zscore",
                color="area_label",
                symbol="outlier_flag",
                facet_row="comparison_pair",
                hover_data=["ndvi_mean", "outlier_direction"],
                title="Z-score do NDVI por cena",
            )
            fig.add_hline(y=2.0, line_dash="dash", line_color="#b91c1c")
            fig.add_hline(y=-2.0, line_dash="dash", line_color="#b91c1c")
            fig.update_layout(height=780)
            fig
            """
        ),
        code_cell("ndvi_outliers[ndvi_outliers['outlier_flag']].head(30)"),
        markdown_cell(
            """
            ## 5. Pairwise Statistical Testing

            Esta secao combina os testes pareados ja existentes no projeto com testes classicos adicionais.

            Perguntas desta etapa:
            - existe diferenca estatisticamente relevante entre 4.0 e convencional?
            - essa diferenca aparece no nivel medio do NDVI, na area sob a curva e nos sinais de problema?
            - qual teste e mais apropriado em cada caso, dado o comportamento dos gaps semanais?
            """
        ),
        code_cell("pair_effect_tests"),
        code_cell("pair_classic_tests"),
        code_cell(
            """
            ndvi_gap_plot = pair_weekly_gaps.copy()
            fig = px.line(
                ndvi_gap_plot.dropna(subset=["gap_ndvi_mean_week_4_0_minus_convencional"]),
                x="week_start",
                y="gap_ndvi_mean_week_4_0_minus_convencional",
                color="comparison_pair",
                markers=True,
                title="Gap semanal de NDVI medio: 4.0 - convencional",
                labels={"gap_ndvi_mean_week_4_0_minus_convencional": "Gap NDVI"},
            )
            fig.add_hline(y=0, line_dash="dash", line_color="#94a3b8")
            fig
            """
        ),
        code_cell(
            """
            frame = pair_classic_tests.copy()
            fig = px.bar(
                frame,
                x="metric_label",
                y="mean_favorable_gap_4_0",
                color="favors",
                facet_row="comparison_pair",
                hover_data=["recommended_test", "recommended_p_value", "paired_effect_size_dz"],
                title="Gap medio favoravel ao 4.0 por metrica",
                color_discrete_map={
                    "favorece_4_0": "#0f766e",
                    "favorece_convencional": "#b91c1c",
                    "inconclusivo": "#64748b",
                },
            )
            fig.update_layout(height=900)
            fig
            """
        ),
        markdown_cell(
            """
            ## 6. Correlation Analysis

            Correlacao nao prova causalidade, mas ajuda a priorizar variaveis que merecem leitura agronomica mais cuidadosa.
            """
        ),
        code_cell("weekly_correlations.head(40)"),
        code_cell(
            """
            corr_plot = weekly_correlations[
                (weekly_correlations["analysis_target"] == "delta_ndvi_seguinte")
                & (weekly_correlations["comparison_pair"] == "geral")
            ].head(15)

            fig = px.bar(
                corr_plot,
                x="strongest_abs_correlation",
                y="feature",
                orientation="h",
                color="direction",
                hover_data=["pearson_r", "pearson_p", "spearman_rho", "spearman_p", "strength"],
                title="Top correlacoes com o delta do NDVI da semana seguinte",
                color_discrete_map={"positiva": "#0f766e", "negativa": "#b91c1c", "sem_relacao": "#64748b"},
            )
            fig.update_layout(height=720, yaxis={"categoryorder": "total ascending"})
            fig
            """
        ),
        markdown_cell(
            """
            ## 7. Deep Dive de Eventos

            Esta secao detalha semanas problema, drivers associados e imagens de apoio.
            """
        ),
        code_cell("event_driver_lift"),
        code_cell(
            """
            fig = px.bar(
                event_driver_lift,
                x="driver",
                y="delta_pp",
                color="evidence_level",
                facet_row="comparison_pair",
                title="Drivers sobre-representados nas semanas problema do NDVI",
                color_discrete_map={"alta": "#0f766e", "media": "#c58b00", "baixa": "#64748b"},
            )
            fig.update_layout(height=760)
            fig
            """
        ),
        code_cell("ndvi_events.head(20)"),
        code_cell("event_gallery_html(ndvi_events, ndvi_clean, max_events=12)"),
        markdown_cell(
            """
            ## 8. Modeling

            A modelagem foi mantida interpretavel. O alvo modelado e a variacao do NDVI na semana seguinte.
            """
        ),
        code_cell("transition_model_summary"),
        code_cell("transition_model_coefficients.head(20)"),
        code_cell(
            """
            fig = px.bar(
                transition_model_coefficients.head(20),
                x="coefficient",
                y="feature",
                orientation="h",
                color="direction",
                title="Coeficientes padronizados do modelo de transicao do NDVI",
                color_discrete_map={"aumenta_ndvi_futuro": "#0f766e", "pressiona_ndvi_futuro": "#b91c1c"},
            )
            fig.update_layout(height=760, yaxis={"categoryorder": "total ascending"})
            fig
            """
        ),
        code_cell(
            """
            fig = px.scatter(
                transition_model_predictions,
                x="target_next_ndvi_delta",
                y="loo_predicted_next_ndvi_delta",
                color="area_label",
                facet_row="comparison_pair",
                title="Ajuste leave-one-area-out: delta real vs previsto do NDVI",
                labels={
                    "target_next_ndvi_delta": "Delta real da proxima semana",
                    "loo_predicted_next_ndvi_delta": "Delta previsto",
                },
            )
            fig.add_shape(type="line", x0=-0.2, y0=-0.2, x1=0.2, y1=0.2, line={"dash": "dash", "color": "#94a3b8"})
            fig.update_layout(height=760)
            fig
            """
        ),
        markdown_cell(
            """
            ## 9. Evaluation

            Sintese das hipoteses, decisoes e lacunas ainda abertas.
            """
        ),
        code_cell("final_hypothesis_register"),
        code_cell("decision_summary"),
        code_cell(
            """
            sample_rows = sample_rows_per_area(ndvi_clean, max_images_per_area=2)
            gallery_html(
                sample_rows,
                title="Galeria visual de apoio por area",
                subtitle_columns=["comparison_pair", "treatment", "ndvi_mean", "soil_pct", "dense_veg_pct"],
            )
            """
        ),
        code_cell(
            """
            pd.DataFrame({"gap": workspace["deep_dive_gaps"]})
            """
        ),
        markdown_cell(
            """
            ## 10. Export

            Esta celula grava os CSVs analiticos finais em `OUTPUT_DIR`.
            """
        ),
        code_cell(
            """
            written_files = save_complete_ndvi_outputs(workspace, OUTPUT_DIR)
            pd.DataFrame({"written_file": [str(path) for path in written_files]})
            """
        ),
    ]

    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    notebook_path = project_dir / "notebooks" / "complete_ndvi_analysis.ipynb"
    notebook_path.parent.mkdir(parents=True, exist_ok=True)
    notebook_path.write_text(json.dumps(build_notebook(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(notebook_path)


if __name__ == "__main__":
    main()
