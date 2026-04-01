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
            # Fase 3 - Analise Final NDVI em CRISP-DM

            Notebook da fase 3, com consolidacao da analise NDVI em estrutura CRISP-DM.

            Estrutura:
            - Business Understanding
            - Data Understanding
            - Data Preparation
            - Modeling
            - Evaluation
            - Deliverables

            Premissas da modelagem:
            - `NDVI` continua sendo o eixo central.
            - `clima`, `MIIP`, `solo` e `operacao` entram como variaveis explicadoras.
            - a modelagem foi mantida interpretavel; nao foi usado deep learning porque a base atual e pequena e nao tem target final de colheita consolidado em todos os pares.
            """
        ),
        code_cell(
            build_runtime_bootstrap_source(
                output_subdir="notebook_outputs/phase3_ndvi",
                extra_imports=["import base64"],
            )
        ),
        code_cell(
            """
            import pandas as pd
            import plotly.express as px
            import plotly.graph_objects as go
            from IPython.display import HTML

            from farmlab.ndvi_crispdm import build_ndvi_crispdm_workspace, save_ndvi_crispdm_outputs


            workspace = build_ndvi_crispdm_workspace(DATA_DIR)
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

            print("Workspace CRISP-DM carregado.")
            """
        ),
        code_cell(
            """
            def sample_rows_per_area(frame: pd.DataFrame, max_images_per_area: int = 3) -> pd.DataFrame:
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
                    subtitle = " | ".join(f"{column}={getattr(row, column)}" for column in subtitle_columns if hasattr(row, column))
                    cards.append(f"<h3>{row.area_label} | {pd.Timestamp(row.date).date()}</h3>")
                    cards.append(f"<p>{subtitle}</p>")
                    if image_b64 is None:
                        cards.append("<p>Imagem nao encontrada.</p>")
                        continue
                    cards.append(f'<img src="data:image/jpeg;base64,{image_b64}" style="max-width: 540px; border-radius: 8px; margin-bottom: 16px;" />')
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
                        cards.append(f'<img src="data:image/jpeg;base64,{image_b64}" style="max-width: 560px; border-radius: 8px; margin-bottom: 18px;" />')
                return HTML("\\n".join(cards))
            """
        ),
        markdown_cell(
            """
            ## 1. Business Understanding

            Perguntas centrais:

            - o que aconteceu em cada area;
            - quais eventos e riscos explicam as quedas e recuperacoes do NDVI;
            - o que os dados atuais indicam sobre convencional x 4.0;
            - o que ainda depende de colheita final, custo e validacao agronomica.
            """
        ),
        code_cell("decision_summary"),
        markdown_cell(
            """
            ## 2. Data Understanding

            Auditoria da base: cobertura de NDVI, clima, MIIP e operacao.
            """
        ),
        code_cell("data_audit"),
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

            A base final de modelagem foi montada em nivel semanal para preservar comparabilidade temporal e interpretabilidade.
            """
        ),
        code_cell("transition_model_frame.head(20)"),
        markdown_cell(
            """
            ## 4. Exploratory Analysis

            Primeiro, a trajetoria semanal do NDVI por par.
            """
        ),
        code_cell(
            """
            fig = px.line(
                ndvi_phase_timeline.sort_values("week_start"),
                x="week_start",
                y="ndvi_mean_week",
                color="area_label",
                facet_row="comparison_pair",
                markers=True,
                title="NDVI semanal por area e por par",
            )
            fig.update_layout(height=850)
            fig
            """
        ),
        code_cell(
            """
            heatmap = (
                ndvi_phase_timeline.pivot_table(
                    index="area_label",
                    columns="week_start",
                    values="risk_flag_count",
                    aggfunc="max",
                )
                .sort_index()
                .fillna(0)
            )

            fig = go.Figure(
                data=go.Heatmap(
                    z=heatmap.values,
                    x=[pd.Timestamp(value).strftime("%Y-%m-%d") for value in heatmap.columns],
                    y=heatmap.index.tolist(),
                    colorscale="YlOrRd",
                    colorbar={"title": "Riscos"},
                )
            )
            fig.update_layout(title="Mapa de calor de risco por semana e area", xaxis_title="Semana", yaxis_title="Area")
            fig
            """
        ),
        markdown_cell(
            """
            ## 5. Statistical Testing

            Nesta secao o par 4.0 x convencional e tratado como serie pareada por semana. Isso e mais forte do que comparar apenas medias agregadas.
            """
        ),
        code_cell("pair_effect_tests"),
        code_cell(
            """
            frame = pair_effect_tests.copy()
            frame["metric_with_pair"] = frame["comparison_pair"] + " | " + frame["metric_label"]

            fig = go.Figure()
            for row in frame.itertuples(index=False):
                color = "#0f766e" if row.winner == "favorece_4_0" else "#b91c1c" if row.winner == "favorece_convencional" else "#475569"
                fig.add_trace(
                    go.Scatter(
                        x=[row.advantage_4_0],
                        y=[row.metric_with_pair],
                        error_x={
                            "type": "data",
                            "symmetric": False,
                            "array": [max(row.ci_high - row.advantage_4_0, 0)],
                            "arrayminus": [max(row.advantage_4_0 - row.ci_low, 0)],
                        },
                        mode="markers",
                        marker={"size": 10, "color": color},
                        showlegend=False,
                        hovertemplate=(
                            "Par/Metrica=%{y}<br>"
                            "Vantagem 4.0=%{x:.4f}<br>"
                            f"p={row.p_value:.4f}<br>"
                            f"Evidencia={row.evidence_level}<extra></extra>"
                        ),
                    )
                )
            fig.add_vline(x=0, line_dash="dash", line_color="#94a3b8")
            fig.update_layout(
                title="Testes pareados do NDVI e riscos por par",
                xaxis_title="Vantagem do 4.0 sobre o convencional",
                yaxis_title="",
                height=820,
            )
            fig
            """
        ),
        markdown_cell(
            """
            ## 6. Driver Analysis

            Frequencia relativa dos drivers nas semanas problema em relacao ao restante da safra.
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
        code_cell("event_gallery_html(ndvi_events, ndvi_clean, max_events=12)"),
        markdown_cell(
            """
            ## 7. Modeling

            O target modelado e o `delta do NDVI da proxima semana`.

            Foi utilizado um modelo linear regularizado e interpretavel para identificar sinais que pressionam ou aliviam a trajetoria do NDVI.
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
            ## 8. Evaluation

            Sintese das hipoteses suportadas, negadas e inconclusivas.
            """
        ),
        code_cell("final_hypothesis_register"),
        code_cell("decision_summary"),
        markdown_cell(
            """
            ## 9. Sintese Final

            Esta secao separa:

            - o que esta provado com os dados atuais;
            - o que e apenas hipotese plausivel;
            - o que ainda depende de custo, colheita final ou validacao agronomica;
            - quais proximos passos fecham a decisao de negocio.
            """
        ),
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
        code_cell(
            """
            written_files = save_ndvi_crispdm_outputs(workspace, OUTPUT_DIR)
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
    notebook_path = project_dir / "notebooks" / "phase3_ndvi_crispdm.ipynb"
    notebook_path.parent.mkdir(parents=True, exist_ok=True)
    notebook_path.write_text(json.dumps(build_notebook(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(notebook_path)


if __name__ == "__main__":
    main()
