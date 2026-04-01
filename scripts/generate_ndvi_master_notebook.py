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
            # Analise Mestre de NDVI

            Notebook unico para rodar a fase 1 e a fase 2 no mesmo fluxo.

            O foco deste notebook e:
            - explicar o comportamento temporal do NDVI;
            - comparar convencional x 4.0 por par;
            - marcar eventos de queda, recuperacao e risco;
            - consolidar um outlook pre-colheita com as limitacoes atuais.

            Requisitos:
            - abrir a partir da raiz do repositorio;
            - manter os dados em `./data`;
            - usar o mesmo ambiente `.venv` do projeto;
            - no Colab, prefira usar `.monolithfarm.paths.json`, `MONOLITHFARM_PROFILE`
              ou as variaveis de ambiente para resolver os caminhos.
            """
        ),
        code_cell(
            build_runtime_bootstrap_source(output_subdir="notebook_outputs/master_ndvi")
        ),
        code_cell(
            """
            import base64
            import math

            import pandas as pd
            import plotly.express as px
            import plotly.graph_objects as go
            from IPython.display import HTML

            from farmlab.ndvi_deepdive import build_ndvi_deep_dive_workspace, save_ndvi_deep_dive_outputs


            workspace = build_ndvi_deep_dive_workspace(DATA_DIR)
            area_inventory = workspace["area_inventory"]
            hypothesis_matrix = workspace["hypothesis_matrix"]
            ndvi_clean = workspace["ndvi_clean"]
            ndvi_pair_diagnostics = workspace["ndvi_pair_diagnostics"]
            ndvi_outlook = workspace["ndvi_outlook"]
            ndvi_phase_timeline = workspace["ndvi_phase_timeline"]
            ndvi_events = workspace["ndvi_events"]

            print("Workspace mestre carregado.")
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


            def closest_rows_by_date(frame: pd.DataFrame, target_date: str) -> pd.DataFrame:
                if frame.empty:
                    return frame.copy()
                target = pd.Timestamp(target_date)
                rows = []
                for _, group in frame.sort_values("date").groupby("season_id", sort=False):
                    candidate = group.loc[(pd.to_datetime(group["date"], errors="coerce") - target).abs().idxmin()]
                    rows.append(candidate)
                return pd.DataFrame(rows).sort_values(["comparison_pair", "area_label"]).reset_index(drop=True)


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
                    cards.append(f'<img src="data:image/jpeg;base64,{image_b64}" style="max-width: 520px; border-radius: 8px; margin-bottom: 16px;" />')
                return HTML("\\n".join(cards))


            def event_gallery_html(events: pd.DataFrame, ndvi_clean: pd.DataFrame, max_events: int = 10) -> HTML:
                if events.empty or ndvi_clean.empty:
                    return HTML("<h2>Galeria de eventos</h2><p>Sem eventos ou imagens disponiveis.</p>")

                priority = {"queda_forte": 0, "queda": 1, "baixo_vigor": 2, "recuperacao": 3, "pico": 4}
                selected = events.copy()
                selected["priority"] = selected["event_type"].map(priority).fillna(9)
                selected = selected.sort_values(["priority", "week_start", "area_label"]).head(max_events)

                cards = ["<h2>Galeria dos eventos NDVI</h2>"]
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
            ## Inventario e Base Pareada

            A fase 1 continua sendo a base comparativa do projeto.
            """
        ),
        code_cell("area_inventory"),
        markdown_cell(
            """
            ## Matriz de Hipoteses da Fase 1

            Aqui fica a leitura mais conservadora da comparacao pareada.
            """
        ),
        code_cell("hypothesis_matrix"),
        markdown_cell(
            """
            ## Diagnostico Temporal de NDVI

            A fase 2 entra aqui: eventos, fases e drivers provaveis.
            """
        ),
        code_cell("ndvi_pair_diagnostics"),
        markdown_cell(
            """
            ## Outlook Pre-Colheita

            Esta tabela nao substitui produtividade final ou ROI.
            Ela resume a condicao relativa esperada a partir da trajetoria do NDVI.
            """
        ),
        code_cell("ndvi_outlook"),
        markdown_cell(
            """
            ## Evolucao Semanal de NDVI
            """
        ),
        code_cell(
            """
            fig = px.line(
                workspace["pairwise_weekly_features"].sort_values("week_start"),
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
        markdown_cell(
            """
            ## Timeline Profunda com Eventos
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
                line_group="area_label",
                markers=True,
                title="NDVI com fases, riscos e eventos",
            )

            events = ndvi_phase_timeline[ndvi_phase_timeline["event_type"].notna()]
            for row in events.itertuples(index=False):
                fig.add_scatter(
                    x=[row.week_start],
                    y=[row.ndvi_mean_week],
                    mode="markers",
                    marker={"size": 11, "symbol": "diamond"},
                    text=[f"{row.area_label}<br>{row.event_type}<br>{row.drivers_summary}"],
                    hoverinfo="text",
                    showlegend=False,
                )
            fig.update_layout(height=950)
            fig
            """
        ),
        markdown_cell(
            """
            ## Heatmap de Risco
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
            ## Drivers dos Eventos NDVI
            """
        ),
        code_cell(
            """
            driver_counts = (
                ndvi_events.groupby(["comparison_pair", "area_label", "primary_driver"], as_index=False)
                .size()
                .rename(columns={"size": "event_count"})
            )

            fig = px.bar(
                driver_counts,
                x="area_label",
                y="event_count",
                color="primary_driver",
                facet_row="comparison_pair",
                title="Drivers primarios dos eventos NDVI",
            )
            fig.update_layout(height=760)
            fig
            """
        ),
        markdown_cell(
            """
            ## Galeria Visual por Area
            """
        ),
        code_cell(
            """
            gallery_rows = sample_rows_per_area(ndvi_clean, max_images_per_area=3)
            gallery_html(
                gallery_rows,
                title="Galeria NDVI por area",
                subtitle_columns=["comparison_pair", "treatment", "ndvi_mean", "soil_pct", "dense_veg_pct"],
            )
            """
        ),
        markdown_cell(
            """
            ## Comparacao Visual por Data
            """
        ),
        code_cell(
            """
            TARGET_DATE = "2025-12-22"
            comparison_rows = closest_rows_by_date(ndvi_clean, TARGET_DATE)
            gallery_html(
                comparison_rows,
                title=f"Comparacao visual por data aproximada: {TARGET_DATE}",
                subtitle_columns=["comparison_pair", "treatment", "ndvi_mean", "soil_pct", "dense_veg_pct"],
            )
            """
        ),
        markdown_cell(
            """
            ## Galeria dos Eventos Mais Importantes
            """
        ),
        code_cell("event_gallery_html(ndvi_events, ndvi_clean, max_events=10)"),
        markdown_cell(
            """
            ## Eventos NDVI em Tabela
            """
        ),
        code_cell("ndvi_events"),
        markdown_cell(
            """
            ## Gaps Atuais
            """
        ),
        code_cell('pd.DataFrame({"gap": workspace["deep_dive_gaps"]})'),
        markdown_cell(
            """
            ## Export dos Artefatos
            """
        ),
        code_cell(
            """
            written_files = save_ndvi_deep_dive_outputs(workspace, OUTPUT_DIR)
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
    notebook_path = project_dir / "notebooks" / "ndvi_master_analysis.ipynb"
    notebook_path.parent.mkdir(parents=True, exist_ok=True)
    notebook_path.write_text(json.dumps(build_notebook(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(notebook_path)


if __name__ == "__main__":
    main()
