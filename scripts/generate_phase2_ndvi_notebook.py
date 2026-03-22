from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent


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
            # Fase 2 - Deep Dive de NDVI

            Notebook para investigar o comportamento temporal do NDVI, seus eventos de estresse/recuperacao
            e a expectativa pre-colheita com base nos dados atuais.

            Saidas principais:
            - `ops_support_daily`
            - `ops_support_weekly`
            - `ndvi_phase_timeline`
            - `ndvi_events`
            - `ndvi_pair_diagnostics`
            - `ndvi_outlook`
            """
        ),
        code_cell(
            """
            from __future__ import annotations

            import importlib.util
            import os
            import subprocess
            import sys
            from pathlib import Path


            def find_project_dir() -> Path:
                if os.environ.get("MONOLITHFARM_PROJECT_DIR"):
                    return Path(os.environ["MONOLITHFARM_PROJECT_DIR"]).expanduser().resolve()
                current = Path.cwd().resolve()
                for candidate in [current, *current.parents]:
                    if (candidate / "pyproject.toml").exists():
                        return candidate
                raise FileNotFoundError("Nao foi possivel localizar `pyproject.toml`. Defina MONOLITHFARM_PROJECT_DIR.")


            PROJECT_DIR = find_project_dir()
            DATA_DIR = Path(os.environ.get("MONOLITHFARM_DATA_DIR", PROJECT_DIR / "data")).expanduser().resolve()
            OUTPUT_DIR = Path(os.environ.get("MONOLITHFARM_OUTPUT_DIR", PROJECT_DIR / "notebook_outputs" / "phase2_ndvi")).expanduser().resolve()
            AUTO_INSTALL = os.environ.get("MONOLITHFARM_AUTO_INSTALL", "0") == "1"

            if str(PROJECT_DIR) not in sys.path:
                sys.path.insert(0, str(PROJECT_DIR))


            def package_available(name: str) -> bool:
                return importlib.util.find_spec(name) is not None


            if AUTO_INSTALL and not package_available("farmlab"):
                try:
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-e", str(PROJECT_DIR)])
                except Exception:
                    subprocess.check_call(["uv", "pip", "install", "--python", sys.executable, "-e", str(PROJECT_DIR)])

            print("PROJECT_DIR =", PROJECT_DIR)
            print("DATA_DIR    =", DATA_DIR)
            print("OUTPUT_DIR  =", OUTPUT_DIR)

            if not DATA_DIR.exists():
                raise FileNotFoundError(f"Diretorio de dados nao encontrado: {DATA_DIR}")
            """
        ),
        code_cell(
            """
            import pandas as pd
            import plotly.express as px
            import plotly.graph_objects as go
            from IPython.display import Image as NotebookImage
            from IPython.display import Markdown, display

            from farmlab.ndvi_deepdive import build_ndvi_deep_dive_workspace, save_ndvi_deep_dive_outputs


            workspace = build_ndvi_deep_dive_workspace(DATA_DIR)
            ndvi_clean = workspace["ndvi_clean"]
            ndvi_phase_timeline = workspace["ndvi_phase_timeline"]
            ndvi_events = workspace["ndvi_events"]
            ndvi_pair_diagnostics = workspace["ndvi_pair_diagnostics"]
            ndvi_outlook = workspace["ndvi_outlook"]

            print("Workspace profundo pronto.")
            """
        ),
        code_cell(
            """
            def display_event_gallery(events: pd.DataFrame, ndvi_clean: pd.DataFrame, *, max_events: int = 8, width: int = 460) -> None:
                display(Markdown("## Galeria dos eventos NDVI"))
                if events.empty or ndvi_clean.empty:
                    print("Sem eventos ou imagens suficientes para a galeria.")
                    return

                priority = {
                    "queda_forte": 0,
                    "queda": 1,
                    "baixo_vigor": 2,
                    "recuperacao": 3,
                    "pico": 4,
                }
                selected = events.copy()
                selected["priority"] = selected["event_type"].map(priority).fillna(9)
                selected = selected.sort_values(["priority", "week_start", "area_label"]).head(max_events)

                for event in selected.itertuples(index=False):
                    candidates = ndvi_clean[ndvi_clean["season_id"] == event.season_id].copy()
                    if candidates.empty:
                        continue
                    candidates["date"] = pd.to_datetime(candidates["date"], errors="coerce")
                    event_date = pd.to_datetime(event.week_start, errors="coerce")
                    match = candidates.loc[(candidates["date"] - event_date).abs().idxmin()]
                    display(Markdown(f"### {event.area_label} | {pd.Timestamp(event.week_start).date()} | {event.event_type}"))
                    display(Markdown(f"- Drivers: {event.drivers_summary}"))
                    display(Markdown(f"- Frase: {event.story_sentence}"))
                    display(
                        pd.DataFrame(
                            {
                                "date": [match["date"]],
                                "ndvi_mean": [match["ndvi_mean"]],
                                "soil_pct": [match["soil_pct"]],
                                "dense_veg_pct": [match["dense_veg_pct"]],
                                "image_path": [match["image_path"]],
                            }
                        )
                    )
                    if pd.notna(match["image_path"]):
                        display(NotebookImage(filename=str(match["image_path"]), width=width))
            """
        ),
        code_cell(
            """
            display(Markdown("## Gaps da fase 2"))
            display(pd.DataFrame({"gap": workspace["deep_dive_gaps"]}))

            display(Markdown("## Diagnostico por par"))
            display(ndvi_pair_diagnostics)

            display(Markdown("## Outlook pre-colheita"))
            display(ndvi_outlook)
            """
        ),
        code_cell(
            """
            display(Markdown("## Timeline semanal de NDVI"))

            fig = px.line(
                ndvi_phase_timeline.sort_values("week_start"),
                x="week_start",
                y="ndvi_mean_week",
                color="area_label",
                facet_row="comparison_pair",
                line_group="area_label",
                markers=True,
                title="NDVI medio semanal com eventos destacados",
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

            fig.update_layout(height=900)
            fig.show()
            """
        ),
        code_cell(
            """
            display(Markdown("## Mapa de calor de risco"))

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
            fig.update_layout(title="Risco semanal por area", xaxis_title="Semana", yaxis_title="Area")
            fig.show()
            """
        ),
        code_cell(
            """
            display(Markdown("## Eventos NDVI"))
            display(ndvi_events)
            """
        ),
        code_cell(
            """
            display_event_gallery(ndvi_events, ndvi_clean, max_events=10)
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
    notebook_path = project_dir / "notebooks" / "phase2_ndvi_deepdive.ipynb"
    notebook_path.parent.mkdir(parents=True, exist_ok=True)
    notebook_path.write_text(json.dumps(build_notebook(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(notebook_path)


if __name__ == "__main__":
    main()
