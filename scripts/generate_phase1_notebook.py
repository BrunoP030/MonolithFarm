from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent


def markdown_cell(source: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": _lines(source),
    }


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
            # Fase 1 - Notebook pareado de NDVI

            Notebook pensado para rodar localmente ou no Colab.

            Uso local:
            - abra o notebook a partir da raiz do repositório;
            - confirme que os dados estão em `./data`;
            - rode a primeira célula para instalar o projeto em modo editável.

            Uso no Colab:
            - clone ou envie este repositório para o ambiente;
            - ajuste `MONOLITHFARM_PROJECT_DIR` e `MONOLITHFARM_DATA_DIR` se o caminho não for detectado automaticamente;
            - rode a primeira célula para instalar o pacote.

            Saídas principais desta fase:
            - `area_inventory`
            - `ndvi_clean`
            - `ops_area_daily`
            - `miip_daily`
            - `pairwise_weekly_features`
            - `hypothesis_matrix`
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
            OUTPUT_DIR = Path(os.environ.get("MONOLITHFARM_OUTPUT_DIR", PROJECT_DIR / "notebook_outputs")).expanduser().resolve()
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
            print("AUTO_INSTALL =", AUTO_INSTALL)

            if not DATA_DIR.exists():
                raise FileNotFoundError(f"Diretorio de dados nao encontrado: {DATA_DIR}")
            """
        ),
        code_cell(
            """
            import pandas as pd
            import plotly.express as px
            from IPython.display import Image as NotebookImage
            from IPython.display import Markdown, display

            from farmlab.pairwise import OUTPUT_TABLES, build_phase1_workspace, save_phase1_outputs


            workspace = build_phase1_workspace(DATA_DIR)
            area_inventory = workspace["area_inventory"]
            ndvi_clean = workspace["ndvi_clean"]
            ops_area_daily = workspace["ops_area_daily"]
            miip_daily = workspace["miip_daily"]
            pairwise_weekly_features = workspace["pairwise_weekly_features"]
            hypothesis_matrix = workspace["hypothesis_matrix"]

            print("Tabelas prontas:", ", ".join(OUTPUT_TABLES))
            """
        ),
        code_cell(
            """
            def sample_rows_per_area(frame: pd.DataFrame, max_images_per_area: int = 3) -> pd.DataFrame:
                if frame.empty:
                    return frame.copy()

                sampled_groups = []
                for _, group in frame.sort_values("date").groupby("season_id", sort=False):
                    if len(group) <= max_images_per_area:
                        sampled_groups.append(group)
                        continue

                    index_positions = sorted({0, len(group) // 2, len(group) - 1})
                    sampled_groups.append(group.iloc[index_positions[:max_images_per_area]])

                return pd.concat(sampled_groups, ignore_index=True)


            def closest_rows_by_date(frame: pd.DataFrame, target_date: str) -> pd.DataFrame:
                if frame.empty:
                    return frame.copy()

                target = pd.Timestamp(target_date)
                rows = []
                for _, group in frame.sort_values("date").groupby("season_id", sort=False):
                    candidate = group.loc[(group["date"] - target).abs().idxmin()]
                    rows.append(candidate)
                return pd.DataFrame(rows).sort_values(["comparison_pair", "area_label"]).reset_index(drop=True)


            def display_ndvi_gallery(frame: pd.DataFrame, title: str, *, width: int = 420) -> None:
                display(Markdown(f"## {title}"))
                if frame.empty:
                    print("Sem imagens NDVI disponiveis para a selecao.")
                    return

                for area_label, group in frame.sort_values(["comparison_pair", "area_label", "date"]).groupby("area_label", sort=False):
                    area_info = group.iloc[0]
                    display(Markdown(f"### {area_label}"))
                    display(
                        group[
                            [
                                "date",
                                "comparison_pair",
                                "treatment",
                                "ndvi_mean",
                                "soil_pct",
                                "dense_veg_pct",
                                "b1_valid_pixels",
                            ]
                        ].reset_index(drop=True)
                    )
                    for row in group.itertuples(index=False):
                        caption = (
                            f"{pd.Timestamp(row.date).date()} | "
                            f"NDVI medio={row.ndvi_mean:.3f} | "
                            f"solo={row.soil_pct:.1f}% | "
                            f"veg densa={row.dense_veg_pct:.1f}%"
                        )
                        display(Markdown(caption))
                        if pd.notna(row.image_path):
                            display(NotebookImage(filename=str(row.image_path), width=width))
                        else:
                            print("Imagem JPG nao encontrada para esta cena.")
                display(Markdown("> As imagens JPG servem para inspecao visual. A analise quantitativa continua usando o CSV de metadados."))
            """
        ),
        code_cell(
            """
            display(Markdown("## Inventario das areas"))
            display(area_inventory)

            display(Markdown("## Lacunas atuais"))
            display(pd.DataFrame({"gap": workspace["gaps"]}))
            """
        ),
        code_cell(
            """
            display(Markdown("## Evolucao semanal de NDVI por par"))

            if pairwise_weekly_features.empty:
                print("Nao ha features semanais suficientes para plotar.")
            else:
                fig = px.line(
                    pairwise_weekly_features.sort_values("week_start"),
                    x="week_start",
                    y="ndvi_mean_week",
                    color="area_label",
                    facet_row="comparison_pair",
                    markers=True,
                    title="NDVI medio semanal por area e par de comparacao",
                )
                fig.update_layout(height=700)
                fig.show()
            """
        ),
        code_cell(
            """
            gallery_rows = sample_rows_per_area(ndvi_clean, max_images_per_area=3)
            display_ndvi_gallery(gallery_rows, "Galeria NDVI por area")
            """
        ),
        code_cell(
            """
            TARGET_DATE = "2025-12-22"
            comparison_rows = closest_rows_by_date(ndvi_clean, TARGET_DATE)
            display_ndvi_gallery(comparison_rows, f"Comparacao visual por data aproximada: {TARGET_DATE}", width=460)
            """
        ),
        code_cell(
            """
            display(Markdown("## Cobertura climatica"))

            weather_daily = workspace["weather_daily"]
            weather_weekly = workspace["weather_weekly"]

            print("Inicio do NDVI:", pd.to_datetime(ndvi_clean["date"], errors="coerce").min())
            print("Inicio do clima local:", weather_daily["date"].min() if not weather_daily.empty else "sem dados")
            print("Fim do clima local:", weather_daily["date"].max() if not weather_daily.empty else "sem dados")

            if not weather_weekly.empty:
                fig = px.bar(
                    weather_weekly,
                    x="week_start",
                    y="precipitation_mm_week",
                    title="Precipitacao semanal da estacao local",
                )
                fig.show()
            """
        ),
        code_cell(
            """
            display(Markdown("## Sinais operacionais por area"))

            operation_columns = [
                "area_label",
                "comparison_pair",
                "harvest_yield_mean_kg_ha",
                "planting_population_mean_ha",
                "fert_dose_gap_abs_mean_kg_ha",
                "overlap_area_pct_bbox",
                "stop_duration_h_per_bbox_ha",
            ]

            display(area_inventory[[column for column in operation_columns if column in area_inventory.columns]])
            """
        ),
        code_cell(
            """
            display(Markdown("## MIIP e limiares"))

            miip_columns = [
                "area_label",
                "comparison_pair",
                "avg_pest_count",
                "alert_hits",
                "control_hits",
                "damage_hits",
                "image_events",
            ]

            display(area_inventory[[column for column in miip_columns if column in area_inventory.columns]])
            """
        ),
        code_cell(
            """
            display(Markdown("## Matriz de hipoteses"))
            display(hypothesis_matrix)

            for row in hypothesis_matrix.itertuples(index=False):
                display(Markdown(f"### Par: `{row.pair}`"))
                display(Markdown(f"- Forca da evidencia: **{row.evidence_strength}**"))
                display(Markdown(f"- Favorece 4.0: {row.supports_4_0}"))
                display(Markdown(f"- Favorece convencional: {row.supports_convencional}"))
                display(Markdown(f"- Lacunas: {row.known_gaps}"))
            """
        ),
        code_cell(
            """
            written_files = save_phase1_outputs(workspace, OUTPUT_DIR)
            pd.DataFrame({"written_file": [str(path) for path in written_files]})
            """
        ),
    ]

    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3.11",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    notebook_path = project_dir / "notebooks" / "phase1_ndvi_pairwise.ipynb"
    notebook_path.parent.mkdir(parents=True, exist_ok=True)
    notebook_path.write_text(json.dumps(build_notebook(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(notebook_path)


if __name__ == "__main__":
    main()
