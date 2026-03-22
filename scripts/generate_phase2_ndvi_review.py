from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from farmlab.ndvi_deepdive import build_ndvi_deep_dive_workspace, save_ndvi_deep_dive_outputs


def parse_args() -> argparse.Namespace:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Gera review aprofundada de NDVI em cima de ./data.")
    parser.add_argument("--data-dir", type=Path, default=project_dir / "data")
    parser.add_argument("--output-dir", type=Path, default=project_dir / "notebook_outputs" / "phase2_ndvi")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir.resolve()
    output_dir = args.output_dir.resolve()
    review_dir = output_dir / "review"
    review_dir.mkdir(parents=True, exist_ok=True)

    workspace = build_ndvi_deep_dive_workspace(data_dir)
    save_ndvi_deep_dive_outputs(workspace, output_dir)

    phase_timeline = workspace["ndvi_phase_timeline"].copy()
    ndvi_events = workspace["ndvi_events"].copy()
    ndvi_pair_diagnostics = workspace["ndvi_pair_diagnostics"].copy()
    ndvi_outlook = workspace["ndvi_outlook"].copy()
    deep_dive_gaps = workspace["deep_dive_gaps"]

    write_summary_json(
        review_dir / "summary.json",
        ndvi_pair_diagnostics=ndvi_pair_diagnostics,
        ndvi_outlook=ndvi_outlook,
        deep_dive_gaps=deep_dive_gaps,
    )
    write_markdown_report(
        review_dir / "review_summary.md",
        ndvi_pair_diagnostics=ndvi_pair_diagnostics,
        ndvi_outlook=ndvi_outlook,
        ndvi_events=ndvi_events,
        deep_dive_gaps=deep_dive_gaps,
    )
    write_ndvi_phase_plot(review_dir / "ndvi_phase_timeline.html", phase_timeline)
    write_outlook_plot(review_dir / "ndvi_outlook.html", ndvi_outlook)
    write_driver_plot(review_dir / "ndvi_driver_breakdown.html", ndvi_events)
    write_risk_heatmap(review_dir / "ndvi_risk_heatmap.html", phase_timeline)

    print(review_dir)


def write_summary_json(
    output_path: Path,
    *,
    ndvi_pair_diagnostics: pd.DataFrame,
    ndvi_outlook: pd.DataFrame,
    deep_dive_gaps: list[str],
) -> None:
    payload = {
        "pairs": ndvi_pair_diagnostics.to_dict(orient="records"),
        "outlook": ndvi_outlook.to_dict(orient="records"),
        "gaps": deep_dive_gaps,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_markdown_report(
    output_path: Path,
    *,
    ndvi_pair_diagnostics: pd.DataFrame,
    ndvi_outlook: pd.DataFrame,
    ndvi_events: pd.DataFrame,
    deep_dive_gaps: list[str],
) -> None:
    lines = [
        "# Revisao NDVI - Fase 2",
        "",
        "## Objetivo",
        "",
        "Aprofundar a leitura temporal do NDVI, marcando quedas, recuperacoes, riscos operacionais e uma expectativa pre-colheita relativa dentro de cada par.",
        "",
        "## Diagnostico Por Par",
        "",
    ]
    for row in ndvi_pair_diagnostics.itertuples(index=False):
        lines.extend(
            [
                f"### {row.pair}",
                "",
                f"- Forca da evidencia temporal: {row.trajectory_evidence_strength}",
                f"- Vencedor temporal no NDVI: {row.trajectory_winner}",
                f"- Sinais a favor do 4.0: {row.supports_4_0}",
                f"- Sinais a favor do convencional: {row.supports_convencional}",
                f"- Interpretacao: {row.ndvi_interpretation}",
                f"- Lacunas: {row.known_gaps}",
                "",
            ]
        )

    lines.extend(
        [
            "## Outlook Pre-Colheita",
            "",
            "```text",
            _format_outlook_lines(ndvi_outlook),
            "```",
            "",
            "## Eventos NDVI Mais Relevantes",
            "",
            "```text",
            _format_event_lines(ndvi_events),
            "```",
            "",
            "## Gaps",
            "",
        ]
    )
    for gap in deep_dive_gaps:
        lines.append(f"- {gap}")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_ndvi_phase_plot(output_path: Path, phase_timeline: pd.DataFrame) -> None:
    if phase_timeline.empty:
        go.Figure().write_html(output_path, include_plotlyjs="cdn")
        return

    fig = px.line(
        phase_timeline.sort_values("week_start"),
        x="week_start",
        y="ndvi_mean_week",
        color="area_label",
        facet_row="comparison_pair",
        line_group="area_label",
        markers=True,
        title="Timeline semanal de NDVI com fases e eventos",
        labels={"week_start": "Semana", "ndvi_mean_week": "NDVI medio"},
    )
    events = phase_timeline[phase_timeline["event_type"].notna()].copy()
    for row in events.itertuples(index=False):
        fig.add_scatter(
            x=[row.week_start],
            y=[row.ndvi_mean_week],
            mode="markers",
            marker={"size": 11, "symbol": "diamond"},
            name=str(row.event_type),
            text=[f"{row.area_label}<br>{row.event_type}<br>{row.drivers_summary}"],
            hoverinfo="text",
            showlegend=False,
            legendgroup=str(row.event_type),
        )
    fig.update_layout(height=900)
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_outlook_plot(output_path: Path, ndvi_outlook: pd.DataFrame) -> None:
    fig = px.bar(
        ndvi_outlook,
        x="area_label",
        y="trajectory_score",
        color="treatment",
        facet_row="comparison_pair",
        text="outlook_band",
        title="Outlook pre-colheita baseado na trajetoria de NDVI",
        labels={"trajectory_score": "Score", "area_label": "Area"},
    )
    fig.update_layout(height=700)
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_driver_plot(output_path: Path, ndvi_events: pd.DataFrame) -> None:
    if ndvi_events.empty:
        go.Figure().write_html(output_path, include_plotlyjs="cdn")
        return
    frame = (
        ndvi_events.groupby(["comparison_pair", "area_label", "primary_driver"], as_index=False)
        .size()
        .rename(columns={"size": "event_count"})
    )
    fig = px.bar(
        frame,
        x="area_label",
        y="event_count",
        color="primary_driver",
        facet_row="comparison_pair",
        title="Drivers primarios dos eventos NDVI",
        labels={"event_count": "Eventos", "area_label": "Area"},
    )
    fig.update_layout(height=750)
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_risk_heatmap(output_path: Path, phase_timeline: pd.DataFrame) -> None:
    if phase_timeline.empty:
        go.Figure().write_html(output_path, include_plotlyjs="cdn")
        return
    heatmap = (
        phase_timeline.pivot_table(
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
    fig.update_layout(title="Mapa de calor de riscos por semana e area", xaxis_title="Semana", yaxis_title="Area")
    fig.write_html(output_path, include_plotlyjs="cdn")


def _format_outlook_lines(ndvi_outlook: pd.DataFrame) -> str:
    lines = []
    for row in ndvi_outlook.itertuples(index=False):
        lines.append(
            " | ".join(
                [
                    str(row.area_label),
                    str(row.comparison_pair),
                    f"score={row.trajectory_score:.1f}",
                    str(row.outlook_band),
                    str(row.expected_vs_pair),
                    str(row.top_risks),
                ]
            )
        )
    return "\n".join(lines)


def _format_event_lines(ndvi_events: pd.DataFrame) -> str:
    if ndvi_events.empty:
        return "Sem eventos fortes identificados."
    lines = []
    for row in ndvi_events.head(20).itertuples(index=False):
        week = pd.Timestamp(row.week_start).strftime("%Y-%m-%d")
        lines.append(f"{week} | {row.area_label} | {row.event_type} | {row.drivers_summary}")
    return "\n".join(lines)


if __name__ == "__main__":
    main()
