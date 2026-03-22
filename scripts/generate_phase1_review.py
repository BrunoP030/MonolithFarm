from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from farmlab.pairwise import build_phase1_workspace, save_phase1_outputs


def parse_args() -> argparse.Namespace:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Gera relatorio e graficos da fase 1 em cima de ./data.")
    parser.add_argument("--data-dir", type=Path, default=project_dir / "data")
    parser.add_argument("--output-dir", type=Path, default=project_dir / "notebook_outputs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir.resolve()
    output_dir = args.output_dir.resolve()
    review_dir = output_dir / "review"
    review_dir.mkdir(parents=True, exist_ok=True)

    workspace = build_phase1_workspace(data_dir)
    save_phase1_outputs(workspace, output_dir)

    area_inventory = workspace["area_inventory"].copy()
    ndvi_clean = workspace["ndvi_clean"].copy()
    miip_daily = workspace["miip_daily"].copy()
    pairwise_weekly_features = workspace["pairwise_weekly_features"].copy()
    hypothesis_matrix = workspace["hypothesis_matrix"].copy()
    weather_daily = workspace["weather_daily"].copy()

    write_summary_json(
        review_dir / "summary.json",
        area_inventory=area_inventory,
        ndvi_clean=ndvi_clean,
        miip_daily=miip_daily,
        hypothesis_matrix=hypothesis_matrix,
        weather_daily=weather_daily,
    )
    write_markdown_report(
        review_dir / "review_summary.md",
        area_inventory=area_inventory,
        ndvi_clean=ndvi_clean,
        miip_daily=miip_daily,
        hypothesis_matrix=hypothesis_matrix,
        weather_daily=weather_daily,
    )
    write_ndvi_plot(review_dir / "ndvi_weekly_by_pair.html", pairwise_weekly_features)
    write_ndvi_validity_plot(review_dir / "ndvi_validity_by_area.html", area_inventory)
    write_harvest_plot(review_dir / "harvest_yield_by_area.html", area_inventory)
    write_pest_plot(review_dir / "miip_pressure_by_area.html", area_inventory)
    write_operations_plot(review_dir / "ops_quality_by_area.html", area_inventory)
    write_weather_coverage_plot(review_dir / "weather_coverage_timeline.html", ndvi_clean, weather_daily)

    print(review_dir)


def write_summary_json(
    output_path: Path,
    *,
    area_inventory: pd.DataFrame,
    ndvi_clean: pd.DataFrame,
    miip_daily: pd.DataFrame,
    hypothesis_matrix: pd.DataFrame,
    weather_daily: pd.DataFrame,
) -> None:
    total_images = pd.to_numeric(area_inventory.get("total_images"), errors="coerce").fillna(0).sum()
    total_valid_images = pd.to_numeric(area_inventory.get("total_valid_images"), errors="coerce").fillna(0).sum()
    payload = {
        "areas": int(len(area_inventory)),
        "pairs": sorted(area_inventory.get("comparison_pair", pd.Series(dtype="object")).dropna().unique().tolist()),
        "ndvi_total_images": int(total_images),
        "ndvi_valid_images": int(total_valid_images),
        "ndvi_invalid_images": int(total_images - total_valid_images),
        "ndvi_start": _iso_timestamp(pd.to_datetime(ndvi_clean.get("date"), errors="coerce").min()),
        "ndvi_end": _iso_timestamp(pd.to_datetime(ndvi_clean.get("date"), errors="coerce").max()),
        "weather_start": _iso_timestamp(pd.to_datetime(weather_daily.get("date"), errors="coerce").min()),
        "weather_end": _iso_timestamp(pd.to_datetime(weather_daily.get("date"), errors="coerce").max()),
        "miip_areas_with_data": sorted(miip_daily.get("area_label", pd.Series(dtype="object")).dropna().unique().tolist()),
        "hypothesis_matrix": hypothesis_matrix.to_dict(orient="records"),
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_markdown_report(
    output_path: Path,
    *,
    area_inventory: pd.DataFrame,
    ndvi_clean: pd.DataFrame,
    miip_daily: pd.DataFrame,
    hypothesis_matrix: pd.DataFrame,
    weather_daily: pd.DataFrame,
) -> None:
    total_images = int(pd.to_numeric(area_inventory["total_images"], errors="coerce").fillna(0).sum())
    total_valid_images = int(pd.to_numeric(area_inventory["total_valid_images"], errors="coerce").fillna(0).sum())
    ndvi_start = pd.to_datetime(ndvi_clean["date"], errors="coerce").min()
    ndvi_end = pd.to_datetime(ndvi_clean["date"], errors="coerce").max()
    weather_start = pd.to_datetime(weather_daily["date"], errors="coerce").min()
    weather_end = pd.to_datetime(weather_daily["date"], errors="coerce").max()

    lines = [
        "# Revisao Fase 1",
        "",
        "## Status",
        "",
        "Nao esta totalmente pelos conformes para prova formal completa.",
        "",
        "A fase 1 ja sustenta comparacoes exploratorias e uma narrativa tecnica inicial com base em NDVI, operacao e MIIP.",
        "Ela ainda nao sustenta, com rigor formal, as teses de retorno economico, causalidade fechada entre manejo e produtividade, ou resistencia de insetos.",
        "",
        "## Inventario",
        "",
        f"- Areas analisadas: {len(area_inventory)}",
        f"- Pares comparaveis: {', '.join(sorted(area_inventory['comparison_pair'].dropna().unique()))}",
        f"- Cenas NDVI: {total_images} totais, {total_valid_images} validas, {total_images - total_valid_images} invalidas",
        f"- Janela NDVI valida: {_fmt_ts(ndvi_start)} ate {_fmt_ts(ndvi_end)}",
        f"- Janela clima local: {_fmt_ts(weather_start)} ate {_fmt_ts(weather_end)}",
        f"- Areas com MIIP: {', '.join(sorted(miip_daily['area_label'].dropna().unique())) if not miip_daily.empty else 'nenhuma'}",
        "",
        "## Julgamento",
        "",
        "- O par `grao` nao tem base formal suficiente para comparar produtividade ou pressao de praga de ponta a ponta.",
        "- O par `silagem` permite uma comparacao melhor, mas ainda com MIIP desbalanceado e sem camada economica.",
        "- O projeto ja mostra onde o 4.0 ajuda ou nao ajuda em sinais operacionais e de monitoramento, mas nao fecha o `vale a pena` em R$/ha.",
        "- Os dados atuais nao provam resistencia de insetos a agrotoxicos. No maximo, sugerem padroes de pressao e possiveis falhas operacionais que exigem validacao agronomica.",
        "",
        "## Evidencia Por Par",
        "",
    ]

    for row in hypothesis_matrix.itertuples(index=False):
        lines.extend(
            [
                f"### {row.pair}",
                "",
                f"- Forca da evidencia: {row.evidence_strength}",
                f"- Favorece 4.0: {row.supports_4_0}",
                f"- Favorece convencional: {row.supports_convencional}",
                f"- Lacunas: {row.known_gaps}",
                "",
            ]
        )

    lines.extend(
        [
            "## Cobertura Por Area",
            "",
            "```text",
            _format_area_lines(area_inventory),
            "```",
            "",
            "## O Que Ainda Falta Para Prova Formal",
            "",
            "- Custos reais por hectare e por operacao para fechar `kg/R$` e retorno economico.",
            "- Colheita consolidada nos dois lados do par `grao`.",
            "- Cobertura MIIP simetrica entre as areas comparadas.",
            "- Chave espacial mais forte que `bbox` para casar operacao, NDVI e armadilhas.",
            "- Validacao agronomica externa para qualquer leitura sobre resistencia ou pressao adaptativa de insetos.",
        ]
    )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_ndvi_plot(output_path: Path, pairwise_weekly_features: pd.DataFrame) -> None:
    if pairwise_weekly_features.empty:
        _write_empty_figure(output_path, "Sem dados semanais suficientes para NDVI.")
        return
    frame = pairwise_weekly_features.sort_values("week_start").copy()
    fig = px.line(
        frame,
        x="week_start",
        y="ndvi_mean_week",
        color="area_label",
        facet_row="comparison_pair",
        markers=True,
        title="NDVI medio semanal por area e por par",
        labels={"week_start": "Semana", "ndvi_mean_week": "NDVI medio"},
    )
    fig.update_layout(height=850)
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_ndvi_validity_plot(output_path: Path, area_inventory: pd.DataFrame) -> None:
    frame = area_inventory[["area_label", "total_valid_images", "invalid_images"]].copy()
    melted = frame.melt(id_vars="area_label", var_name="metric", value_name="count")
    fig = px.bar(
        melted,
        x="area_label",
        y="count",
        color="metric",
        barmode="group",
        title="Cobertura e perda de cenas NDVI por area",
        labels={"area_label": "Area", "count": "Cenas"},
    )
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_harvest_plot(output_path: Path, area_inventory: pd.DataFrame) -> None:
    frame = area_inventory[["area_label", "comparison_pair", "treatment", "harvest_yield_mean_kg_ha"]].copy()
    fig = px.bar(
        frame,
        x="area_label",
        y="harvest_yield_mean_kg_ha",
        color="treatment",
        facet_row="comparison_pair",
        title="Produtividade media de colheita por area",
        labels={"harvest_yield_mean_kg_ha": "kg/ha", "area_label": "Area"},
    )
    fig.update_layout(height=700)
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_pest_plot(output_path: Path, area_inventory: pd.DataFrame) -> None:
    frame = area_inventory[
        ["area_label", "comparison_pair", "avg_pest_count", "alert_hits", "control_hits", "damage_hits", "miip_days"]
    ].copy()
    fig = go.Figure()
    fig.add_bar(name="Media de pragas", x=frame["area_label"], y=frame["avg_pest_count"])
    fig.add_bar(name="Alert hits", x=frame["area_label"], y=frame["alert_hits"])
    fig.add_bar(name="Damage hits", x=frame["area_label"], y=frame["damage_hits"])
    fig.add_scatter(
        name="Dias MIIP",
        x=frame["area_label"],
        y=frame["miip_days"],
        mode="markers+text",
        text=frame["miip_days"].fillna(0).astype("Int64").astype(str),
        textposition="top center",
        yaxis="y2",
    )
    fig.update_layout(
        title="Pressao de pragas e cobertura MIIP por area",
        barmode="group",
        yaxis={"title": "Contagem"},
        yaxis2={"title": "Dias MIIP", "overlaying": "y", "side": "right"},
    )
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_operations_plot(output_path: Path, area_inventory: pd.DataFrame) -> None:
    frame = area_inventory[
        [
            "area_label",
            "comparison_pair",
            "fert_dose_gap_abs_mean_kg_ha",
            "overlap_area_pct_bbox",
            "stop_duration_h_per_bbox_ha",
            "harvest_days",
            "fert_days",
        ]
    ].copy()
    fig = make_ops_quality_figure(frame)
    fig.write_html(output_path, include_plotlyjs="cdn")


def make_ops_quality_figure(frame: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_bar(name="Gap absoluto adubacao (kg/ha)", x=frame["area_label"], y=frame["fert_dose_gap_abs_mean_kg_ha"])
    fig.add_bar(name="Sobreposicao (ha/ha)", x=frame["area_label"], y=frame["overlap_area_pct_bbox"])
    fig.add_bar(name="Parada (h/ha)", x=frame["area_label"], y=frame["stop_duration_h_per_bbox_ha"])
    fig.add_scatter(
        name="Dias colheita",
        x=frame["area_label"],
        y=frame["harvest_days"],
        mode="markers+text",
        text=frame["harvest_days"].fillna(0).astype("Int64").astype(str),
        textposition="top center",
        yaxis="y2",
    )
    fig.update_layout(
        title="Qualidade operacional consolidada por area",
        barmode="group",
        yaxis={"title": "Indicadores operacionais"},
        yaxis2={"title": "Dias", "overlaying": "y", "side": "right"},
    )
    return fig


def write_weather_coverage_plot(output_path: Path, ndvi_clean: pd.DataFrame, weather_daily: pd.DataFrame) -> None:
    ndvi_span = pd.DataFrame(
        {
            "source": ["NDVI valido", "Clima local"],
            "start": [
                pd.to_datetime(ndvi_clean["date"], errors="coerce").min(),
                pd.to_datetime(weather_daily["date"], errors="coerce").min(),
            ],
            "end": [
                pd.to_datetime(ndvi_clean["date"], errors="coerce").max(),
                pd.to_datetime(weather_daily["date"], errors="coerce").max(),
            ],
        }
    )
    fig = px.timeline(
        ndvi_span,
        x_start="start",
        x_end="end",
        y="source",
        color="source",
        title="Janela temporal de NDVI versus cobertura climatica local",
    )
    fig.update_yaxes(autorange="reversed")
    fig.write_html(output_path, include_plotlyjs="cdn")


def _write_empty_figure(output_path: Path, title: str) -> None:
    fig = go.Figure()
    fig.update_layout(title=title)
    fig.write_html(output_path, include_plotlyjs="cdn")


def _format_area_lines(area_inventory: pd.DataFrame) -> str:
    columns = [
        "area_label",
        "comparison_pair",
        "treatment",
        "total_valid_images",
        "invalid_images",
        "harvest_yield_mean_kg_ha",
        "avg_pest_count",
        "miip_days",
        "harvest_days",
        "fert_dose_gap_abs_mean_kg_ha",
        "overlap_area_pct_bbox",
        "stop_duration_h_per_bbox_ha",
    ]
    lines = []
    for row in area_inventory[columns].itertuples(index=False):
        lines.append(
            " | ".join(
                [
                    str(row.area_label),
                    str(row.comparison_pair),
                    str(row.treatment),
                    f"ndvi_valid={_fmt_number(row.total_valid_images, 0)}",
                    f"ndvi_invalid={_fmt_number(row.invalid_images, 0)}",
                    f"yield_kg_ha={_fmt_number(row.harvest_yield_mean_kg_ha, 2)}",
                    f"pest_avg={_fmt_number(row.avg_pest_count, 2)}",
                    f"miip_days={_fmt_number(row.miip_days, 0)}",
                    f"harvest_days={_fmt_number(row.harvest_days, 0)}",
                    f"fert_gap={_fmt_number(row.fert_dose_gap_abs_mean_kg_ha, 2)}",
                    f"overlap={_fmt_number(row.overlap_area_pct_bbox, 4)}",
                    f"stop_h_ha={_fmt_number(row.stop_duration_h_per_bbox_ha, 4)}",
                ]
            )
        )
    return "\n".join(lines)


def _fmt_number(value: object, decimals: int) -> str:
    if value is None or pd.isna(value):
        return "NA"
    return f"{float(value):.{decimals}f}"


def _fmt_ts(value: pd.Timestamp | object) -> str:
    if value is None or pd.isna(value):
        return "NA"
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _iso_timestamp(value: pd.Timestamp | object) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


if __name__ == "__main__":
    main()
