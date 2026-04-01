from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from farmlab.complete_analysis import build_complete_ndvi_workspace, save_complete_ndvi_outputs


def parse_args() -> argparse.Namespace:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Gera a revisao estatistica completa do projeto NDVI.")
    parser.add_argument("--data-dir", type=Path, default=project_dir / "data")
    parser.add_argument("--output-dir", type=Path, default=project_dir / "notebook_outputs" / "complete_ndvi")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir.resolve()
    output_dir = args.output_dir.resolve()
    review_dir = output_dir / "review"
    review_dir.mkdir(parents=True, exist_ok=True)

    workspace = build_complete_ndvi_workspace(data_dir)
    save_complete_ndvi_outputs(workspace, output_dir)

    write_summary_json(review_dir / "summary.json", workspace)
    write_markdown_report(review_dir / "review_summary.md", workspace)
    write_outlier_plot(review_dir / "ndvi_outliers.html", workspace["ndvi_outliers"])
    write_classic_tests_plot(review_dir / "pair_classic_tests.html", workspace["pair_classic_tests"])
    write_correlation_plot(review_dir / "weekly_correlations.html", workspace["weekly_correlations"])
    write_trend_plot(review_dir / "ndvi_trends.html", workspace["ndvi_trend_tests"])

    print(review_dir)


def write_summary_json(output_path: Path, workspace: dict[str, pd.DataFrame | list[str] | object]) -> None:
    payload = {
        "dataset_overview_rows": int(len(workspace["dataset_overview"])),
        "pair_classic_tests_rows": int(len(workspace["pair_classic_tests"])),
        "weekly_correlations_rows": int(len(workspace["weekly_correlations"])),
        "ndvi_outliers_flagged": int(workspace["ndvi_outliers"]["outlier_flag"].fillna(False).sum()),
        "decision_summary": workspace["decision_summary"].to_dict(orient="records"),
        "final_hypothesis_register": workspace["final_hypothesis_register"].to_dict(orient="records"),
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_markdown_report(output_path: Path, workspace: dict[str, pd.DataFrame | list[str] | object]) -> None:
    dataset_overview = workspace["dataset_overview"]
    data_audit = workspace["data_audit"]
    ndvi_stats_by_area = workspace["ndvi_stats_by_area"]
    ndvi_outliers = workspace["ndvi_outliers"]
    pair_classic_tests = workspace["pair_classic_tests"]
    weekly_correlations = workspace["weekly_correlations"]
    decision_summary = workspace["decision_summary"]
    final_hypothesis_register = workspace["final_hypothesis_register"]
    transition_model_summary = workspace["transition_model_summary"]

    lines = [
        "# Revisao Completa NDVI",
        "",
        "## 1. Business Understanding",
        "",
        "Comparar areas de milho com manejo convencional e manejo apoiado por tecnologias 4.0, explicando diferencas de vigor vegetativo, risco e desempenho tecnico.",
        "",
        "## 2. Data Understanding",
        "",
        f"- Bases auditadas: {len(dataset_overview)}",
        f"- Areas auditadas: {len(data_audit)}",
        f"- Status de auditoria: {', '.join(f'{row.area_label}={row.audit_status}' for row in data_audit.itertuples(index=False))}",
        "",
        "### Estatistica descritiva por area",
        "",
    ]

    for row in ndvi_stats_by_area.itertuples(index=False):
        lines.append(
            f"- {row.area_label}: media={row.mean:.3f}, mediana={row.median:.3f}, desvio={_fmt_float(row.std)}, CV={_fmt_float(row.cv)}, cenas_validas={row.images_valid}"
        )

    flagged = ndvi_outliers[ndvi_outliers["outlier_flag"]].copy()
    lines.extend(
        [
            "",
            "## 3. Outliers",
            "",
            f"- Cenas com outlier por z-score/robust z-score: {len(flagged)}",
        ]
    )
    for row in flagged.head(12).itertuples(index=False):
        lines.append(
            f"- {row.area_label} | {pd.Timestamp(row.date).date()} | ndvi={row.ndvi_mean:.3f} | z={_fmt_float(row.ndvi_zscore)} | direcao={row.outlier_direction}"
        )

    lines.extend(
        [
            "",
            "## 4. Testes Pareados Classicos",
            "",
        ]
    )
    for row in pair_classic_tests.itertuples(index=False):
        lines.append(
            f"- {row.comparison_pair} | {row.metric_label}: gap_favoravel_4_0={_fmt_float(row.mean_favorable_gap_4_0)}, p={_fmt_float(row.recommended_p_value)}, teste={row.recommended_test}, efeito={_fmt_float(row.paired_effect_size_dz)}, leitura={row.favors}"
        )

    lines.extend(
        [
            "",
            "## 5. Correlacoes Prioritarias",
            "",
        ]
    )
    top_corr = (
        weekly_correlations[
            (weekly_correlations["analysis_target"] == "delta_ndvi_seguinte")
            & (weekly_correlations["comparison_pair"] == "geral")
        ]
        .head(10)
    )
    for row in top_corr.itertuples(index=False):
        lines.append(
            f"- {row.feature}: correlacao={_fmt_float(row.strongest_abs_correlation)}, direcao={row.direction}, forca={row.strength}, p_pearson={_fmt_float(row.pearson_p)}, p_spearman={_fmt_float(row.spearman_p)}"
        )

    lines.extend(
        [
            "",
            "## 6. Modelagem",
            "",
        ]
    )
    if not transition_model_summary.empty:
        row = transition_model_summary.iloc[0]
        lines.append(
            f"- Modelo: {row['model_choice']} | MAE in-sample={row['in_sample_mae']:.4f} | MAE LOO={row['loo_mae']:.4f} | R2 LOO={_fmt_float(row['loo_r2'])}"
        )
        lines.append(f"- Observacao: {row['model_note']}")

    lines.extend(
        [
            "",
            "## 7. Hipoteses e Decisao",
            "",
        ]
    )
    for row in final_hypothesis_register.itertuples(index=False):
        lines.append(f"- {row.comparison_pair} | {row.hypothesis_id} | {row.status} | {row.hypothesis}")
        lines.append(f"  Base: {row.proof_basis}")

    lines.extend(["", "## 8. Decisao Operacional", ""])
    for row in decision_summary.itertuples(index=False):
        lines.append(f"- {row.comparison_pair}: {row.decision_message}")
        lines.append(f"  Etapa seguinte: {row.next_step}")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_outlier_plot(output_path: Path, ndvi_outliers: pd.DataFrame) -> None:
    frame = ndvi_outliers.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    fig = px.scatter(
        frame,
        x="date",
        y="ndvi_zscore",
        color="area_label",
        symbol="outlier_flag",
        facet_row="comparison_pair",
        title="Z-score do NDVI por cena",
        hover_data=["ndvi_mean", "outlier_direction"],
    )
    fig.add_hline(y=2.0, line_dash="dash", line_color="#b91c1c")
    fig.add_hline(y=-2.0, line_dash="dash", line_color="#b91c1c")
    fig.update_layout(height=780)
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_classic_tests_plot(output_path: Path, pair_classic_tests: pd.DataFrame) -> None:
    if pair_classic_tests.empty:
        _write_empty_figure(output_path, "Sem testes classicos disponiveis.")
        return
    fig = px.bar(
        pair_classic_tests,
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
    fig.update_layout(height=920)
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_correlation_plot(output_path: Path, weekly_correlations: pd.DataFrame) -> None:
    frame = weekly_correlations[
        (weekly_correlations["analysis_target"] == "delta_ndvi_seguinte")
        & (weekly_correlations["comparison_pair"] == "geral")
    ].head(15)
    if frame.empty:
        _write_empty_figure(output_path, "Sem correlacoes suficientes.")
        return
    fig = px.bar(
        frame,
        x="strongest_abs_correlation",
        y="feature",
        orientation="h",
        color="direction",
        hover_data=["pearson_r", "pearson_p", "spearman_rho", "spearman_p", "strength"],
        title="Top correlacoes com o delta do NDVI da semana seguinte",
        color_discrete_map={"positiva": "#0f766e", "negativa": "#b91c1c", "sem_relacao": "#64748b"},
    )
    fig.update_layout(height=720, yaxis={"categoryorder": "total ascending"})
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_trend_plot(output_path: Path, ndvi_trend_tests: pd.DataFrame) -> None:
    if ndvi_trend_tests.empty:
        _write_empty_figure(output_path, "Sem tendencias disponiveis.")
        return
    fig = px.bar(
        ndvi_trend_tests,
        x="area_label",
        y="slope_ndvi_per_week",
        color="trend_direction",
        facet_row="comparison_pair",
        title="Tendencia linear do NDVI por semana",
        hover_data=["p_value", "r_squared", "weeks"],
    )
    fig.update_layout(height=760)
    fig.write_html(output_path, include_plotlyjs="cdn")


def _write_empty_figure(output_path: Path, title: str) -> None:
    fig = go.Figure()
    fig.add_annotation(text=title, showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
    fig.write_html(output_path, include_plotlyjs="cdn")


def _fmt_float(value: object) -> str:
    if value is None or pd.isna(value):
        return "NA"
    return f"{float(value):.4f}"


if __name__ == "__main__":
    main()
