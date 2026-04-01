from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from farmlab.ndvi_crispdm import build_ndvi_crispdm_workspace, save_ndvi_crispdm_outputs


def parse_args() -> argparse.Namespace:
    project_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Gera a revisao final NDVI em estilo CRISP-DM a partir de ./data.")
    parser.add_argument("--data-dir", type=Path, default=project_dir / "data")
    parser.add_argument("--output-dir", type=Path, default=project_dir / "notebook_outputs" / "phase3_ndvi")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir.resolve()
    output_dir = args.output_dir.resolve()
    review_dir = output_dir / "review"
    review_dir.mkdir(parents=True, exist_ok=True)

    workspace = build_ndvi_crispdm_workspace(data_dir)
    save_ndvi_crispdm_outputs(workspace, output_dir)

    write_summary_json(review_dir / "summary.json", workspace)
    write_markdown_report(review_dir / "review_summary.md", workspace)
    write_data_audit_plot(review_dir / "data_audit.html", workspace["data_audit"])
    write_pair_effect_plot(review_dir / "pair_effect_forest.html", workspace["pair_effect_tests"])
    write_driver_lift_plot(review_dir / "event_driver_lift.html", workspace["event_driver_lift"])
    write_model_coefficients_plot(review_dir / "transition_model_coefficients.html", workspace["transition_model_coefficients"])
    write_model_fit_plot(review_dir / "transition_model_fit.html", workspace["transition_model_predictions"])
    write_decision_summary_plot(review_dir / "decision_summary.html", workspace["decision_summary"])

    print(review_dir)


def write_summary_json(output_path: Path, workspace: dict[str, pd.DataFrame | list[str] | object]) -> None:
    payload = {
        "data_audit_rows": int(len(workspace["data_audit"])),
        "pair_effect_tests_rows": int(len(workspace["pair_effect_tests"])),
        "event_driver_lift_rows": int(len(workspace["event_driver_lift"])),
        "transition_model_rows": int(len(workspace["transition_model_frame"])),
        "decision_summary": workspace["decision_summary"].to_dict(orient="records"),
        "final_hypothesis_register": workspace["final_hypothesis_register"].to_dict(orient="records"),
        "deep_dive_gaps": workspace["deep_dive_gaps"],
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_markdown_report(output_path: Path, workspace: dict[str, pd.DataFrame | list[str] | object]) -> None:
    data_audit = workspace["data_audit"]
    pair_effect_tests = workspace["pair_effect_tests"]
    event_driver_lift = workspace["event_driver_lift"]
    transition_model_summary = workspace["transition_model_summary"]
    final_hypothesis_register = workspace["final_hypothesis_register"]
    decision_summary = workspace["decision_summary"]
    deep_dive_gaps = workspace["deep_dive_gaps"]

    lines = [
        "# Revisao Final NDVI - CRISP-DM",
        "",
        "## Enquadramento",
        "",
        "Esta entrega organiza o projeto como uma analise de ciencia de dados aplicada ao NDVI.",
        "O foco e separar o que os dados permitem afirmar, o que permanece como hipotese e o que ainda depende de medicao adicional.",
        "",
        "## 1. Business Understanding",
        "",
        "- Pergunta central: o que aconteceu em cada area, por que aconteceu e como isso afeta a comparacao entre convencional e 4.0?",
        "- Eixo principal: NDVI.",
        "- Dados de apoio: clima, MIIP, solo e operacao.",
        "",
        "## 2. Data Understanding",
        "",
        f"- Areas auditadas: {len(data_audit)}",
        f"- Pares avaliados: {', '.join(sorted(data_audit['comparison_pair'].dropna().unique()))}",
        f"- Status de auditoria: {', '.join(f'{row.area_label}={row.audit_status}' for row in data_audit.itertuples(index=False))}",
        "",
        "## 3. Data Preparation",
        "",
        "- O pipeline consolidou NDVI limpo, serie semanal por par, riscos, eventos, cobertura das fontes e tabela de modelagem de transicao do NDVI.",
        "- A comparacao foi mantida separada entre `grao` e `silagem`.",
        "",
        "## 4. Modelagem e Testes",
        "",
        "- Testes pareados por semana: bootstrap + sign-flip nos gaps entre 4.0 e convencional.",
        "- Drivers de semanas problema: lift de ocorrencia em relacao as semanas normais.",
        "- Modelo: regressao ridge interpretavel para `delta do NDVI da semana seguinte`.",
    ]

    if not transition_model_summary.empty:
        row = transition_model_summary.iloc[0]
        lines.extend(
            [
                "",
                f"- Desempenho do modelo: MAE in-sample={row['in_sample_mae']:.4f}, MAE leave-one-area-out={row['loo_mae']:.4f}, R2 leave-one-area-out={_fmt_float(row['loo_r2'])}",
                f"- Escolha do modelo: {row['model_choice']}",
                f"- Justificativa: {row['model_note']}",
            ]
        )

    lines.extend(
        [
            "",
            "## 5. Evidencia Pareada",
            "",
        ]
    )

    for pair, pair_frame in pair_effect_tests.groupby("comparison_pair", sort=True):
        lines.append(f"### {pair}")
        lines.append("")
        for row in pair_frame.itertuples(index=False):
            lines.append(
                f"- {row.metric_label}: winner={row.winner}, vantagem_4_0={row.advantage_4_0:.4f}, IC95% [{row.ci_low:.4f}, {row.ci_high:.4f}], p={row.p_value:.4f}, evidencia={row.evidence_level}"
            )
        top_driver = event_driver_lift[event_driver_lift["comparison_pair"] == pair].head(1)
        if not top_driver.empty:
            driver = top_driver.iloc[0]
            lines.append(
                f"- Driver mais associado as semanas problema: {driver['driver']} (delta={driver['delta_pp']:.2f} pp, lift={_fmt_float(driver['lift_ratio'])}, evidencia={driver['evidence_level']})"
            )
        lines.append("")

    lines.extend(
        [
            "## 6. Hipoteses",
            "",
        ]
    )

    for row in final_hypothesis_register.itertuples(index=False):
        lines.append(f"- {row.comparison_pair} | {row.hypothesis_id} | {row.status} | {row.hypothesis}")
        lines.append(f"  Base: {row.proof_basis}")

    lines.extend(
        [
            "",
            "## 7. Decisao",
            "",
        ]
    )

    for row in decision_summary.itertuples(index=False):
        lines.append(f"- {row.comparison_pair}: {row.decision_message}")
        lines.append(f"  Etapa seguinte: {row.next_step}")

    lines.extend(
        [
            "",
            "## 8. Gaps Ainda Abertos",
            "",
        ]
    )
    for gap in deep_dive_gaps:
        lines.append(f"- {gap}")

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_data_audit_plot(output_path: Path, data_audit: pd.DataFrame) -> None:
    frame = data_audit.copy()
    fig = px.bar(
        frame,
        x="area_label",
        y=["ndvi_valid_ratio", "weather_coverage_ratio", "miip_coverage_ratio"],
        barmode="group",
        color_discrete_sequence=["#174c3c", "#558b2f", "#c58b00"],
        title="Cobertura por area: NDVI valido, clima e MIIP",
    )
    fig.update_layout(yaxis_title="Razao de cobertura", legend_title="")
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_pair_effect_plot(output_path: Path, pair_effect_tests: pd.DataFrame) -> None:
    if pair_effect_tests.empty:
        _write_empty_figure(output_path, "Sem testes pareados disponiveis.")
        return

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
                hovertemplate=(
                    "Par/Metrica=%{y}<br>"
                    "Vantagem 4.0=%{x:.4f}<br>"
                    f"p={row.p_value:.4f}<br>"
                    f"Evidencia={row.evidence_level}<extra></extra>"
                ),
                showlegend=False,
            )
        )
    fig.add_vline(x=0, line_dash="dash", line_color="#94a3b8")
    fig.update_layout(
        title="Testes pareados do NDVI e riscos por par",
        xaxis_title="Vantagem do 4.0 sobre o convencional",
        yaxis_title="",
        height=820,
    )
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_driver_lift_plot(output_path: Path, event_driver_lift: pd.DataFrame) -> None:
    if event_driver_lift.empty:
        _write_empty_figure(output_path, "Sem lift de drivers disponivel.")
        return
    fig = px.bar(
        event_driver_lift,
        x="driver",
        y="delta_pp",
        color="evidence_level",
        facet_row="comparison_pair",
        title="Drivers sobre-representados nas semanas problema do NDVI",
        labels={"delta_pp": "Delta em pontos percentuais", "driver": "Driver"},
        color_discrete_map={"alta": "#0f766e", "media": "#c58b00", "baixa": "#64748b"},
    )
    fig.update_layout(height=760)
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_model_coefficients_plot(output_path: Path, coefficients: pd.DataFrame) -> None:
    if coefficients.empty:
        _write_empty_figure(output_path, "Sem coeficientes de modelo disponiveis.")
        return
    frame = coefficients.head(20).copy()
    fig = px.bar(
        frame,
        x="coefficient",
        y="feature",
        orientation="h",
        color="direction",
        title="Coeficientes padronizados do modelo de transicao do NDVI",
        color_discrete_map={"aumenta_ndvi_futuro": "#0f766e", "pressiona_ndvi_futuro": "#b91c1c"},
    )
    fig.update_layout(height=740, yaxis={"categoryorder": "total ascending"})
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_model_fit_plot(output_path: Path, predictions: pd.DataFrame) -> None:
    if predictions.empty:
        _write_empty_figure(output_path, "Sem predicoes do modelo disponiveis.")
        return
    fig = px.scatter(
        predictions,
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
    fig.write_html(output_path, include_plotlyjs="cdn")


def write_decision_summary_plot(output_path: Path, decision_summary: pd.DataFrame) -> None:
    if decision_summary.empty:
        _write_empty_figure(output_path, "Sem sumario decisorio disponivel.")
        return
    fig = px.bar(
        decision_summary,
        x="comparison_pair",
        y="supported_hypotheses",
        color="temporal_winner",
        text="expected_vs_pair_4_0",
        title="Hipoteses suportadas e leitura decisoria por par",
        labels={"supported_hypotheses": "Hipoteses suportadas", "comparison_pair": "Par"},
    )
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
