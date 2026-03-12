from __future__ import annotations

from html import escape
from io import StringIO
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from farmlab.analysis import apply_manual_mapping, summarize_costs
from farmlab.config import COST_INPUT_COLUMNS, DEFAULT_DATA_DIR, SEASON_MAPPING_COLUMNS
from farmlab.database import DEFAULT_DB_PATH, ensure_workspace


COLOR_MAP = {
    "Convencional": "#C46B2A",
    "Tecnologia 4.0": "#2F6F4F",
    "Indefinido": "#6A7480",
}

TEXT_REPLACEMENTS = {
    "nao": "não",
    "Nao": "Não",
    "area": "área",
    "Area": "Área",
    "talhao": "talhão",
    "Talhao": "Talhão",
    "medio": "médio",
    "Media": "Média",
    "periodo": "período",
    "util": "útil",
    "eletronicas": "eletrônicas",
    "proximas": "próximas",
    "Populacao": "População",
    "seguranca": "segurança",
    "validacao": "validação",
}


st.set_page_config(page_title="MonolithFarm | NDVI", layout="wide", initial_sidebar_state="expanded")


def main() -> None:
    _inject_theme()
    controls = _render_sidebar()

    try:
        workspace, db_state = ensure_workspace(
            Path(controls["data_dir"]),
            Path(controls["db_path"]),
            force_refresh=controls["refresh_db"],
        )
    except Exception as exc:
        st.error(f"Falha ao carregar os dados: {exc}")
        st.stop()

    manual_mapping = _read_uploaded_csv(controls["mapping_upload"])
    cost_input = _read_uploaded_csv(controls["cost_upload"])

    season_mapping = _prepare_season_mapping(apply_manual_mapping(workspace["season_mapping"], manual_mapping))
    evidence = workspace["evidence"].merge(
        season_mapping[["season_id", "area_display", "treatment_display", "treatment"]],
        on="season_id",
        how="left",
    )
    evidence = _prepare_evidence(evidence)

    cost_summary = summarize_costs(cost_input, evidence)
    cost_summary = _prepare_cost_summary(cost_summary, evidence)

    _render_hero(workspace, db_state, evidence)

    tabs = st.tabs(["Visão Geral", "Comparação", "Série NDVI", "Pragas & Clima", "Diagnóstico", "Banco"])
    with tabs[0]:
        render_overview(workspace, season_mapping, db_state, evidence)
    with tabs[1]:
        render_comparison(evidence, cost_summary)
    with tabs[2]:
        render_ndvi(workspace, season_mapping)
    with tabs[3]:
        render_pests_and_weather(workspace)
    with tabs[4]:
        render_evidence(evidence)
        render_gaps(workspace["gaps"])
    with tabs[5]:
        render_database_status(db_state, workspace)


def render_overview(workspace: dict, season_mapping: pd.DataFrame, db_state: dict, evidence: pd.DataFrame) -> None:
    inventory = workspace["inventory"]
    crop_window = workspace["crop_window"]

    _section("Leitura rápida", "Panorama visual da safra, do banco e do mapeamento dos recortes.")
    cards = st.columns(4)
    _metric_card(cards[0], "Recortes NDVI", str(inventory["ndvi_seasons"]), "Áreas satelitais em monitoramento", "olive")
    _metric_card(cards[1], "Imagens úteis", str(inventory["ndvi_images"]), "Cenas disponíveis no banco local", "amber")
    _metric_card(cards[2], "Armadilhas", str(inventory["unique_traps"]), "Pontos únicos entre modelos convencionais e 4.0", "forest")
    _metric_card(cards[3], "Banco local", f"{db_state['size_mb']} MB", _display_source(db_state.get("loaded_from")), "stone")

    panels = st.columns([1.2, 0.8])
    panels[0].markdown(
        _panel(
            "Janela operacional detectada",
            f"Plantio entre <strong>{_fmt_dt(crop_window['planting_start'])}</strong> e <strong>{_fmt_dt(crop_window['planting_end'])}</strong>.<br>"
            f"Colheita entre <strong>{_fmt_dt(crop_window['harvest_start'])}</strong> e <strong>{_fmt_dt(crop_window['harvest_end'])}</strong>.",
        ),
        unsafe_allow_html=True,
    )
    top_area = evidence.dropna(subset=["yield_mean_kg_ha"]).head(1)
    top_text = "Sem produtividade consolidada até o momento."
    if not top_area.empty:
        row = top_area.iloc[0]
        top_text = f"<strong>{escape(row['area_display'])}</strong> lidera a produtividade média com <strong>{_fmt_number(row['yield_mean_kg_ha'])} kg/ha</strong>."
    panels[1].markdown(_panel("Destaque atual", top_text), unsafe_allow_html=True)

    _section("Mapa lógico das áreas", "Relacionamento operacional dos recortes NDVI com talhões e armadilhas de referência.")
    st.dataframe(
        season_mapping[
            ["area_display", "treatment_display", "plot_display", "closest_trap", "distance_to_closest_trap_m", "supporting_traps", "season_id"]
        ],
        hide_index=True,
        use_container_width=True,
        column_config={
            "area_display": st.column_config.TextColumn("Área"),
            "treatment_display": st.column_config.TextColumn("Tratamento"),
            "plot_display": st.column_config.TextColumn("Talhão sugerido"),
            "closest_trap": st.column_config.TextColumn("Armadilha"),
            "distance_to_closest_trap_m": st.column_config.NumberColumn("Distância (m)", format="%.0f"),
            "supporting_traps": st.column_config.TextColumn("Armadilhas de apoio"),
            "season_id": st.column_config.TextColumn("Season ID"),
        },
    )


def render_comparison(evidence: pd.DataFrame, cost_summary: pd.DataFrame | None) -> None:
    _section("Comparação entre áreas", "Produtividade, vigor e intensidade de monitoramento em uma mesma leitura.")

    insights = st.columns(3)
    _insight_card(insights[0], "Maior produtividade", _headline(evidence.dropna(subset=["yield_mean_kg_ha"]).head(1), "yield_mean_kg_ha", " kg/ha"))
    _insight_card(insights[1], "Maior NDVI médio", _headline(evidence.dropna(subset=["ndvi_mean"]).sort_values("ndvi_mean", ascending=False).head(1), "ndvi_mean", "", 3))
    _insight_card(insights[2], "Maior cobertura 4.0", _headline(evidence.sort_values("electronic_traps", ascending=False).head(1), "electronic_traps", " armadilhas"))

    treatment_summary = (
        evidence.dropna(subset=["treatment_display"])
        .groupby("treatment_display", as_index=False)
        .agg(
            areas=("season_id", "nunique"),
            yield_mean_kg_ha=("yield_mean_kg_ha", "mean"),
            ndvi_mean=("ndvi_mean", "mean"),
            electronic_traps=("electronic_traps", "sum"),
            conventional_traps=("conventional_traps", "sum"),
        )
        .sort_values("treatment_display")
    )
    if not treatment_summary.empty:
        duel_columns = st.columns(max(len(treatment_summary), 1))
        for index, row in treatment_summary.reset_index(drop=True).iterrows():
            duel_columns[index].markdown(
                _duel_card(
                    row["treatment_display"],
                    [
                        ("Áreas", _fmt_optional(row["areas"], 0)),
                        ("Produtividade média", _fmt_optional(row["yield_mean_kg_ha"], 0, " kg/ha")),
                        ("NDVI médio", _fmt_optional(row["ndvi_mean"], 3)),
                        ("Armadilhas 4.0", _fmt_optional(row["electronic_traps"], 0)),
                    ],
                ),
                unsafe_allow_html=True,
            )

    charts = st.columns(2)
    yield_frame = evidence.dropna(subset=["yield_mean_kg_ha"]).copy()
    if not yield_frame.empty:
        fig = px.bar(
            yield_frame.sort_values("yield_mean_kg_ha", ascending=True),
            x="yield_mean_kg_ha",
            y="area_display",
            color="treatment_display",
            orientation="h",
            color_discrete_map=COLOR_MAP,
            labels={"yield_mean_kg_ha": "Produtividade média (kg/ha)", "area_display": "Área", "treatment_display": "Tratamento"},
        )
        _style_figure(fig)
        charts[0].plotly_chart(fig, use_container_width=True)
    else:
        charts[0].info("Ainda não foi possível consolidar produtividade por área.")

    scatter_frame = evidence.dropna(subset=["yield_mean_kg_ha", "ndvi_mean"]).copy()
    if not scatter_frame.empty:
        fig = px.scatter(
            scatter_frame,
            x="ndvi_mean",
            y="yield_mean_kg_ha",
            color="treatment_display",
            size="electronic_traps",
            size_max=24,
            text="area_display",
            color_discrete_map=COLOR_MAP,
            labels={"ndvi_mean": "NDVI médio", "yield_mean_kg_ha": "Produtividade média (kg/ha)", "treatment_display": "Tratamento"},
        )
        fig.update_traces(textposition="top center")
        _style_figure(fig)
        charts[1].plotly_chart(fig, use_container_width=True)
    else:
        charts[1].info("Ainda não há dados suficientes para correlacionar NDVI médio e produtividade.")

    st.dataframe(
        evidence[
            ["area_display", "treatment_display", "yield_mean_kg_ha", "ndvi_mean", "ndvi_peak", "population_mean_ha", "electronic_traps", "conventional_traps"]
        ],
        hide_index=True,
        use_container_width=True,
        column_config={
            "area_display": st.column_config.TextColumn("Área"),
            "treatment_display": st.column_config.TextColumn("Tratamento"),
            "yield_mean_kg_ha": st.column_config.NumberColumn("Produtividade média (kg/ha)", format="%.0f"),
            "ndvi_mean": st.column_config.NumberColumn("NDVI médio", format="%.3f"),
            "ndvi_peak": st.column_config.NumberColumn("Pico de NDVI", format="%.3f"),
            "population_mean_ha": st.column_config.NumberColumn("População média (plantas/ha)", format="%.0f"),
            "electronic_traps": st.column_config.NumberColumn("Armadilhas 4.0", format="%d"),
            "conventional_traps": st.column_config.NumberColumn("Armadilhas convencionais", format="%d"),
        },
    )

    _section("Eficiência econômica", "Espaço reservado para comparar custo por hectare com retorno produtivo.")
    if cost_summary is None or cost_summary.empty:
        st.markdown(_panel("Custo ainda não carregado", "Envie um CSV de custo por hectare para liberar a comparação econômica completa."), unsafe_allow_html=True)
    else:
        st.dataframe(
            cost_summary,
            hide_index=True,
            use_container_width=True,
            column_config={
                "area_display": st.column_config.TextColumn("Área"),
                "treatment_display": st.column_config.TextColumn("Tratamento"),
                "total_cost_per_ha_brl": st.column_config.NumberColumn("Custo total (R$/ha)", format="%.2f"),
                "yield_mean_kg_ha": st.column_config.NumberColumn("Produtividade média (kg/ha)", format="%.0f"),
                "kg_per_brl": st.column_config.NumberColumn("Kg por R$", format="%.2f"),
            },
        )


def render_ndvi(workspace: dict, season_mapping: pd.DataFrame) -> None:
    _section("Leitura temporal do NDVI", "Escolha uma área para navegar pela série histórica e pelos quadros disponíveis.")
    ndvi_daily = workspace["ndvi_daily"].merge(season_mapping[["season_id", "area_display", "treatment_display"]], on="season_id", how="left")
    ndvi_daily["date"] = pd.to_datetime(ndvi_daily["date"], errors="coerce")
    if ndvi_daily.empty:
        st.info("Não há observações NDVI úteis dentro da janela de cultivo detectada.")
        return

    columns = st.columns([1.3, 0.9])
    fig = px.line(ndvi_daily, x="date", y="b1_mean", color="area_display", markers=True, labels={"b1_mean": "NDVI médio", "date": "Data", "area_display": "Área"})
    _style_figure(fig, show_legend=True)
    columns[0].plotly_chart(fig, use_container_width=True)

    options = season_mapping[season_mapping["season_id"].isin(ndvi_daily["season_id"].unique())].copy()
    options["selector"] = options["area_display"] + " · " + options["treatment_display"]
    selected = columns[1].selectbox("Área observada", options["selector"].tolist())
    row = options[options["selector"] == selected].iloc[0]
    season_frames = ndvi_daily[ndvi_daily["season_id"] == row["season_id"]].sort_values("date")
    labels = season_frames["date"].dt.strftime("%Y-%m-%d").tolist()
    selected_date = columns[1].select_slider("Data da cena", options=labels)
    image_row = season_frames[season_frames["date"].dt.strftime("%Y-%m-%d") == selected_date].iloc[0]

    grid = columns[1].columns(2)
    _metric_card(grid[0], "NDVI médio", f"{image_row['b1_mean']:.3f}", "Vigor no recorte", "forest")
    _metric_card(grid[1], "Vegetação densa", f"{image_row['b1_pct_veg_densa']:.1f}%", "Cobertura verde densa", "olive")
    _metric_card(grid[0], "Solo exposto", f"{image_row['b1_pct_solo']:.1f}%", "Participação de solo", "amber")
    _metric_card(grid[1], "Pixels válidos", f"{int(image_row['b1_valid_pixels'])}", "Base de cálculo", "stone")

    image_path = Path(image_row["image_path"])
    if image_path.exists():
        columns[1].image(str(image_path), caption=f"{row['area_display']} em {selected_date}", use_container_width=True)
    else:
        columns[1].info("A imagem JPG desta data não foi encontrada no pacote atual.")


def render_pests_and_weather(workspace: dict) -> None:
    _section("Pressão biológica e contexto climático", "Clima diário e contagem média de pragas ao longo da safra.")
    pest_daily = workspace["pest_daily"].copy()
    pest_daily["trap_type_display"] = pest_daily["trapType"].map(_display_trap_type)
    weather_daily = workspace["weather_daily"].copy()

    cards = st.columns(3)
    _metric_card(cards[0], "Chuva acumulada", _fmt_optional(weather_daily["precipitation_mm"].sum(), 1, " mm") if not weather_daily.empty else "-", "Somatório diário da estação", "forest")
    tech_mean = pest_daily.loc[pest_daily["trap_type_display"] == "Eletrônica", "avg_pest_count"].mean() if not pest_daily.empty else None
    conventional_mean = pest_daily.loc[pest_daily["trap_type_display"] == "Convencional", "avg_pest_count"].mean() if not pest_daily.empty else None
    _metric_card(cards[1], "Média eletrônica", _fmt_optional(tech_mean, 1), "Contagem média em armadilhas 4.0", "olive")
    _metric_card(cards[2], "Média convencional", _fmt_optional(conventional_mean, 1), "Contagem média em armadilhas convencionais", "amber")

    charts = st.columns(2)
    if not pest_daily.empty:
        fig = px.line(
            pest_daily,
            x="event_date",
            y="avg_pest_count",
            color="trap_type_display",
            markers=True,
            color_discrete_map={"Eletrônica": COLOR_MAP["Tecnologia 4.0"], "Convencional": COLOR_MAP["Convencional"]},
            labels={"event_date": "Data", "avg_pest_count": "Contagem média de pragas", "trap_type_display": "Tipo de armadilha"},
        )
        _style_figure(fig)
        charts[0].plotly_chart(fig, use_container_width=True)
    else:
        charts[0].info("Sem série diária consolidada de pragas.")

    if not weather_daily.empty:
        fig = px.bar(weather_daily, x="date", y="precipitation_mm", color_discrete_sequence=[COLOR_MAP["Tecnologia 4.0"]], labels={"date": "Data", "precipitation_mm": "Precipitação diária (mm)"})
        _style_figure(fig)
        charts[1].plotly_chart(fig, use_container_width=True)
    else:
        charts[1].info("Sem série diária de clima.")


def render_evidence(evidence: pd.DataFrame) -> None:
    _section("Diagnóstico por área", "Narrativas curtas para sustentar hipóteses técnicas a partir do banco analítico.")
    if evidence.empty:
        st.info("Ainda não há consolidação suficiente para gerar evidências por área.")
        return

    for row in evidence.itertuples(index=False):
        st.markdown(
            _evidence_card(
                row.area_display,
                row.treatment_display,
                row.diagnostic_display,
                [
                    ("Produtividade média", _fmt_optional(row.yield_mean_kg_ha, 0, " kg/ha")),
                    ("NDVI médio", _fmt_optional(row.ndvi_mean, 3)),
                    ("Armadilhas 4.0", _fmt_optional(row.electronic_traps, 0)),
                    ("Armadilhas convencionais", _fmt_optional(row.conventional_traps, 0)),
                ],
            ),
            unsafe_allow_html=True,
        )


def render_gaps(gaps: list[str]) -> None:
    _section("Lacunas a resolver", "Pontos que ainda limitam uma prova econômica e agronômica definitiva.")
    columns = st.columns(2)
    for index, item in enumerate(gaps):
        columns[index % 2].markdown(_panel(f"Lacuna {index + 1}", _beautify_text(item)), unsafe_allow_html=True)


def render_database_status(db_state: dict, workspace: dict) -> None:
    _section("Estado do banco local", "Persistência, volume e cobertura atual das tabelas analíticas.")
    cards = st.columns(4)
    _metric_card(cards[0], "Arquivo", "Disponível" if db_state["exists"] else "Ausente", "Status do arquivo DuckDB", "forest" if db_state["exists"] else "stone")
    _metric_card(cards[1], "Tamanho", f"{db_state['size_mb']} MB", "Volume materializado em disco", "olive")
    _metric_card(cards[2], "Origem", _display_source(db_state.get("loaded_from")), "Fonte da sessão atual", "amber")
    _metric_card(cards[3], "Última atualização", _fmt_dt(db_state.get("refreshed_at")), "Carimbo de ingestão", "stone")
    st.markdown(_panel("Arquivo ativo", f"<code>{escape(db_state['db_path'])}</code>"), unsafe_allow_html=True)
    st.dataframe(
        pd.DataFrame(
            [
                {"Tabela": "Solo", "Linhas": workspace["inventory"]["soil_samples"]},
                {"Tabela": "NDVI", "Linhas": workspace["inventory"]["ndvi_images"]},
                {"Tabela": "Armadilhas", "Linhas": workspace["inventory"]["trap_records"]},
                {"Tabela": "Clima", "Linhas": workspace["inventory"]["weather_rows"]},
                {"Tabela": "Plantio", "Linhas": workspace["inventory"]["planting_rows"]},
                {"Tabela": "Colheita", "Linhas": workspace["inventory"]["harvest_rows"]},
            ]
        ),
        hide_index=True,
        use_container_width=True,
    )


def _render_sidebar() -> dict:
    with st.sidebar:
        st.markdown("<div class='sidebar-title'>MonolithFarm</div>", unsafe_allow_html=True)
        st.markdown("<div class='sidebar-copy'>Organize os dados, atualize o banco local e navegue pelas comparações entre áreas com foco em NDVI.</div>", unsafe_allow_html=True)
        data_dir = st.text_input("Pasta dos dados", value=str(DEFAULT_DATA_DIR))
        db_path = st.text_input("Banco local", value=str(DEFAULT_DB_PATH))
        refresh_db = st.button("Atualizar banco agora", use_container_width=True)
        st.markdown("<div class='sidebar-subtitle'>Entradas opcionais</div>", unsafe_allow_html=True)
        mapping_upload = st.file_uploader("Mapeamento manual das áreas (CSV)", type="csv")
        cost_upload = st.file_uploader("Custos por hectare (CSV)", type="csv")
        st.markdown(
            (
                "<div class='sidebar-note'>"
                f"<strong>Templates esperados</strong><br>mapeamento: <code>{', '.join(SEASON_MAPPING_COLUMNS)}</code><br>"
                f"custos: <code>{', '.join(COST_INPUT_COLUMNS)}</code></div>"
            ),
            unsafe_allow_html=True,
        )
    return {"data_dir": data_dir, "db_path": db_path, "refresh_db": refresh_db, "mapping_upload": mapping_upload, "cost_upload": cost_upload}


def _render_hero(workspace: dict, db_state: dict, evidence: pd.DataFrame) -> None:
    top_row = evidence.dropna(subset=["yield_mean_kg_ha"]).head(1)
    top_area = "Ainda sem produtividade consolidada"
    top_detail = "Atualize o banco ou carregue mais dados para completar o panorama."
    if not top_row.empty:
        row = top_row.iloc[0]
        top_area = row["area_display"]
        top_detail = f"Lidera a produtividade média com {_fmt_number(row['yield_mean_kg_ha'])} kg/ha."
    st.markdown(
        (
            "<div class='hero-shell'>"
            "<div class='hero-kicker'>NDVI · Ciência de Dados · Operação Agrícola</div>"
            "<h1>Painel Analítico da Safra de Milho</h1>"
            "<p>Uma leitura visual integrada para explicar diferenças de vigor, produtividade e monitoramento entre áreas convencionais e áreas com tecnologia 4.0.</p>"
            "<div class='hero-grid'>"
            f"<div class='hero-chip'><span>Banco</span><strong>{escape(_display_source(db_state.get('loaded_from')))}</strong></div>"
            f"<div class='hero-chip'><span>Área em destaque</span><strong>{escape(top_area)}</strong></div>"
            f"<div class='hero-chip'><span>Resumo</span><strong>{escape(top_detail)}</strong></div>"
            "</div></div>"
        ),
        unsafe_allow_html=True,
    )


def _prepare_season_mapping(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    area_source = "area_label" if "area_label" in prepared.columns else "suggested_area_label"
    treatment_source = "treatment" if "treatment" in prepared.columns else "suggested_treatment"
    prepared["area_display"] = prepared[area_source].map(_display_area_label)
    prepared["treatment_display"] = prepared[treatment_source].map(_display_treatment)
    prepared["plot_display"] = prepared["suggested_plot"].map(_display_plot)
    return prepared


def _prepare_evidence(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    if "area_display" not in prepared.columns:
        prepared["area_display"] = pd.NA
    if "treatment_display" not in prepared.columns:
        prepared["treatment_display"] = pd.NA

    area_source = "area_label" if "area_label" in prepared.columns else "suggested_area_label"
    treatment_source = "treatment" if "treatment" in prepared.columns else "suggested_treatment"

    if area_source in prepared.columns:
        prepared["area_display"] = prepared["area_display"].fillna(prepared[area_source].map(_display_area_label))
    else:
        prepared["area_display"] = prepared["area_display"].fillna(prepared["season_id"].map(lambda value: _display_area_label(f"Área {str(value)[:8]}")))

    if treatment_source in prepared.columns:
        prepared["treatment_display"] = prepared["treatment_display"].fillna(prepared[treatment_source].map(_display_treatment))

    prepared["diagnostic_display"] = prepared["diagnostic"].map(_beautify_text)
    return prepared.sort_values(["yield_mean_kg_ha", "ndvi_mean"], ascending=False, na_position="last").reset_index(drop=True)


def _prepare_cost_summary(cost_summary: pd.DataFrame | None, evidence: pd.DataFrame) -> pd.DataFrame | None:
    if cost_summary is None or cost_summary.empty:
        return None
    return cost_summary.merge(evidence[["season_id", "area_display", "treatment_display"]].drop_duplicates(), on="season_id", how="left")


def _section(title: str, description: str) -> None:
    st.markdown(f"<div class='section-title'><h2>{escape(title)}</h2><p>{escape(description)}</p></div>", unsafe_allow_html=True)


def _metric_card(column, label: str, value: str, detail: str, tone: str) -> None:
    column.markdown(
        f"<div class='metric-card {tone}'><div class='metric-label'>{escape(label)}</div><div class='metric-value'>{escape(value)}</div><div class='metric-detail'>{escape(detail)}</div></div>",
        unsafe_allow_html=True,
    )


def _insight_card(column, label: str, headline: str) -> None:
    column.markdown(f"<div class='insight-card'><div class='insight-label'>{escape(label)}</div><div class='insight-headline'>{escape(headline)}</div></div>", unsafe_allow_html=True)


def _headline(frame: pd.DataFrame, metric: str, suffix: str, precision: int = 0) -> str:
    if frame.empty:
        return "Sem dados suficientes"
    row = frame.iloc[0]
    return f"{row['area_display']} · {_fmt_optional(row[metric], precision, suffix)}"


def _panel(title: str, body: str) -> str:
    return f"<div class='section-panel'><div class='panel-title'>{escape(title)}</div><div class='panel-body'>{body}</div></div>"


def _evidence_card(title: str, treatment: str, diagnostic: str, metrics: list[tuple[str, str]]) -> str:
    items = "".join(f"<div class='evidence-metric'><span>{escape(label)}</span><strong>{escape(value)}</strong></div>" for label, value in metrics)
    return f"<div class='evidence-card'><div class='evidence-top'><h3>{escape(title)}</h3><span class='badge'>{escape(treatment)}</span></div><p>{escape(diagnostic)}</p><div class='evidence-grid'>{items}</div></div>"


def _duel_card(title: str, metrics: list[tuple[str, str]]) -> str:
    items = "".join(f"<div class='duel-stat'><span>{escape(label)}</span><strong>{escape(value)}</strong></div>" for label, value in metrics)
    return f"<div class='duel-card'><div class='duel-kicker'>Comparativo de tratamento</div><h3>{escape(title)}</h3><div class='duel-grid'>{items}</div></div>"


def _style_figure(fig, show_legend: bool = True) -> None:
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,250,243,0.92)",
        font={"family": "Trebuchet MS", "size": 14, "color": "#203126"},
        margin={"l": 16, "r": 16, "t": 24, "b": 16},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0},
        showlegend=show_legend,
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(38,58,45,0.08)", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(38,58,45,0.08)", zeroline=False)


def _display_area_label(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "Área sem nome"
    text = str(value).strip()
    if text.startswith("area_"):
        text = f"Área {text.split('_', 1)[1]}"
    return _beautify_text(text)


def _display_treatment(value: object) -> str:
    mapping = {"convencional": "Convencional", "tecnologia_4_0": "Tecnologia 4.0", "indefinido": "Indefinido"}
    return mapping.get(str(value), _beautify_text(str(value)))


def _display_plot(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "Sem talhão"
    text = str(value)
    if text in {"sem_plot", "sem_talhao", "sem_talhão"}:
        return "Sem talhão"
    if text.replace(".0", "").isdigit():
        return f"Talhão {text.replace('.0', '')}"
    return _beautify_text(text)


def _display_trap_type(value: object) -> str:
    return {"ELECTRONIC": "Eletrônica", "CONVENTIONAL": "Convencional"}.get(str(value), _beautify_text(str(value)))


def _display_source(value: object) -> str:
    return {"duckdb": "Banco local", "raw_files": "Arquivos brutos"}.get(str(value), "Desconhecida")


def _beautify_text(text: object) -> str:
    result = str(text) if text is not None else "-"
    for source, target in TEXT_REPLACEMENTS.items():
        result = result.replace(source, target)
    return result


def _fmt_dt(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    return pd.Timestamp(value).strftime("%d/%m/%Y %H:%M")


def _fmt_number(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):,.0f}".replace(",", ".")


def _fmt_optional(value, precision: int = 0, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return "-"
    numeric = float(value)
    if precision == 0:
        base = _fmt_number(numeric)
    else:
        base = f"{numeric:,.{precision}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{base}{suffix}"


def _read_uploaded_csv(uploaded_file) -> pd.DataFrame | None:
    if uploaded_file is None:
        return None
    return pd.read_csv(StringIO(uploaded_file.getvalue().decode("utf-8")))


def _inject_theme() -> None:
    css_path = Path(__file__).parent / ".streamlit" / "custom.css"
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
