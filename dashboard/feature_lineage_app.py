from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.lineage.column_catalog import (
    build_feature_catalog,
    build_raw_column_catalog,
    build_workspace_column_catalog,
)
from dashboard.lineage.column_lineage import (
    build_column_lineage_index,
    build_lineage_coverage_report,
    lineage_detail_from_row,
    raw_columns_for_feature,
    thresholds_for_feature,
)
from dashboard.lineage.data_profiler import dataframe_overview, profile_dataframe
from dashboard.lineage.doc_scraper import (
    build_documentation_index,
    documentation_for_source_group,
    load_or_refresh_documentation_cache,
)
from dashboard.lineage.docs_registry import DRIVER_DOCUMENTATION, driver_documentation_rows
from dashboard.lineage.lineage_graph import (
    column_flow_dot,
    csv_flow_dot,
    driver_flow_dot,
    feature_flow_dot,
    pipeline_overview_dot,
)
from dashboard.lineage.quality_rules import run_quality_rules
from dashboard.lineage.registry import (
    CHART_REGISTRY,
    CSV_LINEAGE_ORDER,
    CSV_REGISTRY,
    FEATURE_REGISTRY,
    HYPOTHESIS_REGISTRY,
    INTERMEDIATE_TABLE_ORDER,
    INTERMEDIATE_TABLE_REGISTRY,
    KEY_COLUMNS,
)
from dashboard.lineage.runtime import (
    build_data_quality_checks,
    build_raw_file_catalog,
    build_workspace_and_outputs,
    get_function_source,
    load_output_csvs,
    load_raw_preview,
    load_resolved_paths,
)
from dashboard.lineage.ui import coverage_note, hero, info_card, inject_global_css, pill_row, section_header


pd.set_option("display.max_colwidth", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

PAIR_COLOR_MAP = {"grao": "#C46B2A", "silagem": "#2F6F4F"}
TREATMENT_DASH_MAP = {"tecnologia_4_0": "solid", "convencional": "dash"}


st.set_page_config(
    page_title="MonolithFarm | Auditoria e Lineage NDVI",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _inject_audit_css() -> None:
    inject_global_css()


@st.cache_resource(show_spinner="Carregando pipeline NDVI completo...")
def _load_workspace_bundle(data_dir: str, output_dir: str) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    return build_workspace_and_outputs(Path(data_dir), Path(output_dir), persist_outputs=True)


@st.cache_data(show_spinner=False)
def _load_outputs_only(output_dir: str) -> dict[str, pd.DataFrame]:
    return load_output_csvs(Path(output_dir))


@st.cache_data(show_spinner=False)
def _load_raw_catalog(data_dir: str) -> tuple[Any, pd.DataFrame]:
    return build_raw_file_catalog(Path(data_dir))


@st.cache_data(show_spinner="Lendo cache de documentação FarmLab...")
def _load_documentation_cache(force: bool = False) -> dict[str, Any]:
    return load_or_refresh_documentation_cache(force=force)


@st.cache_data(show_spinner="Catalogando colunas brutas...")
def _load_raw_column_catalog_cached(data_dir: str, docs_generated_at: str | None) -> pd.DataFrame:
    _, raw_catalog = build_raw_file_catalog(Path(data_dir))
    docs_cache = load_or_refresh_documentation_cache(force=False)
    return build_raw_column_catalog(raw_catalog, load_raw_preview, docs_cache)


def main() -> None:
    _inject_audit_css()
    paths = load_resolved_paths()
    page = _render_sidebar(paths)
    raw_paths, raw_catalog = _load_raw_catalog(str(paths.data_dir))
    docs_cache = _load_documentation_cache(False)
    doc_index = build_documentation_index(docs_cache)

    mode = st.session_state.get("lineage_mode", "workspace")
    workspace: dict[str, Any] | None = None
    outputs: dict[str, pd.DataFrame]
    if mode == "outputs_only":
        outputs = _load_outputs_only(str(paths.output_dir))
    else:
        workspace, outputs = _load_workspace_bundle(str(paths.data_dir), str(paths.output_dir))

    quality = build_data_quality_checks(workspace or {}, raw_catalog)
    quality_summary, quality_examples = run_quality_rules(workspace, outputs)

    if page == "Home":
        render_home(paths, raw_catalog, workspace, outputs, quality, quality_summary, doc_index)
    elif page == "Auditoria de cobertura":
        render_audit_coverage(paths, raw_catalog, workspace, outputs, quality_summary, docs_cache)
    elif page == "Catálogo de fontes brutas":
        render_raw_files(raw_catalog, docs_cache)
    elif page == "Dicionário de colunas brutas":
        render_raw_column_dictionary(paths, raw_catalog, docs_cache)
    elif page == "Rastreabilidade de coluna":
        render_column_lineage(paths, raw_catalog, workspace, outputs, docs_cache)
    elif page == "Tabelas intermediárias":
        render_intermediate_tables(workspace, outputs)
    elif page == "Catálogo de features":
        render_feature_explorer(workspace, outputs)
    elif page == "Explorador de drivers":
        render_driver_explorer(workspace, outputs)
    elif page == "CSVs finais":
        render_final_csvs(outputs, workspace, paths, docs_cache)
    elif page == "Rastreio por linha / semana / área":
        render_row_trace(workspace, outputs)
    elif page == "Hipóteses e evidências":
        render_hypotheses(workspace, outputs)
    elif page == "Gráficos":
        render_charts(workspace, outputs)
    elif page == "Qualidade dos dados":
        render_data_quality(quality_summary, quality_examples)
    elif page == "Documentação FarmLab":
        render_documentation_explorer(docs_cache, doc_index)


def _render_sidebar(paths) -> str:
    st.sidebar.title("Auditoria NDVI")
    st.sidebar.caption("Nova camada de inspeção e rastreabilidade focada em NDVI.")
    st.session_state["lineage_mode"] = st.sidebar.radio(
        "Modo de carregamento",
        options=["workspace", "outputs_only"],
        index=1,
        format_func=lambda value: "Workspace completo" if value == "workspace" else "Somente outputs prontos",
        help="Workspace completo habilita rastreio de intermediárias e linhas. Outputs prontos acelera a abertura, mas limita a auditoria.",
    )
    st.sidebar.markdown("### Contexto")
    st.sidebar.code(
        f"Perfil: {paths.profile_name}\n"
        f"Projeto: {paths.project_dir}\n"
        f"Dados: {paths.data_dir}\n"
        f"Outputs: {paths.output_dir}",
        language="text",
    )
    if st.sidebar.button("Limpar cache e recarregar"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    return st.sidebar.radio(
        "Páginas",
        options=[
            "Home",
            "Auditoria de cobertura",
            "Catálogo de fontes brutas",
            "Dicionário de colunas brutas",
            "Rastreabilidade de coluna",
            "Tabelas intermediárias",
            "Catálogo de features",
            "Explorador de drivers",
            "CSVs finais",
            "Rastreio por linha / semana / área",
            "Hipóteses e evidências",
            "Gráficos",
            "Qualidade dos dados",
            "Documentação FarmLab",
        ],
    )


def render_home(
    paths,
    raw_catalog: pd.DataFrame,
    workspace: dict[str, Any] | None,
    outputs: dict[str, pd.DataFrame],
    quality: dict[str, pd.DataFrame],
    quality_summary: pd.DataFrame,
    doc_index: pd.DataFrame,
) -> None:
    hero(
        "Auditoria e rastreabilidade NDVI",
        "Explorador técnico para responder o que é cada dado, de onde veio, como foi calculado, onde foi usado e quais hipóteses ou gráficos dependem dele.",
    )

    cols = st.columns(6)
    cols[0].metric("Arquivos brutos", int((raw_catalog["kind"] == "file").sum()))
    cols[1].metric("Tabelas intermediárias", len(INTERMEDIATE_TABLE_ORDER))
    cols[2].metric("CSVs finais", len(outputs))
    cols[3].metric("Features rastreadas", len(FEATURE_REGISTRY))
    cols[4].metric("Drivers documentados", len(DRIVER_DOCUMENTATION))
    cols[5].metric("Docs indexados", len(doc_index))

    final_columns = sum(len(frame.columns) for frame in outputs.values())
    mapped_features = len(FEATURE_REGISTRY)
    st.caption(
        f"Escopo carregado: {final_columns} colunas em CSVs finais/intermediários exportados, "
        f"{mapped_features} features principais, {len(DRIVER_DOCUMENTATION)} drivers e "
        "lineage programática com fallback manual auditável."
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        info_card("1. Origem", "Abra fontes brutas e dicionário para ver arquivo, coluna, exemplos reais, documentação e uso no pipeline.")
    with c2:
        info_card("2. Transformação", "Use coluna, feature ou tabela intermediária para ver função Python, filtros, joins, agregações e thresholds.")
    with c3:
        info_card("3. Evidência", "Conecte a feature aos CSVs finais, gráficos, hipóteses H1-H4, drivers e linha auditada.")

    with st.expander("Checklist de auditoria implementado", expanded=False):
        st.markdown(
            """
            - **Cobertura:** use `Auditoria de cobertura` para ver se colunas finais, features e brutos estão rastreados.
            - **Rastreabilidade de coluna:** use `Rastreabilidade de coluna` para ir de coluna final para bruto, função, filtro, agregação, gráficos e hipóteses.
            - **Rastreabilidade de feature:** use `Catálogo de features` para ver raw columns resolvidas, código, thresholds, distribuição e CSVs dependentes.
            - **Rastreabilidade de linha:** use `Rastreio por linha / semana / área` para conectar linha final, timeline, registros diários, bruto NDVI e JPG.
            - **Qualidade:** use `Qualidade dos dados` para linhas com valid pixels zero, faixas físicas e inconsistências estruturais.
            - **Documentação externa:** use `Documentação FarmLab` para rotas, trechos extraídos da SPA e fallback consolidado.
            """
        )

    section_header("Pipeline resumido", "Leitura de dados brutos, criação de intermediárias, feature store semanal, drivers, CSVs finais e hipóteses.")
    dot = pipeline_overview_dot()
    try:
        st.graphviz_chart(dot)
    except Exception:
        st.code(dot, language="dot")

    section_header("Checks rápidos de qualidade", "Alertas que indicam se vale abrir a página de qualidade antes de interpretar hipóteses.", accent="#b45309")
    if quality_summary.empty:
        st.info("Sem regras de qualidade disponíveis para o modo atual.")
    else:
        problem_count = int(quality_summary["affected_rows"].gt(0).sum())
        st.metric("Regras com atenção", problem_count)
        st.dataframe(
            quality_summary[["domain", "severity", "dataset", "affected_rows", "rule", "impact"]].head(12),
            hide_index=True,
            use_container_width=True,
        )

    with st.expander("Checks legados resumidos", expanded=False):
        qcols = st.columns(3)
        for idx, key in enumerate(["ndvi", "weather", "operations"]):
            qcols[idx].markdown(f"**{key}**")
            if quality[key].empty:
                qcols[idx].info("Sem checks disponíveis.")
            else:
                qcols[idx].dataframe(quality[key], hide_index=True, use_container_width=True)

    if workspace is not None:
        st.subheader("Notebook principal")
        notebook_path = paths.project_dir / "notebooks" / "complete_ndvi_analysis.ipynb"
        executed_path = paths.project_dir / "notebooks" / "executado" / "complete_ndvi_analysis.ipynb"
        st.code(
            f"Principal: {notebook_path}\nExecutado: {executed_path}\nOutputs atuais: {paths.output_dir}",
            language="text",
        )


def render_audit_coverage(
    paths,
    raw_catalog: pd.DataFrame,
    workspace: dict[str, Any] | None,
    outputs: dict[str, pd.DataFrame],
    quality_summary: pd.DataFrame,
    docs_cache: dict[str, Any],
) -> None:
    hero(
        "Auditoria de cobertura",
        "Painel para verificar se a interface realmente cobre brutos, features, drivers, colunas finais, documentação e dados reais.",
        eyebrow="Controle de qualidade da rastreabilidade",
    )
    raw_columns = _load_raw_column_catalog_cached(str(paths.data_dir), docs_cache.get("generated_at"))
    feature_catalog = build_feature_catalog()
    lineage_index = build_column_lineage_index(raw_columns, workspace, outputs)
    coverage = build_lineage_coverage_report(lineage_index, outputs)

    final_records = lineage_index[lineage_index["layer"] == "csv_final"] if not lineage_index.empty else pd.DataFrame()
    strong_statuses = {"mapeado", "mapeado_por_feature", "mapeado_por_driver", "mapeado_por_driver_dinamico"}
    strong_final_pct = (
        float(final_records["mapping_status"].isin(strong_statuses).mean()) if not final_records.empty else 0.0
    )
    auditable_final_pct = float(final_records["mapping_status"].ne("nao_catalogado").mean()) if not final_records.empty else 0.0
    raw_used_pct = float(raw_columns["usage_status"].eq("usada").mean()) if not raw_columns.empty else 0.0
    feature_with_raw_pct = float(feature_catalog["raw_columns_resolved"].astype(str).str.len().gt(0).mean()) if not feature_catalog.empty else 0.0
    quality_attention = int(quality_summary["affected_rows"].gt(0).sum()) if not quality_summary.empty else 0

    cols = st.columns(6)
    cols[0].metric("Registros lineage", len(lineage_index))
    cols[1].metric("Colunas brutas", len(raw_columns))
    cols[2].metric("Features com bruto", f"{feature_with_raw_pct:.0%}")
    cols[3].metric("Finais auditáveis", f"{auditable_final_pct:.0%}")
    cols[4].metric("Brutas usadas", f"{raw_used_pct:.0%}")
    cols[5].metric("Regras com atenção", quality_attention)

    coverage_note()
    st.caption(
        "A meta prática é que toda coluna apareça em algum registro auditável. O app separa a confiança: "
        f"{strong_final_pct:.0%} das colunas finais têm mapeamento forte por feature/driver; as demais ficam como parcial quando são texto, metadado, estatística auxiliar ou CSV exportado sem registry manual específico."
    )

    tabs = st.tabs(["Resumo", "Features", "Colunas finais", "Colunas brutas", "Critérios 100%"])
    with tabs[0]:
        left, right = st.columns([1.1, 0.9])
        with left:
            section_header("Cobertura por CSV final", "Cada CSV é avaliado coluna por coluna contra o índice de lineage.")
            st.dataframe(coverage, hide_index=True, use_container_width=True)
            if not coverage.empty:
                fig = px.bar(
                    coverage.sort_values("coverage_pct"),
                    x="coverage_pct",
                    y="csv",
                    orientation="h",
                    range_x=[0, 1],
                    title="Percentual de colunas com registro de lineage",
                    color="coverage_pct",
                    color_continuous_scale=["#fee2e2", "#fef3c7", "#dcfce7"],
                )
                fig.update_layout(height=max(420, 32 * len(coverage)))
                st.plotly_chart(fig, use_container_width=True)
        with right:
            section_header("Status do mapeamento", "Distribuição dos níveis de confiança dos registros.")
            if not lineage_index.empty:
                status_counts = lineage_index["mapping_status"].value_counts(dropna=False).reset_index()
                status_counts.columns = ["mapping_status", "records"]
                st.dataframe(status_counts, hide_index=True, use_container_width=True)
                st.plotly_chart(px.pie(status_counts, names="mapping_status", values="records", hole=0.48), use_container_width=True)
            section_header("Documentação FarmLab/cache", "Fonte externa e fallback local usados na interface.", accent="#2f6f4f")
            st.json(
                {
                    "generated_at": docs_cache.get("generated_at"),
                    "routes": len(docs_cache.get("routes", [])),
                    "bundle_records": sum(len(bundle.get("snippets", [])) for bundle in docs_cache.get("bundles", [])),
                    "manual_sources": len(docs_cache.get("manual_sources", {})),
                }
            )

    with tabs[1]:
        section_header("Cobertura de features", "As features principais precisam mostrar definição, bruto resolvido, função, filtros, CSVs, gráficos e hipóteses.")
        feature_audit = feature_catalog.copy()
        feature_audit["has_raw_columns"] = feature_audit["raw_columns_resolved"].astype(str).str.len().gt(0)
        feature_audit["has_sources"] = feature_audit["raw_sources"].astype(str).str.len().gt(0)
        feature_audit["has_transformation"] = feature_audit["transformation"].astype(str).str.len().gt(0)
        feature_audit["has_downstream"] = feature_audit["appears_in_tables"].astype(str).str.len().gt(0) | feature_audit["appears_in_csvs"].astype(str).str.len().gt(0)
        feature_audit["has_chart_or_hypothesis"] = feature_audit["hypotheses"].astype(str).str.len().gt(0) | feature_audit["charts"].astype(str).str.len().gt(0)
        st.dataframe(feature_audit, hide_index=True, use_container_width=True)
        weak = feature_audit[
            ~(feature_audit[["has_raw_columns", "has_sources", "has_transformation", "has_downstream"]].all(axis=1))
        ]
        if weak.empty:
            st.success("Todas as features catalogadas possuem origem, fonte, transformação e downstream auditável.")
        else:
            st.warning("Há features com algum campo técnico incompleto.")
            st.dataframe(weak, hide_index=True, use_container_width=True)

    with tabs[2]:
        section_header("Colunas finais", "Use esta tabela para encontrar rapidamente colunas finais fortes, parciais ou inferidas.")
        if final_records.empty:
            st.info("Sem registros finais no modo atual.")
        else:
            status_filter = st.multiselect(
                "Filtrar por status",
                sorted(final_records["mapping_status"].dropna().unique().tolist()),
                key="coverage_final_status",
            )
            final_view = final_records.copy()
            if status_filter:
                final_view = final_view[final_view["mapping_status"].isin(status_filter)]
            st.dataframe(
                final_view[
                    [
                        "table",
                        "column",
                        "definition",
                        "raw_columns",
                        "upstream_columns",
                        "generated_by",
                        "mapping_status",
                        "mapping_confidence",
                        "limitations",
                    ]
                ],
                hide_index=True,
                use_container_width=True,
            )

    with tabs[3]:
        section_header("Colunas brutas", "Mostra o que é usado diretamente, usado como chave, ignorado pelo modelo atual ou apenas contexto.")
        if raw_columns.empty:
            st.info("Sem catálogo bruto.")
        else:
            usage = raw_columns["usage_status"].value_counts(dropna=False).reset_index()
            usage.columns = ["usage_status", "columns"]
            st.dataframe(usage, hide_index=True, use_container_width=True)
            st.plotly_chart(px.bar(usage, x="usage_status", y="columns", title="Uso das colunas brutas"), use_container_width=True)
            st.write("Colunas de contexto ou ignoradas que continuam visíveis para auditoria")
            st.dataframe(
                raw_columns[raw_columns["usage_status"].ne("usada")][
                    ["source_key", "source_group", "column", "documentation", "pipeline_usage", "usage_status", "farm_docs_url"]
                ].head(400),
                hide_index=True,
                use_container_width=True,
            )

    with tabs[4]:
        section_header("Critérios de aceitação", "Checklist operacional para saber se a interface está pronta para auditoria humana.")
        criteria = [
            {
                "criterion": "A. Rastreabilidade de coluna",
                "status": "ok" if not lineage_index.empty and not final_records.empty else "falha",
                "evidence": f"{len(lineage_index)} registros de lineage; {len(final_records)} colunas finais rastreadas.",
            },
            {
                "criterion": "B. Rastreabilidade de feature",
                "status": "ok" if feature_with_raw_pct >= 1.0 else "atenção",
                "evidence": f"{feature_with_raw_pct:.0%} das features possuem colunas brutas resolvidas.",
            },
            {
                "criterion": "C. Rastreabilidade de linha",
                "status": "ok" if workspace is not None else "limitado",
                "evidence": "Modo workspace completo carregado." if workspace is not None else "Modo outputs_only não carrega intermediárias para linha.",
            },
            {
                "criterion": "D. Dados reais visíveis",
                "status": "ok" if len(raw_catalog) and len(outputs) else "falha",
                "evidence": f"{len(raw_catalog)} fontes brutas catalogadas e {len(outputs)} CSVs finais carregados.",
            },
            {
                "criterion": "E. Qualidade de dados",
                "status": "ok" if not quality_summary.empty else "limitado",
                "evidence": f"{len(quality_summary)} regras calculadas; {quality_attention} com linhas afetadas.",
            },
        ]
        st.dataframe(pd.DataFrame(criteria), hide_index=True, use_container_width=True)
        st.markdown("**Interpretação dos status**")
        pill_row(["ok = atende", "atenção = auditável, mas com parte parcial", "limitado = depende do modo ou da fonte", "falha = precisa corrigir"], status="mapeado")


def render_raw_files(raw_catalog: pd.DataFrame, docs_cache: dict[str, Any]) -> None:
    st.title("Catálogo de fontes brutas")
    st.write(
        "Esta tela mostra os arquivos reais detectados em `data/`, agrupados por fonte, com preview, colunas, cobertura temporal "
        "e documentação vinculada quando encontrada."
    )
    query = st.text_input("Buscar por nome de arquivo, coluna ou fonte")
    catalog = raw_catalog.copy()
    if query:
        q = query.lower()
        catalog = catalog[
            catalog["source_key"].str.lower().str.contains(q)
            | catalog["description"].str.lower().str.contains(q)
            | catalog["source_group"].str.lower().str.contains(q)
            | catalog["column_names"].astype(str).str.lower().str.contains(q)
            | catalog["path"].str.lower().str.contains(q)
        ]

    for group_name, group in catalog.groupby("source_group", sort=False):
        st.subheader(group_name)
        source_doc = documentation_for_source_group(group_name, docs_cache)
        with st.expander(f"Documentação vinculada: {group_name}", expanded=False):
            st.write(source_doc.get("summary", ""))
            st.write(source_doc.get("practical_context", ""))
            st.write(f"**Status:** {source_doc.get('documentation_status')}")
            if source_doc.get("farm_docs_url"):
                st.link_button("Abrir documentação FarmLab", source_doc["farm_docs_url"])
            if source_doc.get("relevant_excerpt"):
                st.info(source_doc["relevant_excerpt"])
        for row in group.itertuples(index=False):
            with st.expander(f"{row.source_key} — {row.description}", expanded=False):
                st.code(str(row.path), language="text")
                meta_cols = st.columns(4)
                meta_cols[0].metric("Tipo", row.kind)
                meta_cols[1].metric("Linhas", "-" if pd.isna(row.rows) else int(row.rows))
                meta_cols[2].metric("Colunas", "-" if pd.isna(row.columns) else int(row.columns))
                meta_cols[3].metric("Arquivos", "-" if pd.isna(row.file_count) else int(row.file_count))
                st.write(
                    {
                        "temporal_min": row.temporal_min,
                        "temporal_max": row.temporal_max,
                        "row_count_method": getattr(row, "row_count_method", "desconhecido"),
                    }
                )
                if row.column_names:
                    st.write("Colunas")
                    st.dataframe(pd.DataFrame({"column": row.column_names}), hide_index=True, use_container_width=True)
                preview = load_raw_preview(Path(row.path), rows=50)
                if not preview.empty:
                    st.write("Preview real")
                    st.dataframe(preview, hide_index=True, use_container_width=True)


def render_raw_column_dictionary(paths, raw_catalog: pd.DataFrame, docs_cache: dict[str, Any]) -> None:
    st.title("Dicionário de colunas brutas")
    st.write(
        "Cada linha representa uma coluna real encontrada nos CSVs brutos. O status `usada` vem da linhagem de features; "
        "`ignorada_no_modelo_atual` significa que a coluna existe, mas não participa da lógica analítica oficial."
    )
    catalog = _load_raw_column_catalog_cached(str(paths.data_dir), docs_cache.get("generated_at"))
    if catalog.empty:
        st.warning("Nenhuma coluna bruta catalogada.")
        return

    search = st.text_input("Buscar coluna, arquivo, fonte, documentação ou uso")
    source_filter = st.multiselect("Fonte", sorted(catalog["source_group"].dropna().unique().tolist()))
    usage_filter = st.multiselect("Status de uso", sorted(catalog["usage_status"].dropna().unique().tolist()))
    filtered = catalog.copy()
    if search:
        q = search.lower()
        text = filtered.astype(str).agg(" ".join, axis=1).str.lower()
        filtered = filtered[text.str.contains(q, na=False)]
    if source_filter:
        filtered = filtered[filtered["source_group"].isin(source_filter)]
    if usage_filter:
        filtered = filtered[filtered["usage_status"].isin(usage_filter)]

    st.metric("Colunas encontradas", len(filtered))
    st.dataframe(filtered, hide_index=True, use_container_width=True)
    if filtered.empty:
        st.info("Nenhuma coluna encontrada com os filtros atuais.")
        return

    selected_column = st.selectbox("Abrir detalhe de coluna", filtered["column"].dropna().unique().tolist())
    if selected_column:
        detail = filtered[filtered["column"] == selected_column]
        st.subheader(selected_column)
        st.dataframe(detail, hide_index=True, use_container_width=True)
        first = detail.iloc[0]
        st.markdown("**Interpretação prática**")
        st.write(first.get("practical_interpretation", ""))
        st.markdown("**Uso no pipeline**")
        st.write(first.get("pipeline_usage", ""))


def render_column_lineage(
    paths,
    raw_catalog: pd.DataFrame,
    workspace: dict[str, Any] | None,
    outputs: dict[str, pd.DataFrame],
    docs_cache: dict[str, Any],
) -> None:
    st.title("Rastreabilidade de coluna")
    st.write(
        "Esta é a visão mais direta para auditoria: escolha uma coluna bruta, intermediária, feature ou coluna final "
        "e veja upstream bruto, transformação, função, filtros, agregações, downstream, gráficos e hipóteses."
    )
    raw_columns = _load_raw_column_catalog_cached(str(paths.data_dir), docs_cache.get("generated_at"))
    lineage_index = build_column_lineage_index(raw_columns, workspace, outputs)
    coverage = build_lineage_coverage_report(lineage_index, outputs)

    if lineage_index.empty:
        st.warning("Nenhuma lineage de coluna foi construída.")
        return

    cols = st.columns(5)
    cols[0].metric("Registros de lineage", len(lineage_index))
    cols[1].metric("Colunas brutas", int((lineage_index["layer"] == "bruto").sum()))
    cols[2].metric("Features", int((lineage_index["layer"] == "feature").sum()))
    cols[3].metric("Colunas finais", int((lineage_index["layer"] == "csv_final").sum()))
    final_records = lineage_index[lineage_index["layer"] == "csv_final"]
    strong = final_records["mapping_status"].isin(["mapeado", "mapeado_por_feature", "mapeado_por_driver", "mapeado_por_driver_dinamico"])
    cols[4].metric("Finais com mapeamento forte", f"{strong.mean():.0%}" if not final_records.empty else "0%")

    tab_explorer, tab_coverage, tab_matrix = st.tabs(["Explorar coluna", "Cobertura por CSV", "Matriz completa"])
    with tab_explorer:
        layer_filter = st.multiselect("Camada", sorted(lineage_index["layer"].unique().tolist()))
        status_filter = st.multiselect("Status do mapeamento", sorted(lineage_index["mapping_status"].unique().tolist()))
        search = st.text_input("Buscar coluna, tabela, bruto, função, hipótese ou gráfico", key="column_lineage_search")
        filtered = lineage_index.copy()
        if layer_filter:
            filtered = filtered[filtered["layer"].isin(layer_filter)]
        if status_filter:
            filtered = filtered[filtered["mapping_status"].isin(status_filter)]
        if search:
            q = search.lower()
            filtered = filtered[filtered.astype(str).agg(" ".join, axis=1).str.lower().str.contains(q, na=False)]

        st.dataframe(
            filtered[
                [
                    "lineage_id",
                    "layer",
                    "table",
                    "column",
                    "raw_columns",
                    "generated_by",
                    "downstream_csvs",
                    "charts",
                    "hypotheses",
                    "mapping_status",
                ]
            ],
            hide_index=True,
            use_container_width=True,
        )
        if filtered.empty:
            st.info("Nenhum registro encontrado com os filtros atuais.")
            return

        selected_id = st.selectbox(
            "Abrir lineage",
            filtered["lineage_id"].tolist(),
            format_func=lambda value: _format_lineage_option(filtered, value),
        )
        selected_row = filtered[filtered["lineage_id"] == selected_id].iloc[0]
        record = lineage_detail_from_row(selected_row)
        _render_column_lineage_detail(record, raw_catalog, workspace, outputs)

    with tab_coverage:
        st.write(
            "Cobertura indica se cada coluna final tem ao menos um registro de lineage. "
            "Status forte/parcial ainda aparece no detalhe da coluna; algumas colunas finais são metadados ou mensagens de decisão."
        )
        st.dataframe(coverage, hide_index=True, use_container_width=True)
        if not coverage.empty:
            st.plotly_chart(px.bar(coverage, x="csv", y="coverage_pct", title="Cobertura de lineage por CSV final"), use_container_width=True)

    with tab_matrix:
        st.dataframe(lineage_index, hide_index=True, use_container_width=True)


def _render_column_lineage_detail(record, raw_catalog: pd.DataFrame, workspace: dict[str, Any] | None, outputs: dict[str, pd.DataFrame]) -> None:
    st.subheader(f"{record.column} em {record.table}")
    st.markdown(
        "".join(
            [
                f'<span class="audit-chip {("raw" if record.layer == "bruto" else "intermediate" if record.layer in {"feature", "intermediario"} else "final")}">{record.layer}</span>',
                f'<span class="audit-chip">{record.mapping_status}</span>',
                f'<span class="audit-chip">{record.mapping_confidence}</span>',
            ]
        ),
        unsafe_allow_html=True,
    )
    left, right = st.columns([1.15, 0.85])
    with left:
        st.markdown("**Definição**")
        st.write(record.definition)
        st.markdown("**Transformação / regra**")
        st.write(record.transformation or "Sem transformação registrada.")
        _render_list("Colunas brutas de origem", record.raw_columns)
        _render_list("Fontes brutas", record.raw_sources)
        _render_list("Upstream intermediário", record.upstream_columns)
        _render_list("Filtros", record.filters)
        _render_list("Joins", record.joins)
        _render_list("Agregações", record.aggregations)
        _render_list("Thresholds / regras de decisão", record.thresholds)
    with right:
        try:
            st.graphviz_chart(column_flow_dot(record))
        except Exception:
            st.code(column_flow_dot(record), language="dot")

    tabs = st.tabs(["Código", "Valores reais", "Downstream", "Limitações"])
    with tabs[0]:
        if record.generated_by and "." in record.generated_by:
            module, function = record.generated_by.rsplit(".", 1)
            try:
                st.code(get_function_source(module, function), language="python")
            except Exception as exc:
                st.warning(f"Não consegui abrir o código automaticamente: {exc}")
                st.code(f"{record.generated_by}\n{record.python_file}", language="text")
        else:
            st.code(record.generated_by or "Sem função catalogada.", language="text")
    with tabs[1]:
        sample = _sample_values_for_lineage(record, raw_catalog, workspace, outputs)
        if sample.empty:
            st.info("Não há amostra real disponível para esta coluna no modo atual.")
        else:
            st.dataframe(sample.head(200), hide_index=False, use_container_width=True)
            if record.column in sample.columns:
                numeric = pd.to_numeric(sample[record.column], errors="coerce")
                if numeric.notna().any():
                    st.plotly_chart(px.histogram(sample, x=record.column, nbins=30, title=f"Distribuição de {record.column}"), use_container_width=True)
    with tabs[2]:
        _render_list("Tabelas seguintes", record.downstream_tables)
        _render_list("CSVs finais", record.downstream_csvs)
        _render_list("Gráficos", record.charts)
        _render_list("Hipóteses", record.hypotheses)
    with tabs[3]:
        _render_list("Limitações", record.limitations)


def _sample_values_for_lineage(record, raw_catalog: pd.DataFrame, workspace: dict[str, Any] | None, outputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    if record.layer == "bruto":
        source = raw_catalog[raw_catalog["source_key"].astype(str) == str(record.table)]
        path = None
        if not source.empty:
            path = Path(str(source.iloc[0]["path"]))
        elif record.raw_sources:
            path = Path(record.raw_sources[0])
        if path is None:
            return pd.DataFrame()
        preview = load_raw_preview(path, rows=200)
        return _prioritize_columns(preview, [record.column, *KEY_COLUMNS, "filename"])
    if record.layer in {"feature", "intermediario"} and workspace is not None and record.table in workspace:
        frame = workspace.get(record.table, pd.DataFrame())
        return _prioritize_columns(frame, [*KEY_COLUMNS, *record.upstream_columns, record.column])
    if record.layer == "csv_final" and record.table in outputs:
        frame = outputs.get(record.table, pd.DataFrame())
        return _prioritize_columns(frame, [*KEY_COLUMNS, *record.upstream_columns, record.column])
    return pd.DataFrame()


def _prioritize_columns(frame: pd.DataFrame, preferred: list[str]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    cols = [column for column in preferred if column in frame.columns]
    remaining = [column for column in frame.columns if column not in cols]
    return frame[cols + remaining[: max(0, 16 - len(cols))]].head(200)


def _format_lineage_option(filtered: pd.DataFrame, lineage_id: str) -> str:
    row = filtered[filtered["lineage_id"] == lineage_id].iloc[0]
    return f"{row['layer']} / {row['table']} / {row['column']} ({row['mapping_status']})"


def render_intermediate_tables(workspace: dict[str, Any] | None, outputs: dict[str, pd.DataFrame]) -> None:
    st.title("Tabelas intermediárias")
    if workspace is None:
        st.warning("Esta página exige o modo Workspace completo.")
        return

    table_name = st.selectbox("Tabela", INTERMEDIATE_TABLE_ORDER)
    spec = INTERMEDIATE_TABLE_REGISTRY[table_name]
    frame = workspace.get(table_name, pd.DataFrame())

    st.subheader(table_name)
    st.write(spec.description)
    cols = st.columns(3)
    cols[0].metric("Linhas", len(frame))
    cols[1].metric("Colunas", len(frame.columns))
    cols[2].metric("Função", spec.function)

    overview = dataframe_overview(frame)
    st.caption(
        f"{overview['rows']} linhas · {overview['columns']} colunas · "
        f"{overview['null_ratio']:.1%} células nulas · colunas temporais: {', '.join(overview['date_columns']) or 'nenhuma'}"
    )

    tab_meta, tab_code, tab_profile, tab_data, tab_trace = st.tabs(["Metadados", "Código", "Perfil", "Dados", "Rastreio da linha"])
    with tab_meta:
        _render_list("Entradas", spec.inputs)
        _render_list("Filtros", spec.filters)
        _render_list("Joins", spec.joins)
        _render_list("Agregações", spec.aggregations)
        _render_list("Colunas criadas", spec.created_columns)
        _render_list("Usada depois em", spec.downstream_tables + spec.related_csvs)
        _render_list("Hipóteses relacionadas", spec.related_hypotheses)
    with tab_code:
        st.code(get_function_source(spec.module, spec.function), language="python")
    with tab_profile:
        st.dataframe(profile_dataframe(frame), hide_index=True, use_container_width=True)
    with tab_data:
        st.dataframe(frame.head(200), hide_index=False, use_container_width=True)
        st.write("Colunas disponíveis")
        st.dataframe(pd.DataFrame({"column": list(frame.columns)}), hide_index=True, use_container_width=True)
    with tab_trace:
        _render_row_trace_for_frame(table_name, frame, workspace, {})


def render_final_csvs(
    outputs: dict[str, pd.DataFrame],
    workspace: dict[str, Any] | None = None,
    paths=None,
    docs_cache: dict[str, Any] | None = None,
) -> None:
    st.title("CSVs finais")
    preferred = [name for name in CSV_LINEAGE_ORDER if name in outputs]
    available = preferred + [name for name in sorted(outputs) if name not in preferred]
    if not available:
        st.warning("Nenhum CSV encontrado em notebook_outputs/complete_ndvi.")
        return
    csv_name = st.selectbox("CSV", available)
    spec = CSV_REGISTRY.get(csv_name)
    frame = outputs[csv_name]

    st.subheader(csv_name)
    st.write(spec.description if spec else "CSV gerado pelo fluxo completo. Ainda sem metadado manual detalhado no registry; o perfil e o preview são reais.")
    cols = st.columns(3)
    cols[0].metric("Linhas", len(frame))
    cols[1].metric("Colunas", len(frame.columns))
    cols[2].metric("Função", spec.function if spec else "não catalogada")

    tab_meta, tab_code, tab_columns, tab_profile, tab_data = st.tabs(["Metadados", "Código", "Colunas", "Perfil", "Dados"])
    with tab_meta:
        if spec:
            try:
                st.graphviz_chart(csv_flow_dot(spec))
            except Exception:
                st.code(csv_flow_dot(spec), language="dot")
            _render_list("Dependências", spec.dependencies)
            _render_list("Hipóteses impactadas", spec.related_hypotheses)
            _render_list("Gráficos relacionados", spec.related_charts)
        else:
            st.info("Este CSV está disponível nos outputs, mas ainda não tem registry manual específico. O app mostra perfil, colunas e dados reais mesmo assim.")
    with tab_code:
        if spec:
            st.code(get_function_source(spec.module, spec.function), language="python")
        else:
            st.code("Sem função geradora catalogada explicitamente para este CSV.", language="text")
    with tab_columns:
        if paths is not None:
            raw_columns = _load_raw_column_catalog_cached(str(paths.data_dir), (docs_cache or {}).get("generated_at"))
            lineage_index = build_column_lineage_index(raw_columns, workspace, outputs)
            csv_lineage = lineage_index[(lineage_index["layer"] == "csv_final") & (lineage_index["table"] == csv_name)]
            if not csv_lineage.empty:
                st.write("Rastreabilidade coluna a coluna")
                st.dataframe(
                    csv_lineage[
                        [
                            "column",
                            "raw_columns",
                            "upstream_columns",
                            "generated_by",
                            "mapping_status",
                            "mapping_confidence",
                            "hypotheses",
                            "charts",
                        ]
                    ],
                    hide_index=True,
                    use_container_width=True,
                )
        docs = []
        for column in frame.columns:
            docs.append(
                {
                    "column": column,
                    "description": spec.column_docs.get(column, "Sem documentação detalhada específica; a coluna é exibida com preview real.")
                    if spec
                    else "Sem documentação manual específica; use perfil, exemplos e lineage por nome de coluna.",
                }
            )
        st.dataframe(pd.DataFrame(docs), hide_index=True, use_container_width=True)
        if workspace is not None:
            all_columns = build_workspace_column_catalog(workspace, outputs)
            st.write("Lineage reversa aproximada por nome de coluna")
            st.dataframe(all_columns[all_columns["column"].isin(frame.columns)].head(300), hide_index=True, use_container_width=True)
    with tab_profile:
        st.dataframe(profile_dataframe(frame), hide_index=True, use_container_width=True)
    with tab_data:
        st.dataframe(frame.head(200), hide_index=False, use_container_width=True)


def render_feature_explorer(workspace: dict[str, Any] | None, outputs: dict[str, pd.DataFrame]) -> None:
    st.title("Catálogo de features")
    if workspace is None:
        st.warning("Esta página exige o modo Workspace completo.")
        st.dataframe(build_feature_catalog(), hide_index=True, use_container_width=True)
        return

    catalog = build_feature_catalog()
    with st.expander("Tabela completa de features rastreadas", expanded=False):
        search = st.text_input("Buscar no catálogo de features")
        filtered_catalog = catalog
        if search:
            q = search.lower()
            filtered_catalog = catalog[catalog.astype(str).agg(" ".join, axis=1).str.lower().str.contains(q, na=False)]
        st.dataframe(filtered_catalog, hide_index=True, use_container_width=True)

    feature_name = st.selectbox("Feature", sorted(FEATURE_REGISTRY))
    spec = FEATURE_REGISTRY[feature_name]
    frame = workspace.get(spec.table_where_born, pd.DataFrame())

    st.subheader(feature_name)
    st.write(spec.definition)
    left, right = st.columns([1.1, 0.9])
    with left:
        st.json(
            {
                "tipo": spec.feature_type,
                "nasce_em": spec.table_where_born,
                "funcao": spec.function,
                "arquivo": spec.file_path,
                "fontes_brutas": spec.raw_sources,
                "colunas_de_origem": spec.source_columns,
                "colunas_brutas_resolvidas": raw_columns_for_feature(feature_name),
                "transformacao": spec.transformation,
                "filtros_envolvidos": spec.filters_involved,
                "thresholds_regras": thresholds_for_feature(feature_name),
            }
        )
    with right:
        flow = feature_flow_dot(spec)
        try:
            st.graphviz_chart(flow)
        except Exception:
            st.code(flow, language="dot")

    tabs = st.tabs(["Código", "Preview real", "Distribuição", "Onde aparece", "Impacto analítico"])
    with tabs[0]:
        st.code(get_function_source(spec.module, spec.function), language="python")
    with tabs[1]:
        preview_cols = [column for column in [*KEY_COLUMNS, *spec.source_columns, feature_name] if column in frame.columns]
        if preview_cols:
            st.dataframe(frame[preview_cols].head(50), hide_index=False, use_container_width=True)
        else:
            st.info("A feature não aparece diretamente na tabela carregada ou depende de múltiplas colunas intermediárias.")
    with tabs[2]:
        if feature_name in frame.columns:
            numeric = pd.to_numeric(frame[feature_name], errors="coerce")
            if numeric.notna().any():
                fig = px.histogram(frame, x=feature_name, color="comparison_pair" if "comparison_pair" in frame.columns else None, nbins=30)
                st.plotly_chart(fig, use_container_width=True)
                if "week_start" in frame.columns and "area_label" in frame.columns:
                    plot_frame = frame.copy()
                    plot_frame["week_start"] = pd.to_datetime(plot_frame["week_start"], errors="coerce")
                    st.plotly_chart(
                        px.line(plot_frame.sort_values("week_start"), x="week_start", y=feature_name, color="area_label", markers=True),
                        use_container_width=True,
                    )
            else:
                st.dataframe(frame[feature_name].astype(str).value_counts(dropna=False).reset_index(), hide_index=True, use_container_width=True)
        else:
            st.info("Sem distribuição direta porque a feature não está materializada na tabela onde nasce no workspace.")
    with tabs[3]:
        _render_list("Tabelas posteriores", spec.appears_in_tables)
        _render_list("CSVs finais dependentes", spec.appears_in_csvs)
    with tabs[4]:
        _render_list("Hipóteses relacionadas", spec.related_hypotheses)
        _render_list("Gráficos relacionados", spec.related_charts)
        related_frames = [outputs[name] for name in outputs if name in spec.appears_in_csvs]
        if related_frames:
            st.write("Preview dos CSVs finais relacionados")
            for csv_name in spec.appears_in_csvs:
                if csv_name in outputs:
                    st.markdown(f"**{csv_name}**")
                    st.dataframe(outputs[csv_name].head(20), hide_index=False, use_container_width=True)


def render_driver_explorer(workspace: dict[str, Any] | None, outputs: dict[str, pd.DataFrame]) -> None:
    st.title("Explorador específico de drivers")
    st.write(
        "Drivers são as explicações operacionais/agronômicas usadas para interpretar semanas-problema do NDVI. "
        "Aqui eles são tratados como entidades de primeira classe: feature de origem, regra, evidência, exemplos e impacto em H3."
    )
    st.dataframe(pd.DataFrame(driver_documentation_rows()), hide_index=True, use_container_width=True)

    driver_name = st.selectbox("Driver", sorted(DRIVER_DOCUMENTATION))
    doc = DRIVER_DOCUMENTATION[driver_name]
    st.subheader(f"{driver_name} — {doc.title}")
    cols = st.columns(4)
    cols[0].metric("Flag real", doc.flag_feature)
    cols[1].metric("Tabela onde nasce", doc.born_table)
    cols[2].metric("Hipóteses", ", ".join(doc.hypotheses))
    cols[3].metric("CSVs finais", len(doc.final_csvs))

    left, right = st.columns([1.15, 0.85])
    with left:
        st.markdown("**Definição operacional**")
        st.write(doc.definition)
        st.markdown("**Regra lógica**")
        st.code(doc.rule, language="text")
        st.markdown("**Interpretação agronômica/técnica**")
        st.write(doc.interpretation)
        _render_list("Colunas que alimentam", doc.source_columns)
        _render_list("Fontes brutas", doc.raw_sources)
        _render_list("Limitações", doc.limitations)
    with right:
        try:
            st.graphviz_chart(driver_flow_dot(doc))
        except Exception:
            st.code(driver_flow_dot(doc), language="dot")

    tabs = st.tabs(["Evidência nos CSVs", "Ocorrência por área/par", "Linhas reais", "Código"])
    with tabs[0]:
        lift = outputs.get("event_driver_lift.csv", pd.DataFrame())
        if not lift.empty and "driver" in lift.columns:
            st.dataframe(lift[lift["driver"] == driver_name], hide_index=True, use_container_width=True)
            subset = lift[lift["driver"] == driver_name].copy()
            if not subset.empty:
                st.plotly_chart(px.bar(subset, x="comparison_pair", y="delta_pp", color="evidence_level", title=f"Lift do driver {driver_name}"), use_container_width=True)
        else:
            st.info("event_driver_lift.csv não está carregado.")
    with tabs[1]:
        timeline = workspace.get("ndvi_phase_timeline", pd.DataFrame()) if workspace else pd.DataFrame()
        if not timeline.empty and doc.flag_feature in timeline.columns:
            rates = (
                timeline.groupby(["comparison_pair", "area_label", "treatment"], as_index=False)
                .agg(occurrence_rate=(doc.flag_feature, "mean"), occurrence_weeks=(doc.flag_feature, "sum"), weeks=("week_start", "nunique"))
                .sort_values(["comparison_pair", "occurrence_rate"], ascending=[True, False])
            )
            st.dataframe(rates, hide_index=True, use_container_width=True)
            st.plotly_chart(
                px.bar(rates, x="area_label", y="occurrence_rate", color="treatment", facet_row="comparison_pair", title=f"Taxa de ocorrência: {driver_name}"),
                use_container_width=True,
            )
        else:
            st.warning("Para taxa por área/par, abra em modo Workspace completo.")
    with tabs[2]:
        timeline = workspace.get("ndvi_phase_timeline", pd.DataFrame()) if workspace else pd.DataFrame()
        if not timeline.empty and doc.flag_feature in timeline.columns:
            cols_to_show = [column for column in [*KEY_COLUMNS, doc.flag_feature, *doc.source_columns, "event_type", "primary_driver", "drivers_summary"] if column in timeline.columns]
            examples = timeline[timeline[doc.flag_feature].astype(bool)][cols_to_show].head(100)
            st.dataframe(examples, hide_index=False, use_container_width=True)
        else:
            st.info("Sem exemplos porque a timeline não está carregada no modo atual.")
    with tabs[3]:
        st.code(get_function_source("farmlab.ndvi_deepdive", "build_ndvi_phase_timeline"), language="python")
        with st.expander("Funções internas de limiar e flags", expanded=False):
            st.code(get_function_source("farmlab.ndvi_deepdive", "_build_ndvi_thresholds"), language="python")
            st.code(get_function_source("farmlab.ndvi_deepdive", "_apply_flag_columns"), language="python")


def render_row_trace(workspace: dict[str, Any] | None, outputs: dict[str, pd.DataFrame]) -> None:
    st.title("Rastreio por linha / semana / área")
    if workspace is None:
        st.warning("Esta página exige o modo Workspace completo.")
        return

    dataset_options = {**{name: workspace[name] for name in INTERMEDIATE_TABLE_ORDER if name in workspace}, **outputs}
    dataset_name = st.selectbox("Tabela ou CSV para rastrear", list(dataset_options))
    frame = dataset_options[dataset_name].copy()

    filters = {}
    filter_cols = st.columns(3)
    for idx, column in enumerate(["season_id", "area_label", "comparison_pair", "treatment"]):
        if column in frame.columns:
            values = [""] + sorted(frame[column].dropna().astype(str).unique().tolist())
            filters[column] = filter_cols[idx % 3].selectbox(column, values, key=f"trace_{dataset_name}_{column}")
    if "week_start" in frame.columns:
        values = [""] + sorted(frame["week_start"].dropna().astype(str).unique().tolist())
        filters["week_start"] = filter_cols[1].selectbox("week_start", values, key=f"trace_{dataset_name}_week")
    if "date" in frame.columns:
        values = [""] + sorted(frame["date"].dropna().astype(str).unique().tolist())
        filters["date"] = filter_cols[2].selectbox("date", values, key=f"trace_{dataset_name}_date")

    _render_row_trace_for_frame(dataset_name, frame, workspace, filters)


def render_hypotheses(workspace: dict[str, Any] | None, outputs: dict[str, pd.DataFrame]) -> None:
    st.title("Hipóteses e evidências")
    if workspace is None:
        st.warning("Esta página exige o modo Workspace completo.")
        return

    tabs = st.tabs(list(HYPOTHESIS_REGISTRY))
    for hypothesis_id, tab in zip(HYPOTHESIS_REGISTRY, tabs):
        spec = HYPOTHESIS_REGISTRY[hypothesis_id]
        with tab:
            st.subheader(f"{hypothesis_id} — {spec.title}")
            st.write(spec.question)
            st.write("**Regra de decisão**")
            st.write(spec.decision_rule)
            _render_list("Métricas", spec.metrics)
            _render_list("Tabelas", spec.tables)
            _render_list("CSVs", spec.csvs)
            _render_list("Limitações", spec.limitations)

            if "final_hypothesis_register.csv" in outputs:
                st.write("Fechamento oficial")
                st.dataframe(
                    outputs["final_hypothesis_register.csv"].query("hypothesis_id == @hypothesis_id"),
                    hide_index=True,
                    use_container_width=True,
                )

            if hypothesis_id == "H1" and "pair_effect_tests.csv" in outputs:
                st.dataframe(outputs["pair_effect_tests.csv"].query("metric == 'ndvi_mean_week'"), hide_index=True, use_container_width=True)
            elif hypothesis_id == "H2" and "pair_effect_tests.csv" in outputs:
                st.dataframe(outputs["pair_effect_tests.csv"].query("metric in ['low_vigor_flag', 'major_drop_flag']"), hide_index=True, use_container_width=True)
            elif hypothesis_id == "H3" and "event_driver_lift.csv" in outputs:
                st.dataframe(outputs["event_driver_lift.csv"], hide_index=True, use_container_width=True)
            elif hypothesis_id == "H4" and "ndvi_outlook" in workspace:
                st.dataframe(workspace["ndvi_outlook"], hide_index=True, use_container_width=True)

            for chart_key in spec.charts:
                st.markdown(f"**Gráfico relacionado: {CHART_REGISTRY[chart_key].title}**")
                fig = build_chart(chart_key, workspace, outputs)
                st.plotly_chart(fig, use_container_width=True)


def render_charts(workspace: dict[str, Any] | None, outputs: dict[str, pd.DataFrame]) -> None:
    st.title("Gráficos")
    if workspace is None:
        st.warning("Esta página exige o modo Workspace completo.")
        return

    chart_key = st.selectbox("Gráfico", list(CHART_REGISTRY))
    spec = CHART_REGISTRY[chart_key]
    st.subheader(spec.title)
    st.write(spec.interpretation)
    _render_list("Dataframes de origem", spec.dataframe_sources)
    st.write("**Origem do cálculo**")
    st.write(spec.calculation_origin)
    with st.expander("Código do gráfico", expanded=False):
        st.code(spec.chart_code, language="python")
    st.plotly_chart(build_chart(chart_key, workspace, outputs), use_container_width=True)

    st.write("Previews das fontes")
    for source in spec.dataframe_sources:
        frame = _resolve_dataframe_source(source, workspace, outputs)
        if frame is not None:
            st.markdown(f"**{source}**")
            st.dataframe(frame.head(30), hide_index=False, use_container_width=True)


def render_data_quality(quality_summary: pd.DataFrame, quality_examples: dict[str, pd.DataFrame]) -> None:
    st.title("Qualidade dos dados")
    st.write(
        "As regras abaixo são checagens explícitas para auditoria humana. Elas não alteram a lógica analítica; "
        "servem para mostrar riscos de integridade, plausibilidade física e impacto analítico."
    )
    if quality_summary.empty:
        st.warning("Nenhuma regra de qualidade foi calculada para o modo atual.")
        return

    cols = st.columns(4)
    cols[0].metric("Regras", len(quality_summary))
    cols[1].metric("Com atenção", int(quality_summary["affected_rows"].gt(0).sum()))
    cols[2].metric("Warnings", int((quality_summary["severity"] == "warning").sum()))
    cols[3].metric("Errors", int((quality_summary["severity"] == "error").sum()))

    domain_filter = st.multiselect("Domínio", sorted(quality_summary["domain"].dropna().unique().tolist()))
    severity_filter = st.multiselect("Severidade", sorted(quality_summary["severity"].dropna().unique().tolist()))
    filtered = quality_summary.copy()
    if domain_filter:
        filtered = filtered[filtered["domain"].isin(domain_filter)]
    if severity_filter:
        filtered = filtered[filtered["severity"].isin(severity_filter)]
    st.dataframe(filtered, hide_index=True, use_container_width=True)

    rule_options = filtered["rule_id"].tolist()
    if not rule_options:
        st.info("Nenhuma regra encontrada com os filtros atuais.")
        return
    selected_rule = st.selectbox("Abrir exemplos da regra", rule_options)
    if selected_rule:
        row = quality_summary[quality_summary["rule_id"] == selected_rule].iloc[0]
        st.subheader(selected_rule)
        st.write(f"**Motivo:** {row['reason']}")
        st.write(f"**Impacto analítico:** {row['impact']}")
        example = quality_examples.get(selected_rule, pd.DataFrame())
        if example.empty:
            st.success("Nenhuma linha afetada.")
        else:
            st.dataframe(example, hide_index=False, use_container_width=True)


def render_documentation_explorer(docs_cache: dict[str, Any], doc_index: pd.DataFrame) -> None:
    st.title("Explorador de documentação FarmLab")
    st.write(
        "A documentação pública foi lida a partir das rotas do FarmLab e do bundle JavaScript da SPA. "
        "Quando a extração automática não traz uma definição completa, a interface mostra o fallback inferido do código e do catálogo local."
    )
    st.json(
        {
            "generated_at": docs_cache.get("generated_at"),
            "source": docs_cache.get("source"),
            "status_note": docs_cache.get("status_note"),
        }
    )
    if st.button("Atualizar cache da documentação agora"):
        _load_documentation_cache.clear()
        refreshed = load_or_refresh_documentation_cache(force=True)
        st.success(f"Cache atualizado em {refreshed.get('generated_at')}")
        st.rerun()

    search = st.text_input("Buscar na documentação extraída/cacheada")
    filtered = doc_index.copy()
    if search:
        q = search.lower()
        filtered = filtered[filtered.astype(str).agg(" ".join, axis=1).str.lower().str.contains(q, na=False)]
    st.metric("Registros de documentação", len(filtered))
    st.dataframe(filtered, hide_index=True, use_container_width=True)

    route_tab, bundle_tab, manual_tab = st.tabs(["Rotas", "Trechos do bundle", "Documentação consolidada"])
    with route_tab:
        st.dataframe(pd.DataFrame(docs_cache.get("routes", [])), hide_index=True, use_container_width=True)
    with bundle_tab:
        bundle_rows = []
        for bundle in docs_cache.get("bundles", []):
            for snippet in bundle.get("snippets", []):
                bundle_rows.append({"url": bundle.get("url"), "term": snippet.get("term"), "text": snippet.get("text")})
        st.dataframe(pd.DataFrame(bundle_rows), hide_index=True, use_container_width=True)
    with manual_tab:
        st.json(docs_cache.get("manual_sources", {}))


def build_chart(chart_key: str, workspace: dict[str, Any], outputs: dict[str, pd.DataFrame]):
    if chart_key == "ndvi_weekly_by_area":
        frame = workspace["ndvi_phase_timeline"].copy().sort_values("week_start")
        fig = px.line(
            frame,
            x="week_start",
            y="ndvi_mean_week",
            color="comparison_pair",
            line_dash="treatment",
            line_group="area_label",
            hover_data=["area_label"],
            facet_row="comparison_pair",
            markers=True,
            color_discrete_map=PAIR_COLOR_MAP,
            line_dash_map=TREATMENT_DASH_MAP,
            title=CHART_REGISTRY[chart_key].title,
        )
        fig.update_layout(height=850)
        return fig
    if chart_key == "gap_weekly":
        frame = outputs["pair_weekly_gaps.csv"].copy()
        fig = px.line(
            frame,
            x="week_start",
            y="gap_ndvi_mean_week_4_0_minus_convencional",
            color="comparison_pair",
            color_discrete_map=PAIR_COLOR_MAP,
            markers=True,
            title=CHART_REGISTRY[chart_key].title,
        )
        fig.add_hline(y=0, line_dash="dash", line_color="#94a3b8")
        return fig
    if chart_key == "ndvi_mean_by_area":
        frame = outputs["ndvi_stats_by_area.csv"].copy()
        fig = px.bar(
            frame,
            x="area_label",
            y="mean",
            color="comparison_pair",
            pattern_shape="treatment",
            color_discrete_map=PAIR_COLOR_MAP,
            title=CHART_REGISTRY[chart_key].title,
        )
        return fig
    if chart_key == "outliers_ndvi":
        frame = outputs["ndvi_outliers.csv"].copy()
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        fig = px.scatter(
            frame,
            x="date",
            y="ndvi_zscore",
            color="comparison_pair",
            symbol="outlier_flag",
            hover_data=["area_label", "ndvi_mean", "outlier_direction"],
            color_discrete_map=PAIR_COLOR_MAP,
            title=CHART_REGISTRY[chart_key].title,
        )
        fig.add_hline(y=2, line_dash="dash", line_color="#b91c1c")
        fig.add_hline(y=-2, line_dash="dash", line_color="#b91c1c")
        return fig
    if chart_key == "drivers_problem_weeks":
        frame = outputs["event_driver_lift.csv"].copy()
        return px.bar(
            frame,
            x="driver",
            y="delta_pp",
            color="evidence_level",
            facet_row="comparison_pair",
            title=CHART_REGISTRY[chart_key].title,
        )
    if chart_key == "correlations":
        frame = outputs["weekly_correlations.csv"].copy()
        if "analysis_target" in frame.columns:
            frame = frame[frame["analysis_target"] == "delta_ndvi_seguinte"].copy()
        frame = frame.sort_values("strongest_abs_correlation", ascending=False).head(20)
        return px.bar(
            frame,
            x="strongest_abs_correlation",
            y="feature",
            orientation="h",
            color="direction",
            title=CHART_REGISTRY[chart_key].title,
        )
    if chart_key == "hypothesis_summary":
        frame = outputs["final_hypothesis_register.csv"][["comparison_pair", "hypothesis_id", "status"]].copy()
        order = {"nao_suportada": 0, "inconclusiva": 1, "suportada": 2}
        pivot = frame.pivot(index="hypothesis_id", columns="comparison_pair", values="status").reindex(index=["H1", "H2", "H3", "H4"])
        z = pivot.apply(lambda column: column.map(order)).astype(float)
        fig = go.Figure(
            data=go.Heatmap(
                z=z.values,
                x=list(z.columns),
                y=list(z.index),
                text=pivot.values,
                texttemplate="%{text}",
                colorscale=[
                    [0.0, "#b91c1c"],
                    [0.33, "#b91c1c"],
                    [0.34, "#64748b"],
                    [0.66, "#64748b"],
                    [0.67, "#0f766e"],
                    [1.0, "#0f766e"],
                ],
                zmin=0,
                zmax=2,
                showscale=False,
            )
        )
        fig.update_layout(title=CHART_REGISTRY[chart_key].title, height=420)
        return fig
    if chart_key == "hypothesis_h1_effect":
        frame = outputs["pair_effect_tests.csv"].query("metric == 'ndvi_mean_week'").copy()
        frame["error_plus"] = frame["ci_high"] - frame["advantage_4_0"]
        frame["error_minus"] = frame["advantage_4_0"] - frame["ci_low"]
        fig = go.Figure(
            data=[
                go.Bar(
                    x=frame["comparison_pair"],
                    y=frame["advantage_4_0"],
                    marker_color=frame["winner"].map({"favorece_4_0": "#0f766e", "favorece_convencional": "#b91c1c", "inconclusivo": "#64748b"}),
                    error_y=dict(type="data", symmetric=False, array=frame["error_plus"], arrayminus=frame["error_minus"]),
                )
            ]
        )
        fig.add_hline(y=0, line_dash="dash", line_color="#94a3b8")
        fig.update_layout(title=CHART_REGISTRY[chart_key].title, height=420)
        return fig
    if chart_key == "hypothesis_h2_problem_rates":
        frame = (
            workspace["ndvi_phase_timeline"]
            .groupby(["comparison_pair", "area_label", "treatment"], as_index=False)
            .agg(low_vigor_rate=("low_vigor_flag", "mean"), major_drop_rate=("major_drop_flag", "mean"))
            .melt(id_vars=["comparison_pair", "area_label", "treatment"], value_vars=["low_vigor_rate", "major_drop_rate"], var_name="problem_metric", value_name="problem_rate")
        )
        return px.bar(
            frame,
            x="area_label",
            y="problem_rate",
            color="problem_metric",
            facet_row="comparison_pair",
            barmode="group",
            title=CHART_REGISTRY[chart_key].title,
        )
    if chart_key == "outlook_pre_harvest":
        frame = workspace["ndvi_outlook"].copy()
        return px.bar(
            frame,
            x="area_label",
            y="trajectory_score",
            color="expected_vs_pair",
            facet_row="comparison_pair",
            title=CHART_REGISTRY[chart_key].title,
        )
    raise KeyError(f"Grafico nao implementado: {chart_key}")


def _render_row_trace_for_frame(dataset_name: str, frame: pd.DataFrame, workspace: dict[str, Any], filters: dict[str, str]) -> None:
    filtered = frame.copy()
    for column, value in filters.items():
        if value and column in filtered.columns:
            filtered = filtered[filtered[column].astype(str) == value]
    st.write(f"Linhas após filtro: {len(filtered)}")
    st.dataframe(filtered.head(100), hide_index=False, use_container_width=True)
    if filtered.empty:
        return

    selectable_indices = filtered.index.tolist()
    selected_index = st.selectbox("Selecionar linha", selectable_indices, format_func=lambda idx: f"índice {idx}")
    row = filtered.loc[selected_index]
    st.write("Linha selecionada")
    st.dataframe(row.to_frame(name="value"), use_container_width=True)

    active_driver_rows = []
    for driver_name, doc in DRIVER_DOCUMENTATION.items():
        if doc.flag_feature in row.index and bool(row.get(doc.flag_feature)):
            active_driver_rows.append(
                {
                    "driver": driver_name,
                    "flag": doc.flag_feature,
                    "definition": doc.definition,
                    "rule": doc.rule,
                }
            )
    if active_driver_rows:
        st.write("Drivers/flags ativos nesta linha")
        st.dataframe(pd.DataFrame(active_driver_rows), hide_index=True, use_container_width=True)

    season_ids = _extract_season_ids(row)
    pair = _get_value(row, "comparison_pair")
    week_start = _to_timestamp(_get_value(row, "week_start"))
    date_value = _to_timestamp(_get_value(row, "date"))

    with st.expander("Timeline da mesma área/par", expanded=True):
        if "ndvi_phase_timeline" in workspace:
            timeline = workspace["ndvi_phase_timeline"].copy()
            if season_ids:
                timeline = timeline[timeline["season_id"].isin(season_ids)]
            elif pair:
                timeline = timeline[timeline["comparison_pair"] == pair]
            if week_start is not None and "week_start" in timeline.columns:
                timeline["week_start"] = pd.to_datetime(timeline["week_start"], errors="coerce")
                recent = timeline[timeline["week_start"] <= week_start].sort_values("week_start").tail(8)
            else:
                recent = timeline.head(50)
            st.dataframe(recent, hide_index=False, use_container_width=True)

    with st.expander("Linhas diárias correspondentes", expanded=False):
        if "ndvi_clean" in workspace:
            ndvi_clean = workspace["ndvi_clean"].copy()
            ndvi_clean["date"] = pd.to_datetime(ndvi_clean["date"], errors="coerce")
            if season_ids:
                ndvi_clean = ndvi_clean[ndvi_clean["season_id"].isin(season_ids)]
            if week_start is not None:
                week_end = week_start + pd.Timedelta(days=6)
                ndvi_clean = ndvi_clean[(ndvi_clean["date"] >= week_start) & (ndvi_clean["date"] <= week_end)]
            elif date_value is not None:
                ndvi_clean = ndvi_clean[ndvi_clean["date"] == date_value]
            st.dataframe(ndvi_clean, hide_index=False, use_container_width=True)
            image_path = _first_existing_image(ndvi_clean)
            if image_path:
                st.image(str(image_path), caption=str(image_path))

        for name in ["ops_area_daily", "miip_daily"]:
            if name in workspace:
                sub = workspace[name].copy()
                if "date" in sub.columns:
                    sub["date"] = pd.to_datetime(sub["date"], errors="coerce")
                if season_ids:
                    sub = sub[sub["season_id"].isin(season_ids)]
                if week_start is not None and "date" in sub.columns:
                    sub = sub[(sub["date"] >= week_start) & (sub["date"] <= week_start + pd.Timedelta(days=6))]
                st.markdown(f"**{name}**")
                st.dataframe(sub.head(100), hide_index=False, use_container_width=True)

    with st.expander("Linhas brutas relevantes", expanded=False):
        if "ndvi_raw" in workspace:
            raw = workspace["ndvi_raw"].copy()
            raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
            if season_ids:
                raw = raw[raw["season_id"].isin(season_ids)]
            if week_start is not None:
                raw = raw[(raw["date"] >= week_start) & (raw["date"] <= week_start + pd.Timedelta(days=6))]
            elif date_value is not None:
                raw = raw[raw["date"] == date_value]
            st.dataframe(raw.head(100), hide_index=False, use_container_width=True)


def _resolve_dataframe_source(source_name: str, workspace: dict[str, Any], outputs: dict[str, pd.DataFrame]) -> pd.DataFrame | None:
    if source_name in workspace and isinstance(workspace[source_name], pd.DataFrame):
        return workspace[source_name]
    if source_name in outputs:
        return outputs[source_name]
    if source_name.endswith(".csv") and source_name in outputs:
        return outputs[source_name]
    return None


def _build_feature_flow(spec) -> str:
    raw_nodes = "\\n".join(f'"raw_{idx}" [label="{source}", shape=box, fillcolor="#fef3c7", style="rounded,filled"];' for idx, source in enumerate(spec.raw_sources))
    raw_edges = "\\n".join(f'"raw_{idx}" -> "born";' for idx, _ in enumerate(spec.raw_sources))
    downstream_nodes = "\\n".join(f'"tab_{idx}" [label="{name}", shape=box, fillcolor="#e0f2fe", style="rounded,filled"];' for idx, name in enumerate(spec.appears_in_tables))
    downstream_edges = "\\n".join(f'"born" -> "tab_{idx}";' for idx, _ in enumerate(spec.appears_in_tables))
    csv_nodes = "\\n".join(f'"csv_{idx}" [label="{name}", shape=box, fillcolor="#dcfce7", style="rounded,filled"];' for idx, name in enumerate(spec.appears_in_csvs))
    csv_edges = "\\n".join(f'"tab_{min(idx, max(len(spec.appears_in_tables)-1, 0)) if spec.appears_in_tables else "born"}" -> "csv_{idx}";' if spec.appears_in_tables else f'"born" -> "csv_{idx}";' for idx, _ in enumerate(spec.appears_in_csvs))
    return f"""
    digraph G {{
      rankdir=LR;
      node [fontname="Arial"];
      {raw_nodes}
      "born" [label="{spec.name}\\n{spec.table_where_born}", shape=box, fillcolor="#dbeafe", style="rounded,filled"];
      {downstream_nodes}
      {csv_nodes}
      {raw_edges}
      {downstream_edges}
      {csv_edges}
    }}
    """


def _render_list(title: str, values: list[str]) -> None:
    st.write(f"**{title}**")
    if not values:
        st.write("—")
        return
    for value in values:
        st.markdown(f"- {value}")


def _extract_season_ids(row: pd.Series) -> list[str]:
    candidates = []
    for column in ["season_id", "season_id_4_0", "season_id_convencional"]:
        value = _get_value(row, column)
        if value is not None and str(value).strip():
            candidates.append(str(value))
    return sorted(set(candidates))


def _get_value(row: pd.Series, column: str) -> str | None:
    if column not in row.index:
        return None
    value = row[column]
    if pd.isna(value):
        return None
    return str(value)


def _to_timestamp(value: str | None):
    if not value:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    return None if pd.isna(ts) else ts


def _first_existing_image(frame: pd.DataFrame) -> Path | None:
    if "image_path" not in frame.columns:
        return None
    for value in frame["image_path"].dropna():
        path = Path(str(value))
        if path.exists():
            return path
    return None


if __name__ == "__main__":
    main()
