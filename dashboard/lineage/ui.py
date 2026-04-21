from __future__ import annotations

from html import escape
from typing import Iterable

import streamlit as st


STATUS_COLORS: dict[str, tuple[str, str, str]] = {
    "mapeado": ("#064e3b", "#d1fae5", "#10b981"),
    "mapeado_por_feature": ("#064e3b", "#d1fae5", "#10b981"),
    "mapeado_por_driver": ("#7c2d12", "#ffedd5", "#f97316"),
    "mapeado_por_driver_dinamico": ("#7c2d12", "#ffedd5", "#f97316"),
    "mapeado_por_dependencia": ("#1e3a8a", "#dbeafe", "#60a5fa"),
    "parcial_por_dependencia_csv": ("#713f12", "#fef3c7", "#f59e0b"),
    "parcial_por_csv_exportado": ("#713f12", "#fef3c7", "#f59e0b"),
    "parcial_por_tabela": ("#713f12", "#fef3c7", "#f59e0b"),
    "contexto_ou_nao_usada_diretamente": ("#334155", "#f1f5f9", "#94a3b8"),
    "ignorada_no_modelo_atual": ("#475569", "#e2e8f0", "#94a3b8"),
    "inferido_do_codigo": ("#1e3a8a", "#dbeafe", "#60a5fa"),
    "erro": ("#7f1d1d", "#fee2e2", "#ef4444"),
}


def inject_global_css() -> None:
    st.markdown(
        """
        <style>
        :root {
            --mf-ink: #111827;
            --mf-muted: #64748b;
            --mf-border: #d9e2ef;
            --mf-soft: #f8fafc;
            --mf-card: rgba(255,255,255,0.92);
            --mf-amber: #c46b2a;
            --mf-green: #2f6f4f;
            --mf-blue: #1f5f8b;
            --mf-red: #b42318;
            --mf-shadow: 0 18px 45px rgba(15, 23, 42, 0.09);
        }
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(47,111,79,0.16), transparent 30rem),
                radial-gradient(circle at top right, rgba(196,107,42,0.18), transparent 27rem),
                linear-gradient(180deg, #f8fafc 0%, #edf2f7 48%, #f8fafc 100%);
            color: var(--mf-ink);
            font-family: "Aptos", "Segoe UI", "IBM Plex Sans", sans-serif;
        }
        section[data-testid="stSidebar"] {
            background:
                linear-gradient(180deg, rgba(15, 23, 42, 0.98) 0%, rgba(30, 41, 59, 0.98) 58%, rgba(31, 95, 139, 0.95) 100%);
            border-right: 1px solid rgba(148, 163, 184, 0.35);
        }
        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3,
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] span {
            color: #f8fafc !important;
        }
        section[data-testid="stSidebar"] div[role="radiogroup"] label {
            border-radius: 12px;
            padding: 0.20rem 0.35rem;
        }
        .block-container {
            padding-top: 1.45rem;
            padding-bottom: 3rem;
            max-width: 1500px;
        }
        h1, h2, h3 {
            letter-spacing: -0.025em;
        }
        div[data-testid="stMetric"] {
            background: var(--mf-card);
            border: 1px solid var(--mf-border);
            border-radius: 18px;
            padding: 0.82rem 0.92rem;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06);
        }
        div[data-testid="stMetric"] label {
            color: var(--mf-muted) !important;
            font-weight: 700;
        }
        div[data-testid="stMetricValue"] {
            color: #0f172a;
            font-weight: 850;
        }
        div[data-testid="stExpander"] {
            border: 1px solid var(--mf-border);
            border-radius: 16px;
            background: rgba(255, 255, 255, 0.74);
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.035);
        }
        div[data-testid="stTabs"] button {
            border-radius: 999px;
            padding-left: 1rem;
            padding-right: 1rem;
            font-weight: 700;
        }
        .audit-hero {
            position: relative;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.34);
            border-radius: 28px;
            background:
                linear-gradient(135deg, rgba(15,23,42,0.98) 0%, rgba(31,95,139,0.94) 54%, rgba(47,111,79,0.95) 100%);
            color: #fff;
            padding: 1.45rem 1.65rem;
            margin-bottom: 1.05rem;
            box-shadow: var(--mf-shadow);
        }
        .audit-hero:before {
            content: "";
            position: absolute;
            inset: auto -18% -70% 38%;
            height: 18rem;
            background:
                radial-gradient(circle, rgba(255,255,255,0.28), transparent 58%),
                radial-gradient(circle at 20% 20%, rgba(245,158,11,0.25), transparent 42%);
            transform: rotate(-10deg);
        }
        .audit-hero h1 {
            color: #fff !important;
            margin: 0.22rem 0 0.45rem 0;
            font-size: clamp(2rem, 3vw, 3.2rem);
            line-height: 1.02;
        }
        .audit-hero p {
            color: rgba(255,255,255,0.86) !important;
            margin: 0;
            max-width: 78rem;
            font-size: 1.02rem;
        }
        .audit-eyebrow {
            display: inline-flex;
            gap: 0.4rem;
            align-items: center;
            border: 1px solid rgba(255,255,255,0.27);
            background: rgba(255,255,255,0.12);
            color: #fff;
            border-radius: 999px;
            padding: 0.28rem 0.65rem;
            font-size: 0.78rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            font-weight: 800;
        }
        .audit-card {
            background: var(--mf-card);
            border: 1px solid var(--mf-border);
            border-radius: 18px;
            padding: 1rem;
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.055);
            min-height: 6rem;
        }
        .audit-card h3 {
            margin: 0 0 0.35rem 0;
            font-size: 1.02rem;
        }
        .audit-card p {
            color: var(--mf-muted);
            margin: 0;
            font-size: 0.92rem;
        }
        .audit-section {
            border-left: 5px solid var(--mf-blue);
            padding: 0.2rem 0 0.2rem 0.82rem;
            margin: 1rem 0 0.7rem 0;
        }
        .audit-section h2, .audit-section h3 {
            margin: 0;
        }
        .audit-section p {
            margin: 0.22rem 0 0 0;
            color: var(--mf-muted);
        }
        .audit-chip, .audit-pill {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.20rem 0.60rem;
            margin: 0.12rem 0.16rem 0.12rem 0;
            background: #e2e8f0;
            color: #0f172a;
            font-size: 0.78rem;
            font-weight: 750;
            border: 1px solid #cbd5e1;
            white-space: nowrap;
        }
        .audit-chip.raw { background: #fef3c7; border-color: #f59e0b; color: #713f12; }
        .audit-chip.intermediate { background: #dbeafe; border-color: #60a5fa; color: #1e3a8a; }
        .audit-chip.final { background: #dcfce7; border-color: #22c55e; color: #064e3b; }
        .audit-chip.warning { background: #fee2e2; border-color: #ef4444; color: #7f1d1d; }
        .audit-kpi-line {
            display: flex;
            gap: 0.6rem;
            flex-wrap: wrap;
            margin: 0.55rem 0 1rem 0;
        }
        .audit-kpi-line .audit-pill {
            background: #fff;
            border-color: var(--mf-border);
        }
        .audit-help {
            background: linear-gradient(135deg, rgba(219,234,254,0.92), rgba(240,253,244,0.88));
            border: 1px solid #bfdbfe;
            border-radius: 18px;
            padding: 0.85rem 1rem;
            color: #1e293b;
        }
        .audit-small {
            color: var(--mf-muted);
            font-size: 0.86rem;
        }
        div[data-testid="stDataFrame"] {
            border-radius: 14px;
            overflow: hidden;
            border: 1px solid rgba(203, 213, 225, 0.9);
        }
        code, pre {
            border-radius: 12px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def hero(title: str, subtitle: str, eyebrow: str = "MonolithFarm Data Audit") -> None:
    st.markdown(
        f"""
        <div class="audit-hero">
            <span class="audit-eyebrow">{escape(eyebrow)}</span>
            <h1>{escape(title)}</h1>
            <p>{escape(subtitle)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_header(title: str, text: str | None = None, *, accent: str = "#1f5f8b") -> None:
    body = f"<p>{escape(text)}</p>" if text else ""
    st.markdown(
        f"""
        <div class="audit-section" style="border-left-color:{accent}">
            <h2>{escape(title)}</h2>
            {body}
        </div>
        """,
        unsafe_allow_html=True,
    )


def info_card(title: str, text: str) -> None:
    st.markdown(
        f"""
        <div class="audit-card">
            <h3>{escape(title)}</h3>
            <p>{escape(text)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def pill(label: str, *, status: str | None = None) -> str:
    fg, bg, border = STATUS_COLORS.get(status or "", ("#0f172a", "#e2e8f0", "#cbd5e1"))
    return (
        f'<span class="audit-pill" style="color:{fg};background:{bg};'
        f'border-color:{border}">{escape(str(label))}</span>'
    )


def pill_row(labels: Iterable[str], *, status: str | None = None) -> None:
    values = [label for label in labels if str(label).strip()]
    if not values:
        st.write("—")
        return
    st.markdown("".join(pill(label, status=status) for label in values), unsafe_allow_html=True)


def coverage_note() -> None:
    st.markdown(
        """
        <div class="audit-help">
            <strong>Como ler a cobertura:</strong>
            <span>“mapeamento forte” significa que a coluna foi ligada a uma feature, driver ou função específica.
            “parcial” significa que há dependência conhecida do CSV/tabela, mas a coluna é metadado, texto de decisão,
            estatística auxiliar ou ainda não tem origem granular automatizada.</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
