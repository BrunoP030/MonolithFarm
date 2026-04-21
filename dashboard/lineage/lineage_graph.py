from __future__ import annotations

from dashboard.lineage.docs_registry import DriverDocumentation
from dashboard.lineage.registry import CsvSpec, FeatureSpec


def pipeline_overview_dot() -> str:
    return """
    digraph G {
      rankdir=LR;
      graph [pad="0.3", nodesep="0.45", ranksep="0.65"];
      node [shape=box, style="rounded,filled", color="#94a3b8", fontname="Arial"];
      raw [label="data/\\nfontes brutas", fillcolor="#f8fafc"];
      docs [label="FarmLab docs\\ncache local", fillcolor="#e0f2fe"];
      clean [label="ndvi_clean\\nlimpeza + renomeacao", fillcolor="#dbeafe"];
      weekly [label="pairwise_weekly_features\\nagregacao semanal", fillcolor="#ecfccb"];
      timeline [label="ndvi_phase_timeline\\nflags + drivers", fillcolor="#fed7aa"];
      tests [label="testes + lifts\\nH1-H4", fillcolor="#fde68a"];
      outputs [label="CSVs finais\\nnotebook_outputs/complete_ndvi", fillcolor="#dcfce7"];
      audit [label="Streamlit audit\\nexploracao e rastreio", fillcolor="#f1f5f9"];
      raw -> clean -> weekly -> timeline -> tests -> outputs -> audit;
      docs -> audit;
      raw -> audit;
      timeline -> audit;
    }
    """


def feature_flow_dot(spec: FeatureSpec) -> str:
    raw_nodes = "\n".join(
        f'"raw_{idx}" [label="{_escape(source)}", shape=box, fillcolor="#fef3c7", style="rounded,filled"];'
        for idx, source in enumerate(spec.raw_sources)
    )
    raw_edges = "\n".join(f'"raw_{idx}" -> "born";' for idx, _ in enumerate(spec.raw_sources))
    downstream_nodes = "\n".join(
        f'"tab_{idx}" [label="{_escape(name)}", shape=box, fillcolor="#e0f2fe", style="rounded,filled"];'
        for idx, name in enumerate(spec.appears_in_tables)
    )
    downstream_edges = "\n".join(f'"born" -> "tab_{idx}";' for idx, _ in enumerate(spec.appears_in_tables))
    csv_nodes = "\n".join(
        f'"csv_{idx}" [label="{_escape(name)}", shape=box, fillcolor="#dcfce7", style="rounded,filled"];'
        for idx, name in enumerate(spec.appears_in_csvs)
    )
    csv_edges = "\n".join(
        f'"tab_{min(idx, max(len(spec.appears_in_tables) - 1, 0))}" -> "csv_{idx}";'
        if spec.appears_in_tables
        else f'"born" -> "csv_{idx}";'
        for idx, _ in enumerate(spec.appears_in_csvs)
    )
    return f"""
    digraph G {{
      rankdir=LR;
      graph [pad="0.3", nodesep="0.4", ranksep="0.55"];
      node [fontname="Arial", color="#94a3b8"];
      {raw_nodes}
      "born" [label="{_escape(spec.name)}\\n{_escape(spec.table_where_born)}", shape=box, fillcolor="#dbeafe", style="rounded,filled"];
      {downstream_nodes}
      {csv_nodes}
      {raw_edges}
      {downstream_edges}
      {csv_edges}
    }}
    """


def driver_flow_dot(doc: DriverDocumentation) -> str:
    source_nodes = "\n".join(
        f'"source_{idx}" [label="{_escape(source)}", fillcolor="#fef3c7"];'
        for idx, source in enumerate(doc.raw_sources)
    )
    source_edges = "\n".join(f'"source_{idx}" -> "feature";' for idx, _ in enumerate(doc.raw_sources))
    csv_nodes = "\n".join(f'"csv_{idx}" [label="{_escape(csv)}", fillcolor="#dcfce7"];' for idx, csv in enumerate(doc.final_csvs))
    csv_edges = "\n".join(f'"driver" -> "csv_{idx}";' for idx, _ in enumerate(doc.final_csvs))
    hyp_nodes = "\n".join(f'"hyp_{idx}" [label="{_escape(hyp)}", fillcolor="#ede9fe"];' for idx, hyp in enumerate(doc.hypotheses))
    hyp_edges = "\n".join(f'"driver" -> "hyp_{idx}";' for idx, _ in enumerate(doc.hypotheses))
    return f"""
    digraph G {{
      rankdir=LR;
      node [shape=box, style="rounded,filled", color="#94a3b8", fontname="Arial"];
      {source_nodes}
      "feature" [label="{_escape(doc.flag_feature)}\\n{_escape(doc.born_table)}", fillcolor="#fed7aa"];
      "driver" [label="{_escape(doc.driver)}", fillcolor="#fecaca"];
      {csv_nodes}
      {hyp_nodes}
      {source_edges}
      "feature" -> "driver";
      {csv_edges}
      {hyp_edges}
    }}
    """


def csv_flow_dot(spec: CsvSpec) -> str:
    dependency_nodes = "\n".join(
        f'"dep_{idx}" [label="{_escape(dep)}", fillcolor="#dbeafe"];' for idx, dep in enumerate(spec.dependencies)
    )
    dependency_edges = "\n".join(f'"dep_{idx}" -> "csv";' for idx, _ in enumerate(spec.dependencies))
    chart_nodes = "\n".join(
        f'"chart_{idx}" [label="{_escape(chart)}", fillcolor="#fef3c7"];' for idx, chart in enumerate(spec.related_charts)
    )
    chart_edges = "\n".join(f'"csv" -> "chart_{idx}";' for idx, _ in enumerate(spec.related_charts))
    hyp_nodes = "\n".join(
        f'"hyp_{idx}" [label="{_escape(hyp)}", fillcolor="#ede9fe"];' for idx, hyp in enumerate(spec.related_hypotheses)
    )
    hyp_edges = "\n".join(f'"csv" -> "hyp_{idx}";' for idx, _ in enumerate(spec.related_hypotheses))
    return f"""
    digraph G {{
      rankdir=LR;
      node [shape=box, style="rounded,filled", color="#94a3b8", fontname="Arial"];
      {dependency_nodes}
      "csv" [label="{_escape(spec.name)}", fillcolor="#dcfce7"];
      {chart_nodes}
      {hyp_nodes}
      {dependency_edges}
      {chart_edges}
      {hyp_edges}
    }}
    """


def column_flow_dot(record) -> str:
    raw_nodes = "\n".join(
        f'"raw_{idx}" [label="{_escape(source)}", fillcolor="#fef3c7"];'
        for idx, source in enumerate(getattr(record, "raw_sources", []) or [])
    )
    raw_col_nodes = "\n".join(
        f'"rawcol_{idx}" [label="{_escape(column)}", fillcolor="#ffedd5"];'
        for idx, column in enumerate(getattr(record, "raw_columns", []) or [])
    )
    raw_edges = "\n".join(f'"raw_{idx}" -> "rawcol_{idx % max(1, len(getattr(record, "raw_columns", []) or [1]))}";' for idx, _ in enumerate(getattr(record, "raw_sources", []) or []))
    raw_col_edges = "\n".join(f'"rawcol_{idx}" -> "transform";' for idx, _ in enumerate(getattr(record, "raw_columns", []) or []))
    upstream_nodes = "\n".join(
        f'"up_{idx}" [label="{_escape(column)}", fillcolor="#e0f2fe"];'
        for idx, column in enumerate(getattr(record, "upstream_columns", []) or [])
    )
    upstream_edges = "\n".join(f'"up_{idx}" -> "transform";' for idx, _ in enumerate(getattr(record, "upstream_columns", []) or []))
    csv_nodes = "\n".join(
        f'"csv_{idx}" [label="{_escape(csv)}", fillcolor="#dcfce7"];'
        for idx, csv in enumerate(getattr(record, "downstream_csvs", []) or [])
    )
    csv_edges = "\n".join(f'"column" -> "csv_{idx}";' for idx, _ in enumerate(getattr(record, "downstream_csvs", []) or []))
    chart_nodes = "\n".join(
        f'"chart_{idx}" [label="{_escape(chart)}", fillcolor="#fef9c3"];'
        for idx, chart in enumerate(getattr(record, "charts", []) or [])
    )
    chart_edges = "\n".join(f'"column" -> "chart_{idx}";' for idx, _ in enumerate(getattr(record, "charts", []) or []))
    return f"""
    digraph G {{
      rankdir=LR;
      node [shape=box, style="rounded,filled", color="#94a3b8", fontname="Arial"];
      {raw_nodes}
      {raw_col_nodes}
      {upstream_nodes}
      "transform" [label="{_escape(getattr(record, "generated_by", "") or "transformacao")}", fillcolor="#dbeafe"];
      "column" [label="{_escape(getattr(record, "column", ""))}\\n{_escape(getattr(record, "table", ""))}", fillcolor="#bfdbfe"];
      {csv_nodes}
      {chart_nodes}
      {raw_edges}
      {raw_col_edges}
      {upstream_edges}
      "transform" -> "column";
      {csv_edges}
      {chart_edges}
    }}
    """


def _escape(value: str) -> str:
    return str(value).replace('"', "'").replace("\n", "\\n")
