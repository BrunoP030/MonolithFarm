from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.lineage.column_catalog import build_feature_catalog, build_raw_column_catalog, build_workspace_column_catalog
from dashboard.lineage.column_lineage import build_column_lineage_index, build_lineage_coverage_report
from dashboard.lineage.doc_scraper import build_documentation_index, load_or_refresh_documentation_cache
from dashboard.lineage.docs_registry import DRIVER_DOCUMENTATION
from dashboard.lineage.manifest import build_critical_lineage_report, build_lineage_acceptance_gates
from dashboard.lineage.project_objectives import project_objectives
from dashboard.lineage.registry import (
    CHART_REGISTRY,
    CSV_LINEAGE_ORDER,
    CSV_REGISTRY,
    FEATURE_REGISTRY,
    HYPOTHESIS_REGISTRY,
    INTERMEDIATE_TABLE_ORDER,
    INTERMEDIATE_TABLE_REGISTRY,
)
from dashboard.lineage.runtime import (
    build_raw_file_catalog,
    get_function_source,
    load_output_csvs,
    load_raw_preview,
    load_resolved_paths,
)
from scripts.bootstrap_data import ensure_data_dir


EDGE_RELATIONS: dict[str, dict[str, str]] = {
    "contains_column": {
        "label": "contém coluna",
        "description": "Arquivo ou tabela que contém esta coluna.",
    },
    "creates_feature": {
        "label": "deriva feature",
        "description": "Transformação que cria uma feature a partir de uma tabela ou coluna.",
    },
    "feeds_table": {
        "label": "alimenta tabela",
        "description": "Feature ou tabela usada para montar uma tabela intermediária posterior.",
    },
    "feeds_csv": {
        "label": "alimenta CSV",
        "description": "Objeto usado na exportação de um CSV final.",
    },
    "raw_origin": {
        "label": "origem bruta",
        "description": "Coluna ou arquivo bruto que participa da derivação.",
    },
    "driver_from_flag": {
        "label": "gera driver",
        "description": "Flag semanal interpretada como driver analítico.",
    },
    "generates_csv": {
        "label": "gera CSV",
        "description": "Tabela intermediária usada pela função geradora do CSV.",
    },
    "supports_hypothesis": {
        "label": "sustenta hipótese",
        "description": "CSV ou coluna final usada como evidência da hipótese.",
    },
    "generates_chart": {
        "label": "gera gráfico",
        "description": "CSV ou coluna final usada como base do gráfico.",
    },
    "lineage": {
        "label": "linhagem",
        "description": "Ligação coluna-a-coluna inferida pelo manifesto de lineage.",
    },
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta o pacote JSON usado pelo MonolithFarm Atlas React.")
    parser.add_argument("--out", default="lineage_atlas/public/atlas-data.json", help="Arquivo JSON de saída.")
    parser.add_argument("--refresh-docs", action="store_true", help="Reconsulta as páginas oficiais do FarmLab antes de exportar.")
    parser.add_argument("--raw-preview-rows", type=int, default=30)
    parser.add_argument("--table-preview-rows", type=int, default=80)
    parser.add_argument(
        "--include-private-samples",
        action="store_true",
        help="Inclui previews/exemplos reais no JSON público. Use apenas em ambiente local controlado.",
    )
    args = parser.parse_args()

    paths = load_resolved_paths()
    ensure_data_dir(paths.data_dir)
    _, raw_catalog = build_raw_file_catalog(paths.data_dir)
    docs_cache = load_or_refresh_documentation_cache(force=args.refresh_docs)
    docs_index = build_documentation_index(docs_cache)
    raw_columns = build_raw_column_catalog(raw_catalog, load_raw_preview, docs_cache, sample_rows=500)
    outputs = load_output_csvs(paths.output_dir)
    lineage_index = build_column_lineage_index(raw_columns, None, outputs)
    coverage = build_lineage_coverage_report(lineage_index, outputs)
    critical = build_critical_lineage_report(lineage_index)
    quality_summary = outputs.get("data_audit.csv")
    gates = build_lineage_acceptance_gates(lineage_index, coverage, critical, outputs, quality_summary)
    workspace_columns = build_workspace_column_catalog(None, outputs)

    payload = {
        "meta": {
            "project": "MonolithFarm Atlas NDVI",
            "projectDir": ".",
            "dataDir": "data",
            "outputDir": "notebook_outputs/complete_ndvi",
            "profile": paths.profile_name,
            "docsGeneratedAt": docs_cache.get("generated_at"),
            "pathPolicy": "O JSON publico usa apenas caminhos relativos seguros; dados completos ficam no Data Vault autenticado.",
        },
        "summary": _summary(raw_catalog, raw_columns, outputs, lineage_index, coverage, docs_index),
        "projectObjectives": project_objectives(),
        "rawFiles": _raw_files(raw_catalog, raw_columns, docs_cache, args.raw_preview_rows),
        "rawColumns": _records(raw_columns),
        "workspaceColumns": _records(workspace_columns),
        "features": _features(),
        "drivers": _drivers(),
        "intermediateTables": _intermediate_tables(outputs, args.table_preview_rows),
        "finalCsvs": _final_csvs(outputs, args.table_preview_rows),
        "hypotheses": _registry_records(HYPOTHESIS_REGISTRY),
        "charts": _registry_records(CHART_REGISTRY),
        "lineage": _records(lineage_index),
        "coverage": _records(coverage),
        "criticalTargets": _records(critical),
        "acceptanceGates": _records(gates),
        "correlations": _correlations(outputs, lineage_index),
        "audit": _audit_index(outputs, paths.data_dir),
        "docs": {
            "routes": docs_cache.get("routes", []),
            "datasetSchemas": docs_cache.get("dataset_schemas", []),
            "manualSources": docs_cache.get("manual_sources", {}),
            "index": _records(docs_index),
        },
        "graph": _build_graph(raw_catalog, raw_columns, outputs, lineage_index),
        "story": _story(outputs),
    }
    if not args.include_private_samples:
        payload = _redact_private_content(payload)
    payload = _sanitize_public_payload(payload, paths)
    payload["entities"] = _entity_index(payload)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(_jsonable(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Atlas data exported to {out_path.resolve()}")


def _redact_private_content(payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(payload)
    payload.setdefault("meta", {})["privateSamplesRedacted"] = True
    payload["rawFiles"] = [_redact_raw_file(item) for item in payload.get("rawFiles", [])]
    payload["rawColumns"] = [_redact_column_record(item) for item in payload.get("rawColumns", [])]
    payload["workspaceColumns"] = [_redact_column_record(item) for item in payload.get("workspaceColumns", [])]
    payload["intermediateTables"] = [_redact_table_record(item) for item in payload.get("intermediateTables", [])]
    payload["finalCsvs"] = [_redact_table_record(item) for item in payload.get("finalCsvs", [])]
    payload["audit"] = []
    if isinstance(payload.get("story"), dict):
        story = dict(payload["story"])
        story["decisionSummary"] = []
        story["hypothesisRegister"] = []
        payload["story"] = story
    return payload


def _redact_raw_file(item: dict[str, Any]) -> dict[str, Any]:
    clean = dict(item)
    clean["preview"] = []
    clean["privatePreview"] = "login_required"
    clean["columnsDetailed"] = [_redact_column_record(column) for column in clean.get("columnsDetailed", [])]
    return clean


def _redact_table_record(item: dict[str, Any]) -> dict[str, Any]:
    clean = dict(item)
    clean["preview"] = []
    clean["privatePreview"] = "login_required"
    clean["profile"] = [_redact_column_record(column) for column in clean.get("profile", [])]
    return clean


def _redact_column_record(item: dict[str, Any]) -> dict[str, Any]:
    clean = dict(item)
    for key in ("examples", "non_null_sample", "sample_values", "sample", "preview"):
        if key in clean:
            clean[key] = ""
    return clean


_PUBLIC_PATH_KEYS = {"path", "file_path", "projectDir", "dataDir", "outputDir", "image_path"}
_ABSOLUTE_PATH_HINT = re.compile(r"(^|[^A-Za-z])([A-Z]:[\\/](?!/)|\\\\|/(?:home|users|mnt|var|tmp)/)", re.IGNORECASE)


def _sanitize_public_payload(payload: Any, paths: Any) -> Any:
    return _sanitize_public_value(payload, paths, "")


def _sanitize_public_value(value: Any, paths: Any, key: str) -> Any:
    if isinstance(value, dict):
        return {item_key: _sanitize_public_value(item_value, paths, str(item_key)) for item_key, item_value in value.items()}
    if isinstance(value, list):
        return [_sanitize_public_value(item, paths, key) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_public_value(item, paths, key) for item in value]
    if isinstance(value, Path):
        return _sanitize_public_string(str(value), paths, key)
    if isinstance(value, str):
        return _sanitize_public_string(value, paths, key)
    return value


def _sanitize_public_string(value: str, paths: Any, key: str) -> str:
    if not value or value.startswith(("http://", "https://", "mailto:")):
        return value
    result = value
    normalized = value.replace("\\", "/")
    contains_private_hint = bool(_ABSOLUTE_PATH_HINT.search(normalized)) or "users/" in normalized.lower()
    if contains_private_hint:
        result = normalized
        result = _replace_root_path(result, paths.project_dir, ".")
        result = _replace_root_path(result, paths.data_dir, "data")
        result = _replace_root_path(result, paths.output_dir, "notebook_outputs/complete_ndvi")
        result = _replace_marker_tail(result, "/data/", "data/")
        result = _replace_marker_tail(result, "/notebook_outputs/", "notebook_outputs/")
        if _ABSOLUTE_PATH_HINT.search(result):
            if key in _PUBLIC_PATH_KEYS:
                return _safe_public_basename(result)
            return re.sub(
                r"(^|[^A-Za-z])([A-Z]:/[^\"'\n\r]+|/(?:home|users|mnt|var|tmp)/[^\"'\n\r]+)",
                lambda match: f"{match.group(1)}[private-path-redacted]",
                result,
                flags=re.IGNORECASE,
            )
    return result


def _replace_root_path(value: str, root: Path, public_root: str) -> str:
    root_text = str(root.resolve()).replace("\\", "/").rstrip("/")
    if not root_text:
        return value
    pattern = re.compile(re.escape(root_text), re.IGNORECASE)
    return pattern.sub(public_root.rstrip("/"), value)


def _replace_marker_tail(value: str, marker: str, public_prefix: str) -> str:
    lower = value.lower()
    marker_lower = marker.lower()
    index = lower.find(marker_lower)
    if index < 0:
        return value
    return public_prefix + value[index + len(marker) :]


def _safe_public_basename(value: str) -> str:
    normalized = value.rstrip("/").replace("\\", "/")
    name = normalized.rsplit("/", 1)[-1]
    return name or "private-path-redacted"


def _summary(raw_catalog: pd.DataFrame, raw_columns: pd.DataFrame, outputs: dict[str, pd.DataFrame], lineage: pd.DataFrame, coverage: pd.DataFrame, docs_index: pd.DataFrame) -> dict[str, Any]:
    return {
        "rawFiles": int((raw_catalog["kind"] == "file").sum()) if not raw_catalog.empty else 0,
        "rawColumns": len(raw_columns),
        "intermediateTables": len(INTERMEDIATE_TABLE_ORDER),
        "finalCsvs": len(outputs),
        "features": len(FEATURE_REGISTRY),
        "drivers": len(DRIVER_DOCUMENTATION),
        "lineageRecords": len(lineage),
        "farmLabDocs": len(docs_index),
        "minCoveragePct": float(coverage["coverage_pct"].min()) if not coverage.empty else 0,
    }


def _raw_files(raw_catalog: pd.DataFrame, raw_columns: pd.DataFrame, docs_cache: dict[str, Any], rows: int) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in raw_catalog.to_dict(orient="records"):
        source_key = str(row.get("source_key", ""))
        path = Path(str(row.get("path", "")))
        column_rows = raw_columns[raw_columns["source_key"].astype(str).eq(source_key)] if not raw_columns.empty else pd.DataFrame()
        try:
            preview = load_raw_preview(path, rows=rows)
        except Exception:
            preview = pd.DataFrame()
        item = dict(row)
        item["id"] = f"raw-file:{source_key}"
        item["columnsDetailed"] = _records(column_rows)
        item["preview"] = _records(preview)
        item["documentation"] = docs_cache.get("manual_sources", {}).get(str(row.get("source_group", "")), {})
        result.append(item)
    return result


def _features() -> list[dict[str, Any]]:
    catalog = build_feature_catalog()
    rows: list[dict[str, Any]] = []
    for name, spec in FEATURE_REGISTRY.items():
        row = _asdict(spec)
        match = catalog[catalog["feature"].eq(name)] if not catalog.empty else pd.DataFrame()
        if not match.empty:
            row.update(match.iloc[0].to_dict())
        row["id"] = f"feature:{name}"
        row["code"] = _safe_source(spec.module, spec.function)
        rows.append(row)
    return sorted(rows, key=lambda item: (str(item.get("feature_type", "")), str(item.get("name", ""))))


def _drivers() -> list[dict[str, Any]]:
    rows = []
    for name, doc in DRIVER_DOCUMENTATION.items():
        row = _asdict(doc)
        row["id"] = f"driver:{name}"
        row["name"] = name
        rows.append(row)
    return rows


def _intermediate_tables(outputs: dict[str, pd.DataFrame], rows: int) -> list[dict[str, Any]]:
    result = []
    for name in INTERMEDIATE_TABLE_ORDER:
        spec = INTERMEDIATE_TABLE_REGISTRY[name]
        frame = outputs.get(f"{name}.csv", pd.DataFrame())
        item = _asdict(spec)
        item["id"] = f"intermediate:{name}"
        item["rowCount"] = len(frame)
        item["columns"] = list(frame.columns)
        item["preview"] = _records(frame.head(rows))
        item["profile"] = _column_profile(frame)
        item["code"] = _safe_source(spec.module, spec.function)
        result.append(item)
    return result


def _final_csvs(outputs: dict[str, pd.DataFrame], rows: int) -> list[dict[str, Any]]:
    ordered = [name for name in CSV_LINEAGE_ORDER if name in outputs] + [name for name in sorted(outputs) if name not in CSV_LINEAGE_ORDER]
    result = []
    for name in ordered:
        frame = outputs[name]
        spec = CSV_REGISTRY.get(name)
        item = _asdict(spec) if spec else {"name": name, "description": "CSV exportado pelo fluxo completo."}
        item["id"] = f"csv:{name}"
        item["name"] = name
        item["rowCount"] = len(frame)
        item["columns"] = list(frame.columns)
        item["preview"] = _records(frame.head(rows))
        item["profile"] = _column_profile(frame)
        if spec:
            item["code"] = _safe_source(spec.module, spec.function)
        result.append(item)
    return result


def _registry_records(registry: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for key, spec in registry.items():
        row = _asdict(spec)
        row.setdefault("id", key)
        row.setdefault("key", key)
        rows.append(row)
    return rows


def _build_graph(raw_catalog: pd.DataFrame, raw_columns: pd.DataFrame, outputs: dict[str, pd.DataFrame], lineage: pd.DataFrame) -> dict[str, Any]:
    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}
    y_positions = {"rawFile": 0, "rawColumn": 0, "intermediate": 0, "feature": 0, "driver": 0, "csv": 0, "csvColumn": 0, "hypothesis": 0, "chart": 0}
    stage_x = {
        "rawFile": 0,
        "rawColumn": 260,
        "intermediate": 520,
        "feature": 780,
        "driver": 780,
        "csv": 1040,
        "csvColumn": 1300,
        "hypothesis": 1560,
        "chart": 1560,
    }
    wrap_after = {
        "rawFile": 8,
        "rawColumn": 16,
        "intermediate": 8,
        "feature": 12,
        "driver": 8,
        "csv": 10,
        "csvColumn": 16,
        "hypothesis": 6,
        "chart": 6,
    }

    def add_node(node_id: str, node_type: str, label: str, subtitle: str = "", ref: str = "", search: str = "") -> None:
        if node_id in nodes:
            return
        idx = y_positions[node_type]
        y_positions[node_type] += 1
        row = idx % wrap_after[node_type]
        lane = idx // wrap_after[node_type]
        nodes[node_id] = {
            "id": node_id,
            "type": node_type,
            "label": label,
            "subtitle": subtitle,
            "ref": ref,
            "search": search,
            "position": {"x": stage_x[node_type] + lane * 226, "y": 72 + row * 92},
        }

    def add_edge(source: str, target: str, label: str = "") -> None:
        if source == target or source not in nodes or target not in nodes:
            return
        edge_id = f"{source}->{target}:{label}"
        edges[edge_id] = {
            "id": edge_id,
            "source": source,
            "target": target,
            "sourceHandle": "out",
            "targetHandle": "in",
            "label": label,
        }

    for row in raw_catalog.to_dict(orient="records"):
        source_key = str(row.get("source_key", ""))
        safe_search = " ".join(
            str(row.get(key, ""))
            for key in ("source_key", "source_group", "description", "kind", "column_names", "temporal_min", "temporal_max")
            if row.get(key) is not None
        )
        add_node(f"raw-file:{source_key}", "rawFile", source_key, str(row.get("source_group", "")), source_key, safe_search)

    raw_column_lookup: dict[str, list[str]] = {}
    for row in raw_columns.to_dict(orient="records"):
        source_key = str(row.get("source_key", ""))
        column = str(row.get("column", ""))
        node_id = f"raw-column:{source_key}:{column}"
        raw_column_lookup.setdefault(column, []).append(node_id)
        add_node(node_id, "rawColumn", column, source_key, f"{source_key}.{column}", json.dumps(row, ensure_ascii=False))
        add_edge(f"raw-file:{source_key}", node_id, "contém")

    for name, spec in INTERMEDIATE_TABLE_REGISTRY.items():
        add_node(f"intermediate:{name}", "intermediate", name, spec.function, name, spec.description)
    for name, spec in FEATURE_REGISTRY.items():
        add_node(f"feature:{name}", "feature", name, spec.feature_type, name, spec.definition)
        add_edge(f"intermediate:{spec.table_where_born}", f"feature:{name}", "cria")
        for table in spec.appears_in_tables:
            if table == spec.table_where_born:
                continue
            add_edge(f"feature:{name}", f"intermediate:{table}", "alimenta")
        for csv_name in spec.appears_in_csvs:
            if csv_name in outputs:
                add_node(f"csv:{csv_name}", "csv", csv_name, "CSV final", csv_name, "")
                add_edge(f"feature:{name}", f"csv:{csv_name}", "exporta")
        for raw_col in spec.source_columns:
            for raw_node in raw_column_lookup.get(raw_col, []):
                add_edge(raw_node, f"feature:{name}", "origem")
                source_key = raw_node.split(":", 2)[1]
                add_edge(f"raw-file:{source_key}", f"feature:{name}", "origem")
    for name, doc in DRIVER_DOCUMENTATION.items():
        add_node(f"driver:{name}", "driver", name, getattr(doc, "flag_feature", ""), name, getattr(doc, "definition", ""))
        flag = getattr(doc, "flag_feature", "")
        if flag in FEATURE_REGISTRY:
            add_edge(f"feature:{flag}", f"driver:{name}", "vira driver")
    for name in outputs:
        add_node(f"csv:{name}", "csv", name, "CSV final", name, "")
        for column in outputs[name].columns:
            col_id = f"csv-column:{name}:{column}"
            add_node(col_id, "csvColumn", column, name, f"{name}.{column}", "")
            add_edge(f"csv:{name}", col_id, "contém")
    for name, spec in CSV_REGISTRY.items():
        if name in outputs:
            for dep in spec.dependencies:
                if dep in INTERMEDIATE_TABLE_REGISTRY:
                    add_edge(f"intermediate:{dep}", f"csv:{name}", "gera")
            for hyp in spec.related_hypotheses:
                add_node(f"hypothesis:{hyp}", "hypothesis", hyp, "Hipótese", hyp, "")
                add_edge(f"csv:{name}", f"hypothesis:{hyp}", "sustenta")
            for chart in spec.related_charts:
                add_node(f"chart:{chart}", "chart", chart, "Gráfico", chart, "")
                add_edge(f"csv:{name}", f"chart:{chart}", "visualiza")
    for hyp in HYPOTHESIS_REGISTRY:
        add_node(f"hypothesis:{hyp}", "hypothesis", hyp, "Hipótese", hyp, "")
    for chart in CHART_REGISTRY:
        add_node(f"chart:{chart}", "chart", chart, "Gráfico", chart, "")

    # Liga registros de lineage coluna-a-coluna aos nós de CSV e features.
    if not lineage.empty:
        for row in lineage.to_dict(orient="records"):
            if row.get("layer") == "csv_final":
                table = str(row.get("table", ""))
                column = str(row.get("column", ""))
                csv_col_id = f"csv-column:{table}:{column}"
                for raw_col in _split_cell(row.get("raw_columns")):
                    for raw_node in raw_column_lookup.get(raw_col, []):
                        add_edge(raw_node, csv_col_id, "linhagem")
                for chart in _split_cell(row.get("charts")):
                    add_edge(csv_col_id, f"chart:{chart}", "aparece")

    return {"nodes": list(nodes.values()), "edges": list(edges.values())}


def _story(outputs: dict[str, pd.DataFrame]) -> dict[str, Any]:
    return {
        "decisionSummary": _records(outputs.get("decision_summary.csv", pd.DataFrame())),
        "hypothesisRegister": _records(outputs.get("final_hypothesis_register.csv", pd.DataFrame())),
        "headline": "Grão e silagem contam histórias diferentes; por isso a conclusão precisa separar os pares.",
        "paragraphs": [
            "No par grão, o sinal temporal favorece o convencional. O 4.0 não aparece como melhor no conjunto porque a vantagem esperada não se sustentou no NDVI médio semanal e o solo exposto aparece como bloco de risco importante nas semanas-problema.",
            "No par silagem, o 4.0 sustenta vantagem temporal de NDVI, mas o fechamento não é simplesmente positivo: o outlook aponta risco de chegar abaixo do par, com risco de motor e sinais operacionais pesando na leitura.",
            "As quedas e recuperações fazem sentido quando a série é lida como trajetória. O pipeline marca queda relevante, baixo vigor e drivers candidatos; depois compara a frequência desses drivers nas semanas ruins contra o restante do ciclo.",
            "O que está sustentado hoje: há evidência de drivers associados às semanas-problema e H1 é suportada para silagem. O que ainda limita a conclusão: causalidade não está fechada, GeoTIFF pixel a pixel não está no pacote local, solo e custos ainda não fecham uma prova econômica/agronômica definitiva.",
        ],
    }


def _column_profile(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for column in frame.columns:
        series = frame[column].head(5000)
        rows.append(
            {
                "column": column,
                "dtype": str(series.dtype),
                "nullPct": float(series.isna().mean()) if len(series) else 0.0,
                "unique": int(series.nunique(dropna=True)),
                "examples": " | ".join(series.dropna().astype(str).drop_duplicates().head(5).tolist()),
            }
        )
    return rows


def _safe_source(module: str, function: str) -> str:
    try:
        return get_function_source(module, function)
    except Exception as exc:
        return f"# Código indisponível: {exc}"


def _asdict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, "__dict__"):
        return dict(value.__dict__)
    return dict(value)


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    clean = frame.replace({pd.NA: None}).where(pd.notna(frame), None)
    return clean.to_dict(orient="records")


def _split_cell(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if pd.isna(value):
        return []
    return [part.strip() for part in str(value).split("|") if part.strip()]


def _split_feature_sources(value: Any) -> list[str]:
    parts: list[str] = []
    for cell in _split_cell(value):
        parts.extend(part.strip() for part in re.split(r"\s*(?:\+|,|;|/)\s*", cell) if part.strip())
    return list(dict.fromkeys(parts))


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value) if not isinstance(value, (dict, list, tuple)) else False:
        return None
    return value


def _build_graph(raw_catalog: pd.DataFrame, raw_columns: pd.DataFrame, outputs: dict[str, pd.DataFrame], lineage: pd.DataFrame) -> dict[str, Any]:
    nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}
    y_positions = {"rawFile": 0, "rawColumn": 0, "intermediate": 0, "feature": 0, "driver": 0, "csv": 0, "csvColumn": 0, "hypothesis": 0, "chart": 0}
    stage_x = {
        "rawFile": 0,
        "rawColumn": 330,
        "intermediate": 690,
        "feature": 1050,
        "driver": 1050,
        "csv": 1410,
        "csvColumn": 1770,
        "hypothesis": 2130,
        "chart": 2130,
    }
    wrap_after = {"rawFile": 7, "rawColumn": 12, "intermediate": 7, "feature": 8, "driver": 8, "csv": 8, "csvColumn": 12, "hypothesis": 4, "chart": 6}

    def add_node(node_id: str, node_type: str, label: str, subtitle: str = "", ref: str = "", search: str = "") -> None:
        if node_id in nodes:
            return
        idx = y_positions[node_type]
        y_positions[node_type] += 1
        row = idx % wrap_after[node_type]
        lane = idx // wrap_after[node_type]
        nodes[node_id] = {
            "id": node_id,
            "type": node_type,
            "label": label,
            "subtitle": subtitle,
            "ref": ref,
            "search": search,
            "stage": node_type,
            "position": {"x": stage_x[node_type] + lane * 252, "y": 80 + row * 118},
        }

    def add_edge(source: str, target: str, relation_type: str, *, confidence: str = "alta") -> None:
        if source == target or source not in nodes or target not in nodes:
            return
        relation = EDGE_RELATIONS[relation_type]
        edge_id = f"{source}->{target}:{relation_type}"
        edges[edge_id] = {
            "id": edge_id,
            "source": source,
            "target": target,
            "sourceHandle": "out",
            "targetHandle": "in",
            "relationType": relation_type,
            "humanLabel": relation["label"],
            "description": relation["description"],
            "confidence": confidence,
            "showLabel": False,
            "label": relation["label"],
        }

    for row in raw_catalog.to_dict(orient="records"):
        source_key = str(row.get("source_key", ""))
        safe_search = " ".join(
            str(row.get(key, ""))
            for key in ("source_key", "source_group", "description", "kind", "column_names", "temporal_min", "temporal_max")
            if row.get(key) is not None
        )
        add_node(f"raw-file:{source_key}", "rawFile", source_key, str(row.get("source_group", "")), source_key, safe_search)

    raw_column_lookup: dict[str, list[str]] = {}
    for row in raw_columns.to_dict(orient="records"):
        source_key = str(row.get("source_key", ""))
        column = str(row.get("column", ""))
        node_id = f"raw-column:{source_key}:{column}"
        raw_column_lookup.setdefault(column, []).append(node_id)
        safe_search = " ".join(
            str(row.get(key, ""))
            for key in ("source_key", "source_group", "column", "documentation", "documentation_status", "pipeline_usage", "practical_interpretation")
            if row.get(key) is not None
        )
        add_node(node_id, "rawColumn", column, source_key, f"{source_key}.{column}", safe_search)
        add_edge(f"raw-file:{source_key}", node_id, "contains_column")

    output_column_lookup: dict[str, list[str]] = {}
    for csv_name, frame in outputs.items():
        for column in frame.columns:
            output_column_lookup.setdefault(str(column), []).append(f"csv-column:{csv_name}:{column}")

    feature_names = set(FEATURE_REGISTRY)
    pending_output_feature_edges: list[tuple[str, str]] = []

    for name, spec in INTERMEDIATE_TABLE_REGISTRY.items():
        add_node(f"intermediate:{name}", "intermediate", name, spec.function, name, spec.description)
    for hyp in HYPOTHESIS_REGISTRY:
        add_node(f"hypothesis:{hyp}", "hypothesis", hyp, "Hipótese", hyp, "")
    for chart in CHART_REGISTRY:
        add_node(f"chart:{chart}", "chart", chart, "Gráfico", chart, "")

    for name, spec in FEATURE_REGISTRY.items():
        add_node(f"feature:{name}", "feature", name, spec.feature_type, name, spec.definition)
        add_edge(f"intermediate:{spec.table_where_born}", f"feature:{name}", "creates_feature")
        for table in spec.appears_in_tables:
            if table == spec.table_where_born:
                continue
            add_edge(f"feature:{name}", f"intermediate:{table}", "feeds_table")
        for csv_name in spec.appears_in_csvs:
            if csv_name in outputs:
                add_node(f"csv:{csv_name}", "csv", csv_name, "CSV final", csv_name, "")
                add_edge(f"feature:{name}", f"csv:{csv_name}", "feeds_csv")
        for hyp in spec.related_hypotheses:
            add_edge(f"feature:{name}", f"hypothesis:{hyp}", "supports_hypothesis")
        for raw_col in spec.source_columns:
            for raw_node in raw_column_lookup.get(raw_col, []):
                add_edge(raw_node, f"feature:{name}", "raw_origin")
                source_key = raw_node.split(":", 2)[1]
                add_edge(f"raw-file:{source_key}", f"feature:{name}", "raw_origin", confidence="media")
        for upstream in _split_feature_sources(spec.source_columns):
            if upstream in feature_names and upstream != name:
                add_edge(f"feature:{upstream}", f"feature:{name}", "creates_feature", confidence="alta")
            for csv_col_id in output_column_lookup.get(upstream, []):
                pending_output_feature_edges.append((csv_col_id, f"feature:{name}"))

    for name, doc in DRIVER_DOCUMENTATION.items():
        add_node(f"driver:{name}", "driver", name, getattr(doc, "flag_feature", ""), name, getattr(doc, "definition", ""))
        flag = getattr(doc, "flag_feature", "")
        if flag in FEATURE_REGISTRY:
            add_edge(f"feature:{flag}", f"driver:{name}", "driver_from_flag")
        for hyp in getattr(doc, "hypotheses", []):
            add_edge(f"driver:{name}", f"hypothesis:{hyp}", "supports_hypothesis")
        for chart in getattr(doc, "charts", []):
            add_edge(f"driver:{name}", f"chart:{chart}", "generates_chart")

    for name in outputs:
        add_node(f"csv:{name}", "csv", name, "CSV final", name, "")
        for column in outputs[name].columns:
            col_id = f"csv-column:{name}:{column}"
            add_node(col_id, "csvColumn", column, name, f"{name}.{column}", "")
            add_edge(f"csv:{name}", col_id, "contains_column")

    for source, target in pending_output_feature_edges:
        add_edge(source, target, "lineage", confidence="media")

    for name, spec in CSV_REGISTRY.items():
        if name in outputs:
            for dep in spec.dependencies:
                if dep in INTERMEDIATE_TABLE_REGISTRY:
                    add_edge(f"intermediate:{dep}", f"csv:{name}", "generates_csv")
            for hyp in spec.related_hypotheses:
                add_edge(f"csv:{name}", f"hypothesis:{hyp}", "supports_hypothesis")
            for chart in spec.related_charts:
                add_edge(f"csv:{name}", f"chart:{chart}", "generates_chart")

    if not lineage.empty:
        for row in lineage.to_dict(orient="records"):
            if row.get("layer") != "csv_final":
                continue
            table = str(row.get("table", ""))
            column = str(row.get("column", ""))
            csv_col_id = f"csv-column:{table}:{column}"
            confidence = str(row.get("mapping_confidence", "media"))
            for raw_col in _split_cell(row.get("raw_columns")):
                for raw_node in raw_column_lookup.get(raw_col, []):
                    add_edge(raw_node, csv_col_id, "lineage", confidence=confidence)

    return {"nodes": list(nodes.values()), "edges": list(edges.values()), "relations": EDGE_RELATIONS}


def _correlations(outputs: dict[str, pd.DataFrame], lineage: pd.DataFrame) -> list[dict[str, Any]]:
    frame = outputs.get("weekly_correlations.csv", pd.DataFrame())
    if frame.empty:
        return []
    records: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        feature = str(row.get("feature", ""))
        target = str(row.get("analysis_target", ""))
        feature_lineage = _lineage_for_name(lineage, feature)
        target_lineage = _lineage_for_name(lineage, target)
        pearson = _safe_float(row.get("pearson_r"))
        p_value = _safe_float(row.get("pearson_p"))
        aliases = _correlation_aliases(feature, feature_lineage)
        records.append(
            {
                **row,
                "id": f"correlation:{target}:{row.get('comparison_pair')}:{feature}",
                "dataframe": "transition_model_frame.csv",
                "filter": f"analysis_target == {target}; comparison_pair == {row.get('comparison_pair')}",
                "period": _period_from_outputs(outputs.get("transition_model_frame.csv", pd.DataFrame()), "week_start"),
                "originFeature": feature_lineage,
                "originTarget": target_lineage,
                "humanInterpretation": _correlation_interpretation(feature, target, pearson, p_value, row.get("direction"), row.get("strength"), row.get("observations")),
                "caveat": "Correlação de Pearson mede associação linear; não prova causalidade.",
                "aliases": aliases,
                "search_terms": " | ".join(
                    [
                        feature,
                        target,
                        str(row.get("comparison_pair", "")),
                        *aliases,
                        str(feature_lineage.get("raw_columns", "") if feature_lineage else ""),
                        str(feature_lineage.get("upstream_columns", "") if feature_lineage else ""),
                    ]
                ),
            }
        )
    return records


def _audit_index(outputs: dict[str, pd.DataFrame], data_dir: Path) -> list[dict[str, Any]]:
    timeline = outputs.get("ndvi_phase_timeline.csv", pd.DataFrame())
    if timeline.empty:
        return []
    events = outputs.get("ndvi_events.csv", pd.DataFrame())
    pair_gaps = outputs.get("pair_weekly_gaps.csv", pd.DataFrame())
    ndvi_clean = outputs.get("ndvi_clean.csv", pd.DataFrame())
    image_lookup = _ndvi_image_lookup(data_dir)
    records: list[dict[str, Any]] = []
    flag_columns = [column for column in timeline.columns if column.endswith("_flag") or column in {"risk_flag_count"}]
    for row in timeline.head(300).to_dict(orient="records"):
        season_id = str(row.get("season_id", ""))
        week_start = str(row.get("week_start", ""))
        comparison_pair = str(row.get("comparison_pair", ""))
        flags = [flag for flag in flag_columns if _truthy(row.get(flag))]
        record = {
            "id": f"audit:{season_id}:{week_start}",
            "season_id": season_id,
            "area_label": row.get("area_label"),
            "comparison_pair": comparison_pair,
            "treatment": row.get("treatment"),
            "week_start": week_start,
            "date": row.get("date"),
            "finalRow": row,
            "activeFlags": flags,
            "context": _audit_context(row, flags),
            "image": _nearest_image(image_lookup.get(season_id, []), week_start),
            "intermediateRows": {
                "ndvi_events": _matching_rows(events, season_id, week_start, limit=8),
                "pair_weekly_gaps": _matching_pair_rows(pair_gaps, comparison_pair, week_start, limit=4),
            },
            "rawRows": {
                "ndvi_clean": _matching_rows(ndvi_clean, season_id, week_start, limit=8),
            },
        }
        records.append(record)
    return records


def _entity_index(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    entities: dict[str, dict[str, Any]] = {}
    collections = {
        "rawFile": payload.get("rawFiles", []),
        "feature": payload.get("features", []),
        "driver": payload.get("drivers", []),
        "intermediate": payload.get("intermediateTables", []),
        "csv": payload.get("finalCsvs", []),
        "hypothesis": payload.get("hypotheses", []),
        "chart": payload.get("charts", []),
    }
    for kind, rows in collections.items():
        for item in rows:
            item_id = item.get("id") or item.get("key") or item.get("name")
            if item_id:
                entities[str(item_id)] = {"kind": kind, "item": item}
    for item in payload.get("rawColumns", []):
        source_key = item.get("source_key")
        column = item.get("column")
        if source_key and column:
            entities[f"raw-column:{source_key}:{column}"] = {"kind": "rawColumn", "item": item}
    for row in payload.get("lineage", []):
        if row.get("layer") == "csv_final":
            entities[f"csv-column:{row.get('table')}:{row.get('column')}"] = {"kind": "csvColumn", "item": row}
    return entities


def _lineage_for_name(lineage: pd.DataFrame, name: str) -> dict[str, Any] | None:
    if lineage.empty or not name:
        return None
    match = lineage[lineage["column"].astype(str).eq(name)]
    if match.empty and name == "delta_ndvi_seguinte":
        match = lineage[lineage["column"].astype(str).eq("target_next_ndvi_delta")]
    if match.empty:
        return None
    row = match.iloc[0].to_dict()
    return {
        "lineage_id": row.get("lineage_id"),
        "column": row.get("column"),
        "table": row.get("table"),
        "raw_columns": row.get("raw_columns"),
        "upstream_columns": row.get("upstream_columns"),
        "definition": row.get("definition"),
        "mapping_status": row.get("mapping_status"),
    }


def _correlation_aliases(feature: str, lineage_row: dict[str, Any] | None) -> list[str]:
    terms = {feature}
    lower = feature.lower()
    lineage_text = " ".join(
        str((lineage_row or {}).get(key, ""))
        for key in ["raw_columns", "upstream_columns", "definition"]
    ).lower()
    if lower in {"soil_pct", "soil_pct_week", "high_soil_flag"} or "solo exposto" in lineage_text or "b1_pct_solo" in lineage_text:
        terms.update(["solo_exposto", "solo exposto", "soil_pct", "soil_pct_week", "b1_pct_solo", "high_soil_flag"])
    if lower in {"ndvi_mean", "ndvi_mean_week", "target_next_ndvi_delta"} or "b1_mean" in lineage_text:
        terms.update(["ndvi", "b1_mean", "ndvi_mean_week"])
    for driver, doc in DRIVER_DOCUMENTATION.items():
        driver_terms = {driver, doc.title, doc.flag_feature, *doc.source_columns}
        if lower in {str(term).lower() for term in driver_terms}:
            terms.update(str(term) for term in driver_terms if term)
        if any(str(term).lower() in lineage_text for term in driver_terms):
            terms.update(str(term) for term in driver_terms if term)
    if lower.startswith("engine_") or "engine_" in lineage_text or "fuel_" in lineage_text:
        doc = DRIVER_DOCUMENTATION.get("risco_de_motor")
        if doc:
            terms.update(["risco_de_motor", doc.title, doc.flag_feature, *doc.source_columns, "engine_temp_hot_share_week"])
    return sorted(term for term in terms if term)


def _correlation_interpretation(feature: str, target: str, pearson: float | None, p_value: float | None, direction: Any, strength: Any, observations: Any) -> str:
    if pearson is None:
        return f"Não há observações suficientes para interpretar a relação entre {feature} e {target}."
    direction_text = "aumenta junto" if pearson > 0 else "se move em sentido oposto"
    p_text = "p-value indisponível" if p_value is None else f"p-value={p_value:.4f}"
    return (
        f"Pearson r={pearson:.3f}: {feature} {direction_text} de {target}. "
        f"A força foi classificada como {strength}; direção={direction}; n={observations}; {p_text}. "
        "Use como evidência associativa, não como causalidade fechada."
    )


def _period_from_outputs(frame: pd.DataFrame, column: str) -> str:
    if frame.empty or column not in frame.columns:
        return "período não identificado no CSV de origem"
    parsed = pd.to_datetime(frame[column], errors="coerce").dropna()
    if parsed.empty:
        return "período não identificado no CSV de origem"
    return f"{parsed.min().date()} a {parsed.max().date()}"


def _safe_float(value: Any) -> float | None:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _ndvi_image_lookup(data_dir: Path) -> dict[str, list[dict[str, str]]]:
    lookup: dict[str, list[dict[str, str]]] = {}
    for path in sorted(data_dir.rglob("ndvi_raw_*.jpg")):
        parts = path.stem.replace("ndvi_raw_", "").rsplit("_", 1)
        if len(parts) != 2:
            continue
        season_id, date = parts
        lookup.setdefault(season_id, []).append(
            {
                "date": date,
                "path": str(path),
                "url": "/@fs/" + str(path.resolve()).replace("\\", "/"),
            }
        )
    return lookup


def _nearest_image(images: list[dict[str, str]], week_start: str) -> dict[str, str] | None:
    if not images or not week_start:
        return None
    week = pd.to_datetime(week_start, errors="coerce")
    if pd.isna(week):
        return images[0]
    best = min(images, key=lambda item: abs((pd.to_datetime(item["date"]) - week).days))
    return best


def _matching_rows(frame: pd.DataFrame, season_id: str, week_start: str, *, limit: int) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    subset = frame.copy()
    if season_id and "season_id" in subset.columns:
        subset = subset[subset["season_id"].astype(str).eq(season_id)]
    if week_start and "week_start" in subset.columns:
        subset = subset[subset["week_start"].astype(str).str.slice(0, 10).eq(str(week_start)[:10])]
    return _records(subset.head(limit))


def _matching_pair_rows(frame: pd.DataFrame, comparison_pair: str, week_start: str, *, limit: int) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    subset = frame.copy()
    if comparison_pair and "comparison_pair" in subset.columns:
        subset = subset[subset["comparison_pair"].astype(str).eq(comparison_pair)]
    if week_start and "week_start" in subset.columns:
        subset = subset[subset["week_start"].astype(str).str.slice(0, 10).eq(str(week_start)[:10])]
    return _records(subset.head(limit))


def _audit_context(row: dict[str, Any], flags: list[str]) -> str:
    ndvi = row.get("ndvi_mean_week")
    area = row.get("area_label")
    week = row.get("week_start")
    flag_text = ", ".join(flags) if flags else "sem flags críticas ativas"
    return f"Semana {week} em {area}: NDVI médio semanal={ndvi}; flags={flag_text}."


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or pd.isna(value):
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "sim", "yes"}


def _story(outputs: dict[str, pd.DataFrame]) -> dict[str, Any]:
    return {
        "decisionSummary": _records(outputs.get("decision_summary.csv", pd.DataFrame())),
        "hypothesisRegister": _records(outputs.get("final_hypothesis_register.csv", pd.DataFrame())),
        "headline": "Grão e silagem contam histórias diferentes; por isso a conclusão precisa separar os pares.",
        "paragraphs": [
            "No par grão, o sinal temporal favorece o convencional. O 4.0 não aparece como melhor no conjunto porque a vantagem esperada não se sustentou no NDVI médio semanal e o solo exposto aparece como bloco de risco importante nas semanas-problema.",
            "No par silagem, o 4.0 sustenta vantagem temporal de NDVI, mas o fechamento não é simplesmente positivo: o outlook aponta risco de chegar abaixo do par, com risco de motor e sinais operacionais pesando na leitura.",
            "As quedas e recuperações fazem sentido quando a série é lida como trajetória. O pipeline marca queda relevante, baixo vigor e drivers candidatos; depois compara a frequência desses drivers nas semanas ruins contra o restante do ciclo.",
            "O que está sustentado hoje: há evidência de drivers associados às semanas-problema e H1 é suportada para silagem. O que ainda limita a conclusão: causalidade não está fechada, GeoTIFF pixel a pixel não está no pacote local, solo e custos ainda não fecham uma prova econômica/agronômica definitiva.",
        ],
    }


if __name__ == "__main__":
    main()
