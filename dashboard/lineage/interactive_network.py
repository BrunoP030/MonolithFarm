from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd

from dashboard.lineage.manifest import CRITICAL_LINEAGE_TARGETS


GROUP_META = {
    "bruto": {"stage": 0, "stage_label": "1. Brutos", "group_label": "Fonte bruta", "color": "#f97316"},
    "raw_column": {"stage": 0, "stage_label": "1. Brutos", "group_label": "Coluna bruta", "color": "#ea580c"},
    "upstream": {"stage": 1, "stage_label": "2. Intermediárias", "group_label": "Intermediária", "color": "#0ea5e9"},
    "feature": {"stage": 2, "stage_label": "3. Feature / Driver", "group_label": "Feature", "color": "#2563eb"},
    "driver": {"stage": 2, "stage_label": "3. Feature / Driver", "group_label": "Driver", "color": "#dc2626"},
    "intermediario": {"stage": 3, "stage_label": "4. Tabelas", "group_label": "Tabela", "color": "#0f766e"},
    "csv_final": {"stage": 4, "stage_label": "5. CSVs", "group_label": "Coluna final", "color": "#16a34a"},
    "csv": {"stage": 4, "stage_label": "5. CSVs", "group_label": "CSV final", "color": "#16a34a"},
    "chart": {"stage": 5, "stage_label": "6. Evidências", "group_label": "Gráfico", "color": "#7c3aed"},
    "hypothesis": {"stage": 5, "stage_label": "6. Evidências", "group_label": "Hipótese", "color": "#9333ea"},
}

STAGE_X = {
    0: 120,
    1: 380,
    2: 640,
    3: 900,
    4: 1160,
    5: 1420,
}


def lineage_network_html(lineage_index: pd.DataFrame, *, focus_query: str = "", max_records: int = 72) -> str:
    """Gera um board de lineage em camadas, com pan/zoom e nós arrastáveis persistentes."""

    nodes, edges = build_lineage_network(lineage_index, focus_query=focus_query, max_records=max_records)
    payload = {
        "nodes": nodes,
        "edges": edges,
        "focus": focus_query,
        "storageKey": _storage_key(focus_query, nodes),
        "stages": _stage_payload(),
        "stats": {"nodes": len(nodes), "edges": len(edges)},
    }
    return _HTML_TEMPLATE.replace("__GRAPH_DATA__", json.dumps(payload, ensure_ascii=False))


def build_lineage_network(
    lineage_index: pd.DataFrame,
    *,
    focus_query: str = "",
    max_records: int = 72,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    if lineage_index is None or lineage_index.empty:
        return [], []

    selected = _select_records(lineage_index, focus_query=focus_query, max_records=max_records)
    column_docs = _column_docs(lineage_index)
    table_docs = _table_docs(lineage_index)
    nodes: dict[str, dict[str, Any]] = {}
    edges: set[tuple[str, str, str]] = set()
    focus_norm = focus_query.strip().lower()

    for row in selected.to_dict(orient="records"):
        layer = str(row.get("layer", "intermediario"))
        column = str(row.get("column", "")).strip()
        table = str(row.get("table", "")).strip()
        row_id = f"rawcol::{column}" if layer == "bruto" and column else str(row.get("lineage_id"))
        group = "raw_column" if layer == "bruto" else layer
        _add_node(
            nodes,
            row_id,
            label=column or row_id,
            group=group,
            subtitle=table,
            description=str(row.get("plain_language") or row.get("definition") or ""),
            table=table,
            status=str(row.get("mapping_status", "")),
            generated_by=str(row.get("generated_by", "")),
            raw_columns=_split(row.get("raw_columns")),
            upstream_columns=_split(row.get("upstream_columns")),
            downstream_csvs=_split(row.get("downstream_csvs")),
            hypotheses=_split(row.get("hypotheses")),
            limitations=_split(row.get("limitations")),
            focus=bool(focus_norm and focus_norm in json.dumps(row, ensure_ascii=False).lower()),
        )

        for raw_col in _split(row.get("raw_columns"))[:6]:
            raw_doc = column_docs.get(raw_col, {})
            raw_id = f"rawcol::{raw_col}"
            _add_node(
                nodes,
                raw_id,
                label=raw_col,
                group="raw_column",
                subtitle=raw_doc.get("table", "coluna bruta"),
                description=raw_doc.get("description", ""),
                table=raw_doc.get("table", ""),
                status=raw_doc.get("status", ""),
                raw_columns=[raw_col],
                focus=bool(focus_norm and focus_norm in raw_col.lower()),
            )
            if raw_id != row_id:
                edges.add((raw_id, row_id, "alimenta"))

        for upstream in _split(row.get("upstream_columns"))[:6]:
            upstream_doc = column_docs.get(upstream, {})
            up_id = f"up::{upstream}"
            _add_node(
                nodes,
                up_id,
                label=upstream,
                group="upstream",
                subtitle=upstream_doc.get("table", "feature intermediária"),
                description=upstream_doc.get("description", ""),
                table=upstream_doc.get("table", ""),
                status=upstream_doc.get("status", ""),
                upstream_columns=[upstream],
                focus=bool(focus_norm and focus_norm in upstream.lower()),
            )
            if up_id != row_id:
                edges.add((up_id, row_id, "alimenta"))

        if table:
            table_group = "csv" if table.endswith(".csv") else "intermediario"
            table_id = f"table::{table}"
            _add_node(
                nodes,
                table_id,
                label=table,
                group=table_group,
                subtitle="tabela/arquivo",
                description=table_docs.get(table, ""),
                table=table,
            )
            edges.add((table_id, row_id, "contém") if group == "csv_final" else (row_id, table_id, "materializa"))

        for csv_name in _split(row.get("downstream_csvs"))[:5]:
            csv_id = f"csv::{csv_name}"
            _add_node(
                nodes,
                csv_id,
                label=csv_name,
                group="csv",
                subtitle="CSV final",
                description=table_docs.get(csv_name, ""),
                table=csv_name,
                downstream_csvs=[csv_name],
            )
            edges.add((row_id, csv_id, "aparece em"))

        for hyp in _split(row.get("hypotheses"))[:4]:
            hyp_id = f"hyp::{hyp}"
            _add_node(nodes, hyp_id, label=hyp, group="hypothesis", subtitle="hipótese", description=f"Hipótese analítica {hyp}.")
            edges.add((row_id, hyp_id, "sustenta"))

        for chart in _split(row.get("charts"))[:3]:
            chart_id = f"chart::{chart}"
            _add_node(nodes, chart_id, label=chart, group="chart", subtitle="gráfico", description=f"Visualização que depende de {column}.")
            edges.add((row_id, chart_id, "visualiza"))

    _layout_nodes(nodes)
    valid_ids = set(nodes)
    edge_rows = [{"from": a, "to": b, "label": label} for a, b, label in sorted(edges) if a in valid_ids and b in valid_ids]
    return list(nodes.values()), edge_rows


def _select_records(lineage_index: pd.DataFrame, *, focus_query: str, max_records: int) -> pd.DataFrame:
    frame = lineage_index.copy()
    focus = focus_query.strip().lower()
    critical_ids = {item["lineage_id"] for item in CRITICAL_LINEAGE_TARGETS}
    critical_names = {item["target"] for item in CRITICAL_LINEAGE_TARGETS}
    if not focus:
        selected = frame[
            frame["lineage_id"].isin(critical_ids)
            | frame["column"].isin(critical_names)
            | frame["layer"].isin(["driver"])
        ].copy()
        return selected.head(max_records)

    text = frame.astype(str).agg(" ".join, axis=1).str.lower()
    exact = frame[
        frame["column"].astype(str).str.lower().eq(focus)
        | frame["lineage_id"].astype(str).str.lower().str.contains(focus, na=False)
        | frame["table"].astype(str).str.lower().str.contains(focus, na=False)
    ].copy()
    primary = exact if not exact.empty else frame[text.str.contains(focus, na=False)].copy()
    if primary.empty:
        return frame.head(max_records)

    tokens: set[str] = set(primary["column"].astype(str).tolist())
    for column in ["raw_columns", "upstream_columns", "downstream_csvs", "hypotheses", "charts"]:
        for cell in primary.get(column, pd.Series(dtype=str)).astype(str):
            tokens.update(_split(cell))
    table_mask = frame["table"].astype(str).str.lower().eq(focus) if focus.endswith(".csv") else pd.Series(False, index=frame.index)
    token_mask = frame["column"].astype(str).isin(tokens) | table_mask
    selected = pd.concat([primary, frame[token_mask]], ignore_index=True).drop_duplicates("lineage_id")
    layer_rank = {"driver": 0, "feature": 1, "csv_final": 2, "intermediario": 3, "bruto": 4}
    selected["_rank"] = selected["layer"].map(layer_rank).fillna(9)
    return selected.sort_values(["_rank", "table", "column"]).drop(columns=["_rank"]).head(max_records)


def _column_docs(lineage_index: pd.DataFrame) -> dict[str, dict[str, str]]:
    docs: dict[str, dict[str, str]] = {}
    for row in lineage_index.to_dict(orient="records"):
        column = str(row.get("column", "")).strip()
        if not column:
            continue
        description = str(row.get("plain_language") or row.get("definition") or "").strip()
        current = docs.get(column)
        if current and len(current.get("description", "")) >= len(description):
            continue
        docs[column] = {"description": description, "table": str(row.get("table", "")), "status": str(row.get("mapping_status", ""))}
    return docs


def _table_docs(lineage_index: pd.DataFrame) -> dict[str, str]:
    docs: dict[str, str] = {}
    for table, group in lineage_index.groupby("table", dropna=False):
        definitions = [str(value) for value in group["definition"].dropna().head(4).tolist() if str(value).strip()]
        docs[str(table)] = " | ".join(definitions[:2])
    return docs


def _add_node(nodes: dict[str, dict[str, Any]], node_id: str, *, label: str, group: str, subtitle: str = "", **extra: Any) -> None:
    if node_id in nodes:
        current = nodes[node_id]
        current["focus"] = bool(current.get("focus") or extra.get("focus"))
        if not current.get("description") and extra.get("description"):
            current["description"] = extra["description"]
        for key in ["raw_columns", "upstream_columns", "downstream_csvs", "hypotheses", "limitations"]:
            merged = sorted(set(current.get(key, []) or []) | set(extra.get(key, []) or []))
            current[key] = merged
        return
    meta = GROUP_META.get(group, GROUP_META["intermediario"])
    nodes[node_id] = {
        "id": node_id,
        "label": label,
        "group": group,
        "group_label": meta["group_label"],
        "stage": meta["stage"],
        "stage_label": meta["stage_label"],
        "color": meta["color"],
        "subtitle": subtitle,
        **extra,
    }


def _layout_nodes(nodes: dict[str, dict[str, Any]]) -> None:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for node in nodes.values():
        grouped.setdefault(int(node.get("stage", 3)), []).append(node)
    for stage, stage_nodes in grouped.items():
        stage_nodes.sort(key=lambda item: (not bool(item.get("focus")), str(item.get("group")), str(item.get("label"))))
        count = len(stage_nodes)
        spacing = 112
        start_y = -((count - 1) * spacing) / 2
        for index, node in enumerate(stage_nodes):
            node["x"] = STAGE_X.get(stage, 1060)
            node["y"] = round(start_y + index * spacing, 2)
            node["width"] = 210 if len(str(node.get("label", ""))) > 22 else 188
            node["height"] = 66


def _stage_payload() -> list[dict[str, Any]]:
    stages: list[dict[str, Any]] = []
    for stage, x in STAGE_X.items():
        label = next(meta["stage_label"] for meta in GROUP_META.values() if meta["stage"] == stage)
        stages.append({"stage": stage, "x": x, "label": label})
    return stages


def _storage_key(focus_query: str, nodes: list[dict[str, Any]]) -> str:
    digest = hashlib.sha1("|".join(sorted(node["id"] for node in nodes)).encode("utf-8")).hexdigest()[:12]
    focus = focus_query.strip().lower().replace(" ", "_") or "geral"
    return f"monolithfarm.lineage.board.v4.{focus}.{digest}"


def _split(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if value is None or pd.isna(value):
        return []
    text = str(value)
    if not text or text.lower() == "nan":
        return []
    return [part.strip() for part in text.split("|") if part.strip()]


_HTML_TEMPLATE = r"""
<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8" />
<style>
  :root { --ink:#0f172a; --muted:#64748b; --border:#d8e3f1; --panel:#ffffff; --soft:#f8fafc; }
  html, body { margin:0; padding:0; background:transparent; font-family:Inter,Aptos,"Segoe UI",system-ui,sans-serif; color:var(--ink); }
  .shell { height:800px; box-sizing:border-box; }
  .stage { position:relative; height:100%; overflow:hidden; border:1px solid var(--border); border-radius:24px; background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%); box-shadow:inset 0 1px 0 rgba(255,255,255,.9); }
  .toolbar { position:absolute; left:18px; right:18px; top:16px; display:flex; gap:10px; align-items:center; z-index:5; }
  .toolbar input, .toolbar select { height:40px; border:1px solid #cbd5e1; border-radius:999px; background:rgba(255,255,255,.96); padding:0 14px; color:#0f172a; outline:none; font-size:13px; box-shadow:0 6px 16px rgba(15,23,42,.05); }
  .toolbar input { flex:1; min-width:180px; }
  .toolbar button, .pill { height:34px; border:1px solid #cbd5e1; border-radius:999px; background:#fff; padding:0 11px; color:#334155; font-weight:780; font-size:12px; white-space:nowrap; box-shadow:0 6px 16px rgba(15,23,42,.05); }
  .legend { position:absolute; left:18px; bottom:15px; z-index:5; display:flex; gap:6px; flex-wrap:wrap; max-width:calc(100% - 36px); }
  .legend span { display:inline-flex; align-items:center; gap:5px; border:1px solid #dbe4ef; background:rgba(255,255,255,.92); border-radius:999px; padding:4px 8px; color:#475569; font-size:11px; }
  .dot { width:8px; height:8px; border-radius:999px; display:inline-block; }
  svg { width:100%; height:100%; display:block; cursor:grab; }
  svg.panning { cursor:grabbing; }
  .lane rect { fill:rgba(255,255,255,.44); stroke:#dbeafe; stroke-width:1; }
  .lane text { fill:#64748b; font-size:13px; font-weight:900; letter-spacing:.04em; text-transform:uppercase; }
  .edge { stroke:#94a3b8; stroke-width:1.25; opacity:.16; pointer-events:none; fill:none; }
  .edge.active { stroke:#0891b2; stroke-width:2.8; opacity:.82; }
  .edge.hidden { opacity:0; }
  .node { cursor:grab; }
  .node.dragging { cursor:grabbing; }
  .node rect { fill:rgba(255,255,255,.98); stroke-width:2.2; filter:drop-shadow(0 12px 18px rgba(15,23,42,.10)); }
  .node .stripe { opacity:.13; stroke-width:0; }
  .node text { pointer-events:none; fill:#0f172a; font-size:12px; font-weight:850; }
  .node .sub { fill:#64748b; font-size:10.5px; font-weight:720; }
  .node .tag { fill:#475569; font-size:9.5px; font-weight:820; text-transform:uppercase; letter-spacing:.05em; }
  .node.dim { opacity:.28; }
  .node.active rect.main { stroke-width:3.4; filter:drop-shadow(0 16px 32px rgba(8,145,178,.28)); }
  .node:hover rect.main { filter:drop-shadow(0 16px 30px rgba(15,23,42,.18)); }
  .detail { position:absolute; right:18px; top:74px; bottom:58px; width:340px; z-index:6; border:1px solid var(--border); border-radius:22px; background:rgba(255,255,255,.97); padding:16px; overflow:auto; box-sizing:border-box; box-shadow:0 22px 42px rgba(15,23,42,.12); backdrop-filter:blur(14px); }
  .detail .kind { color:#64748b; font-size:12px; font-weight:900; letter-spacing:.08em; text-transform:uppercase; }
  .detail h2 { margin:.28rem 0 .45rem 0; font-size:22px; line-height:1.12; }
  .detail p { color:#334155; font-size:13.5px; line-height:1.48; }
  .field { border-top:1px solid #e2e8f0; padding-top:11px; margin-top:11px; }
  .field b { display:block; color:#475569; font-size:12px; margin-bottom:5px; }
  .chips { display:flex; flex-wrap:wrap; gap:5px; }
  .chip { border:1px solid #dbe4ef; background:#f8fafc; border-radius:999px; padding:3px 8px; font-size:11px; color:#0f172a; }
  .instructions { color:#64748b; font-size:12px; line-height:1.45; background:#f8fafc; border:1px solid #e2e8f0; border-radius:14px; padding:10px 12px; margin-top:12px; }
  @media(max-width:780px){ .toolbar{flex-wrap:wrap}.toolbar input{flex-basis:100%}.detail{left:18px;right:18px;top:auto;bottom:58px;width:auto;height:230px} }
</style>
</head>
<body>
<div class="shell">
  <div class="stage">
    <div class="toolbar">
      <input id="search" placeholder="Buscar nó, coluna, CSV, driver..." />
      <select id="edgeMode"><option value="focus" selected>Relações do nó</option><option value="all">Todas as relações</option></select>
      <button id="reset">Centralizar</button>
      <button id="clear">Resetar nós</button>
      <span class="pill" id="counter"></span>
    </div>
    <svg id="svg" viewBox="-20 -440 1560 880">
      <g id="viewport"><g id="lanes"></g><g id="edges"></g><g id="nodes"></g></g>
    </svg>
    <div class="legend" id="legend"></div>
    <aside class="detail" id="detail">
      <div class="kind">Board de lineage</div>
      <h2>Selecione um nó</h2>
      <p>Use scroll para zoom, arraste o fundo para navegar e arraste os nós para ajustar o mapa. As posições ficam salvas para esta busca.</p>
    </aside>
  </div>
</div>
<script>
const payload = __GRAPH_DATA__;
const allNodes = payload.nodes.map(node => ({...node}));
const allEdges = payload.edges.map(edge => ({...edge}));
const byId = new Map(allNodes.map(node => [node.id, node]));
const svg = document.getElementById("svg");
const viewport = document.getElementById("viewport");
const lanesLayer = document.getElementById("lanes");
const edgeLayer = document.getElementById("edges");
const nodeLayer = document.getElementById("nodes");
const detail = document.getElementById("detail");
const search = document.getElementById("search");
const edgeMode = document.getElementById("edgeMode");
const counter = document.getElementById("counter");
let selectedId = null;
let transform = {x:0,y:0,k:1};
let draggingNode = null;
let panning = null;
let currentNodes = allNodes.slice();
let currentEdges = allEdges.slice();
let savedPositions = loadPositions();

function loadPositions(){try{return JSON.parse(localStorage.getItem(payload.storageKey)||"{}");}catch(_){return {};}}
function savePositions(){try{localStorage.setItem(payload.storageKey,JSON.stringify(savedPositions));}catch(_){}}
function escapeHtml(value){return String(value??"").replace(/[&<>"']/g,ch=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#039;"}[ch]));}
function short(value,max=30){value=String(value??"");return value.length>max?value.slice(0,max-1)+"…":value;}
function nodeText(node){return [node.label,node.subtitle,node.description,node.table,node.status,node.generated_by,...(node.raw_columns||[]),...(node.upstream_columns||[]),...(node.downstream_csvs||[]),...(node.hypotheses||[])].join(" ").toLowerCase();}
function connected(id){const ids=new Set([id]); allEdges.forEach(edge=>{if(edge.from===id)ids.add(edge.to); if(edge.to===id)ids.add(edge.from);}); return ids;}
function applySavedPositions(){allNodes.forEach(node=>{const p=savedPositions[node.id]; if(p&&Number.isFinite(p.x)&&Number.isFinite(p.y)){node.x=p.x; node.y=p.y;}});}
function filterNodes(){
  const q=search.value.trim().toLowerCase();
  if(!q){currentNodes=allNodes.slice(); currentEdges=allEdges.slice(); return;}
  const seedIds=allNodes.filter(node=>nodeText(node).includes(q)).map(node=>node.id);
  const keep=new Set(seedIds);
  seedIds.forEach(id=>connected(id).forEach(next=>keep.add(next)));
  currentNodes=allNodes.filter(node=>keep.has(node.id));
  currentEdges=allEdges.filter(edge=>keep.has(edge.from)&&keep.has(edge.to));
  if(selectedId && !keep.has(selectedId)) selectedId=seedIds[0]||currentNodes[0]?.id||null;
}
function initSelection(){
  if(selectedId && byId.has(selectedId)) return;
  selectedId = allNodes.find(n=>n.focus)?.id || allNodes.find(n=>String(n.label).toLowerCase()===String(payload.focus).toLowerCase())?.id || allNodes[0]?.id || null;
}
function renderLanes(){
  lanesLayer.innerHTML=payload.stages.map(stage=>`<g class="lane">
    <rect x="${stage.x-110}" y="-360" width="220" height="710" rx="22"></rect>
    <text x="${stage.x-93}" y="-327">${escapeHtml(stage.label)}</text>
  </g>`).join("");
}
function edgePath(a,b){
  const ax=a.x+(a.width||190)/2, ay=a.y;
  const bx=b.x-(b.width||190)/2, by=b.y;
  const mid=Math.max(70,Math.abs(bx-ax)*.46);
  return `M ${ax} ${ay} C ${ax+mid} ${ay}, ${bx-mid} ${by}, ${bx} ${by}`;
}
function render(){
  applySavedPositions();
  filterNodes();
  initSelection();
  renderLanes();
  const visibleIds=new Set(currentNodes.map(n=>n.id));
  const neighborIds=selectedId?connected(selectedId):new Set();
  const focusOnly=edgeMode.value==="focus";
  const visibleEdges=currentEdges.filter(edge=>visibleIds.has(edge.from)&&visibleIds.has(edge.to));
  counter.textContent=`${currentNodes.length} nós · ${visibleEdges.length} relações`;
  edgeLayer.innerHTML=visibleEdges.map(edge=>{
    const a=byId.get(edge.from), b=byId.get(edge.to); if(!a||!b) return "";
    const active=edge.from===selectedId||edge.to===selectedId;
    const hidden=focusOnly && !active;
    return `<path class="edge ${active?'active':''} ${hidden?'hidden':''}" d="${edgePath(a,b)}"><title>${escapeHtml(edge.label||"")}</title></path>`;
  }).join("");
  nodeLayer.innerHTML=currentNodes.map(node=>{
    const active=node.id===selectedId;
    const dim=focusOnly && selectedId && !active && !neighborIds.has(node.id);
    const width=node.width||190, height=node.height||66;
    return `<g class="node ${active?'active':''} ${dim?'dim':''}" data-id="${escapeHtml(node.id)}" transform="translate(${node.x-width/2},${node.y-height/2})">
      <rect class="main" width="${width}" height="${height}" rx="15" ry="15" stroke="${node.color}"></rect>
      <rect class="stripe" width="8" height="${height}" rx="4" fill="${node.color}"></rect>
      <text class="tag" x="17" y="17">${escapeHtml(node.group_label)}</text>
      <text x="17" y="37">${escapeHtml(short(node.label,28))}</text>
      <text class="sub" x="17" y="55">${escapeHtml(short(node.subtitle||node.stage_label,31))}</text>
    </g>`;
  }).join("");
  nodeLayer.querySelectorAll(".node").forEach(el=>{
    el.addEventListener("pointerdown",event=>startNodeDrag(event,el.dataset.id));
    el.addEventListener("click",event=>{event.stopPropagation(); selectedId=el.dataset.id; render(); showDetail(selectedId);});
  });
  showDetail(selectedId);
  applyTransform();
}
function showDetail(id){
  const node=byId.get(id);
  if(!node){detail.innerHTML='<div class="kind">Board de lineage</div><h2>Nenhum nó visível</h2><p>Ajuste a busca.</p>'; return;}
  detail.innerHTML=`<div class="kind">${escapeHtml(node.stage_label)} · ${escapeHtml(node.group_label)}</div>
    <h2>${escapeHtml(node.label)}</h2>
    <p>${escapeHtml(node.description||"Sem descrição específica. Abra o detalhe Streamlit abaixo do mapa para ver exemplos reais, função e perfil.")}</p>
    ${field("Tabela/arquivo",node.table)}${field("Status",node.status)}${field("Criado por",node.generated_by)}
    ${chips("Colunas brutas",node.raw_columns)}${chips("Upstream",node.upstream_columns)}${chips("CSVs finais",node.downstream_csvs)}
    ${chips("Hipóteses",node.hypotheses)}${chips("Limitações",node.limitations)}
    <div class="instructions">Interação: arraste este nó para reposicionar. Clique em outro nó sem perder o layout salvo. Use “Resetar nós” para voltar ao alinhamento em camadas.</div>`;
}
function field(label,value){return value?`<div class="field"><b>${escapeHtml(label)}</b>${escapeHtml(value)}</div>`:"";}
function chips(label,items){if(!items||!items.length)return ""; return `<div class="field"><b>${escapeHtml(label)}</b><div class="chips">${items.map(item=>`<span class="chip">${escapeHtml(item)}</span>`).join("")}</div></div>`;}
function applyTransform(){viewport.setAttribute("transform",`translate(${transform.x} ${transform.y}) scale(${transform.k})`);}
function svgPoint(event){const pt=svg.createSVGPoint(); pt.x=event.clientX; pt.y=event.clientY; const ctm=svg.getScreenCTM().inverse(); const p=pt.matrixTransform(ctm); return {x:(p.x-transform.x)/transform.k,y:(p.y-transform.y)/transform.k,rawX:p.x,rawY:p.y};}
function startNodeDrag(event,id){event.stopPropagation(); selectedId=id; draggingNode=byId.get(id); event.currentTarget.setPointerCapture(event.pointerId); event.currentTarget.classList.add("dragging");}
svg.addEventListener("pointerdown",event=>{if(event.target.closest(".node"))return; const p=svgPoint(event); panning={x:event.clientX,y:event.clientY,tx:transform.x,ty:transform.y}; svg.classList.add("panning");});
svg.addEventListener("pointermove",event=>{
  if(draggingNode){const p=svgPoint(event); draggingNode.x=p.x; draggingNode.y=p.y; savedPositions[draggingNode.id]={x:draggingNode.x,y:draggingNode.y}; render(); return;}
  if(panning){transform.x=panning.tx+(event.clientX-panning.x); transform.y=panning.ty+(event.clientY-panning.y); applyTransform();}
});
svg.addEventListener("pointerup",()=>{if(draggingNode){savePositions(); draggingNode=null;} panning=null; svg.classList.remove("panning");});
svg.addEventListener("pointerleave",()=>{if(draggingNode){savePositions(); draggingNode=null;} panning=null; svg.classList.remove("panning");});
svg.addEventListener("wheel",event=>{event.preventDefault(); const scale=event.deltaY<0?1.11:.90; const p=svgPoint(event); transform.x=p.rawX-(p.rawX-transform.x)*scale; transform.y=p.rawY-(p.rawY-transform.y)*scale; transform.k=Math.max(.38,Math.min(2.7,transform.k*scale)); applyTransform();},{passive:false});
document.getElementById("reset").addEventListener("click",()=>{transform={x:0,y:0,k:1}; applyTransform();});
document.getElementById("clear").addEventListener("click",()=>{savedPositions={}; localStorage.removeItem(payload.storageKey); allNodes.forEach(node=>{delete node.x; delete node.y;}); location.reload();});
search.addEventListener("input",()=>{selectedId=null; render();});
edgeMode.addEventListener("change",()=>render());
document.getElementById("legend").innerHTML=[...new Map(allNodes.map(n=>[n.group,n])).values()].map(n=>`<span><i class="dot" style="background:${n.color}"></i>${escapeHtml(n.group_label)}</span>`).join("");
search.value=payload.focus||"";
render();
</script>
</body>
</html>
"""
