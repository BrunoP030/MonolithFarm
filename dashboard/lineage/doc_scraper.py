from __future__ import annotations

import html
import ast
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from dashboard.lineage.docs_registry import FARMLAB_DOC_ROUTES, SOURCE_DOCUMENTATION


CACHE_DIR = Path(__file__).resolve().parent / "cache"
DEFAULT_CACHE_PATH = CACHE_DIR / "farmlab_docs_cache.json"
DOC_SEARCH_TERMS = [
    "ndvi",
    "b1_valid_pixels",
    "satelite",
    "meteorologia",
    "metos",
    "cropman",
    "solo",
    "miip",
    "armadilha",
    "geotiff",
    "onesoil",
    "ekos",
    "telemetria",
    "produtividade",
]


def load_or_refresh_documentation_cache(*, force: bool = False, cache_path: Path = DEFAULT_CACHE_PATH) -> dict[str, Any]:
    if cache_path.exists() and not force:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    return refresh_documentation_cache(cache_path=cache_path)


def refresh_documentation_cache(*, cache_path: Path = DEFAULT_CACHE_PATH) -> dict[str, Any]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    route_records: list[dict[str, Any]] = []
    script_urls: set[str] = set()
    base_url = "https://farm.labs.unimar.br"
    route_urls: dict[str, str] = {"docs_root": f"{base_url}/docs", **FARMLAB_DOC_ROUTES}

    for key, url in route_urls.items():
        record = _fetch_route(key, url)
        route_records.append(record)
        for asset in record.get("assets", []):
            if asset.endswith(".js"):
                script_urls.add(urllib.parse.urljoin(base_url, asset))

    bundle_records: list[dict[str, Any]] = []
    seen_scripts: set[str] = set()
    pending_scripts = sorted(script_urls)
    while pending_scripts and len(seen_scripts) < 40:
        script_url = pending_scripts.pop(0)
        if script_url in seen_scripts:
            continue
        seen_scripts.add(script_url)
        bundle = _fetch_bundle(script_url)
        bundle_records.append(bundle)
        for route_url in bundle.get("doc_routes", []):
            key = _route_key(route_url)
            if key not in route_urls:
                route_urls[key] = route_url
        for asset_url in bundle.get("discovered_assets", []):
            if asset_url not in seen_scripts and asset_url not in pending_scripts:
                pending_scripts.append(asset_url)

    seen_route_urls = {record.get("url") for record in route_records}
    for key, url in sorted(route_urls.items()):
        if url in seen_route_urls:
            continue
        route_records.append(_fetch_route(key, url))
        seen_route_urls.add(url)

    dataset_schemas = _merge_dataset_schemas(bundle_records)

    cache = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "https://farm.labs.unimar.br",
        "status_note": (
            "O portal FarmLab e uma SPA. As rotas HTML retornam o shell e o conteudo util foi extraido "
            "do bundle JavaScript quando possivel. Rotas internas /docs foram descobertas no bundle e "
            "registradas no cache. Trechos manuais complementares ficam em docs_registry.py."
        ),
        "routes": route_records,
        "bundles": bundle_records,
        "dataset_schemas": dataset_schemas,
        "manual_sources": {
            key: {
                "title": doc.title,
                "source_group": doc.source_group,
                "summary": doc.summary,
                "practical_context": doc.practical_context,
                "farm_docs_url": doc.farm_docs_url,
                "documentation_status": doc.documentation_status,
                "relevant_excerpt": doc.relevant_excerpt,
            }
            for key, doc in SOURCE_DOCUMENTATION.items()
        },
    }
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    return cache


def build_documentation_index(cache: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for route in cache.get("routes", []):
        rows.append(
            {
                "kind": "route",
                "key": route.get("key"),
                "title": route.get("title") or route.get("key"),
                "url": route.get("url"),
                "status": route.get("status"),
                "source": "farm.labs.unimar.br",
                "text": route.get("description") or route.get("status_note") or "",
            }
        )
    for bundle in cache.get("bundles", []):
        for snippet in bundle.get("snippets", []):
            rows.append(
                {
                    "kind": "bundle_snippet",
                    "key": snippet.get("term"),
                    "title": f"Trecho JS: {snippet.get('term')}",
                    "url": bundle.get("url"),
                    "status": bundle.get("status"),
                    "source": "bundle_js_renderizado",
                    "text": snippet.get("text", ""),
                }
            )
    for dataset in cache.get("dataset_schemas", []):
        rows.append(
            {
                "kind": "dataset_schema",
                "key": dataset.get("id"),
                "title": dataset.get("title"),
                "url": dataset.get("url"),
                "status": "schema_extraido_do_bundle",
                "source": dataset.get("source_bundle", "bundle_js_renderizado"),
                "text": " ".join(str(dataset.get(key, "")) for key in ["desc", "rows", "area"] if dataset.get(key)),
            }
        )
        for column in dataset.get("cols", []):
            rows.append(
                {
                    "kind": "dataset_column",
                    "key": column.get("col"),
                    "title": f"{dataset.get('title')} / {column.get('col')}",
                    "url": dataset.get("url"),
                    "status": "coluna_extraida_do_bundle",
                    "source": dataset.get("source_bundle", "bundle_js_renderizado"),
                    "text": column.get("desc", ""),
                }
            )
    for key, doc in cache.get("manual_sources", {}).items():
        rows.append(
            {
                "kind": "manual_source",
                "key": key,
                "title": doc.get("title"),
                "url": doc.get("farm_docs_url"),
                "status": doc.get("documentation_status"),
                "source": "docs_registry.py",
                "text": " ".join(
                    value
                    for value in [doc.get("summary", ""), doc.get("practical_context", ""), doc.get("relevant_excerpt", "")]
                    if value
                ),
            }
        )
    return pd.DataFrame(rows)


def documentation_for_source_group(source_group: str, cache: dict[str, Any]) -> dict[str, Any]:
    manual = cache.get("manual_sources", {}).get(source_group)
    if manual:
        return manual
    return {
        "title": source_group,
        "summary": "Sem documentação externa específica vinculada automaticamente.",
        "practical_context": "Use o preview real e o catálogo de colunas para auditar esta fonte.",
        "farm_docs_url": "",
        "documentation_status": "sem_documentacao_externa_encontrada",
        "relevant_excerpt": "",
    }


def _fetch_route(key: str, url: str) -> dict[str, Any]:
    try:
        text = _read_url(url)
    except Exception as exc:  # pragma: no cover - network fallback
        return {
            "key": key,
            "url": url,
            "status": "erro_ao_acessar",
            "error": str(exc),
            "assets": [],
            "status_note": "Nao foi possivel acessar a rota durante a geracao do cache.",
        }
    assets = re.findall(r'(?:src|href)="([^"]+)"', text)
    title = _extract_tag(text, "title")
    description = _extract_meta_description(text)
    is_spa_shell = bool(assets) and "id=\"root\"" in text or "/assets/index-" in text
    return {
        "key": key,
        "url": url,
        "status": "shell_spa_detectado" if is_spa_shell else "html_extraido",
        "title": title,
        "description": description,
        "assets": assets,
        "html_length": len(text),
        "status_note": (
            "Rota retorna shell SPA; conteudo principal precisa ser extraido do bundle JavaScript."
            if is_spa_shell
            else "Rota retornou HTML com potencial conteudo direto."
        ),
    }


def _fetch_bundle(url: str) -> dict[str, Any]:
    try:
        text = _read_url(url)
    except Exception as exc:  # pragma: no cover - network fallback
        return {"url": url, "status": "erro_ao_acessar_bundle", "error": str(exc), "snippets": []}
    snippets: list[dict[str, Any]] = []
    for term in DOC_SEARCH_TERMS:
        for snippet in _extract_snippets(text, term, limit=3):
            snippets.append({"term": term, "text": snippet})
    return {
        "url": url,
        "status": "bundle_extraido",
        "length": len(text),
        "snippets": snippets,
        "datasets": _extract_dataset_schemas(text, source_url=url),
        "doc_routes": _discover_doc_routes(text, url),
        "discovered_assets": _discover_js_assets(text, url),
    }


def _read_url(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "MonolithFarm-Lineage-Audit/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="ignore")


def _discover_js_assets(text: str, current_url: str) -> list[str]:
    base_url = urllib.parse.urljoin(current_url, "/")
    assets = sorted(set(re.findall(r"assets/[A-Za-z0-9_.-]+\.js", text)))
    return [urllib.parse.urljoin(base_url, asset) for asset in assets]


def _discover_doc_routes(text: str, current_url: str) -> list[str]:
    base = urllib.parse.urljoin(current_url, "/")
    routes = sorted(set(re.findall(r"/docs/[A-Za-z0-9_./-]+", text)))
    return [urllib.parse.urljoin(base, route) for route in routes if route.startswith("/docs/")][:120]


def _route_key(url: str) -> str:
    path = urllib.parse.urlparse(url).path.strip("/")
    key = path.replace("docs/", "").replace("/", "_").replace("-", "_")
    return key or "docs_root"


def _merge_dataset_schemas(bundle_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for bundle in bundle_records:
        for dataset in bundle.get("datasets", []):
            dataset_id = str(dataset.get("id", ""))
            if not dataset_id:
                continue
            if dataset_id not in merged or len(dataset.get("cols", [])) > len(merged[dataset_id].get("cols", [])):
                merged[dataset_id] = dataset
    return [merged[key] for key in sorted(merged)]


def _extract_dataset_schemas(text: str, *, source_url: str) -> list[dict[str, Any]]:
    marker = "G={"
    if marker not in text or "cols:[" not in text:
        return []
    start = text.find(marker) + 2
    end = text.find(",q=()=>", start)
    if end == -1:
        end = _balanced_end(text, start, "{", "}")
    if end == -1:
        return []
    body = text[start:end]
    datasets: list[dict[str, Any]] = []
    for dataset_id, block in _dataset_blocks(body):
        dataset = {
            "id": dataset_id,
            "title": _extract_prop_string(block, "title"),
            "desc": _extract_prop_string(block, "desc"),
            "access": _extract_prop_string(block, "access"),
            "updated": _extract_prop_string(block, "updated"),
            "rows": _extract_prop_string(block, "rows"),
            "area": _extract_prop_string(block, "area"),
            "files": _extract_records(block, "files", ["name", "format", "desc"]),
            "cols": _extract_records(block, "cols", ["col", "tipo", "desc"]),
            "guides": _extract_records(block, "guides", ["label", "to"]),
            "url": _dataset_url(dataset_id),
            "source_bundle": source_url,
        }
        if dataset["title"] or dataset["cols"] or dataset["files"]:
            datasets.append(dataset)
    return datasets


def _dataset_blocks(body: str) -> list[tuple[str, str]]:
    blocks: list[tuple[str, str]] = []
    cursor = 0
    while True:
        match = re.search(r"([A-Za-z_][A-Za-z0-9_]*):\{", body[cursor:])
        if not match:
            break
        dataset_id = match.group(1)
        block_start = cursor + match.end() - 1
        block_end = _balanced_end(body, block_start, "{", "}")
        if block_end == -1:
            break
        blocks.append((dataset_id, body[block_start : block_end + 1]))
        cursor = block_end + 1
    return blocks


def _balanced_end(text: str, start: int, open_char: str, close_char: str) -> int:
    depth = 0
    quote = ""
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if char in {'"', "'"}:
            quote = char
            continue
        if char == open_char:
            depth += 1
        elif char == close_char:
            depth -= 1
            if depth == 0:
                return index
    return -1


def _extract_prop_string(block: str, prop: str) -> str:
    match = re.search(rf"{re.escape(prop)}:((?:\"(?:\\.|[^\"])*\")|(?:'(?:\\.|[^'])*'))", block)
    return _decode_js_string(match.group(1)) if match else ""


def _extract_records(block: str, prop: str, fields: list[str]) -> list[dict[str, str]]:
    array_start = block.find(f"{prop}:[")
    if array_start == -1:
        return []
    bracket_start = block.find("[", array_start)
    bracket_end = _balanced_end(block, bracket_start, "[", "]")
    if bracket_end == -1:
        return []
    array_text = block[bracket_start + 1 : bracket_end]
    records: list[dict[str, str]] = []
    cursor = 0
    while True:
        item_start = array_text.find("{", cursor)
        if item_start == -1:
            break
        item_end = _balanced_end(array_text, item_start, "{", "}")
        if item_end == -1:
            break
        item = array_text[item_start : item_end + 1]
        record = {field: _extract_prop_string(item, field) for field in fields}
        if any(record.values()):
            records.append(record)
        cursor = item_end + 1
    return records


def _decode_js_string(value: str) -> str:
    try:
        return str(ast.literal_eval(value))
    except Exception:
        if value[:1] in {'"', "'"} and value[-1:] == value[:1]:
            value = value[1:-1]
        value = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), value)
        return html.unescape(value.replace("\\n", " ").replace("\\t", " ").replace('\\"', '"').replace("\\'", "'"))


def _dataset_url(dataset_id: str) -> str:
    route_map = {
        "satelite": FARMLAB_DOC_ROUTES["satelite"],
        "meteorologia": FARMLAB_DOC_ROUTES["meteorologia"],
        "solo": FARMLAB_DOC_ROUTES["solo"],
        "ekos_camadas": FARMLAB_DOC_ROUTES["ekos_camadas"],
        "miip": FARMLAB_DOC_ROUTES["miip"],
    }
    return route_map.get(dataset_id, f"https://farm.labs.unimar.br/docs/dados/{dataset_id}")


def _extract_snippets(text: str, term: str, *, limit: int = 3, window: int = 700) -> list[str]:
    snippets: list[str] = []
    for match in re.finditer(re.escape(term), text, flags=re.IGNORECASE):
        start = max(0, match.start() - window)
        end = min(len(text), match.end() + window)
        snippet = text[start:end]
        snippet = _clean_js_text(snippet)
        if snippet and snippet not in snippets:
            snippets.append(snippet)
        if len(snippets) >= limit:
            break
    return snippets


def _clean_js_text(value: str) -> str:
    value = html.unescape(value)
    value = value.replace("\\n", " ").replace("\\t", " ")
    value = re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), value)
    value = re.sub(r"[{}\\[\\]();=]+", " ", value)
    value = re.sub(r"[,]+", ", ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _extract_tag(text: str, tag: str) -> str:
    match = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", text, flags=re.IGNORECASE | re.DOTALL)
    return html.unescape(match.group(1).strip()) if match else ""


def _extract_meta_description(text: str) -> str:
    match = re.search(r'<meta[^>]+name="description"[^>]+content="([^"]+)"', text, flags=re.IGNORECASE)
    return html.unescape(match.group(1).strip()) if match else ""
