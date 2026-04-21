from __future__ import annotations

import html
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

    for key, url in FARMLAB_DOC_ROUTES.items():
        record = _fetch_route(key, url)
        route_records.append(record)
        for asset in record.get("assets", []):
            if asset.endswith(".js"):
                script_urls.add(urllib.parse.urljoin(base_url, asset))

    bundle_records: list[dict[str, Any]] = []
    for script_url in sorted(script_urls):
        bundle_records.append(_fetch_bundle(script_url))

    cache = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "https://farm.labs.unimar.br",
        "status_note": (
            "O portal FarmLab e uma SPA. As rotas HTML retornam o shell e o conteudo util foi extraido "
            "do bundle JavaScript quando possivel. Trechos manuais complementares ficam em docs_registry.py."
        ),
        "routes": route_records,
        "bundles": bundle_records,
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
    }


def _read_url(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "MonolithFarm-Lineage-Audit/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="ignore")


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
