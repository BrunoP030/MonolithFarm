from dashboard.lineage.registry import (
    CHART_REGISTRY,
    CSV_REGISTRY,
    FEATURE_REGISTRY,
    HYPOTHESIS_REGISTRY,
    INTERMEDIATE_TABLE_REGISTRY,
)
from dashboard.lineage.docs_registry import DRIVER_DOCUMENTATION

_RUNTIME_EXPORTS = {
    "ResolvedPaths",
    "build_raw_file_catalog",
    "build_workspace_and_outputs",
    "get_function_source",
    "load_resolved_paths",
}

__all__ = [
    "CHART_REGISTRY",
    "CSV_REGISTRY",
    "FEATURE_REGISTRY",
    "DRIVER_DOCUMENTATION",
    "HYPOTHESIS_REGISTRY",
    "INTERMEDIATE_TABLE_REGISTRY",
    "ResolvedPaths",
    "build_raw_file_catalog",
    "build_workspace_and_outputs",
    "get_function_source",
    "load_resolved_paths",
]


def __getattr__(name: str):
    """Carrega dependencias pesadas do pipeline somente quando necessario."""

    if name in _RUNTIME_EXPORTS:
        from dashboard.lineage import runtime

        return getattr(runtime, name)
    raise AttributeError(f"module 'dashboard.lineage' has no attribute {name!r}")
