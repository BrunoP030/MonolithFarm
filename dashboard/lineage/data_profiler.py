from __future__ import annotations

from typing import Any

import pandas as pd


def dataframe_overview(frame: pd.DataFrame) -> dict[str, Any]:
    if frame is None or frame.empty:
        return {
            "rows": 0,
            "columns": 0,
            "null_cells": 0,
            "null_ratio": 0.0,
            "numeric_columns": 0,
            "date_columns": [],
        }
    null_cells = int(frame.isna().sum().sum())
    total_cells = int(frame.shape[0] * frame.shape[1])
    numeric_columns = int(frame.select_dtypes(include=["number", "bool"]).shape[1])
    date_columns = [column for column in frame.columns if _looks_temporal(frame[column])]
    return {
        "rows": int(len(frame)),
        "columns": int(len(frame.columns)),
        "null_cells": null_cells,
        "null_ratio": float(null_cells / total_cells) if total_cells else 0.0,
        "numeric_columns": numeric_columns,
        "date_columns": date_columns,
    }


def profile_dataframe(frame: pd.DataFrame, *, max_examples: int = 5) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(
            columns=[
                "column",
                "dtype",
                "null_count",
                "null_pct",
                "unique_count_sample",
                "min",
                "max",
                "mean",
                "std",
                "examples",
            ]
        )
    rows: list[dict[str, Any]] = []
    sample = frame.head(5000)
    for column in frame.columns:
        series = sample[column]
        numeric = pd.to_numeric(series, errors="coerce")
        numeric_non_null = numeric.dropna()
        examples = [str(value) for value in series.dropna().astype(str).unique()[:max_examples]]
        null_count = int(series.isna().sum())
        row: dict[str, Any] = {
            "column": column,
            "dtype": str(frame[column].dtype),
            "null_count": null_count,
            "null_pct": float(null_count / len(series)) if len(series) else 0.0,
            "unique_count_sample": int(series.nunique(dropna=True)),
            "min": pd.NA,
            "max": pd.NA,
            "mean": pd.NA,
            "std": pd.NA,
            "examples": " | ".join(examples),
        }
        if not numeric_non_null.empty and _numeric_parse_is_meaningful(series, numeric):
            row.update(
                {
                    "min": float(numeric_non_null.min()),
                    "max": float(numeric_non_null.max()),
                    "mean": float(numeric_non_null.mean()),
                    "std": float(numeric_non_null.std()) if len(numeric_non_null) > 1 else 0.0,
                }
            )
        else:
            parsed_dates = pd.to_datetime(series, errors="coerce")
            parsed_dates = parsed_dates.dropna()
            if not parsed_dates.empty and parsed_dates.notna().mean() > 0.5:
                row.update({"min": parsed_dates.min(), "max": parsed_dates.max()})
        rows.append(row)
    return pd.DataFrame(rows)


def column_distribution(frame: pd.DataFrame, column: str, *, bins: int = 20) -> pd.DataFrame:
    if frame is None or frame.empty or column not in frame.columns:
        return pd.DataFrame()
    numeric = pd.to_numeric(frame[column], errors="coerce").dropna()
    if numeric.empty:
        counts = frame[column].astype(str).value_counts(dropna=False).head(20).reset_index()
        counts.columns = [column, "count"]
        return counts
    cut = pd.cut(numeric, bins=min(bins, max(1, numeric.nunique())), duplicates="drop")
    return cut.value_counts().sort_index().reset_index(name="count").rename(columns={"index": "range"})


def _looks_temporal(series: pd.Series) -> bool:
    if series.empty:
        return False
    name = str(series.name).lower()
    if any(token in name for token in ["date", "data", "timestamp", "week_start"]):
        return True
    parsed = pd.to_datetime(series.head(200), errors="coerce")
    return bool(parsed.notna().mean() > 0.7)


def _numeric_parse_is_meaningful(original: pd.Series, numeric: pd.Series) -> bool:
    non_null = original.dropna()
    if non_null.empty:
        return False
    return bool(numeric.notna().sum() / len(non_null) >= 0.8)
