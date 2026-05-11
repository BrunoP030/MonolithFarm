from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description="Read a private tabular file page as JSON.")
    parser.add_argument("--path", required=True)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--query", default="")
    args = parser.parse_args()

    path = Path(args.path).resolve()
    offset = max(0, args.offset)
    limit = min(max(1, args.limit), 500)
    query = args.query.strip().lower()

    suffix = path.suffix.lower()
    if suffix == ".csv":
        result = read_csv_page(path, offset, limit, query)
    elif suffix == ".parquet":
        result = read_parquet_page(path, offset, limit, query)
    else:
        raise ValueError(f"Unsupported table extension: {suffix}")

    json.dump(result, sys.stdout, ensure_ascii=False, allow_nan=False)


def read_csv_page(path: Path, offset: int, limit: int, query: str) -> dict[str, Any]:
    sep = sniff_separator(path)
    encoding = sniff_encoding(path)
    if not query:
        return read_csv_page_window(path, offset, limit, sep, encoding)

    rows: list[dict[str, Any]] = []
    columns: list[str] = []
    total_rows = 0
    matched_rows = 0

    for chunk in pd.read_csv(path, dtype=str, encoding=encoding, sep=sep, chunksize=5000):
        chunk = chunk.where(pd.notna(chunk), None)
        if not columns:
            columns = [str(column) for column in chunk.columns]
        total_rows += len(chunk)
        if query:
            mask = chunk.astype(str).apply(lambda row: row.str.lower().str.contains(query, regex=False).any(), axis=1)
            chunk = chunk[mask]
        chunk_len = len(chunk)
        previous_rows = matched_rows if query else total_rows - chunk_len
        if query:
            matched_rows += chunk_len
        start = max(0, offset - previous_rows)
        if start < chunk_len and len(rows) < limit:
            take = chunk.iloc[start : start + (limit - len(rows))]
            rows.extend(clean_records(take))

    return {
        "columns": columns,
        "rows": rows,
        "offset": offset,
        "limit": limit,
        "totalRows": total_rows,
        "matchedRows": matched_rows if query else total_rows,
        "hasMore": offset + len(rows) < (matched_rows if query else total_rows),
        "query": query,
    }


def read_csv_page_window(path: Path, offset: int, limit: int, sep: str, encoding: str) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    has_more = False
    with path.open("r", encoding=encoding, errors="replace", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=sep)
        columns = [str(column) for column in (reader.fieldnames or [])]
        for index, row in enumerate(reader):
            if index < offset:
                continue
            if len(rows) >= limit:
                has_more = True
                break
            rows.append({str(key): (None if value == "" else value) for key, value in row.items()})
    return {
        "columns": columns,
        "rows": rows,
        "offset": offset,
        "limit": limit,
        "totalRows": None,
        "matchedRows": offset + len(rows) + (1 if has_more else 0),
        "hasMore": has_more,
        "query": "",
    }


def read_parquet_page(path: Path, offset: int, limit: int, query: str) -> dict[str, Any]:
    frame = pd.read_parquet(path)
    total_rows = len(frame)
    frame = frame.where(pd.notna(frame), None)
    if query:
        mask = frame.astype(str).apply(lambda row: row.str.lower().str.contains(query, regex=False).any(), axis=1)
        frame = frame[mask]
    matched_rows = len(frame)
    page = frame.iloc[offset : offset + limit]
    return {
        "columns": [str(column) for column in frame.columns],
        "rows": clean_records(page),
        "offset": offset,
        "limit": limit,
        "totalRows": total_rows,
        "matchedRows": matched_rows,
        "hasMore": offset + limit < matched_rows,
        "query": query,
    }


def clean_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    records = frame.to_dict(orient="records")
    clean: list[dict[str, Any]] = []
    for row in records:
        clean.append({str(key): scalar(value) for key, value in row.items()})
    return clean


def scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if pd.isna(value):
        return None
    return str(value)


def sniff_separator(path: Path) -> str:
    sample = path.read_bytes()[:8192].decode(sniff_encoding(path), errors="ignore")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except Exception:
        first_line = sample.splitlines()[0] if sample.splitlines() else ""
        return ";" if first_line.count(";") > first_line.count(",") else ","


def sniff_encoding(path: Path) -> str:
    sample = path.read_bytes()[:4096]
    for encoding in ("utf-8-sig", "utf-8", "latin1"):
        try:
            sample.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return "latin1"


if __name__ == "__main__":
    main()
