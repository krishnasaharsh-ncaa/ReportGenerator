from __future__ import annotations

import os
from typing import Iterable

import pandas as pd


CSV_ENCODINGS = ("utf-8", "utf-8-sig", "cp1252", "latin-1")


def read_csv_flexible(path: str, **kwargs) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in CSV_ENCODINGS:
        try:
            return pd.read_csv(path, encoding=encoding, **kwargs)
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise ValueError(
            f"Could not decode CSV {os.path.basename(path)} with encodings: {', '.join(CSV_ENCODINGS)}"
        ) from last_error
    return pd.read_csv(path, **kwargs)


def _normalize_column_name(value: object) -> str:
    return " ".join(str(value).strip().split()).lower()


def read_excel_with_detected_header(
    path: str,
    *,
    required_columns: Iterable[str],
    column_aliases: dict[str, str] | None = None,
    header_rows: Iterable[int] = range(6),
) -> pd.DataFrame:
    required = set(required_columns)
    aliases = {k.lower(): v for k, v in (column_aliases or {}).items()}
    seen_headers: list[tuple[int, list[str]]] = []

    for header_row in header_rows:
        df = pd.read_excel(path, header=header_row)
        df.columns = [str(col).strip() for col in df.columns]

        rename_map: dict[str, str] = {}
        for col in df.columns:
            normalized = _normalize_column_name(col)
            canonical = aliases.get(normalized)
            if canonical:
                rename_map[col] = canonical
        if rename_map:
            df = df.rename(columns=rename_map)

        columns = set(df.columns)
        if required.issubset(columns):
            return df

        seen_headers.append((header_row, [str(col) for col in df.columns]))

    preview = "; ".join(
        f"header={header}: {cols[:8]}" for header, cols in seen_headers
    )
    missing = sorted(required - set(seen_headers[-1][1] if seen_headers else []))
    raise ValueError(
        f"Missing required columns in {os.path.basename(path)}: {missing}. "
        f"Tried header rows {list(header_rows)}. Seen columns: {preview}"
    )
