from __future__ import annotations

import csv
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


def read_csv_with_detected_header(
    path: str,
    *,
    required_columns: Iterable[str],
    column_aliases: dict[str, str] | None = None,
    delimiters: Iterable[str] = (",", ";", "\t"),
    header_rows: Iterable[int] = range(15),
    **kwargs,
) -> pd.DataFrame:
    header_row_candidates = list(header_rows)
    required = set(required_columns)
    aliases = {k.lower(): v for k, v in (column_aliases or {}).items()}
    attempts: list[str] = []
    last_error: Exception | None = None

    for encoding in CSV_ENCODINGS:
        try:
            with open(path, "r", encoding=encoding, newline="") as handle:
                raw_lines = handle.readlines()
        except UnicodeDecodeError as exc:
            last_error = exc
            continue

        for delimiter in delimiters:
            for header_row, line in enumerate(raw_lines[: max(header_row_candidates, default=0) + 1]):
                parsed = next(csv.reader([line], delimiter=delimiter), [])
                normalized_fields = {_normalize_column_name(field) for field in parsed if str(field).strip()}
                canonical_fields = {aliases.get(field, field) for field in normalized_fields}
                if required.issubset(canonical_fields):
                    try:
                        df = pd.read_csv(
                            path,
                            encoding=encoding,
                            sep=delimiter,
                            skiprows=header_row,
                            engine="python",
                            on_bad_lines="skip",
                            **kwargs,
                        )
                    except (UnicodeDecodeError, pd.errors.ParserError) as exc:
                        last_error = exc
                        attempts.append(
                            f"encoding={encoding}, sep={repr(delimiter)}, skiprows={header_row}, raw-header-match parse failed"
                        )
                        continue

                    df.columns = [str(col).strip() for col in df.columns]
                    rename_map: dict[str, str] = {}
                    for col in df.columns:
                        normalized = _normalize_column_name(col)
                        canonical = aliases.get(normalized)
                        if canonical:
                            rename_map[col] = canonical
                    if rename_map:
                        df = df.rename(columns=rename_map)
                    df = _coalesce_duplicate_columns(df)
                    if required.issubset(set(df.columns)):
                        return df

    for encoding in CSV_ENCODINGS:
        for delimiter in delimiters:
            for header_row in header_row_candidates:
                try:
                    df = pd.read_csv(
                        path,
                        encoding=encoding,
                        sep=delimiter,
                        engine="python",
                        on_bad_lines="skip",
                        skiprows=header_row,
                        **kwargs,
                    )
                except (UnicodeDecodeError, pd.errors.ParserError) as exc:
                    last_error = exc
                    continue

                df.columns = [str(col).strip() for col in df.columns]
                rename_map: dict[str, str] = {}
                for col in df.columns:
                    normalized = _normalize_column_name(col)
                    canonical = aliases.get(normalized)
                    if canonical:
                        rename_map[col] = canonical
                if rename_map:
                    df = df.rename(columns=rename_map)
                df = _coalesce_duplicate_columns(df)

                if required.issubset(set(df.columns)):
                    return df

                attempts.append(
                    f"encoding={encoding}, sep={repr(delimiter)}, skiprows={header_row}, cols={list(df.columns)[:8]}"
                )

    preview = "; ".join(attempts[:10])
    raise ValueError(
        f"Could not locate CSV header for {os.path.basename(path)} with required columns {sorted(required)}. "
        f"Tried: {preview}"
    ) from last_error


def _normalize_column_name(value: object) -> str:
    return " ".join(str(value).strip().split()).lower()


def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.is_unique:
        return df

    collapsed = pd.DataFrame(index=df.index)
    seen: set[str] = set()
    for column in df.columns:
        if column in seen:
            continue
        same_named = df.loc[:, df.columns == column]
        if same_named.shape[1] == 1:
            collapsed[column] = same_named.iloc[:, 0]
        else:
            collapsed[column] = same_named.bfill(axis=1).iloc[:, 0]
        seen.add(column)
    return collapsed


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
        df = _coalesce_duplicate_columns(df)

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
