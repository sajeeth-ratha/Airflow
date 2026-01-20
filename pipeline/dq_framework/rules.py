# pipeline/dq_framework/rules.py

from typing import List, Dict, Optional, Any
import pandas as pd


def required_columns(df: pd.DataFrame, columns: List[str]) -> Optional[Dict[str, Any]]:
    missing = [c for c in columns if c not in df.columns]
    return {"missing": missing} if missing else None


def no_nulls(df: pd.DataFrame, columns: List[str]) -> Optional[Dict[str, Any]]:
    bad = {}
    for c in columns:
        if c in df.columns:
            n = int(df[c].isna().sum())
            if n > 0:
                bad[c] = n
    return bad or None


def unique_key(df: pd.DataFrame, columns: List[str]) -> Optional[Dict[str, Any]]:
    if not all(c in df.columns for c in columns):
        return {"error": f"Key columns missing: {columns}"}
    dup = int(df.duplicated(subset=columns).sum())
    return {"duplicate_rows": dup, "key": columns} if dup else None


def ranges(df: pd.DataFrame, rules: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    errors = {}
    for col, bounds in (rules or {}).items():
        if col not in df.columns:
            errors[col] = {"error": "column_missing"}
            continue
        s = df[col]
        if not pd.api.types.is_numeric_dtype(s):
            errors[col] = {"error": "not_numeric"}
            continue

        if bounds.get("min") is not None:
            bad = int((s < bounds["min"]).sum())
            if bad:
                errors.setdefault(col, {})["below_min"] = bad

        if bounds.get("max") is not None:
            bad = int((s > bounds["max"]).sum())
            if bad:
                errors.setdefault(col, {})["above_max"] = bad

    return errors or None


def warn_if_negative(df: pd.DataFrame, columns: List[str]) -> Optional[Dict[str, Any]]:
    warns = {}
    for c in columns or []:
        if c in df.columns and pd.api.types.is_numeric_dtype(df[c]):
            bad = int((df[c] < 0).sum())
            if bad:
                warns[c] = bad
    return warns or None
