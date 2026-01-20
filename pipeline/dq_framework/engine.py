# pipeline/dq_framework/engine.py

from typing import Dict, Any
import yaml
import pandas as pd

from pipeline.dq_framework.exceptions import DataQualityError
from pipeline.dq_framework import rules as R

SUPPORTED = {
    "required_columns": R.required_columns,
    "no_nulls": R.no_nulls,
    "unique_key": R.unique_key,
    "ranges": R.ranges,
    "warn_if_negative": R.warn_if_negative,  # warning only
}

def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def run_checks(df: pd.DataFrame, dataset_name: str, config: Dict[str, Any], verbose: bool = True):
    cfg = (config or {}).get(dataset_name)
    if not cfg:
        raise ValueError(f"No config for dataset '{dataset_name}'")

    errors = {}
    warnings = {}

    for check_name, params in cfg.items():
        fn = SUPPORTED.get(check_name)
        if not fn:
            raise ValueError(f"Unsupported check '{check_name}'")

        result = fn(df, params)
        if result:
            if check_name.startswith("warn_"):
                warnings[check_name] = result
            else:
                errors[check_name] = result

        if verbose:
            print(f"[DQ] {dataset_name} | {check_name}: {'OK' if not result else 'ISSUES'}")

    if errors:
        raise DataQualityError(dataset_name, errors)

    return {"status": "PASS", "warnings": warnings}
