"""Salience (weight) computation and override management."""
from __future__ import annotations

import numpy as np
import pandas as pd

HIERARCHY_LEVELS = ["Ctry", "SMO Category", "Brand", "Sub Brand", "Form"]

SPLIT_KEYS: dict[str, list[str]] = {
    "Ctry": ["Ctry"],
    "SMO Category": ["Ctry", "SMO Category"],
    "Brand": ["Ctry", "SMO Category", "Brand"],
    "Sub Brand": ["Ctry", "SMO Category", "Brand", "Sub Brand"],
    "Form": ["Ctry", "SMO Category", "Brand", "Sub Brand", "Form"],
    "SKU": ["Ctry", "SMO Category", "Brand", "Sub Brand", "Form", "SKU"],
}


def compute_basis(
    df: pd.DataFrame,
    month_cols: list[str],
    mode: str = "last_3",
    selected_months: list[str] | None = None,
) -> pd.Series:
    """
    Compute a scalar basis value per SKU row.

    mode: 'last_3' | 'last_6' | 'last_9' | 'last_12' | 'selected'
    selected_months: used when mode == 'selected'
    Returns a Series indexed like df.
    """
    available = [c for c in month_cols if c in df.columns]
    if not available:
        return pd.Series(np.nan, index=df.index)

    if mode == "selected" and selected_months:
        cols = [c for c in selected_months if c in df.columns]
    elif mode.startswith("last_"):
        n = int(mode.split("_")[1])
        cols = available[-n:] if len(available) >= n else available
    else:
        cols = available

    if not cols:
        return pd.Series(np.nan, index=df.index)

    sub = df[cols].apply(pd.to_numeric, errors="coerce")
    return sub.mean(axis=1)  # ignores NaN


def compute_salience(
    sku_df: pd.DataFrame,
    basis: pd.Series,
    split_level: str,
    sku_col: str,
    hier_col_map: dict[str, str],
    global_exclusions: set[str] | None = None,
    overrides: dict | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    """
    Compute salience weights.

    Returns:
      salience_df: DataFrame with columns = group_keys + [sku_col, 'basis', 'salience', 'flag']
      blocking_groups: list of dicts describing zero/missing basis groups
    """
    global_exclusions = global_exclusions or set()
    overrides = overrides or {}

    group_keys = [hier_col_map.get(k, k) for k in SPLIT_KEYS[split_level] if k != "SKU"]
    if split_level == "SKU":
        group_keys = [hier_col_map.get(k, k) for k in SPLIT_KEYS["Form"]]

    working = sku_df.copy()
    working["_basis"] = basis.values if len(basis) == len(working) else np.nan

    # Apply global exclusions
    sku_mapped = hier_col_map.get("SKU", "SKU")
    if sku_mapped in working.columns:
        working = working[~working[sku_mapped].isin(global_exclusions)].copy()

    rows = []
    blocking_groups = []

    for group_vals, grp in working.groupby(group_keys, sort=False, dropna=False):
        if not isinstance(group_vals, tuple):
            group_vals = (group_vals,)
        group_id = dict(zip(group_keys, group_vals))

        grp_basis = pd.to_numeric(grp["_basis"], errors="coerce")
        total = grp_basis.sum(min_count=1)

        if pd.isna(total) or total == 0:
            blocking_groups.append({
                "group": group_id,
                "reason": "zero_or_missing_basis",
                "n_skus": len(grp),
            })
            for _, row in grp.iterrows():
                sku_val = row.get(sku_mapped, "")
                override_key = (tuple(group_vals), sku_val)
                sal = overrides.get(override_key, np.nan)
                rows.append({**group_id, sku_mapped: sku_val, "basis": row["_basis"], "salience": sal, "flag": "manual_override" if not pd.isna(sal) else "blocked"})
        else:
            for _, row in grp.iterrows():
                sku_val = row.get(sku_mapped, "")
                override_key = (tuple(group_vals), sku_val)
                if override_key in overrides:
                    sal = overrides[override_key]
                    flag = "manual_override"
                else:
                    b = pd.to_numeric(row["_basis"], errors="coerce")
                    sal = (b / total) if not pd.isna(b) else 0.0
                    flag = "computed"
                rows.append({**group_id, sku_mapped: sku_val, "basis": row["_basis"], "salience": sal, "flag": flag})

    salience_df = pd.DataFrame(rows)
    return salience_df, blocking_groups


def normalize_salience(salience_df: pd.DataFrame, group_keys: list[str], sku_col: str) -> pd.DataFrame:
    """Force salience within each group to sum to 1."""
    df = salience_df.copy()
    df["salience"] = pd.to_numeric(df["salience"], errors="coerce").fillna(0)
    totals = df.groupby(group_keys)["salience"].transform("sum")
    mask = totals > 0
    df.loc[mask, "salience"] = df.loc[mask, "salience"] / totals[mask]
    df.loc[~mask, "salience"] = 0.0
    return df
