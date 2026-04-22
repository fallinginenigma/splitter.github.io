"""Salience (weight) computation and override management."""
from __future__ import annotations

import numpy as np
import pandas as pd

HIERARCHY_LEVELS = ["Country", "SMO Category", "Brand", "Sub Brand", "Form"]

SPLIT_KEYS: dict[str, list[str]] = {
    "Country": ["Country"],
    "SMO Category": ["Country", "SMO Category"],
    "Brand": ["Country", "SMO Category", "Brand"],
    "Sub Brand": ["Country", "SMO Category", "Brand", "Sub Brand"],
    "Form": ["Country", "SMO Category", "Brand", "Sub Brand", "Form"],
    "SFU_v": ["Country", "SMO Category", "Brand", "Sub Brand", "Form", "SFU_v"],
}

# GBB Type → split behaviour rules.
# Keys are the exact GBB Type strings as they appear in the SAS sheet.
# Each entry defines:
#   split_level : hierarchy level to use when splitting (must be a key in SPLIT_KEYS)
#   action      : one of "split" | "exceptions" | "ignore"
#                 "split"      → normal split using salience
#                 "exceptions" → route user to Exception list to pick/confirm SKUs
#                 "ignore"     → exclude from split; add to exception list with note
#   user_defined: if True, ask user to select the split level (overrides split_level default)
GBB_TYPE_RULES: dict[str, dict] = {
    "Base": {
        "split_level": "Brand",
        "action": "split",
        "user_defined": False,
        "description": "Base volume — split across the brand.",
    },
    "Brand Building Activities": {
        "split_level": "Brand",
        "action": "split",
        "user_defined": False,
        "description": "Split across the brand.",
    },
    "Promotions - Go To Market": {
        "split_level": "Form",
        "action": "exceptions",
        "user_defined": False,
        "description": "Use all SKUs or ask which SKUs — routes to Exception list.",
    },
    "New Channels": {
        "split_level": "Form",
        "action": "exceptions",
        "user_defined": False,
        "description": "Ask which SKUs — routes to Exception list.",
    },
    "Initiatives": {
        "split_level": "Form",
        "action": "ignore",
        "user_defined": False,
        "description": "Ignore / add to exception list; prompt user to provide inputs.",
    },
    "Pricing Strategy": {
        "split_level": "Brand",
        "action": "split",
        "user_defined": False,
        "description": "Split across the brand.",
    },
    "Market Trend": {
        "split_level": "Sub Brand",
        "action": "split",
        "user_defined": False,
        "description": "Split at Sub Brand level.",
    },
    "Customer Inventory Strategy": {
        "split_level": "Brand",
        "action": "split",
        "user_defined": True,
        "description": "Split at Brand level — split level defined by user.",
    },
}


def _match_gbb_type(raw: str) -> str | None:
    """
    Match a raw GBB Type string (which may include a leading number prefix like
    '0.Base', '1. Brand Building Activities', '2.Promotions - Go To Market')
    to a canonical key in GBB_TYPE_RULES.

    Returns the matching key, or None if no match.
    """
    if not raw:
        return None
    raw_strip = raw.strip()
    # Exact match first
    if raw_strip in GBB_TYPE_RULES:
        return raw_strip
    # Strip leading number+dot prefix (e.g. "1. " or "0." or "8.")
    import re
    without_prefix = re.sub(r"^\d+\s*\.\s*", "", raw_strip).strip()
    if without_prefix in GBB_TYPE_RULES:
        return without_prefix
    # Fuzzy substring: check if any canonical key appears as substring
    for key in GBB_TYPE_RULES:
        if key.lower() in without_prefix.lower() or without_prefix.lower() in key.lower():
            return key
    return None


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
    sfuv_df: pd.DataFrame,
    basis: pd.Series,
    split_level: str,
    sfuv_col: str,
    hier_col_map: dict[str, str],
    global_exclusions: set[str] | None = None,
    overrides: dict | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    """
    Compute salience weights.

    Returns:
      salience_df: DataFrame with columns = group_keys + [sfuv_col, 'basis', 'salience', 'flag']
      blocking_groups: list of dicts describing zero/missing basis groups
    """
    global_exclusions = global_exclusions or set()
    overrides = overrides or {}

    group_keys = [hier_col_map.get(k, k) for k in SPLIT_KEYS[split_level] if k != "SFU_v"]
    if split_level == "SFU_v":
        group_keys = [hier_col_map.get(k, k) for k in SPLIT_KEYS["Form"]]

    working = sfuv_df.copy()
    working["_basis"] = basis.values if len(basis) == len(working) else np.nan

    # Apply global exclusions
    sfuv_mapped = hier_col_map.get("SFU_v", "SFU_v")
    if sfuv_mapped in working.columns:
        working = working[~working[sfuv_mapped].isin(global_exclusions)].copy()

    agg_keys = [k for k in group_keys if k in working.columns]
    if sfuv_mapped in working.columns:
        agg_keys = agg_keys + [sfuv_mapped]
    if not agg_keys or sfuv_mapped not in agg_keys:
        return pd.DataFrame(), []

    working = (
        working.groupby(agg_keys, sort=False, dropna=False)["_basis"]
        .mean()
        .reset_index()
    )

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
                sfuv_val = row.get(sfuv_mapped, "")
                override_key = (tuple(group_vals), sfuv_val)
                sal = overrides.get(override_key, np.nan)
                rows.append({**group_id, sfuv_mapped: sfuv_val, "basis": row["_basis"], "salience": sal, "flag": "manual_override" if not pd.isna(sal) else "blocked"})
        else:
            for _, row in grp.iterrows():
                sfuv_val = row.get(sfuv_mapped, "")
                override_key = (tuple(group_vals), sfuv_val)
                if override_key in overrides:
                    sal = overrides[override_key]
                    flag = "manual_override"
                else:
                    b = pd.to_numeric(row["_basis"], errors="coerce")
                    sal = (b / total) if not pd.isna(b) else 0.0
                    flag = "computed"
                rows.append({**group_id, sfuv_mapped: sfuv_val, "basis": row["_basis"], "salience": sal, "flag": flag})

    salience_df = pd.DataFrame(rows)
    return salience_df, blocking_groups


def compute_equal_salience(
    sfuv_df: pd.DataFrame,
    split_level: str,
    sfuv_col: str,
    hier_col_map: dict[str, str],
    global_exclusions: set[str] | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    """
    Compute equal (1/N) salience weights per split group.

    Each SKU within a group receives 1 / (number of SKUs in that group).
    Returns the same schema as compute_salience so callers are interchangeable.
    """
    global_exclusions = global_exclusions or set()
    group_keys_logical = [k for k in SPLIT_KEYS[split_level] if k != "SFU_v"]
    group_keys = [hier_col_map.get(k, k) for k in group_keys_logical]
    sfuv_mapped = hier_col_map.get("SFU_v", sfuv_col)

    working = sfuv_df.copy()
    if sfuv_mapped in working.columns:
        working = working[~working[sfuv_mapped].isin(global_exclusions)].copy()

    valid_group_keys = [k for k in group_keys if k in working.columns]
    if not valid_group_keys or working.empty or sfuv_mapped not in working.columns:
        return pd.DataFrame(), []

    working = working[valid_group_keys + [sfuv_mapped]].drop_duplicates().copy()

    rows = []

    for group_vals, grp in working.groupby(valid_group_keys, sort=False, dropna=False):
        if not isinstance(group_vals, tuple):
            group_vals = (group_vals,)
        group_id = dict(zip(valid_group_keys, group_vals))
        n = grp[sfuv_mapped].astype(str).nunique()
        sal = 1.0 / n if n > 0 else 0.0
        for _, row in grp.iterrows():
            sfuv_val = row.get(sfuv_mapped, "") if sfuv_mapped in grp.columns else ""
            rows.append({**group_id, sfuv_mapped: sfuv_val, "basis": 1.0, "salience": sal, "flag": "equal"})

    return pd.DataFrame(rows), []


def normalize_salience(salience_df: pd.DataFrame, group_keys: list[str], sfuv_col: str) -> pd.DataFrame:
    """Force salience within each group to sum to 1."""
    df = salience_df.copy()
    df["salience"] = pd.to_numeric(df["salience"], errors="coerce").fillna(0)
    totals = df.groupby(group_keys)["salience"].transform("sum")
    mask = totals > 0
    df.loc[mask, "salience"] = df.loc[mask, "salience"] / totals[mask]
    df.loc[~mask, "salience"] = 0.0
    return df
