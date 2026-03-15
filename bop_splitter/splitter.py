"""Core split engine: apply salience + exceptions to produce SKU-level output."""
from __future__ import annotations

import numpy as np
import pandas as pd

from .salience import SPLIT_KEYS
from .exceptions import ExceptionStore


def run_split(
    sas_df: pd.DataFrame,
    sfuv_df: pd.DataFrame,
    salience_df: pd.DataFrame,
    sas_months: list[str],
    split_level: str,
    bb_id_col: str,
    sfuv_col: str,
    hier_col_map: dict[str, str],  # logical -> actual column name
    exc_store: ExceptionStore,
    bb_split_levels: dict[str, str] | None = None,  # bb_id -> split_level override
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform the split.

    When bb_split_levels is provided, each Building Block is split at its own
    granularity level (looked up by bb_id, falling back to split_level global).

    Returns:
      output_wide: wide DataFrame (SKU rows × month columns)
      validation: DataFrame with issues
    """
    bb_split_levels = bb_split_levels or {}
    sfuv_id_col = hier_col_map.get("SFU_v", sfuv_col)
    specific_sfuv_col = hier_col_map.get("SFU_v") # Case where SFU_v is directly mapped in SAS

    # Pre-build salience sub-frames indexed by split level for fast per-BB lookup
    # salience_df may have a _split_level column (set when per-BB levels are used)
    has_level_col = "_split_level" in salience_df.columns
    sal_by_level: dict[str, pd.DataFrame] = {}
    all_levels = set(bb_split_levels.values()) | {split_level}
    for lvl in all_levels:
        if has_level_col:
            sub = salience_df[salience_df["_split_level"] == lvl].drop(columns=["_split_level"], errors="ignore")
        else:
            sub = salience_df.copy()
        sal_by_level[lvl] = sub

    def _get_bb_salience(
        bb_level: str, bb_group_vals: tuple, bb_group_cols: list[str]
    ) -> dict[str, dict[str, float] | float]:
        """Filter salience table to this BB's group.

        Returns {sfuv: value} where value is either:
          - a dict {month: salience} when per-month columns exist in salience_df, or
          - a scalar float (scalar 'salience' column) otherwise.
        """
        sal_sub = sal_by_level.get(bb_level, salience_df)
        for col, val in zip(bb_group_cols, bb_group_vals):
            if col in sal_sub.columns:
                sal_sub = sal_sub[sal_sub[col].astype(str) == str(val)]

        month_cols = [c for c in sal_sub.columns if c in sas_months]
        result: dict[str, dict[str, float] | float] = {}
        for _, row in sal_sub.iterrows():
            sfuv = str(row.get(sfuv_id_col, ""))
            scalar = float(row.get("salience", 0) or 0)
            if month_cols:
                per_month = {}
                for m in sas_months:
                    v = row.get(m, np.nan)
                    per_month[m] = float(v) if not pd.isna(v) else scalar
                result[sfuv] = per_month
            else:
                result[sfuv] = scalar
        return result

    # Output accumulator
    output_rows: dict[tuple, dict] = {}
    sku_meta: dict[tuple, dict] = {}
    validation_issues: list[dict] = []

    # Finest-level group keys for output column ordering
    finest_group_keys = [hier_col_map.get(k, k) for k in SPLIT_KEYS["Form"] if k != "SFU_v"]

    for bb_idx, bb_row in sas_df.iterrows():
        bb_id = str(bb_row.get(bb_id_col, f"BB_{bb_idx}"))
        bb_level = bb_split_levels.get(bb_id, split_level)

        # 1. Target specific SFU_v if available in SAS row
        pinned_sfuv = str(bb_row.get(specific_sfuv_col, "")).strip() if specific_sfuv_col in bb_row.index else ""

        # Defaults to avoid NameError if pinned
        bb_sas_keys: list[str] = []
        bb_group_vals: tuple = ()

        if pinned_sfuv and pinned_sfuv != "" and pinned_sfuv != "nan":
            # Force split level to fixed SFU_v
            mask = sfuv_df[sfuv_id_col].astype(str) == pinned_sfuv
            matched_sfuvs_df = sfuv_df[mask]
        else:
            bb_group_keys_logical = [k for k in SPLIT_KEYS[bb_level] if k != "SFU_v"]
            bb_sas_keys = [hier_col_map.get(k, k) for k in bb_group_keys_logical]
            bb_group_vals = tuple(bb_row.get(c, "") for c in bb_sas_keys)

            # Find matching SFU_v rows
            mask = pd.Series([True] * len(sfuv_df), index=sfuv_df.index)
            for col, val in zip(bb_sas_keys, bb_group_vals):
                if col in sfuv_df.columns:
                    mask &= sfuv_df[col].astype(str) == str(val)
            matched_sfuvs_df = sfuv_df[mask]

        if matched_sfuvs_df.empty:
            validation_issues.append({
                "type": "unmatched_BB",
                "bb_id": bb_id,
                "detail": f"No matching SFU_vs found for this building block (pinned={pinned_sfuv}).",
            })
            continue

        all_sfuvs = matched_sfuvs_df[sfuv_id_col].astype(str).tolist() if sfuv_id_col in matched_sfuvs_df.columns else []
        eligible_sfuvs = exc_store.get_eligible_skus(bb_id, all_sfuvs)

        if not eligible_sfuvs:
            validation_issues.append({
                "type": "empty_eligible_set",
                "bb_id": bb_id,
                "detail": "All SFU_vs excluded — no eligible items remain.",
            })
            continue

        # Per-BB salience lookup (filtered to this BB's group at its split level)
        bb_sal_lookup = _get_bb_salience(bb_level, bb_group_vals, bb_sas_keys)

        # Register SFU_v metadata (use finest available group keys for output)
        for _, sfuv_row in matched_sfuvs_df.iterrows():
            sfuv_val = str(sfuv_row.get(sfuv_id_col, ""))
            if sfuv_val not in eligible_sfuvs:
                continue
            # use a mock group val if pinned
            if pinned_sfuv:
                bb_group_vals = ("PINNED",)
            sfuv_key = bb_group_vals + (sfuv_val,)
            if sfuv_key not in sku_meta:
                meta = {c: sfuv_row.get(c, "") for c in finest_group_keys if c in sfuv_row.index}
                meta[sfuv_id_col] = sfuv_val
                sku_meta[sfuv_key] = meta
                output_rows[sfuv_key] = {m: 0.0 for m in sas_months}

        for month in sas_months:
            bb_val = pd.to_numeric(bb_row.get(month, 0), errors="coerce")
            if pd.isna(bb_val):
                bb_val = 0.0

            # Fixed quantities
            fixed_total = 0.0
            fixed_alloc: dict[str, float] = {}
            for sfuv_val in eligible_sfuvs:
                fq = exc_store.get_fixed_qty(bb_id, sfuv_val, month)
                if fq is not None:
                    fixed_alloc[sfuv_val] = fq
                    fixed_total += fq

            remaining = bb_val - fixed_total
            if remaining < 0:
                validation_issues.append({
                    "type": "negative_remainder",
                    "bb_id": bb_id,
                    "month": month,
                    "bb_value": bb_val,
                    "fixed_total": fixed_total,
                    "detail": f"Fixed quantities ({fixed_total}) exceed BB value ({bb_val}). Please fix.",
                })
                remaining = 0.0

            proportional_sfuvs = [s for s in eligible_sfuvs if s not in fixed_alloc]

            # Resolve per-month or scalar salience for each proportional SFU_v
            sal_vals: dict[str, float] = {}
            for sfuv_val in proportional_sfuvs:
                entry = bb_sal_lookup.get(sfuv_val, 0.0)
                if isinstance(entry, dict):
                    sal_vals[sfuv_val] = entry.get(month, 0.0)
                else:
                    sal_vals[sfuv_val] = float(entry)
            sal_total = sum(sal_vals.values())

            for sfuv_val in eligible_sfuvs:
                sfuv_key = bb_group_vals + (sfuv_val,)
                if sfuv_key not in output_rows:
                    output_rows[sfuv_key] = {m: 0.0 for m in sas_months}

                if sfuv_val in fixed_alloc:
                    output_rows[sfuv_key][month] = output_rows[sfuv_key].get(month, 0.0) + fixed_alloc[sfuv_val]
                elif sal_total > 0:
                    share = sal_vals.get(sfuv_val, 0.0) / sal_total
                    output_rows[sfuv_key][month] = output_rows[sfuv_key].get(month, 0.0) + remaining * share
                else:
                    if proportional_sfuvs:
                        output_rows[sfuv_key][month] = output_rows[sfuv_key].get(month, 0.0) + remaining / len(proportional_sfuvs)
                    validation_issues.append({
                        "type": "zero_salience_fallback",
                        "bb_id": bb_id,
                        "month": month,
                        "detail": "All proportional saliences are 0; used equal split fallback.",
                    })

    # Assemble wide output
    if not output_rows:
        front_cols = [c for c in finest_group_keys if c] + [sfuv_id_col]
        output_wide = pd.DataFrame(columns=front_cols + sas_months)
    else:
        records = []
        for sfuv_key, month_vals in output_rows.items():
            meta = sku_meta.get(sfuv_key, {})
            record = {**meta, **month_vals}
            records.append(record)
        output_wide = pd.DataFrame(records)
        front_cols = [c for c in finest_group_keys if c in output_wide.columns] + [sfuv_id_col]
        front_cols = [c for c in front_cols if c in output_wide.columns]
        month_cols_present = [m for m in sas_months if m in output_wide.columns]
        output_wide = output_wide[front_cols + month_cols_present]

    validation_df = pd.DataFrame(validation_issues) if validation_issues else pd.DataFrame(
        columns=["type", "bb_id", "group", "month", "detail"]
    )

    return output_wide, validation_df
