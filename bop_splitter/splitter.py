"""Core split engine: apply salience + exceptions to produce SKU-level output."""
from __future__ import annotations

import numpy as np
import pandas as pd

from .salience import SPLIT_KEYS
from .exceptions import ExceptionStore


def run_split(
    sas_df: pd.DataFrame,
    sku_df: pd.DataFrame,
    salience_df: pd.DataFrame,
    sas_months: list[str],
    split_level: str,
    bb_id_col: str,
    sku_col: str,
    hier_col_map: dict[str, str],  # logical -> actual column name
    exc_store: ExceptionStore,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform the split.

    Returns:
      output_wide: wide DataFrame (SKU rows × month columns)
      validation: DataFrame with issues
    """
    group_keys_logical = SPLIT_KEYS[split_level]  # logical names
    # Map to actual column names in sas_df / sku_df
    sas_group_keys = [hier_col_map.get(k, k) for k in group_keys_logical if k != "SKU"]
    sku_group_keys = [hier_col_map.get(k, k) for k in group_keys_logical if k != "SKU"]

    sku_id_col = hier_col_map.get("SKU", sku_col)

    # Index salience by group + sku for fast lookup
    sal_index_cols = [c for c in salience_df.columns if c not in ("basis", "salience", "flag")]
    salience_df = salience_df.copy()

    # Build salience lookup: (group_tuple, sku) -> salience
    sal_lookup: dict[tuple, float] = {}
    sal_group_cols = [c for c in salience_df.columns if c not in (sku_id_col, "basis", "salience", "flag")]
    for _, row in salience_df.iterrows():
        g_key = tuple(row[c] for c in sal_group_cols)
        sku_val = row.get(sku_id_col, "")
        sal_lookup[(g_key, sku_val)] = float(row.get("salience", 0) or 0)

    # Output accumulator: (sku_key_tuple) -> {month: value}
    output_rows: dict[tuple, dict] = {}
    # sku_key = tuple of (sku group keys + sku value)
    sku_meta: dict[tuple, dict] = {}  # sku_key -> column dict
    validation_issues: list[dict] = []

    for bb_idx, bb_row in sas_df.iterrows():
        bb_id = str(bb_row.get(bb_id_col, f"BB_{bb_idx}"))

        # Get group values for this BB
        bb_group_vals = tuple(bb_row.get(c, "") for c in sas_group_keys)

        # Find matching SKU rows
        mask = pd.Series([True] * len(sku_df), index=sku_df.index)
        for col, val in zip(sku_group_keys, bb_group_vals):
            if col in sku_df.columns:
                mask &= sku_df[col].astype(str) == str(val)

        matched_skus_df = sku_df[mask]

        if matched_skus_df.empty:
            validation_issues.append({
                "type": "unmatched_BB",
                "bb_id": bb_id,
                "group": dict(zip(sas_group_keys, bb_group_vals)),
                "detail": "No matching SKUs found for this building block.",
            })
            continue

        all_skus = matched_skus_df[sku_id_col].astype(str).tolist() if sku_id_col in matched_skus_df.columns else []
        eligible_skus = exc_store.get_eligible_skus(bb_id, all_skus)

        if not eligible_skus:
            validation_issues.append({
                "type": "empty_eligible_set",
                "bb_id": bb_id,
                "group": dict(zip(sas_group_keys, bb_group_vals)),
                "detail": "All SKUs excluded — no eligible SKUs remain.",
            })
            continue

        # Register SKU metadata
        for _, sku_row in matched_skus_df.iterrows():
            sku_val = str(sku_row.get(sku_id_col, ""))
            if sku_val not in eligible_skus:
                continue
            sku_key = bb_group_vals + (sku_val,)
            if sku_key not in sku_meta:
                meta = {c: sku_row.get(c, "") for c in sku_group_keys}
                meta[sku_id_col] = sku_val
                sku_meta[sku_key] = meta
                output_rows[sku_key] = {m: 0.0 for m in sas_months}

        for month in sas_months:
            bb_val = pd.to_numeric(bb_row.get(month, 0), errors="coerce")
            if pd.isna(bb_val):
                bb_val = 0.0

            # Fixed quantities
            fixed_total = 0.0
            fixed_alloc: dict[str, float] = {}
            for sku_val in eligible_skus:
                fq = exc_store.get_fixed_qty(bb_id, sku_val, month)
                if fq is not None:
                    fixed_alloc[sku_val] = fq
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

            # SKUs that get proportional split
            proportional_skus = [s for s in eligible_skus if s not in fixed_alloc]

            # Salience for proportional SKUs
            sal_vals = {}
            for sku_val in proportional_skus:
                sal_key = (bb_group_vals, sku_val)
                sal_vals[sku_val] = sal_lookup.get(sal_key, 0.0)

            sal_total = sum(sal_vals.values())

            for sku_val in eligible_skus:
                sku_key = bb_group_vals + (sku_val,)
                if sku_key not in output_rows:
                    output_rows[sku_key] = {m: 0.0 for m in sas_months}

                if sku_val in fixed_alloc:
                    output_rows[sku_key][month] = output_rows[sku_key].get(month, 0.0) + fixed_alloc[sku_val]
                elif sal_total > 0:
                    share = sal_vals.get(sku_val, 0.0) / sal_total
                    output_rows[sku_key][month] = output_rows[sku_key].get(month, 0.0) + remaining * share
                else:
                    # Equal split fallback
                    if proportional_skus:
                        output_rows[sku_key][month] = output_rows[sku_key].get(month, 0.0) + remaining / len(proportional_skus)
                    validation_issues.append({
                        "type": "zero_salience_fallback",
                        "bb_id": bb_id,
                        "month": month,
                        "detail": "All proportional saliences are 0; used equal split fallback.",
                    })

    # Assemble wide output
    if not output_rows:
        output_wide = pd.DataFrame(columns=sku_group_keys + [sku_id_col] + sas_months)
    else:
        records = []
        for sku_key, month_vals in output_rows.items():
            meta = sku_meta.get(sku_key, {})
            record = {**meta, **month_vals}
            records.append(record)
        output_wide = pd.DataFrame(records)
        # Reorder columns
        front_cols = sku_group_keys + [sku_id_col]
        front_cols = [c for c in front_cols if c in output_wide.columns]
        month_cols_present = [m for m in sas_months if m in output_wide.columns]
        output_wide = output_wide[front_cols + month_cols_present]

    validation_df = pd.DataFrame(validation_issues) if validation_issues else pd.DataFrame(
        columns=["type", "bb_id", "group", "month", "detail"]
    )

    return output_wide, validation_df
