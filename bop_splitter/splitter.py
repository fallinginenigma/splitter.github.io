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
    sas_sfuv_col: str | None,
    hier_col_map: dict[str, str],  # logical -> actual column name
    sas_hier_col_map: dict[str, str] | None,
    exc_store: ExceptionStore,
    bb_split_levels: dict[str, str] | None = None,  # bb_id -> split_level override
    forecast_boundaries: dict[str, tuple[pd.Timestamp | None, pd.Timestamp | None]] | None = None,  # SFU_v -> (from, to)
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform the split.

    When bb_split_levels is provided, each Building Block is split at its own
    granularity level (looked up by bb_id, falling back to split_level global).
    
    When forecast_boundaries is provided, months outside the Forecast From/To range
    for each SFU_v will be set to 0.

    Returns:
      output_wide: wide DataFrame (SKU rows × month columns)
      validation: DataFrame with issues
    """
    bb_split_levels = bb_split_levels or {}
    sas_hier_col_map = sas_hier_col_map or {}

    def _norm(val) -> str:
        return str(val).strip()

    sfuv_id_col = sfuv_col if sfuv_col in sfuv_df.columns else hier_col_map.get("SFU_v", sfuv_col)
    if sfuv_id_col not in sfuv_df.columns:
        for cand in ["SFU_v", "SFU_SFU Version", "SKU"]:
            if cand in sfuv_df.columns:
                sfuv_id_col = cand
                break

    # Pinned SFU_v comes from SAS row context, not the SKU table identifier mapping.
    specific_sfuv_col = sas_sfuv_col if sas_sfuv_col and sas_sfuv_col in sas_df.columns else None

    # Pre-build salience sub-frames indexed by split level for fast per-BB lookup
    # salience_df may have a _split_level column (set when per-BB levels are used)
    has_level_col = "_split_level" in salience_df.columns
    sal_by_level: dict[str, pd.DataFrame] = {}
    all_levels = set(bb_split_levels.values()) | {split_level}
    for lvl in all_levels:
        if has_level_col:
            sub = salience_df[salience_df["_split_level"].astype(str).str.strip() == str(lvl).strip()].drop(columns=["_split_level"], errors="ignore")
        else:
            sub = salience_df.copy()
        sal_by_level[lvl] = sub

    def _get_bb_salience(
        bb_level: str, bb_group_vals: tuple, bb_group_cols: list[str]
    ) -> dict[str, float]:
        """Filter salience table to this BB's group.

        Returns {sfuv: salience} as a scalar weight applied to all months.
        """
        sal_sub = sal_by_level.get(bb_level, salience_df)
        for col, val in zip(bb_group_cols, bb_group_vals):
            if col in sal_sub.columns:
                sal_sub = sal_sub[sal_sub[col].astype(str).str.strip() == str(val).strip()]

        sal_id_cols = []
        for cand in [
            sfuv_id_col,
            hier_col_map.get("SFU_v"),
            "SFU_v",
            "SFU_SFU Version",
            "SKU",
        ]:
            if cand and cand in sal_sub.columns and cand not in sal_id_cols:
                sal_id_cols.append(cand)

        result: dict[str, float] = {}
        for _, row in sal_sub.iterrows():
            scalar = float(row.get("salience", 0) or 0)
            for id_col in sal_id_cols:
                sfuv = _norm(row.get(id_col, ""))
                if sfuv:
                    result[sfuv] = max(result.get(sfuv, 0.0), scalar)
        return result

    def _build_id_lookup(sal_slice: pd.DataFrame) -> dict[str, float]:
        sal_id_cols = []
        for cand in [
            sfuv_id_col,
            hier_col_map.get("SFU_v"),
            "SFU_v",
            "SFU_SFU Version",
            "SKU",
        ]:
            if cand and cand in sal_slice.columns and cand not in sal_id_cols:
                sal_id_cols.append(cand)

        out: dict[str, float] = {}
        for _, row in sal_slice.iterrows():
            scalar = float(row.get("salience", 0) or 0)
            for id_col in sal_id_cols:
                sid = _norm(row.get(id_col, ""))
                if sid:
                    out[sid] = max(out.get(sid, 0.0), scalar)
        return out

    def _get_salience_lookup_with_scope(
        bb_level: str,
        bb_group_vals: tuple,
        bb_group_cols: list[str],
        ids_to_check: list[str],
    ) -> tuple[dict[str, float], str]:
        lookup_group = _get_bb_salience(bb_level, bb_group_vals, bb_group_cols)
        if any(_norm(s) in lookup_group for s in ids_to_check):
            return lookup_group, "group"

        level_slice = sal_by_level.get(bb_level, salience_df)
        lookup_level = _build_id_lookup(level_slice) if level_slice is not None and not level_slice.empty else {}
        if any(_norm(s) in lookup_level for s in ids_to_check):
            return lookup_level, "level"

        return _build_id_lookup(salience_df), "global"

    # Output accumulator
    output_rows: dict[tuple, dict] = {}
    sku_meta: dict[tuple, dict] = {}
    validation_issues: list[dict] = []

    # Finest-level group keys for output column ordering
    finest_group_keys = [hier_col_map.get(k, k) for k in SPLIT_KEYS["Form"] if k != "SFU_v"]

    for bb_idx, bb_row in sas_df.iterrows():
        bb_id = str(bb_row.get(bb_id_col, f"BB_{bb_idx}"))
        bb_level = bb_split_levels.get(str(bb_id), split_level)

        # 1. Target specific SFU_v if available in SAS row
        pinned_sfuv = _norm(bb_row.get(specific_sfuv_col, "")) if specific_sfuv_col in bb_row.index else ""

        # Defaults to avoid NameError if pinned
        bb_sas_keys: list[str] = []
        bb_sku_keys: list[str] = []
        bb_group_vals: tuple = ()

        if pinned_sfuv and pinned_sfuv != "" and pinned_sfuv != "nan":
            # Force split level to fixed SFU_v
            mask = sfuv_df[sfuv_id_col].astype(str).str.strip() == pinned_sfuv
            matched_sfuvs_df = sfuv_df[mask]
        else:
            bb_group_keys_logical = [k for k in SPLIT_KEYS[bb_level] if k != "SFU_v"]
            bb_sas_keys = [sas_hier_col_map.get(k) or hier_col_map.get(k, k) for k in bb_group_keys_logical]
            bb_sku_keys = [hier_col_map.get(k, k) for k in bb_group_keys_logical]
            bb_group_vals = tuple(bb_row.get(c, "") for c in bb_sas_keys)

            # Find matching SFU_v rows
            mask = pd.Series([True] * len(sfuv_df), index=sfuv_df.index)
            for col, val in zip(bb_sku_keys, bb_group_vals):
                if col in sfuv_df.columns:
                    mask &= sfuv_df[col].astype(str).str.strip() == str(val).strip()
            matched_sfuvs_df = sfuv_df[mask]

        if matched_sfuvs_df.empty:
            # Build detailed error message
            if pinned_sfuv and pinned_sfuv != "" and pinned_sfuv != "nan":
                detail_msg = f"No matching SFU_vs found. Pinned SFU_v '{pinned_sfuv}' does not exist in SFU_v data."
            else:
                # Show what we were trying to match
                match_criteria = ", ".join([f"{col}='{val}'" for col, val in zip(bb_sas_keys, bb_group_vals)])
                detail_msg = f"No matching SFU_vs found for split level '{bb_level}'. Criteria: {match_criteria}"
            
            validation_issues.append({
                "type": "unmatched_BB",
                "bb_id": bb_id,
                "detail": detail_msg,
            })
            continue

        all_sfuvs = matched_sfuvs_df[sfuv_id_col].astype(str).str.strip().tolist() if sfuv_id_col in matched_sfuvs_df.columns else []
        eligible_sfuvs = exc_store.get_eligible_skus(bb_id, all_sfuvs)

        if not eligible_sfuvs:
            bb_exc = exc_store.bb_exceptions.get(str(bb_id), {})
            bb_includes = set(bb_exc.get("include", set()))
            globally_excluded = [s for s in all_sfuvs if s in exc_store.global_exclusions]
            validation_issues.append({
                "type": "empty_eligible_set",
                "bb_id": bb_id,
                "detail": (
                    "All SFU_vs excluded — no eligible items remain. "
                    f"matched={len(all_sfuvs)}, globally_excluded={len(globally_excluded)}, "
                    f"bb_includes={len(bb_includes)}"
                ),
            })
            continue

        # Per-BB salience lookup (filtered to this BB's group at its split level)
        bb_sal_lookup, sal_lookup_scope = _get_salience_lookup_with_scope(
            bb_level, bb_group_vals, bb_sku_keys, eligible_sfuvs
        )

        # Register SFU_v metadata (use finest available group keys for output)
        for _, sfuv_row in matched_sfuvs_df.iterrows():
            sfuv_val = _norm(sfuv_row.get(sfuv_id_col, ""))
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

            # Resolve scalar salience for each proportional SFU_v
            sal_vals: dict[str, float] = {}
            for sfuv_val in proportional_sfuvs:
                sal_vals[sfuv_val] = float(bb_sal_lookup.get(sfuv_val, 0.0))
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
                    salience_rows = sum(1 for s in proportional_sfuvs if s in bb_sal_lookup)
                    validation_issues.append({
                        "type": "zero_salience_fallback",
                        "bb_id": bb_id,
                        "bb_level": bb_level,
                        "month": month,
                        "sal_lookup_scope": sal_lookup_scope,
                        "sal_lookup_keys_sample": ", ".join(list(bb_sal_lookup.keys())[:10]),
                        "matched_sfuvs": ", ".join(all_sfuvs),
                        "proportional_sfuvs": ", ".join(proportional_sfuvs),
                        "salience_rows": salience_rows,
                        "sal_total": float(sal_total),
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

    # Enforce forecast date boundaries (Stat_DB: Forecast From / Forecast To)
    if forecast_boundaries and not output_wide.empty and sfuv_id_col in output_wide.columns:
        from .loader import parse_month_to_date
        
        # Track how many cells were zeroed
        zeroed_count = 0
        
        for idx, row in output_wide.iterrows():
            sfu_v = str(row.get(sfuv_id_col, ""))
            if sfu_v not in forecast_boundaries:
                continue
            
            from_date, to_date = forecast_boundaries[sfu_v]
            
            for month_col in month_cols_present:
                month_ts = parse_month_to_date(month_col)
                if month_ts is None:
                    continue
                
                # Normalize to start of month
                month_ts = month_ts.replace(day=1).normalize()
                
                # Check if month is outside the forecast boundary
                outside_boundary = False
                
                if from_date is not None and month_ts < from_date:
                    outside_boundary = True
                
                if to_date is not None and month_ts > to_date:
                    outside_boundary = True
                
                if outside_boundary:
                    # Zero out this cell
                    if pd.notna(output_wide.at[idx, month_col]) and output_wide.at[idx, month_col] != 0:
                        output_wide.at[idx, month_col] = 0.0
                        zeroed_count += 1
        
        if zeroed_count > 0:
            validation_issues.append({
                "type": "forecast_boundary_enforcement",
                "bb_id": "ALL",
                "detail": f"Zeroed {zeroed_count} cell(s) outside Forecast From/To date boundaries from Stat DB.",
            })

    validation_df = pd.DataFrame(validation_issues) if validation_issues else pd.DataFrame(
        columns=["type", "bb_id", "group", "month", "detail"]
    )

    return output_wide, validation_df
