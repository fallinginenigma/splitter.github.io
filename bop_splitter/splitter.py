"""Core split engine: apply salience + exceptions to produce SKU-level output."""
from __future__ import annotations

import numpy as np
import pandas as pd

from .salience import SPLIT_KEYS
from .exceptions import ExceptionStore


# ──────────────────────────────────────────────────────────────────────────────
# Financial Year & 12-Month Fallback Utilities
# ──────────────────────────────────────────────────────────────────────────────

def detect_fy_range(month_cols: list[str]) -> tuple[int, int] | None:
    """
    Detect financial year (July-June) from month columns in format "MMM-YY".
    
    Returns:
        Tuple of (fy_start_year, fy_end_year) or None if insufficient months found.
        Example: If months range from Jul-25 to Jun-26, returns (2025, 2026).
    """
    if not month_cols:
        return None
    
    try:
        parsed = []
        for col in month_cols:
            ts = pd.to_datetime(col, format="%b-%y")
            parsed.append((ts.month, ts.year))
        
        if not parsed:
            return None
        
        # Extract years
        years = sorted(set(y for _, y in parsed))
        months_by_year = {}
        for m, y in parsed:
            if y not in months_by_year:
                months_by_year[y] = set()
            months_by_year[y].add(m)
        
        # FY runs Jul-Jun, so if we have Jul-Jun across two years, that's one FY
        # Example: Jul-25, Aug-25, ..., Jun-26 → FY 2025-26
        if len(years) == 2:
            fy_start, fy_end = years[0], years[1]
            if 7 in months_by_year.get(fy_start, set()) and 6 in months_by_year.get(fy_end, set()):
                return (fy_start, fy_end)
        elif len(years) == 1:
            # Single year: could be Jul-Dec (start of FY) or Jan-Jun (end of FY)
            year = years[0]
            months = months_by_year[year]
            if 7 in months:  # Has July, so it's the start
                return (year, year + 1)
            elif 6 in months:  # Has June but no July, so it's the end
                return (year - 1, year)
        
        return None
    except Exception:
        return None


def identify_extended_months(
    sas_months: list[str],
    forecast_months: list[str],
    fy_range: tuple[int, int] | None,
) -> list[str]:
    """
    Identify forecast months that extend beyond SAS months but are within the FY.
    
    Args:
        sas_months: Months available in SAS (e.g., ["Jul-25", ..., "Jun-26"])
        forecast_months: All forecast months user selected (may include extended months)
        fy_range: (fy_start_year, fy_end_year) tuple from detect_fy_range()
    
    Returns:
        List of forecast months that are beyond max(sas_months) but within the FY.
        Example: If SAS ends at Jun-26 but forecast includes Jul-26, Aug-26, returns those.
    """
    if not fy_range or not sas_months or not forecast_months:
        return []
    
    try:
        # Parse all months
        sas_parsed = [pd.to_datetime(m, format="%b-%y") for m in sas_months]
        forecast_parsed = [pd.to_datetime(m, format="%b-%y") for m in forecast_months]
        
        max_sas = max(sas_parsed)
        fy_start_year, fy_end_year = fy_range
        
        # FY ends in June of fy_end_year
        fy_end_date = pd.Timestamp(year=fy_end_year, month=6, day=1)
        
        # Find forecast months that are after max(sas) but within FY
        extended = []
        for month_str, month_ts in zip(forecast_months, forecast_parsed):
            if month_ts > max_sas and month_ts <= fy_end_date:
                extended.append(month_str)
        
        return extended
    except Exception:
        return []


def get_prior_year_month(month_str: str) -> str | None:
    """
    Get the prior year equivalent of a month.
    
    Args:
        month_str: Month in "MMM-YY" format (e.g., "Jul-26")
    
    Returns:
        Prior year month string (e.g., "Jul-25") or None if parsing fails.
    """
    try:
        ts = pd.to_datetime(month_str, format="%b-%y")
        prior = ts - pd.DateOffset(years=1)
        return prior.strftime("%b-%y")
    except Exception:
        return None


def fetch_shipment_fallback_values(
    shipments_df: pd.DataFrame,
    bb_group_vals: tuple,
    bb_sku_keys: list[str],
    extended_months: list[str],
    hier_col_map: dict[str, str],
    sfuv_col: str,
) -> dict[str, dict[str, float]]:
    """
    Fetch prior year shipment values for extended months to use as BB volume fallback.
    
    Args:
        shipments_df: Historical Shipments data
        bb_group_vals: Hierarchy values for this BB (e.g., ("Country", "SMO Cat", ...))
        bb_sku_keys: Mapped column names for hierarchy (e.g., ["Country Col", "SMO Cat Col", ...])
        extended_months: Months that extend beyond SAS (e.g., ["Jul-26", "Aug-26"])
        hier_col_map: Logical -> actual column mapping
        sfuv_col: SFU_v column name in shipments
    
    Returns:
        Dict: {extended_month_str: {sfuv: total_volume}}
        Example: {"Jul-26": {"SFU1_v1": 1000.0, "SFU2_v1": 2000.0}}
    """
    if shipments_df is None or shipments_df.empty or not extended_months:
        return {}
    
    result = {}
    
    for ext_month in extended_months:
        prior_month = get_prior_year_month(ext_month)
        if not prior_month or prior_month not in shipments_df.columns:
            continue
        
        # Filter shipments to this BB's group
        filtered = shipments_df.copy()
        for col, val in zip(bb_sku_keys, bb_group_vals):
            if col in filtered.columns:
                filtered = filtered[filtered[col].astype(str).str.strip() == str(val).strip()]
        
        if filtered.empty:
            continue
        
        # Aggregate by SFU_v
        if sfuv_col not in filtered.columns:
            continue
        
        try:
            month_values = filtered.groupby(sfuv_col)[prior_month].apply(
                lambda x: x.sum() if not x.isna().all() else 0.0
            ).to_dict()
            result[ext_month] = {
                str(k).strip(): float(v) if pd.notna(v) else 0.0
                for k, v in month_values.items()
            }
        except Exception:
            continue
    
    return result


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
    shipments_df: pd.DataFrame | None = None,  # Historical Shipments for 12-month fallback
    fff_df: pd.DataFrame | None = None,  # Existing Final Fcst to Finance values for upload-delta logic
    all_sas_months: list[str] | None = None,  # All SAS months (including past) to detect FY
    monthly_salience: dict[str, dict[str, float]] | None = None,  # month -> {SFU_v: salience_fraction}
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Perform the split.

    When bb_split_levels is provided, each Building Block is split at its own
    granularity level (looked up by bb_id, falling back to split_level global).
    
    When forecast_boundaries is provided, months outside the Forecast From/To range
    for each SFU_v will be set to 0.
    
    When shipments_df/fff_df are provided, months beyond current FY are converted to
    upload adjustments per SFU_v as: prior-year Shipments(same month) - existing FFF.

        Returns:
            output_wide: wide DataFrame (SKU rows × month columns)
            validation: DataFrame with issues
            allocation_trace: granular BB × SFU_v × month allocations used to build output_wide
    """
    bb_split_levels = bb_split_levels or {}
    sas_hier_col_map = sas_hier_col_map or {}
    validation_issues = []
    monthly_salience = monthly_salience or {}

    def _month_ts(month_str: str) -> pd.Timestamp | None:
        try:
            return pd.to_datetime(month_str, format="%b-%y").replace(day=1).normalize()
        except Exception:
            return None

    # Current FY is inferred from SAS headers (FY = Jul-Jun).
    post_fy_months: list[str] = []
    fy_end_date: pd.Timestamp | None = None
    if all_sas_months:
        fy_range = detect_fy_range(all_sas_months)
        if fy_range is not None:
            fy_end_date = pd.Timestamp(year=fy_range[1], month=6, day=1)
        else:
            parsed_sas_months = [t for t in (_month_ts(m) for m in all_sas_months) if t is not None]
            if parsed_sas_months:
                max_sas = max(parsed_sas_months)
                fy_end_year = max_sas.year if max_sas.month <= 6 else max_sas.year + 1
                fy_end_date = pd.Timestamp(year=fy_end_year, month=6, day=1)

    if fy_end_date is not None:
        for m in sas_months:
            ts = _month_ts(m)
            if ts is not None and ts > fy_end_date:
                post_fy_months.append(m)

    # Validation: Check that all required month columns exist
    missing_months = [m for m in sas_months if m not in sas_df.columns]
    if missing_months:
        validation_issues.append({
            "type": "missing_month_columns",
            "detail": f"The following month columns are missing from SAS data: {missing_months}. Available: {list(sas_df.columns)}",
            "missing_months": missing_months,
        })
        # Return early with error
        return pd.DataFrame(), pd.DataFrame([validation_issues[0]]), pd.DataFrame()
    
    # Validation: Check that bb_id_col exists in sas_df
    if bb_id_col not in sas_df.columns:
        validation_issues.append({
            "type": "missing_bb_id_column",
            "detail": f"Building Block ID column '{bb_id_col}' not found in SAS data. Available: {list(sas_df.columns)}",
            "bb_id_col": bb_id_col,
        })
        # Return early with error
        return pd.DataFrame(), pd.DataFrame([validation_issues[0]]), pd.DataFrame()

    def _norm(val) -> str:
        return str(val).strip()

    # Helper: Resolve SFU_v identifier column with explicit fallback chain
    def _resolve_sfuv_column(df: pd.DataFrame, primary: str, hier_map: dict, fallback_candidates: list[str] = None) -> str:
        """Resolve SFU_v identifier column from DataFrame columns with fallback chain.
        
        Args:
            df: DataFrame to resolve column from
            primary: Primary column name to try first
            hier_map: Hierarchy column map (logical -> actual)
            fallback_candidates: List of fallback column names to try
            
        Returns:
            Resolved column name
            
        Raises:
            ValueError: If no valid column found
        """
        if fallback_candidates is None:
            fallback_candidates = ["SFU_v", "SFU_SFU Version", "SKU"]
        
        candidates = [primary, hier_map.get("SFU_v")] + fallback_candidates
        candidates = [c for c in candidates if c]  # Remove None values
        
        for cand in candidates:
            if cand in df.columns:
                if cand != primary and primary not in df.columns:
                    # Fallback was used; could log this if needed
                    pass
                return cand
        
        # No valid column found - raise error
        raise ValueError(
            f"No SFU_v identifier column found in DataFrame.\n"
            f"Tried: {candidates}\n"
            f"Available columns: {list(df.columns)}"
        )
    
    # Resolve SFU_v column with error handling
    try:
        sfuv_id_col = _resolve_sfuv_column(sfuv_df, sfuv_col, hier_col_map)
    except ValueError as e:
        validation_issues.append({
            "type": "missing_sfuv_column",
            "detail": str(e),
        })
        # Return empty output with error
        return pd.DataFrame(), pd.DataFrame([{"type": "missing_sfuv_column", "detail": str(e)}]), pd.DataFrame()

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

    # Helper: Get all available SFU_v ID columns from a DataFrame
    def _get_id_columns(df: pd.DataFrame) -> list[str]:
        """Extract available SFU_v identifier columns from DataFrame in priority order."""
        id_cols = []
        for cand in [
            sfuv_id_col,
            hier_col_map.get("SFU_v"),
            "SFU_v",
            "SFU_SFU Version",
            "SKU",
        ]:
            if cand and cand in df.columns and cand not in id_cols:
                id_cols.append(cand)
        return id_cols

    def _build_id_lookup(sal_slice: pd.DataFrame) -> dict[str, float]:
        """Build SFU_v -> salience lookup from a salience DataFrame slice."""
        sal_id_cols = _get_id_columns(sal_slice)
        
        if not sal_id_cols:
            validation_issues.append({
                "type": "no_salience_id_columns",
                "detail": f"No SFU_v identifier columns found in salience lookup slice. Available: {list(sal_slice.columns)}",
            })
            return {}

        out: dict[str, float] = {}
        for _, row in sal_slice.iterrows():
            scalar = float(row.get("salience", 0) or 0)
            for id_col in sal_id_cols:
                sid = _norm(row.get(id_col, ""))
                if sid:
                    out[sid] = max(out.get(sid, 0.0), scalar)
        return out

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

        # Validate: Check if salience filtering resulted in empty frame
        if sal_sub.empty:
            validation_issues.append({
                "type": "empty_salience_frame",
                "detail": f"No salience data found for BB level={bb_level}, groups={list(zip(bb_group_cols, bb_group_vals))}",
                "level": bb_level,
                "groups": list(zip(bb_group_cols, bb_group_vals)),
            })
            return {}

        # Use helper to build lookup
        return _build_id_lookup(sal_sub)

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
    allocation_trace_rows: list[dict] = []

    # Finest-level group keys for output column ordering
    finest_group_keys = [hier_col_map.get(k, k) for k in SPLIT_KEYS["Form"] if k != "SFU_v"]

    for bb_idx, bb_row in sas_df.iterrows():
        bb_id = str(bb_row.get(bb_id_col, f"BB_{bb_idx}"))
        bb_level = bb_split_levels.get(str(bb_id), split_level)

        # PRE-COMPUTE month values for this BB (optimization: O(1) lookup in inner loop instead of repeated pandas access)
        bb_month_values: dict[str, float] = {}
        for month in sas_months:
            val = pd.to_numeric(bb_row.get(month, 0), errors="coerce")
            bb_month_values[month] = 0.0 if pd.isna(val) else float(val)

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
            
            # Validate: Ensure all group values are non-None and normalized
            normalized_group = tuple(_norm(str(v)) if v is not None else "" for v in bb_group_vals)
            sfuv_key = normalized_group + (sfuv_val,)
            
            if sfuv_key not in sku_meta:
                meta = {c: sfuv_row.get(c, "") for c in finest_group_keys if c in sfuv_row.index}
                meta[sfuv_id_col] = sfuv_val
                sku_meta[sfuv_key] = meta
                output_rows[sfuv_key] = {m: 0.0 for m in sas_months}

        for month in sas_months:
            # Use pre-computed cached value (O(1) dict lookup)
            bb_val = bb_month_values[month]

            # Fixed quantities
            fixed_total = 0.0
            fixed_alloc: dict[str, float] = {}
            for sfuv_val in eligible_sfuvs:
                fq = exc_store.get_fixed_qty(bb_id, sfuv_val, month)
                if fq is not None:
                    fixed_alloc[sfuv_val] = fq
                    fixed_total += fq

            remaining = bb_val - fixed_total

            proportional_sfuvs = [s for s in eligible_sfuvs if s not in fixed_alloc]

            # Resolve month-specific salience first; fall back to scalar salience.
            month_sal_lookup = monthly_salience.get(month, {}) if monthly_salience else {}
            sal_vals: dict[str, float] = {}
            for sfuv_val in proportional_sfuvs:
                sfuv_key_norm = _norm(sfuv_val)
                monthly_weight = pd.to_numeric(month_sal_lookup.get(sfuv_key_norm), errors="coerce")
                if pd.notna(monthly_weight):
                    sal_vals[sfuv_val] = float(monthly_weight)
                else:
                    sal_vals[sfuv_val] = float(bb_sal_lookup.get(sfuv_val, 0.0))
            sal_total = sum(sal_vals.values())

            for sfuv_val in eligible_sfuvs:
                sfuv_key = bb_group_vals + (sfuv_val,)
                if sfuv_key not in output_rows:
                    output_rows[sfuv_key] = {m: 0.0 for m in sas_months}

                if sfuv_val in fixed_alloc:
                    alloc_val = float(fixed_alloc[sfuv_val])
                    output_rows[sfuv_key][month] = output_rows[sfuv_key].get(month, 0.0) + alloc_val
                    allocation_trace_rows.append({
                        "bb_id": bb_id,
                        "bb_level": bb_level,
                        "month": month,
                        "sfuv": sfuv_val,
                        "alloc": alloc_val,
                        "is_fixed": True,
                    })
                elif sal_total > 0:
                    share = sal_vals.get(sfuv_val, 0.0) / sal_total
                    alloc_val = float(remaining * share)
                    output_rows[sfuv_key][month] = output_rows[sfuv_key].get(month, 0.0) + alloc_val
                    allocation_trace_rows.append({
                        "bb_id": bb_id,
                        "bb_level": bb_level,
                        "month": month,
                        "sfuv": sfuv_val,
                        "alloc": alloc_val,
                        "is_fixed": False,
                    })
                else:
                    if proportional_sfuvs:
                        alloc_val = float(remaining / len(proportional_sfuvs))
                        output_rows[sfuv_key][month] = output_rows[sfuv_key].get(month, 0.0) + alloc_val
                        allocation_trace_rows.append({
                            "bb_id": bb_id,
                            "bb_level": bb_level,
                            "month": month,
                            "sfuv": sfuv_val,
                            "alloc": alloc_val,
                            "is_fixed": False,
                        })
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

    # Post-FY upload adjustment: for months beyond current FY,
    # set adjustment = prior-year Shipments(same month) - existing FFF.
    if (
        post_fy_months
        and not output_wide.empty
        and sfuv_id_col in output_wide.columns
        and shipments_df is not None
        and not shipments_df.empty
    ):
        def _resolve_id_col(df: pd.DataFrame) -> str | None:
            for cand in [sfuv_id_col, hier_col_map.get("SFU_v"), sfuv_col, "SFU_v", "SFU_SFU Version", "SKU"]:
                if cand and cand in df.columns:
                    return cand
            return None

        ship_id_col = _resolve_id_col(shipments_df)
        fff_id_col = _resolve_id_col(fff_df) if fff_df is not None else None

        ship_lookup: dict[tuple[str, str], float] = {}
        fff_lookup: dict[tuple[str, str], float] = {}

        if ship_id_col:
            for month in post_fy_months:
                prev_month = get_prior_year_month(month)
                if not prev_month or prev_month not in shipments_df.columns:
                    continue
                sub = shipments_df[[ship_id_col, prev_month]].copy()
                sub[prev_month] = pd.to_numeric(sub[prev_month], errors="coerce").fillna(0.0)
                grouped = sub.groupby(ship_id_col, dropna=False)[prev_month].sum()
                for sku, val in grouped.items():
                    ship_lookup[(_norm(sku), month)] = float(val)

        if fff_df is not None and not fff_df.empty and fff_id_col:
            for month in post_fy_months:
                if month not in fff_df.columns:
                    continue
                sub = fff_df[[fff_id_col, month]].copy()
                sub[month] = pd.to_numeric(sub[month], errors="coerce").fillna(0.0)
                grouped = sub.groupby(fff_id_col, dropna=False)[month].sum()
                for sku, val in grouped.items():
                    fff_lookup[(_norm(sku), month)] = float(val)

        adjusted_cells = 0
        for idx, row in output_wide.iterrows():
            sku = _norm(row.get(sfuv_id_col, ""))
            if not sku:
                continue
            for month in post_fy_months:
                if month not in output_wide.columns:
                    continue
                ship_val = ship_lookup.get((sku, month), 0.0)
                fff_val = fff_lookup.get((sku, month), 0.0)
                output_wide.at[idx, month] = ship_val - fff_val
                adjusted_cells += 1

        if adjusted_cells > 0:
            validation_issues.append({
                "type": "post_fy_upload_adjustment",
                "bb_id": "ALL",
                "detail": (
                    f"Applied post-FY upload adjustment to {adjusted_cells} cell(s): "
                    "value = prior-year Shipments - existing FFF."
                ),
            })

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

    allocation_trace_df = pd.DataFrame(allocation_trace_rows) if allocation_trace_rows else pd.DataFrame(
        columns=["bb_id", "bb_level", "month", "sfuv", "alloc", "is_fixed"]
    )

    return output_wide, validation_df, allocation_trace_df
