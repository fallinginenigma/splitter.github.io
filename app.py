"""BOP Splitter – Streamlit application."""
from __future__ import annotations

import datetime
import hashlib
import re
from copy import deepcopy

import numpy as np
import pandas as pd
import streamlit as st

from bop_splitter.loader import (
    load_excel,
    detect_month_columns,
    detect_hierarchy_columns,
)
from bop_splitter.salience import (
    compute_basis,
    compute_salience,
    normalize_salience,
    HIERARCHY_LEVELS,
    SPLIT_KEYS,
)
from bop_splitter.exceptions import ExceptionStore
from bop_splitter.splitter import run_split
from bop_splitter.exporter import build_excel_output

# ──────────────────────────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="BOP Splitter",
    page_icon="🔀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────────────
# Session-state helpers
# ──────────────────────────────────────────────────────────────────────────────
def _init_state():
    defaults = {
        "sheets": {},                  # sheet_name -> DataFrame
        "sheet_map": {},               # logical role -> sheet name
        "col_maps": {},                # sheet_role -> {logical -> actual}
        "month_cols": {},              # sheet_role -> [month col names]
        "sku_df_filtered": None,
        "sas_df_filtered": None,
        "salience_df": None,
        "blocking_groups": [],
        "sal_overrides": {},           # (group_tuple, sku) -> float
        "exc_store": ExceptionStore(),
        "output_wide": None,
        "validation_df": None,
        "split_level": "Brand",
        "basis_source": "Consumption",
        "basis_mode": "last_3",
        "basis_months_selected": [],
        "sas_months_selected": [],
        "filters": {},
        "step": 1,
        "bb_id_col": None,
        "run_settings": {},
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar – progress tracker
# ──────────────────────────────────────────────────────────────────────────────
STEPS = [
    "1. Upload & Map Sheets",
    "2. Map Columns",
    "3. Filters & Split Level",
    "4. Basis & Salience",
    "5. Exceptions",
    "6. Run Split",
    "7. Download",
]

with st.sidebar:
    st.title("🔀 BOP Splitter")
    st.caption("Building Block → SKU Forecast Splitter")
    st.divider()
    for i, label in enumerate(STEPS, 1):
        icon = "✅" if st.session_state.step > i else ("▶️" if st.session_state.step == i else "⬜")
        st.markdown(f"{icon} {label}")
    st.divider()
    if st.button("↺ Reset All", use_container_width=True):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ──────────────────────────────────────────────────────────────────────────────
LOGICAL_SHEETS = ["Shipments", "Consumption", "Retailing", "Statistical Forecast", "SAS"]
SKU_SHEETS = ["Shipments", "Consumption", "Retailing", "Statistical Forecast"]
LOGICAL_HIER = HIERARCHY_LEVELS  # Ctry, SMO Category, Brand, Sub Brand, Form
ALL_LOGICAL = LOGICAL_HIER + ["SKU"]


def _guess_sheet(candidates: list[str], keywords: list[str]) -> str | None:
    for kw in keywords:
        for c in candidates:
            if kw.lower() in c.lower():
                return c
    return None


def _make_bb_id(df: pd.DataFrame, hier_cols: list[str]) -> pd.Series:
    """Generate a stable BB_ID from row index + hierarchy hash."""
    def _row_hash(row):
        raw = "|".join(str(row.get(c, "")) for c in hier_cols)
        h = hashlib.md5(raw.encode()).hexdigest()[:6]
        return h

    hashes = df.apply(_row_hash, axis=1)
    ids = [f"BB_{i:04d}_{h}" for i, h in enumerate(hashes)]
    return pd.Series(ids, index=df.index)


def _get_df(role: str) -> pd.DataFrame | None:
    sheet_name = st.session_state.sheet_map.get(role)
    if not sheet_name or sheet_name not in st.session_state.sheets:
        return None
    return st.session_state.sheets[sheet_name]


def _mapped_col(role: str, logical: str) -> str | None:
    return st.session_state.col_maps.get(role, {}).get(logical)


def _sku_merged() -> pd.DataFrame | None:
    """Merge all SKU sheets into a single deduplicated DataFrame."""
    frames = []
    hier_cols_actual = []
    for role in SKU_SHEETS:
        df = _get_df(role)
        if df is None:
            continue
        cmap = st.session_state.col_maps.get(role, {})
        hier_actual = [cmap.get(h) for h in LOGICAL_HIER if cmap.get(h)]
        sku_actual = cmap.get("SKU")
        if not sku_actual or not hier_actual:
            continue
        keep = hier_actual + [sku_actual]
        keep = [c for c in keep if c in df.columns]
        frames.append(df[keep].drop_duplicates())
        hier_cols_actual = keep  # last one wins – assumes consistent mapping
    if not frames:
        return None
    merged = pd.concat(frames, ignore_index=True).drop_duplicates()
    return merged


# ──────────────────────────────────────────────────────────────────────────────
# STEP 1 – Upload & Sheet Mapping
# ──────────────────────────────────────────────────────────────────────────────
def step1_upload():
    st.header("Step 1 — Upload File & Map Sheets")
    uploaded = st.file_uploader(
        "Upload Excel file (.xlsx, .xlsm, .xlsb)",
        type=["xlsx", "xlsm", "xlsb"],
        key="file_uploader",
    )
    if not uploaded:
        st.info("Please upload an Excel file to begin.")
        return

    with st.spinner("Reading workbook…"):
        try:
            sheets = load_excel(uploaded)
        except Exception as e:
            st.error(f"Failed to read file: {e}")
            return

    st.session_state.sheets = sheets
    st.success(f"Loaded **{len(sheets)}** sheets: {', '.join(sheets.keys())}")

    st.subheader("Map Sheets to Roles")
    sheet_names = list(sheets.keys())
    none_opt = ["— none —"] + sheet_names

    guesses = {
        "Shipments": _guess_sheet(sheet_names, ["ship"]),
        "Consumption": _guess_sheet(sheet_names, ["cons", "offtake"]),
        "Retailing": _guess_sheet(sheet_names, ["retail", "sell"]),
        "Statistical Forecast": _guess_sheet(sheet_names, ["stat", "forecast", "fcst"]),
        "SAS": _guess_sheet(sheet_names, ["sas", "block", "bb"]),
    }

    new_map = {}
    cols = st.columns(3)
    for i, role in enumerate(LOGICAL_SHEETS):
        guess = st.session_state.sheet_map.get(role) or guesses.get(role) or "— none —"
        idx = none_opt.index(guess) if guess in none_opt else 0
        sel = cols[i % 3].selectbox(f"**{role}**", none_opt, index=idx, key=f"sheet_map_{role}")
        if sel != "— none —":
            new_map[role] = sel

    # Preview selected sheets
    if new_map:
        preview_role = st.selectbox("Preview sheet", list(new_map.keys()), key="preview_role")
        if preview_role:
            preview_df = sheets[new_map[preview_role]]
            st.dataframe(preview_df.head(5), use_container_width=True)

    if st.button("Confirm Sheet Mapping →", type="primary", disabled=len(new_map) == 0):
        if "SAS" not in new_map:
            st.error("SAS sheet is required.")
            return
        st.session_state.sheet_map = new_map
        st.session_state.step = max(st.session_state.step, 2)
        st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# STEP 2 – Column Mapping
# ──────────────────────────────────────────────────────────────────────────────
def step2_columns():
    st.header("Step 2 — Column Mapping")

    for role in LOGICAL_SHEETS:
        if role not in st.session_state.sheet_map:
            continue
        df = _get_df(role)
        if df is None:
            continue

        with st.expander(f"**{role}** — {st.session_state.sheet_map[role]}", expanded=(role == "SAS")):
            cols_all = list(df.columns)
            none_opt = ["— auto —"] + cols_all
            cmap = st.session_state.col_maps.get(role, {})

            # Auto-detect months
            detected_months = detect_month_columns(df)
            confirmed_months = st.multiselect(
                "Month columns",
                cols_all,
                default=cmap.get("_months", detected_months),
                key=f"months_{role}",
            )
            cmap["_months"] = confirmed_months

            # Hierarchy columns
            hier_ui = st.columns(len(LOGICAL_HIER))
            for j, lh in enumerate(LOGICAL_HIER):
                guess = cmap.get(lh) or _guess_col(cols_all, lh)
                idx = none_opt.index(guess) if guess in none_opt else 0
                sel = hier_ui[j].selectbox(lh, none_opt, index=idx, key=f"col_{role}_{lh}")
                if sel != "— auto —":
                    cmap[lh] = sel
                else:
                    cmap.pop(lh, None)

            # SKU column (not for SAS)
            if role != "SAS":
                sku_guess = cmap.get("SKU") or _guess_col(cols_all, "SKU")
                sku_idx = none_opt.index(sku_guess) if sku_guess in none_opt else 0
                sku_sel = st.selectbox("SKU column", none_opt, index=sku_idx, key=f"col_{role}_SKU")
                if sku_sel != "— auto —":
                    cmap["SKU"] = sku_sel
                else:
                    cmap.pop("SKU", None)

            # BB_ID column (SAS only)
            if role == "SAS":
                bb_guess = cmap.get("BB_ID") or _guess_col(cols_all, "BB_ID", "building block", "bb_id")
                bb_opts = ["— generate —"] + cols_all
                bb_idx = bb_opts.index(bb_guess) if bb_guess in bb_opts else 0
                bb_sel = st.selectbox("Building Block ID column", bb_opts, index=bb_idx, key="col_SAS_BB_ID")
                cmap["BB_ID"] = bb_sel if bb_sel != "— generate —" else None

            st.session_state.col_maps[role] = cmap

    # Ensure BB_ID in SAS
    sas_df = _get_df("SAS")
    if sas_df is not None:
        sas_cmap = st.session_state.col_maps.get("SAS", {})
        bb_id_col = sas_cmap.get("BB_ID")
        if not bb_id_col:
            # Generate
            hier_for_hash = [sas_cmap.get(h) for h in LOGICAL_HIER if sas_cmap.get(h)]
            st.info("No BB_ID column selected — a `BB_ID` column will be generated automatically.")
            st.session_state.bb_id_col = "BB_ID"
        else:
            st.session_state.bb_id_col = bb_id_col

    if st.button("Confirm Column Mapping →", type="primary"):
        # Validate SAS has at least some hier cols
        sas_cmap = st.session_state.col_maps.get("SAS", {})
        if not any(sas_cmap.get(h) for h in LOGICAL_HIER):
            st.warning("No hierarchy columns mapped for SAS — results may be poor.")
        st.session_state.step = max(st.session_state.step, 3)
        st.rerun()


def _guess_col(cols: list[str], *keywords: str) -> str:
    for kw in keywords:
        for c in cols:
            if kw.lower() in c.lower():
                return c
    return "— auto —"


# ──────────────────────────────────────────────────────────────────────────────
# STEP 3 – Filters & Split Level
# ──────────────────────────────────────────────────────────────────────────────
def step3_filters():
    st.header("Step 3 — Filters & Split Level")

    sas_df = _get_df("SAS")
    if sas_df is None:
        st.error("SAS sheet not loaded.")
        return

    sas_cmap = st.session_state.col_maps.get("SAS", {})

    # Build BB_ID if needed
    if st.session_state.bb_id_col not in sas_df.columns:
        hier_for_hash = [sas_cmap.get(h) for h in LOGICAL_HIER if sas_cmap.get(h) and sas_cmap.get(h) in sas_df.columns]
        sas_df = sas_df.copy()
        sas_df["BB_ID"] = _make_bb_id(sas_df, hier_for_hash)
        st.session_state.sheets[st.session_state.sheet_map["SAS"]] = sas_df
        st.session_state.bb_id_col = "BB_ID"

    # ---- Filters ----
    st.subheader("Filter Data")
    filters = st.session_state.filters.copy()
    filter_cols = st.columns(len(LOGICAL_HIER))
    for i, lh in enumerate(LOGICAL_HIER):
        col_actual = sas_cmap.get(lh)
        if not col_actual or col_actual not in sas_df.columns:
            filter_cols[i].caption(f"*{lh}* not mapped")
            continue
        all_vals = sorted(sas_df[col_actual].dropna().unique().astype(str).tolist())
        prev = filters.get(lh, [])
        sel = filter_cols[i].multiselect(lh, all_vals, default=prev, key=f"filter_{lh}")
        filters[lh] = sel
    st.session_state.filters = filters

    # Apply filters
    sas_filtered = sas_df.copy()
    sku_merged = _sku_merged() or pd.DataFrame()
    sku_filtered = sku_merged.copy()

    for lh, vals in filters.items():
        if not vals:
            continue
        col_sas = sas_cmap.get(lh)
        if col_sas and col_sas in sas_filtered.columns:
            sas_filtered = sas_filtered[sas_filtered[col_sas].astype(str).isin(vals)]
        # Apply to sku_filtered using any available role's col map
        for role in SKU_SHEETS:
            rcmap = st.session_state.col_maps.get(role, {})
            col_sku = rcmap.get(lh)
            if col_sku and col_sku in sku_filtered.columns:
                sku_filtered = sku_filtered[sku_filtered[col_sku].astype(str).isin(vals)]
                break

    st.info(f"**{len(sas_filtered)}** Building Blocks | **{len(sku_filtered)}** SKU rows after filters")

    # ---- Split Level ----
    st.subheader("Split Level")
    split_level = st.radio(
        "Split SAS Building Blocks at:",
        list(SPLIT_KEYS.keys()),
        index=list(SPLIT_KEYS.keys()).index(st.session_state.split_level),
        horizontal=True,
        key="split_level_radio",
    )
    match_desc = " + ".join(SPLIT_KEYS[split_level])
    st.caption(f"Match keys: **{match_desc}**")

    if st.button("Apply Filters & Split Level →", type="primary"):
        st.session_state.split_level = split_level
        st.session_state.sas_df_filtered = sas_filtered.reset_index(drop=True)
        st.session_state.sku_df_filtered = sku_filtered.reset_index(drop=True)
        st.session_state.step = max(st.session_state.step, 4)
        st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# STEP 4 – Basis & Salience
# ──────────────────────────────────────────────────────────────────────────────
def step4_salience():
    st.header("Step 4 — Basis & Salience")

    BASIS_SOURCES = [r for r in SKU_SHEETS if r in st.session_state.sheet_map]
    if not BASIS_SOURCES:
        st.error("No SKU sheets mapped. Go back to Step 1.")
        return

    col1, col2 = st.columns(2)

    basis_source = col1.selectbox(
        "Basis Source Sheet",
        BASIS_SOURCES,
        index=BASIS_SOURCES.index(st.session_state.basis_source) if st.session_state.basis_source in BASIS_SOURCES else 0,
        key="basis_source_sel",
    )

    basis_mode_options = {
        "last_3": "Past 3 months avg",
        "last_6": "Past 6 months avg",
        "last_9": "Past 9 months avg",
        "last_12": "Past 12 months avg",
        "selected": "Selected specific months",
    }
    basis_mode = col2.selectbox(
        "Window",
        list(basis_mode_options.keys()),
        format_func=lambda k: basis_mode_options[k],
        index=list(basis_mode_options.keys()).index(st.session_state.basis_mode),
        key="basis_mode_sel",
    )

    basis_months_selected = []
    if basis_mode == "selected":
        basis_df = _get_df(basis_source)
        cmap = st.session_state.col_maps.get(basis_source, {})
        avail_months = cmap.get("_months", detect_month_columns(basis_df) if basis_df is not None else [])
        basis_months_selected = st.multiselect("Select months for basis", avail_months, default=st.session_state.basis_months_selected, key="basis_months_sel")

    # ---- Compute ----
    if st.button("Compute Salience", type="primary"):
        basis_df_raw = _get_df(basis_source)
        if basis_df_raw is None:
            st.error("Basis source sheet not found.")
            return
        sku_filtered = st.session_state.sku_df_filtered
        if sku_filtered is None or sku_filtered.empty:
            st.error("No filtered SKU data. Complete Step 3 first.")
            return

        cmap = st.session_state.col_maps.get(basis_source, {})
        month_cols = cmap.get("_months", detect_month_columns(basis_df_raw))
        sku_col_actual = cmap.get("SKU")

        # Merge basis values onto sku_filtered by SKU
        basis_df_slim = basis_df_raw.copy()
        basis_vals = compute_basis(basis_df_slim, month_cols, basis_mode, basis_months_selected)
        basis_df_slim["_basis_val"] = basis_vals

        # Map SKU col from basis source to sku_filtered
        # Use Consumption/Shipments/Retailing SKU col
        sku_basis_col = cmap.get("SKU")
        hier_logical_list = LOGICAL_HIER

        # Determine which hier cols are actually available
        hier_col_map = {}
        for lh in LOGICAL_HIER:
            for role in [basis_source] + SKU_SHEETS:
                rc = st.session_state.col_maps.get(role, {}).get(lh)
                if rc:
                    hier_col_map[lh] = rc
                    break
        # Also include SKU
        for role in SKU_SHEETS:
            sc = st.session_state.col_maps.get(role, {}).get("SKU")
            if sc:
                hier_col_map["SKU"] = sc
                break

        split_level = st.session_state.split_level
        exc_store = st.session_state.exc_store

        # Merge basis onto sku_filtered
        # sku_filtered may come from merged sku sheets
        # We need _basis on the sku_filtered rows
        # Strategy: join basis_df_slim on sku col + hier cols

        merge_keys = [v for k, v in hier_col_map.items() if k != "SKU" and v in sku_filtered.columns and v in basis_df_slim.columns]
        sku_key_sku = hier_col_map.get("SKU")
        if sku_key_sku and sku_key_sku in sku_filtered.columns and sku_key_sku in basis_df_slim.columns:
            merge_keys = merge_keys + [sku_key_sku]

        if merge_keys and sku_basis_col:
            basis_slim = basis_df_slim[merge_keys + ["_basis_val"]].drop_duplicates(subset=merge_keys)
            sku_with_basis = sku_filtered.merge(basis_slim, on=merge_keys, how="left")
        else:
            sku_with_basis = sku_filtered.copy()
            sku_with_basis["_basis_val"] = np.nan

        basis_series = sku_with_basis["_basis_val"] if "_basis_val" in sku_with_basis.columns else pd.Series(np.nan, index=sku_with_basis.index)

        # Rename _basis_val back to a temp col expected by compute_salience
        sku_work = sku_with_basis.copy()

        sal_df, blocking = compute_salience(
            sku_df=sku_work,
            basis=basis_series,
            split_level=split_level,
            sku_col=hier_col_map.get("SKU", "SKU"),
            hier_col_map=hier_col_map,
            global_exclusions=exc_store.global_exclusions,
            overrides=st.session_state.sal_overrides,
        )

        st.session_state.salience_df = sal_df
        st.session_state.blocking_groups = blocking
        st.session_state.basis_source = basis_source
        st.session_state.basis_mode = basis_mode
        st.session_state.basis_months_selected = basis_months_selected

    sal_df = st.session_state.salience_df
    blocking = st.session_state.blocking_groups

    if sal_df is None:
        st.info("Click **Compute Salience** to proceed.")
        return

    # ---- Show salience table ----
    st.subheader(f"Salience Table — {len(sal_df)} rows")
    st.dataframe(sal_df, use_container_width=True, height=300)

    # ---- Blocking groups ----
    if blocking:
        st.error(f"⚠️ **{len(blocking)} blocked group(s)** with zero/missing basis. Action required:")
        for i, bg in enumerate(blocking):
            with st.expander(f"Blocked group {i+1}: {bg['group']}"):
                st.write(f"Reason: {bg['reason']} | SKUs: {bg['n_skus']}")

        st.subheader("Override Salience for Blocked Groups")
        st.caption("Enter manual salience values below (they must sum to 1 per group after normalizing).")

        override_rows = sal_df[sal_df["flag"] == "blocked"].copy()
        if not override_rows.empty:
            _sku_col_for_edit = hier_col_map_from_state().get("SKU", "SKU")
            _ctx_cols = [c for c in override_rows.columns if c not in ("basis", "flag")]
            edited = st.data_editor(
                override_rows[_ctx_cols].assign(salience=override_rows["salience"].fillna(0.0)),
                use_container_width=True,
                key="sal_override_editor",
                num_rows="fixed",
                disabled=[c for c in _ctx_cols if c != "salience"],
            )
            if st.button("Apply Overrides"):
                _sku_col_in_sal = hier_col_map_from_state().get("SKU", "SKU")
                _group_cols = [c for c in override_rows.columns if c not in (_sku_col_in_sal, "basis", "salience", "flag")]
                for idx_row, (orig_idx, row) in enumerate(override_rows.iterrows()):
                    g_key = tuple(row[c] for c in _group_cols if c in row.index)
                    sku_val = row.get(_sku_col_in_sal, "")
                    new_sal = edited.iloc[idx_row]["salience"]
                    st.session_state.sal_overrides[(g_key, sku_val)] = float(new_sal)
                st.success("Overrides saved. Click Compute Salience again to refresh.")

    # Normalize button
    if sal_df is not None and not sal_df.empty:
        if st.button("Normalize Salience to 1.0 per Group"):
            group_cols = [c for c in sal_df.columns if c not in ("basis", "salience", "flag", hier_col_map_from_state().get("SKU", "SKU"))]
            st.session_state.salience_df = normalize_salience(sal_df, group_cols, hier_col_map_from_state().get("SKU", "SKU"))
            st.rerun()

    # Check all groups resolved
    still_blocked = [r for r in (blocking or []) if not any(
        not pd.isna(v) for _, row in sal_df[sal_df["flag"].isin(["manual_override"])].iterrows()
        for v in [row.get("salience")]
    )]
    n_blocked = len(sal_df[sal_df["flag"] == "blocked"])

    if n_blocked > 0:
        st.warning(f"{n_blocked} salience row(s) still blocked. Override or proceed if acceptable.")

    # SAS months selection
    st.subheader("SAS Month Selection")
    sas_df = st.session_state.sas_df_filtered
    if sas_df is not None:
        sas_cmap = st.session_state.col_maps.get("SAS", {})
        sas_months_all = sas_cmap.get("_months", detect_month_columns(sas_df))
        sas_months_sel = st.multiselect(
            "SAS months to split (default: all)",
            sas_months_all,
            default=st.session_state.sas_months_selected or sas_months_all,
            key="sas_months_sel",
        )
        st.session_state.sas_months_selected = sas_months_sel

    if st.button("Confirm Salience →", type="primary", disabled=(sal_df is None)):
        st.session_state.step = max(st.session_state.step, 5)
        st.rerun()


def hier_col_map_from_state() -> dict[str, str]:
    """Build logical->actual col map from session state."""
    hcm = {}
    for lh in LOGICAL_HIER + ["SKU"]:
        for role in SKU_SHEETS + ["SAS"]:
            rc = st.session_state.col_maps.get(role, {}).get(lh)
            if rc:
                hcm[lh] = rc
                break
    return hcm


# ──────────────────────────────────────────────────────────────────────────────
# STEP 5 – Exceptions
# ──────────────────────────────────────────────────────────────────────────────
def step5_exceptions():
    st.header("Step 5 — Exceptions")
    exc_store: ExceptionStore = st.session_state.exc_store
    hcm = hier_col_map_from_state()
    sku_col = hcm.get("SKU", "SKU")

    sku_filtered = st.session_state.sku_df_filtered
    sas_filtered = st.session_state.sas_df_filtered
    all_skus = sorted(sku_filtered[sku_col].dropna().astype(str).unique().tolist()) if (sku_filtered is not None and sku_col in (sku_filtered.columns if sku_filtered is not None else [])) else []

    tab_global, tab_bb, tab_log = st.tabs(["Global Exclusions", "Per-BB Exceptions", "Exception Log"])

    # ---- Global exclusions ----
    with tab_global:
        st.subheader("Globally Excluded SKUs")
        st.caption("These SKUs will never receive allocation in any split.")
        new_excl = st.multiselect("Add global exclusions", all_skus, default=list(exc_store.global_exclusions), key="global_excl_sel")
        if st.button("Save Global Exclusions", key="save_global_excl"):
            # Add newly selected
            for sku in new_excl:
                if sku not in exc_store.global_exclusions:
                    exc_store.add_global_exclusion(sku)
            # Remove deselected
            for sku in list(exc_store.global_exclusions):
                if sku not in new_excl:
                    exc_store.remove_global_exclusion(sku)
            st.success(f"Global exclusions updated: {len(exc_store.global_exclusions)} SKU(s)")

    # ---- Per-BB exceptions ----
    with tab_bb:
        st.subheader("Per Building-Block Exceptions")
        if sas_filtered is None or sas_filtered.empty:
            st.warning("No SAS data loaded.")
        else:
            bb_id_col = st.session_state.bb_id_col or "BB_ID"
            bb_ids = sas_filtered[bb_id_col].astype(str).tolist() if bb_id_col in sas_filtered.columns else []

            selected_bb = st.selectbox("Select Building Block", bb_ids, key="exc_bb_sel")
            if selected_bb:
                st.caption(f"**BB_ID:** `{selected_bb}`")
                bb_row = sas_filtered[sas_filtered[bb_id_col].astype(str) == selected_bb].iloc[0] if len(sas_filtered[sas_filtered[bb_id_col].astype(str) == selected_bb]) > 0 else None
                if bb_row is not None:
                    hier_info = {lh: bb_row.get(hcm.get(lh, ""), "") for lh in LOGICAL_HIER if hcm.get(lh)}
                    st.json(hier_info)

                exc_bb = exc_store.bb_exceptions.get(selected_bb, {})
                cur_include = list(exc_bb.get("include", set()))
                cur_exclude = list(exc_bb.get("exclude", set()))
                cur_fixed = exc_bb.get("fixed_qty", {})

                c1, c2 = st.columns(2)

                with c1:
                    st.markdown("**Force Include SKUs**")
                    new_include = st.multiselect("Include", all_skus, default=cur_include, key=f"inc_{selected_bb}")
                with c2:
                    st.markdown("**Force Exclude SKUs**")
                    new_exclude = st.multiselect("Exclude", all_skus, default=cur_exclude, key=f"exc_{selected_bb}")

                # Fixed quantities
                st.markdown("**Fixed Quantity Allocations** (by SKU × Month)")
                sas_months = st.session_state.sas_months_selected or []
                if sas_months:
                    fixed_skus = st.multiselect("SKUs with fixed qty", all_skus, default=list(cur_fixed.keys()), key=f"fq_skus_{selected_bb}")
                    if fixed_skus:
                        fq_data = []
                        for sku in fixed_skus:
                            row_data = {"SKU": sku}
                            for m in sas_months:
                                row_data[m] = cur_fixed.get(sku, {}).get(m, 0.0)
                            fq_data.append(row_data)
                        fq_df = pd.DataFrame(fq_data)
                        edited_fq = st.data_editor(fq_df, use_container_width=True, key=f"fq_editor_{selected_bb}", num_rows="fixed")
                    else:
                        edited_fq = None
                else:
                    st.caption("Select SAS months in Step 4 first.")
                    edited_fq = None

                notes = st.text_input("Notes (optional)", key=f"exc_notes_{selected_bb}")

                if st.button("Save BB Exceptions", key=f"save_bb_{selected_bb}"):
                    # Include
                    old_include = set(exc_store.bb_exceptions.get(selected_bb, {}).get("include", set()))
                    for sku in set(new_include) - old_include:
                        exc_store.add_bb_include(selected_bb, sku, notes)
                    for sku in old_include - set(new_include):
                        exc_store.remove_bb_include(selected_bb, sku)
                    # Exclude
                    old_exclude = set(exc_store.bb_exceptions.get(selected_bb, {}).get("exclude", set()))
                    for sku in set(new_exclude) - old_exclude:
                        exc_store.add_bb_exclude(selected_bb, sku, notes)
                    for sku in old_exclude - set(new_exclude):
                        exc_store.remove_bb_exclude(selected_bb, sku)
                    # Fixed qty
                    if edited_fq is not None:
                        for _, fq_row in edited_fq.iterrows():
                            sku_v = fq_row["SKU"]
                            for m in sas_months:
                                qty = fq_row.get(m, 0.0)
                                if qty and float(qty) != 0.0:
                                    exc_store.set_fixed_qty(selected_bb, sku_v, m, float(qty), notes)
                    st.success("BB exceptions saved.")

    # ---- Log ----
    with tab_log:
        st.subheader("Exception Log")
        log_df = exc_store.log_as_df()
        if log_df.empty:
            st.info("No exceptions logged yet.")
        else:
            st.dataframe(log_df, use_container_width=True)

    if st.button("Proceed to Split →", type="primary"):
        st.session_state.step = max(st.session_state.step, 6)
        st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# STEP 6 – Run Split
# ──────────────────────────────────────────────────────────────────────────────
def step6_run():
    st.header("Step 6 — Run Split")

    # Pre-flight checks
    issues = []
    if st.session_state.sas_df_filtered is None:
        issues.append("No SAS data — complete Steps 1-3")
    if st.session_state.sku_df_filtered is None:
        issues.append("No SKU data — complete Steps 1-3")
    if st.session_state.salience_df is None:
        issues.append("Salience not computed — complete Step 4")
    if not st.session_state.sas_months_selected:
        issues.append("No SAS months selected — complete Step 4")

    if issues:
        for iss in issues:
            st.error(f"• {iss}")
        return

    sas_df = st.session_state.sas_df_filtered
    sku_df = st.session_state.sku_df_filtered
    sal_df = st.session_state.salience_df
    sas_months = st.session_state.sas_months_selected
    split_level = st.session_state.split_level
    bb_id_col = st.session_state.bb_id_col or "BB_ID"
    exc_store = st.session_state.exc_store
    hcm = hier_col_map_from_state()

    # Summary
    st.markdown(f"""
| Parameter | Value |
|---|---|
| Building Blocks | {len(sas_df)} |
| SKU rows | {len(sku_df)} |
| Split level | **{split_level}** |
| SAS months | {', '.join(sas_months)} |
| Global exclusions | {len(exc_store.global_exclusions)} |
| BB-specific exceptions | {len(exc_store.bb_exceptions)} |
    """)

    if st.button("▶ Run Split Now", type="primary"):
        with st.spinner("Running split…"):
            try:
                output_wide, validation_df = run_split(
                    sas_df=sas_df,
                    sku_df=sku_df,
                    salience_df=sal_df,
                    sas_months=sas_months,
                    split_level=split_level,
                    bb_id_col=bb_id_col,
                    sku_col=hcm.get("SKU", "SKU"),
                    hier_col_map=hcm,
                    exc_store=exc_store,
                )
                st.session_state.output_wide = output_wide
                st.session_state.validation_df = validation_df
                st.session_state.run_settings = {
                    "run_timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
                    "split_level": split_level,
                    "basis_source": st.session_state.basis_source,
                    "basis_mode": st.session_state.basis_mode,
                    "sas_months": ", ".join(sas_months),
                    "global_exclusions": ", ".join(exc_store.global_exclusions),
                    "n_bb": len(sas_df),
                    "n_sku_rows_input": len(sku_df),
                    "n_sku_rows_output": len(output_wide),
                }
            except Exception as e:
                st.error(f"Split failed: {e}")
                import traceback
                st.code(traceback.format_exc())
                return

        st.success(f"Split complete! **{len(output_wide)}** output rows.")

        if not validation_df.empty:
            st.warning(f"**{len(validation_df)} validation issue(s)** — review before downloading.")
            st.dataframe(validation_df, use_container_width=True)
        else:
            st.success("No validation issues.")

        st.session_state.step = max(st.session_state.step, 7)

    # Preview if already run
    if st.session_state.output_wide is not None:
        st.subheader("Output Preview (first 20 rows)")
        st.dataframe(st.session_state.output_wide.head(20), use_container_width=True)

        if st.button("Proceed to Download →", type="primary"):
            st.session_state.step = max(st.session_state.step, 7)
            st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# STEP 7 – Download
# ──────────────────────────────────────────────────────────────────────────────
def step7_download():
    st.header("Step 7 — Download Results")

    if st.session_state.output_wide is None:
        st.warning("No output yet — complete Step 6.")
        return

    output_wide = st.session_state.output_wide
    sal_df = st.session_state.salience_df or pd.DataFrame()
    exc_log = st.session_state.exc_store.log_as_df()
    val_df = st.session_state.validation_df or pd.DataFrame()
    run_settings = st.session_state.run_settings

    col1, col2, col3 = st.columns(3)
    col1.metric("Output SKU rows", len(output_wide))
    col2.metric("Salience rows", len(sal_df))
    col3.metric("Validation issues", len(val_df))

    with st.spinner("Building Excel file…"):
        xlsx_bytes = build_excel_output(
            output_wide=output_wide,
            salience_df=sal_df,
            exception_log=exc_log,
            validation_df=val_df,
            run_settings=run_settings,
        )

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    st.download_button(
        label="📥 Download BOP_Split_Output.xlsx",
        data=xlsx_bytes,
        file_name=f"BOP_Split_Output_{ts}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True,
    )

    st.divider()
    tab_out, tab_sal, tab_exc, tab_val = st.tabs(["Split Forecast", "Salience Table", "Exception Log", "Validation"])

    with tab_out:
        st.dataframe(output_wide, use_container_width=True, height=400)
    with tab_sal:
        st.dataframe(sal_df, use_container_width=True, height=400)
    with tab_exc:
        if exc_log.empty:
            st.info("No exceptions logged.")
        else:
            st.dataframe(exc_log, use_container_width=True)
    with tab_val:
        if val_df.empty:
            st.success("No validation issues.")
        else:
            st.dataframe(val_df, use_container_width=True)


# ──────────────────────────────────────────────────────────────────────────────
# Main router
# ──────────────────────────────────────────────────────────────────────────────
def main():
    step = st.session_state.step

    # Allow jumping back via sidebar nav
    with st.sidebar:
        st.divider()
        st.subheader("Jump to Step")
        max_step = st.session_state.step
        jump = st.radio("", STEPS[:max_step], index=min(max_step - 1, len(STEPS) - 1), label_visibility="collapsed", key="jump_nav")
        if jump:
            target = STEPS.index(jump) + 1
            if target != st.session_state.step:
                st.session_state.step = target
                st.rerun()

    if step == 1:
        step1_upload()
    elif step == 2:
        step2_columns()
    elif step == 3:
        step3_filters()
    elif step == 4:
        step4_salience()
    elif step == 5:
        step5_exceptions()
    elif step == 6:
        step6_run()
    elif step >= 7:
        step7_download()


if __name__ == "__main__":
    main()
