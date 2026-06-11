"""BOP Splitter – Streamlit application."""
from __future__ import annotations

import datetime
import hashlib
import json
import pathlib
import pickle
import re
import uuid
from copy import deepcopy

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from bop_splitter.loader import (
    load_excel,
    detect_month_columns,
    detect_hierarchy_columns,
    detect_bop_col_maps,
    MONTHLY_MEASURES,
    MONTHLY_MEASURE_COL,
    SAS_HIERARCHY_MAP,
    MONTHLY_HIERARCHY_MAP,
    MONTHLY_SFU_V_COL,
    MONTHLY_SFU_VERSION_COL,
    SAS_SFU_V_COL,
    SAS_GBB_TYPE_COL,
    SAS_GBB_TYPE_VARIANTS,
    find_gbb_type_column,
    BIBLE_HIERARCHY_MAP,
    BIBLE_SFU_V_COL,
    BIBLE_SALIENCE_SFU_COL,
    STAT_DB_SFU_COL,
    STAT_DB_SFU_VERSION_COL,
    STAT_DB_FORECAST_FROM_COL,
    STAT_DB_FORECAST_TO_COL,
    parse_month_to_date,
)
from bop_splitter.databricks_loader import fetch_table as fetch_databricks_table, test_connection as test_databricks_connection
from bop_splitter.salience import (
    compute_basis,
    compute_salience,
    compute_equal_salience,
    normalize_salience,
    HIERARCHY_LEVELS,
    SPLIT_KEYS,
    GBB_TYPE_RULES,
    normalize_gbb_type,
    resolve_gbb_type_rule,
)
from bop_splitter.exceptions import ExceptionStore
from bop_splitter.splitter import run_split
from bop_splitter.exporter import build_excel_output
from bop_splitter.config_profile import (
    build_profile,
    apply_profile,
    profile_to_json,
    profile_from_json,
)

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
# Session persistence (survive browser refresh via URL session ID)
# ──────────────────────────────────────────────────────────────────────────────
_TMP_DIR = pathlib.Path(".tmp")
_SESSION_TTL_HOURS = 24

# Keys that must never be written to disk (security)
_SKIP_PERSIST = {"db_token"}


def _is_non_persistable_state_key(key: str) -> bool:
    """Return True for session_state keys that should not be persisted/restored."""
    # Streamlit does not allow assigning values to certain widget keys.
    if key.endswith("_btn"):
        return True
    if key.endswith("_uploader"):  # file_uploader widgets
        return True
    if "_editor" in key:  # st.data_editor and related widget-managed state
        return True
    return key in _SKIP_PERSIST

def _session_id() -> str:
    """Return the session ID from the URL query param, creating one if absent."""
    sid = st.query_params.get("sid")
    if not sid:
        sid = uuid.uuid4().hex
        st.query_params["sid"] = sid
    return sid

def _session_file(sid: str) -> pathlib.Path:
    _TMP_DIR.mkdir(exist_ok=True)
    return _TMP_DIR / f"{sid}.pkl"

def _save_session(sid: str) -> None:
    """Pickle the current session state to .tmp/{sid}.pkl."""
    try:
        data = {
            k: v
            for k, v in st.session_state.items()
            if not _is_non_persistable_state_key(k)
        }
        with open(_session_file(sid), "wb") as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        # Opportunistically clean up expired session files
        cutoff = datetime.datetime.now() - datetime.timedelta(hours=_SESSION_TTL_HOURS)
        for p in _TMP_DIR.glob("*.pkl"):
            try:
                if datetime.datetime.fromtimestamp(p.stat().st_mtime) < cutoff:
                    p.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception:
        pass  # persistence failure must never crash the app

def _restore_session(sid: str) -> bool:
    """Restore session state from disk. Returns True if successful."""
    fpath = _session_file(sid)
    if not fpath.exists():
        return False
    try:
        with open(fpath, "rb") as f:
            data = pickle.load(f)
        for k, v in data.items():
            if _is_non_persistable_state_key(k):
                continue
            try:
                st.session_state[k] = v
            except Exception:
                # Some widget-managed keys (for example buttons/uploaders)
                # cannot be assigned through session_state; skip them safely.
                continue
        return True
    except Exception:
        return False


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
        "split_level": "Form",        # default global split level
        "bb_split_levels": {},        # bb_id -> split_level string (per-BB override)
        "basis_source": "Consumption",
        "basis_mode": "last_3",
        "basis_months_selected": [],
        "sas_months_selected": [],
        "sas_forecast_months_selected": [],  # user-selected future forecast months
        "forecast_horizon_mode": "Current FY",
        "_forecast_horizon_prev": "Current FY",
        "_forecast_horizon_epoch": 0,
        "sfu_basis_sources": {},      # SFU_SFU Version -> basis config (source, mode, selected)
        "sfu_basis_overrides": {},    # SFU_SFU Version -> optional manual final basis override
        "sfu_manual_months": [],      # Global manual month selector for SFU basis (legacy)
        "sfu_specific_months": {},    # SFU_SFU Version -> list[str] of manually chosen months
        "sfu_monthly_salience": {},   # SFU_SFU Version -> {month: salience_fraction}
        "sfu_remarks": {},            # SFU_SFU Version -> user remarks string
        "step5_brand_filter": [],     # Step 5 live preview filter by Brand
        "step5_form_filter": [],      # Step 5 live preview filter by Form
        "exception_log_remarks": {},  # exception log row key -> user remark string
        "filters": {},
        "step": 1,
        "max_step": 1,
        "_loaded_file_id": None,
        "_sas_scaled_once": False,
        "data_source": "none",       # "none" | "excel" | "databricks"
        "_is_bop": False,            # True when standard SAP BEx BOP export detected
        "db_host": "",
        "db_http_path": "",
        "db_token": "",
        "db_tables": {},             # role -> fully-qualified table name
        "db_row_limit": 100_000,
        "bb_id_col": None,
        "run_settings": {},
        "forecast_boundaries": None,  # SFU_v -> (from_date, to_date) from Stat_DB
        "sas_df_split_input": None,
        "last_ff_reference_df": None,
        "last_ff_plan_col": None,
        "sas_df_topline_scope": None,
        "sas_df_prior_cycle_excluded": None,
        "bop_cycle_active_token": None,
        "bop_gap_ccm_df": None,
        "bop_gap_sku_df": None,
        "bop_final_matched_sku_df": None,
        "split_trace_df": None,
        "matching_new_total_df": None,
        "matching_last_bop_df": None,
        "matching_split_adj_df": None,
        "output_wide_final": None,
        "planner_adjustment_audit_df": None,
        "planner_residual_df": None,
                "confirm_reset_all": False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _inject_refresh_warning() -> None:
        """Warn before browser refresh/close so users don't lose context unexpectedly."""
        components.html(
                """
                <script>
                (function() {
                    if (window.__bopBeforeUnloadInstalled) {
                        return;
                    }
                    window.__bopBeforeUnloadInstalled = true;
                    window.addEventListener('beforeunload', function (event) {
                        event.preventDefault();
                        event.returnValue = '';
                    });
                })();
                </script>
                """,
                height=0,
                width=0,
        )

# ── Restore session from disk if a session ID is present in the URL ───────────
_sid = _session_id()
if "step" not in st.session_state:
    # Fresh Streamlit session — try to restore from the persisted pickle
    _restored = _restore_session(_sid)
    _init_state()  # fill any keys that aren't in the saved snapshot
    if _restored:
        # Backward-compatibility cleanup for sessions saved before editor keys were skipped.
        st.session_state.pop("bb_split_level_editor", None)
    if _restored and st.session_state.get("step", 1) > 1:
        st.toast("✅ Session restored — welcome back!", icon="🔀")
else:
    _init_state()  # normal rerun — just fill missing defaults

_inject_refresh_warning()

# One-time cleanup for sessions that may already contain stale editor widget state.
if "_bb_editor_state_cleaned" not in st.session_state:
    st.session_state.pop("bb_split_level_editor", None)
    st.session_state["_bb_editor_state_cleaned"] = True

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar – progress tracker
# ──────────────────────────────────────────────────────────────────────────────
STEPS = [
    "1. Upload & Map Sheets",
    "2. Map Columns",
    "3. Filters & Split Level",
    "4. Exclusions",
    "5. Basis & Salience",
    "6. Run Split",
    "7. Reasonability Check",
    "8. Download",
]

with st.sidebar:
    st.title("🔀 BOP Splitter")
    st.caption("Building Block → SFU_v Forecast Splitter")
    st.divider()
    
    # Combined progress tracker and navigation
    max_step = st.session_state.max_step
    jump = st.radio(
        "Progress & Navigation",
        STEPS[:max_step],
        index=min(st.session_state.step - 1, max_step - 1),
        format_func=lambda label: (
            f"▶️ {label}" if STEPS.index(label) + 1 == st.session_state.step
            else f"✅ {label}" if STEPS.index(label) + 1 <= st.session_state.max_step
            else f"⬜ {label}"
        ),
        label_visibility="collapsed"
    )
    if jump:
        target = STEPS.index(jump) + 1
        if target != st.session_state.step:
            st.session_state.step = target
            st.rerun()
    
    st.divider()

    # ── Monthly Profile: Save & Load ─────────────────────────────────────────
    with st.expander("💾 Monthly Profile — Save / Load", expanded=False):
        st.caption(
            "Save all your choices (sheet mapping, column mapping, filters, "
            "split levels, basis config, exclusions, exceptions) to a small JSON file. "
            "Next month, upload your new Excel and reload this profile — "
            "only review and change what's different."
        )

        # ── Save ────────────────────────────────────────────────────────────
        st.markdown("**Save current settings**")
        st.caption(
            "💡 **Recommended naming:** `Country_Category_Month` — e.g. `UK_BabyPants_Mar2026`. "
            "This makes it easy to find the right profile when you reload next month."
        )
        profile_name = st.text_input(
            "Profile name",
            value=f"bop_profile_{datetime.date.today().strftime('%Y_%m')}",
            placeholder="e.g. UK_BabyPants_Mar2026",
            key="profile_name_input",
        )
        if st.button("⬇️ Download profile as JSON", width='stretch', key="profile_save_btn"):
            try:
                profile_dict = build_profile(st.session_state, ExceptionStore)
                profile_json = profile_to_json(profile_dict)
                st.download_button(
                    label="📥 Click here to download",
                    data=profile_json,
                    file_name=f"{profile_name}.json",
                    mime="application/json",
                    key="profile_download_btn",
                )
            except Exception as _pe:
                st.error(f"Could not build profile: {_pe}")

        st.divider()

        # ── Load ────────────────────────────────────────────────────────────
        st.markdown("**Load saved profile**")
        uploaded_profile = st.file_uploader(
            "Upload a .json profile",
            type=["json"],
            key="profile_uploader",
            help="Upload a profile saved from a previous month to pre-fill all settings.",
        )
        if uploaded_profile is not None:
            if st.button("✅ Apply profile", width='stretch', key="profile_apply_btn"):
                try:
                    raw = uploaded_profile.read().decode("utf-8")
                    profile_dict = profile_from_json(raw)
                    load_notes = apply_profile(profile_dict, st.session_state, ExceptionStore)
                    # After loading, keep the user at step 1 so they upload fresh data
                    # but mark max_step so they can navigate freely
                    st.session_state.step = 1
                    st.session_state.max_step = 8
                    st.success("Profile loaded! Upload your new Excel file and all previous settings will be pre-filled.")
                    with st.expander("What was loaded", expanded=False):
                        st.text("\n".join(load_notes))
                    st.rerun()
                except ValueError as _ve:
                    st.error(f"Invalid profile file: {_ve}")
                except Exception as _le:
                    st.error(f"Could not apply profile: {_le}")

    st.divider()
    if not st.session_state.get("confirm_reset_all", False):
        if st.button("↺ Reset All", width='stretch', key="reset_all_init_btn"):
            st.session_state.confirm_reset_all = True
            st.rerun()
    else:
        st.warning(
            "Are you sure? This will clear all loaded data, mappings, filters, and outputs for this session."
        )
        c_reset, c_cancel = st.columns(2)
        if c_reset.button("Yes, reset", width='stretch', key="reset_all_confirm_btn"):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()
        if c_cancel.button("Cancel", width='stretch', key="reset_all_cancel_btn"):
            st.session_state.confirm_reset_all = False
            st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ──────────────────────────────────────────────────────────────────────────────
LOGICAL_SHEETS = [
    "Shipments", "Consumption", "Retailing", "Statistical Forecast",
    "Final Fcst to Finance",   # BOP: derived from Monthly sheet
    "SAS",
    "Bible",  # Authoritative SKU/SFU hierarchy mapping
    "Stat_DB",  # Forecast date boundaries per SFU
    "Sellout",  # Historical sellout data for salience basis
]
SKU_SHEETS = [
    "Shipments", "Consumption", "Retailing", "Statistical Forecast",
    "Final Fcst to Finance",   # BOP: derived from Monthly sheet
    "Bible",  # Can also serve as SKU source
    "Sellout",  # Sellout data with SFU_v identifier
]
LOGICAL_HIER = HIERARCHY_LEVELS  # Country, SMO Category, Brand, Sub Brand, Form
ALL_LOGICAL = LOGICAL_HIER + ["SFU_v"]
SELLOUT_UI_LABEL = "Retailing (Sellout)"


def _role_display_name(role: str) -> str:
    return SELLOUT_UI_LABEL if role == "Sellout" else role


def _role_from_display_name(role: str) -> str:
    return "Sellout" if role == SELLOUT_UI_LABEL else role


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
    role = _role_from_display_name(role)
    sheet_name = st.session_state.sheet_map.get(role)
    if not sheet_name or sheet_name not in st.session_state.sheets:
        return None
    return st.session_state.sheets[sheet_name]


def _mapped_col(role: str, logical: str) -> str | None:
    role = _role_from_display_name(role)
    return st.session_state.col_maps.get(role, {}).get(logical)


def _sku_merged() -> pd.DataFrame | None:
    """Merge all SFU_v sheets into a single deduplicated DataFrame.
    
    Priority order:
    1. Bible sheet (if available) - authoritative SKU/SFU hierarchy mapping
    2. Other SKU sheets (Shipments, Consumption, etc.)
    
    Also applies SFU version defaulting: replaces missing or "Not Assigned" values in the
    SFU version column with "00" and notifies the user.
    """
    frames = []
    hier_cols_actual = []
    defaulted_count = 0  # Track how many SFU versions were defaulted
    
    # First, try to use Bible sheet if available
    bible_df = _get_df("Bible")
    if bible_df is not None:
        cmap = st.session_state.col_maps.get("Bible", {})
        hier_actual = [cmap.get(h) for h in LOGICAL_HIER if cmap.get(h)]
        sku_actual = cmap.get("SFU_v") or cmap.get("SKU")
        
        if sku_actual and hier_actual:
            keep = hier_actual + [sku_actual]
            keep = [c for c in keep if c in bible_df.columns]
            bible_frame = bible_df[keep].drop_duplicates()
            
            # If Bible has data, use it exclusively (it's the authoritative source)
            if not bible_frame.empty:
                st.info(
                    f"ℹ️ Using **Bible** sheet as authoritative SFU hierarchy mapping "
                    f"({len(bible_frame)} unique SFU_v rows with {', '.join(hier_actual)} hierarchy)"
                )
                return bible_frame
    
    # Fallback: merge other SFU_v sheets if Bible is not available or empty
    for role in SKU_SHEETS:
        if role == "Bible":  # Skip Bible, we already tried it above
            continue
        df = _get_df(role)
        if df is None:
            continue
        cmap = st.session_state.col_maps.get(role, {})
        hier_actual = [cmap.get(h) for h in LOGICAL_HIER if cmap.get(h)]
        sku_actual = cmap.get("SFU_v") or cmap.get("SKU")
        if not sku_actual or not hier_actual:
            continue
        keep = hier_actual + [sku_actual]
        keep = [c for c in keep if c in df.columns]
        
        df_copy = df[keep].copy()
        
        # Default SFU version: replace missing or "Not Assigned" with "00" (Monthly sheet only)
        if role in MONTHLY_MEASURES and MONTHLY_SFU_VERSION_COL in df_copy.columns:
            # Count rows before defaulting
            before_count = (
                df_copy[MONTHLY_SFU_VERSION_COL].isna().sum() +
                (df_copy[MONTHLY_SFU_VERSION_COL].astype(str).str.strip().str.lower() == "not assigned").sum()
            )
            # Replace NaN and "Not Assigned" with "00"
            df_copy[MONTHLY_SFU_VERSION_COL] = df_copy[MONTHLY_SFU_VERSION_COL].fillna("00")
            df_copy.loc[
                df_copy[MONTHLY_SFU_VERSION_COL].astype(str).str.strip().str.lower() == "not assigned",
                MONTHLY_SFU_VERSION_COL
            ] = "00"
            defaulted_count += before_count
        
        frames.append(df_copy.drop_duplicates())
        hier_cols_actual = keep  # last one wins – assumes consistent mapping
    
    if not frames:
        return None
    
    # Optimization: deduplicate only the columns we care about (hier_cols_actual)
    # to avoid full row comparison. Faster than concat then drop_duplicates.
    merged = pd.concat(frames, ignore_index=True).drop_duplicates(subset=hier_cols_actual if hier_cols_actual else None)
    
    # Notify user if any SFU versions were defaulted
    if defaulted_count > 0:
        st.warning(
            f"⚠️ **SFU Version Defaulting:** Found **{defaulted_count}** missing or 'Not Assigned' SFU version values "
            f"in the Monthly tab. These have been defaulted to **'00'**."
        )
    
    return merged


def _get_forecast_date_boundaries() -> dict[str, tuple[pd.Timestamp | None, pd.Timestamp | None]] | None:
    """Load forecast date boundaries from Stat DB export sheet.
    
    Returns a dictionary mapping SFU_v (SFU + SFU Version) to (Forecast From, Forecast To) dates.
    Returns None if Stat_DB sheet is not available or doesn't have required columns.
    """
    stat_db_df = _get_df("Stat_DB")
    if stat_db_df is None:
        return None
    
    cmap = st.session_state.col_maps.get("Stat_DB", {})
    sfu_col = cmap.get("SFU")
    version_col = cmap.get("SFU Version")
    from_col = cmap.get("Forecast From")
    to_col = cmap.get("Forecast To")
    
    if not all([sfu_col, version_col, from_col, to_col]):
        return None
    
    # Check columns exist
    required_cols = [sfu_col, version_col, from_col, to_col]
    if not all(c in stat_db_df.columns for c in required_cols):
        return None
    
    boundaries = {}
    parse_errors = []
    
    for row_idx, row in stat_db_df.iterrows():
        sfu = str(row.get(sfu_col, "")).strip()
        version = str(row.get(version_col, "")).strip()
        
        if not sfu or not version:
            continue

        # "_" SFU Version rows represent workbook grand totals, not real SFU versions.
        if version == "_":
            continue
        
        # Create composite SFU_v key (matching MONTHLY_SFU_VERSION_COL format)
        sfu_v = f"{sfu}_{version}"
        
        # Parse Forecast From date with error reporting
        from_date_raw = row.get(from_col)
        from_date = _parse_forecast_date(from_date_raw, from_col, row_idx)
        
        # Parse Forecast To date with error reporting
        to_date_raw = row.get(to_col)
        to_date = _parse_forecast_date(to_date_raw, to_col, row_idx)
        
        # Normalize to start of month for comparison
        if from_date is not None:
            from_date = from_date.replace(day=1).normalize()
        
        if to_date is not None:
            to_date = to_date.replace(day=1).normalize()
        
        boundaries[sfu_v] = (from_date, to_date)
    
    return boundaries if boundaries else None


# ──────────────────────────────────────────────────────────────────────────────
# BOP-format helpers
# ──────────────────────────────────────────────────────────────────────────────
def _is_bop_sheets(sheets: dict) -> bool:
    """True when *sheets* came from a standard SAP BEx BOP export
    (loader already derived per-measure DataFrames from the Monthly tab)."""
    return "SAS" in sheets and any(m in sheets for m in MONTHLY_MEASURES)


def _show_bop_load_summary(sheets: dict) -> None:
    """Render a compact, read-only summary of auto-detected BOP content."""
    sas_df = sheets.get("SAS", pd.DataFrame())
    sas_months = detect_month_columns(sas_df)

    st.success("✅ **SAP BEx BOP export detected** — sheets and columns have been auto-mapped.")

    # Check for Bible, Stat_DB, and Retailing (Sellout) sheets
    has_bible = "Bible" in st.session_state.sheet_map
    has_stat_db = "Stat_DB" in st.session_state.sheet_map
    has_sellout = "Sellout" in st.session_state.sheet_map

    has_last_bop = "Last BOP" in st.session_state.sheet_map

    # Entry Type check — warn if the SAS sheet does not appear to be a BOP file
    if not sas_df.empty and "Entry Type" in sas_df.columns:
        first_entry_type = sas_df["Entry Type"].dropna().iloc[0] if not sas_df["Entry Type"].dropna().empty else None
        if first_entry_type is not None and str(first_entry_type).strip().upper() != "BOP":
            st.warning(
                f"⚠️ The first **Entry Type** value in the SAS sheet is **'{first_entry_type}'**, not **'BOP'**. "
                "This file may not be a standard BOP export — please verify the data before proceeding."
            )

    # Metrics row
    m_cols = st.columns(4)
    m_cols[0].metric("Building Blocks", f"{len(sas_df):,}")
    m_cols[0].caption(
        f"{len(sas_months)} months: "
        + (f"{sas_months[0]} → {sas_months[-1]}" if sas_months else "none found")
    )
    for i, measure in enumerate(MONTHLY_MEASURES, 1):
        if measure in sheets:
            mdf = sheets[measure]
            if MONTHLY_SFU_VERSION_COL in mdf.columns:
                n_sku = mdf[MONTHLY_SFU_VERSION_COL].nunique()
            else:
                n_sku = "?"
            m_cols[min(i, 3)].metric(measure, f"{len(mdf):,} rows")
            m_cols[min(i, 3)].caption(f"{n_sku:,} unique SFU_vs")

    # Localized summary notes for optional sources
    optional_notes = []
    if has_bible:
        optional_notes.append(
            "📚 **Bible** detected: authoritative source for SFU hierarchy mappings "
            "(SMO Category, Brand, Sub Brand, Form)."
        )
    if has_stat_db:
        optional_notes.append(
            "📅 **Stat_DB** detected: forecast date boundaries will be enforced per SFU."
        )
    if has_sellout:
        optional_notes.append(
            "📊 **Retailing (Sellout)** detected: available as historical basis input for salience in Step 5."
        )
    if has_last_bop:
        optional_notes.append(
            "📋 **Last BOP** detected: used in Step 7 Reasonability Check "
            "(Last BOP plus split adjustments)."
        )

    if optional_notes:
        st.markdown("**Detected optional inputs**")
        for note in optional_notes:
            st.caption(note)

    # Last BOP metric (separate row if present)
    if "Last BOP" in sheets:
        lb_df = sheets["Last BOP"]
        lb_sfuvs = lb_df[MONTHLY_SFU_VERSION_COL].nunique() if MONTHLY_SFU_VERSION_COL in lb_df.columns else "?"
        lb_months = detect_month_columns(lb_df)
        st.caption(
            f"**Last BOP:** {len(lb_df):,} rows · {lb_sfuvs} unique SFU_vs · "
            + (f"{lb_months[0]} → {lb_months[-1]}" if lb_months else "no month cols found")
        )

    # Hierarchy mapping table
    with st.expander("Auto-mapped columns (click to review)", expanded=True):
        mapping_rows = [
            {
                "Logical Level": lv,
                "SAS column": SAS_HIERARCHY_MAP[lv],
                "Monthly column → renamed to SAS name": MONTHLY_HIERARCHY_MAP[lv],
            }
            for lv in SAS_HIERARCHY_MAP
        ]
        mapping_rows.append({
            "Logical Level": "SFU_v",
            "SAS column": "Specific SFU_v (optional — pinned BBs only)",
            "Monthly column → renamed to SAS name": MONTHLY_SFU_VERSION_COL,
        })

        # GBB Type — detect using resilient header matching
        gbb_col_found = find_gbb_type_column(sas_df.columns)
        mapping_rows.append({
            "Logical Level": "GBB Type",
            "SAS column": gbb_col_found if gbb_col_found else "⚠️ not found in SAS sheet",
            "Monthly column → renamed to SAS name": "— (SAS only)",
        })

        st.dataframe(pd.DataFrame(mapping_rows), width='stretch', hide_index=True)

        if gbb_col_found:
            st.caption(
                f"✅ **GBB Type** detected as `{gbb_col_found}` in SAS. "
                "Split Level and Action will be auto-derived in Step 3."
            )
            # Show unique GBB Type values present
            unique_gbb = sas_df[gbb_col_found].dropna().astype(str).str.strip()
            unique_gbb = sorted(unique_gbb[unique_gbb != ""].unique().tolist())
            if unique_gbb:
                st.caption(f"Unique GBB Types found: {', '.join(f'`{v}`' for v in unique_gbb)}")
        else:
            st.warning(
                "⚠️ **GBB Type** column not found in the SAS sheet "
                f"(looked for: {', '.join(f'`{v}`' for v in SAS_GBB_TYPE_VARIANTS)}). "
                "You can assign GBB Types manually per Building Block in Step 3."
            )

    # SAS preview
    if not sas_df.empty:
        with st.expander("Preview SAS (Building Blocks) — first 5 rows"):
            # Pin GBB Type column first in the preview if it exists
            gbb_preview_col = find_gbb_type_column(sas_df.columns)
            if gbb_preview_col:
                priority_cols = [gbb_preview_col] + [c for c in sas_df.columns if c != gbb_preview_col]
                st.dataframe(sas_df[priority_cols].head(5), width='stretch')
            else:
                st.dataframe(sas_df.head(5), width='stretch')

    # Monthly preview per measure
    for measure in MONTHLY_MEASURES:
        if measure in sheets:
            with st.expander(f"Preview Monthly · {measure} — first 5 rows"):
                st.dataframe(sheets[measure].head(5), width='stretch')


# ──────────────────────────────────────────────────────────────────────────────
# STEP 1 – Upload & Sheet Mapping  (Excel OR Azure Databricks)
# ──────────────────────────────────────────────────────────────────────────────
def step1_upload():
    st.header("Step 1 — Load Data")

    # ── Already-loaded summary (navigated back) ────────────────────────────────
    if st.session_state.sheets and st.session_state.sheet_map:
        # If BOP format, show full summary with Bible/Stat_DB/Sellout detection
        if st.session_state.get("_is_bop"):
            _show_bop_load_summary(st.session_state.sheets)
            if st.button("Proceed to Column Mapping →", type="primary", key="bop_proceed_already_loaded"):
                st.session_state.step = max(st.session_state.step, 2)
                st.session_state.max_step = max(st.session_state.max_step, 2)
                st.rerun()
            st.divider()
            st.caption("Or upload a new file below to replace the current data")
            st.divider()
        else:
            # Generic format
            src = st.session_state.data_source
            src_label = {"excel": "Excel file", "databricks": "Azure Databricks"}.get(src, "file")
            total_rows = sum(len(v) for v in st.session_state.sheets.values())
            st.success(
                f"**{src_label}** data already loaded — "
                f"{', '.join(st.session_state.sheets.keys())} "
                f"({total_rows:,} rows total). "
                "Use the button below to continue, or load new data."
            )
            if st.button("Continue with current data →", type="primary", key="generic_continue"):
                st.session_state.step = max(st.session_state.step, 2)
                st.session_state.max_step = max(st.session_state.max_step, 2)
                st.rerun()
            st.divider()

    # ── Load options ───────────────────────────────────────────────────────────
    tab_excel, tab_db = st.tabs(["📂 Upload Excel", "🔗 Azure Databricks"])

    # ── TAB 1: Excel upload ────────────────────────────────────────────────────
    with tab_excel:
        uploaded = st.file_uploader(
            "Upload Excel file (.xlsx, .xlsm, .xlsb)",
            type=["xlsx", "xlsm", "xlsb"],
            key="file_uploader",
        )
        if uploaded:
            file_id = (uploaded.name, uploaded.size)
            if st.session_state.get("_loaded_file_id") != file_id or not st.session_state.sheets:
                with st.spinner("Reading workbook…"):
                    try:
                        sheets = load_excel(uploaded)
                    except Exception as e:
                        st.error(f"Failed to read file: {e}")
                        return
                is_bop = _is_bop_sheets(sheets)
                st.session_state.sheets = sheets
                st.session_state["_loaded_file_id"] = file_id
                st.session_state["_sas_scaled_once"] = False
                st.session_state.data_source = "excel"
                st.session_state["_is_bop"] = is_bop
                if is_bop:
                    auto_map, auto_cmaps = detect_bop_col_maps(sheets)
                    st.session_state.sheet_map = auto_map
                    st.session_state.col_maps = auto_cmaps
        elif st.session_state.data_source == "excel" and st.session_state.sheets:
            pass  # navigated back — use previously loaded Excel data
        elif not st.session_state.sheets:
            st.info("Upload an Excel workbook containing your BOP and SFU_v data.")
            return  # nothing to show below

        sheets = st.session_state.sheets
        if not sheets:
            return

        # ── BOP fast path: everything auto-mapped, no manual selection needed ──
        if st.session_state.get("_is_bop"):
            _show_bop_load_summary(sheets)
            if st.button("Proceed to Column Mapping →", type="primary", key="bop_proceed_new_upload"):
                st.session_state.step = max(st.session_state.step, 2)
                st.session_state.max_step = max(st.session_state.max_step, 2)
                st.rerun()
            return

        # ── Generic path: manual sheet-to-role mapping ─────────────────────────
        st.success(f"Loaded **{len(sheets)}** sheet(s): {', '.join(sheets.keys())}")
        
        # Get sheet names first (needed for detection below)
        sheet_names = list(sheets.keys())
        
        # Check if Bible sheet is detected
        bible_detected = any("bible" in s.lower() for s in sheet_names)
        stat_db_detected = any("stat" in s.lower() and "db" in s.lower() for s in sheet_names)
        
        if bible_detected:
            st.info(
                "📚 **Bible sheet detected!** This sheet will be used as the authoritative source for "
                "SFU hierarchy mappings (SMO Category, Brand, Sub Brand, Form). Make sure it contains "
                "columns: `SFU_v` (or `Salience SFU`), `SMO Category`, `Brand`, `Sub Brand`, and `Form`."
            )
        
        if stat_db_detected:
            st.info(
                "📅 **Stat DB export detected!** This sheet will be used to enforce forecast date boundaries. "
                "Make sure it contains columns: `SFU`, `SFU Version`, `Forecast From`, and `Forecast To`. "
                "Forecasts will only be created within the date range specified for each SFU."
            )
        
        st.subheader("Map Sheets to Roles")
        none_opt = ["— none —"] + sheet_names
        guesses = {
            "Shipments": _guess_sheet(sheet_names, ["ship"]),
            "Consumption": _guess_sheet(sheet_names, ["cons", "offtake"]),
            "Retailing": _guess_sheet(sheet_names, ["retail", "sell"]),
            "Statistical Forecast": _guess_sheet(sheet_names, ["stat", "forecast", "fcst"]),
            "SAS": _guess_sheet(sheet_names, ["sas", "block", "bb"]),
            "Bible": _guess_sheet(sheet_names, ["bible", "master", "mapping"]),
            "Stat_DB": _guess_sheet(sheet_names, ["stat db", "statdb", "stat_db"]),
            "Sellout": _guess_sheet(sheet_names, ["sellout", "sell out", "sell-out"]),
        }

        new_map = {}
        cols = st.columns(3)
        for i, role in enumerate(LOGICAL_SHEETS):
            guess = st.session_state.sheet_map.get(role) or guesses.get(role) or "— none —"
            idx = none_opt.index(guess) if guess in none_opt else 0
            sel = cols[i % 3].selectbox(f"**{_role_display_name(role)}**", none_opt, index=idx, key=f"sheet_map_{role}")
            if sel != "— none —":
                new_map[role] = sel

        if new_map:
            preview_role = st.selectbox(
                "Preview sheet",
                list(new_map.keys()),
                key="preview_role",
                format_func=_role_display_name,
            )
            if preview_role:
                st.dataframe(sheets[new_map[preview_role]].head(5), width='stretch')

        if st.button("Confirm Sheet Mapping →", type="primary", disabled=len(new_map) == 0):
            if "SAS" not in new_map:
                st.error("SAS sheet is required.")
                return
            st.session_state.sheet_map = new_map
            st.session_state.data_source = "excel"
            st.session_state.step = max(st.session_state.step, 2)
            st.session_state.max_step = max(st.session_state.max_step, 2)
            st.rerun()

    # ── TAB 2: Azure Databricks ────────────────────────────────────────────────
    with tab_db:
        st.subheader("Azure Databricks Connection")
        st.caption(
            "Connect to a Databricks SQL Warehouse and pull tables directly. "
            "Requires `databricks-sql-connector` (`pip install databricks-sql-connector`)."
        )

        col1, col2 = st.columns(2)
        db_host = col1.text_input(
            "Server Hostname",
            value=st.session_state.db_host,
            placeholder="adb-xxxxxxxxxxxx.azuredatabricks.net",
            key="db_host_input",
        )
        db_http = col2.text_input(
            "HTTP Path",
            value=st.session_state.db_http_path,
            placeholder="/sql/1.0/warehouses/xxxxxxxxxxxxxxxx",
            key="db_http_input",
        )
        db_token = st.text_input(
            "Access Token (PAT)",
            value=st.session_state.db_token,
            type="password",
            placeholder="dapi…",
            key="db_token_input",
            help="Personal Access Token from your Databricks workspace → User Settings → Access Tokens.",
        )

        ctest_col, _ = st.columns([1, 3])
        if ctest_col.button("Test Connection", key="db_test_btn"):
            if not db_host or not db_http or not db_token:
                st.warning("Fill in all three connection fields first.")
            else:
                with st.spinner("Testing…"):
                    ok, msg = test_databricks_connection(db_host, db_http, db_token)
                if ok:
                    st.success(f"✅ {msg}")
                else:
                    st.error(f"Connection failed: {msg}")

        st.divider()
        st.subheader("Map Roles to Databricks Tables")
        st.caption(
            "Enter fully-qualified table names (e.g. `catalog.schema.table_name`). "
            "**SAS / Building Blocks** is required; the others are optional."
        )

        db_tables_input = {}
        for role in LOGICAL_SHEETS:
            required_tag = " *(required)*" if role == "SAS" else " *(optional)*"
            db_tables_input[role] = st.text_input(
                f"{_role_display_name(role)}{required_tag}",
                value=st.session_state.db_tables.get(role, ""),
                placeholder="catalog.schema.table_name",
                key=f"db_table_{role}",
            )

        row_limit = st.number_input(
            "Row limit per table",
            min_value=1_000,
            max_value=2_000_000,
            value=st.session_state.db_row_limit,
            step=10_000,
            key="db_row_limit_input",
            help="Fetches at most this many rows from each table.",
        )

        if st.button("Load from Databricks →", type="primary", key="db_load_btn"):
            if not db_host or not db_http or not db_token:
                st.error("Server hostname, HTTP path and access token are all required.")
            elif not db_tables_input.get("SAS", "").strip():
                st.error("The **SAS (Building Blocks)** table name is required.")
            else:
                # Save connection settings
                st.session_state.db_host = db_host
                st.session_state.db_http_path = db_http
                st.session_state.db_token = db_token
                st.session_state.db_tables = db_tables_input
                st.session_state.db_row_limit = int(row_limit)

                loaded_sheets: dict[str, pd.DataFrame] = {}
                has_error = False

                for role, tbl in db_tables_input.items():
                    if not tbl.strip():
                        continue
                    with st.spinner(f"Fetching **{_role_display_name(role)}** from `{tbl}`…"):
                        try:
                            df = fetch_databricks_table(
                                db_host, db_http, db_token, tbl.strip(), int(row_limit)
                            )
                            loaded_sheets[role] = df
                            st.success(f"✓ **{_role_display_name(role)}**: {len(df):,} rows loaded")
                        except ImportError as exc:
                            st.error(str(exc))
                            has_error = True
                            break
                        except Exception as exc:
                            st.error(f"Failed to load **{_role_display_name(role)}** from `{tbl}`: {exc}")
                            has_error = True

                if not has_error and loaded_sheets:
                    # Store data and auto-map roles (role name = "sheet" name)
                    st.session_state.sheets = loaded_sheets
                    st.session_state.sheet_map = {r: r for r in loaded_sheets}
                    st.session_state["_loaded_file_id"] = None
                    st.session_state["_sas_scaled_once"] = False
                    st.session_state.data_source = "databricks"
                    st.session_state.step = max(st.session_state.step, 2)
                    st.session_state.max_step = max(st.session_state.max_step, 2)
                    st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# STEP 2 – Column Mapping
# ──────────────────────────────────────────────────────────────────────────────
def step2_columns():
    st.header("Step 2 — Column Mapping")

    st.subheader("Forecast Horizon")
    horizon_mode = st.radio(
        "Select planning horizon:",
        ["Current FY", "Next 18 Months"],
        index=0 if st.session_state.get("forecast_horizon_mode", "Current FY") == "Current FY" else 1,
        key="forecast_horizon_mode",
        horizontal=True,
        help=(
            "Current FY: defaults future-month selection to SAS deck horizon. "
            "Next 18 Months: defaults to all future months available."
        ),
    )
    if st.session_state.get("_forecast_horizon_prev") != horizon_mode:
        st.session_state.sas_forecast_months_selected = []
        st.session_state._forecast_horizon_prev = horizon_mode
        st.session_state._forecast_horizon_epoch = st.session_state.get("_forecast_horizon_epoch", 0) + 1
        for role_name, role_cmap in st.session_state.col_maps.items():
            if role_name in (
                "Statistical Forecast",
                "Final Fcst to Finance",
                "Stat",
                "FFF",
                "Consumption",
                "Retailing",
            ):
                role_cmap.pop("_months", None)
        st.rerun()

    st.divider()

    # ── BOP: show auto-mapped summary; manual dropdowns kept as override ───────
    if st.session_state.get("_is_bop"):
        st.info(
            "**SAP BEx BOP format** — columns have been auto-mapped from the SAS and "
            "Monthly sheets. The mappings below are pre-filled; expand any section to "
            "override a column if needed."
        )

        # Actuals basis is always Shipments
        if "Shipments" in st.session_state.sheets:
            st.session_state["monthly_basis_measure"] = "Shipments"
            mdf_preview = st.session_state.sheets["Shipments"]
            month_preview = detect_month_columns(mdf_preview)
            st.info(
                f"📦 **Basis Measure: Shipments** (always used as the actuals basis for SFU_v weighting)  \n"
                f"**{len(mdf_preview):,} rows** · "
                f"{mdf_preview[MONTHLY_SFU_V_COL].nunique() if MONTHLY_SFU_V_COL in mdf_preview.columns else '?':,} "
                f"unique SFU_vs · {len(month_preview)} month columns"
                + (f" ({month_preview[0]} → {month_preview[-1]})" if month_preview else "")
            )
        st.divider()

    for role in LOGICAL_SHEETS:
        if role not in st.session_state.sheet_map:
            continue
        df = _get_df(role)
        if df is None:
            continue

        with st.expander(f"**{_role_display_name(role)}** — {st.session_state.sheet_map[role]}", expanded=(role == "SAS")):
            cols_all = list(df.columns)
            none_opt = ["— auto —"] + cols_all
            cmap = st.session_state.col_maps.get(role, {})

            # Auto-detect months (skip for Stat_DB and Bible - they don't have month columns)
            if role not in ("Stat_DB", "Bible"):
                detected_months = detect_month_columns(df)
            else:
                detected_months = []
            
            if "_months" not in cmap and detected_months:
                if role == "SAS":
                    # For SAS: default to all future months
                    _next_m = (pd.Timestamp.now().normalize().replace(day=1) + pd.DateOffset(months=1))
                    _sas_default = []
                    for _m in detected_months:
                        _ts = parse_month_to_date(_m)
                        if _ts is not None and _ts >= _next_m:
                            _sas_default.append(_m)
                    _sas_month_default = _sas_default  # strictly future SAS months only
                elif role == "Shipments":
                    current_month_start = pd.Timestamp.now().normalize().replace(day=1)
                    _sas_default = []
                    for _m in detected_months:
                        _ts = parse_month_to_date(_m)
                        if _ts is not None and _ts < current_month_start:  # strictly past months only
                            _sas_default.append(_m)
                    _sas_month_default = _sas_default  # do NOT fall back to all months for Shipments
                elif role in ("Statistical Forecast", "Final Fcst to Finance", "Stat", "FFF", "Consumption", "Retailing"):
                    current_month_start = pd.Timestamp.now().normalize().replace(day=1)
                    _sas_default = []
                    horizon_mode = st.session_state.get("forecast_horizon_mode", "Current FY")
                    max_sas_month = None
                    if horizon_mode == "Current FY":
                        sas_df_for_max = _get_df("SAS")
                        if sas_df_for_max is not None:
                            sas_months = detect_month_columns(sas_df_for_max)
                            if sas_months:
                                parsed_sas = [
                                    parse_month_to_date(m)
                                    for m in sas_months
                                    if parse_month_to_date(m) is not None
                                ]
                                if parsed_sas:
                                    max_sas_month = max(parsed_sas)
                    for _m in detected_months:
                        _ts = parse_month_to_date(_m)
                        if _ts is not None and _ts > current_month_start:
                            if horizon_mode == "Current FY" and max_sas_month is not None:
                                if _ts <= max_sas_month:
                                    _sas_default.append(_m)
                            else:
                                _sas_default.append(_m)
                    _sas_month_default = _sas_default  # future months, limited by horizon mode
                else:
                    _sas_month_default = detected_months
            else:
                _sas_month_default = cmap.get("_months", detected_months)
            
            # Show month columns selector (skip for Stat_DB and Bible)
            if role not in ("Stat_DB", "Bible") and detected_months:
                horizon_mode = st.session_state.get("forecast_horizon_mode", "Current FY")
                if role in ("Statistical Forecast", "Final Fcst to Finance", "Stat", "FFF", "Consumption", "Retailing") and horizon_mode == "Current FY":
                    sas_df_for_options = _get_df("SAS")
                    max_sas_month_for_options = None
                    if sas_df_for_options is not None:
                        sas_months_for_options = detect_month_columns(sas_df_for_options)
                        if sas_months_for_options:
                            parsed_sas_for_options = [
                                parse_month_to_date(m)
                                for m in sas_months_for_options
                                if parse_month_to_date(m) is not None
                            ]
                            if parsed_sas_for_options:
                                max_sas_month_for_options = max(parsed_sas_for_options)

                    months_to_show = []
                    if max_sas_month_for_options is not None:
                        for m in detected_months:
                            _ts = parse_month_to_date(m)
                            if _ts is not None and _ts <= max_sas_month_for_options:
                                months_to_show.append(m)
                    else:
                        months_to_show = detected_months
                else:
                    months_to_show = detected_months

                confirmed_months = st.multiselect(
                    "Month columns",
                    months_to_show,
                    default=[m for m in _sas_month_default if m in months_to_show],
                    key=f"months_{role}_{st.session_state.get('_forecast_horizon_epoch', 0)}",
                )
                cmap["_months"] = confirmed_months
            else:
                cmap["_months"] = []

            # Hierarchy columns (skip for Stat_DB - it doesn't use hierarchy)
            if role != "Stat_DB":
                hier_ui = st.columns(len(LOGICAL_HIER))
                for j, lh in enumerate(LOGICAL_HIER):
                    if lh == "Country":
                        guess = cmap.get(lh) or _guess_col(cols_all, "Country")
                    else:
                        guess = cmap.get(lh) or _guess_col(cols_all, lh)
                    idx = none_opt.index(guess) if guess in none_opt else 0
                    sel = hier_ui[j].selectbox(lh, none_opt, index=idx, key=f"col_{role}_{lh}")
                    if sel != "— auto —":
                        cmap[lh] = sel
                    else:
                        cmap.pop(lh, None)

            # SFU_v column (not for SAS or Stat_DB)
            if role not in ("SAS", "Stat_DB"):
                if role == "Bible":
                    # Bible sheet: try SFU_v first, then Salience SFU
                    sku_guess = cmap.get("SFU_v") or cmap.get("SKU") or _guess_col(cols_all, "SFU_v", "Salience SFU", "Material", "SKU")
                elif role == "Sellout":
                    # Retailing (Sellout) sheet: prefer explicit SFU_v column if present
                    sku_guess = (
                        cmap.get("SFU_v")
                        or _guess_col(cols_all, "SFU_v", MONTHLY_SFU_VERSION_COL, "Salience SFU", "Material", "SKU")
                    )
                elif role in ("Shipments", "Statistical Forecast", "Final Fcst to Finance", "Stat", "FFF"):
                    # For these BOP roles, prioritize MONTHLY_SFU_VERSION_COL ("SFU_SFU Version") as default
                    sku_guess = cmap.get("SFU_v") or cmap.get("SKU") or (
                        MONTHLY_SFU_VERSION_COL if MONTHLY_SFU_VERSION_COL in cols_all
                        else _guess_col(cols_all, "SFU_SFU Version", "SFU_v", "SKU")
                    )
                else:
                    sku_guess = cmap.get("SFU_v") or cmap.get("SKU") or _guess_col(cols_all, "SFU_v", "SKU")
                sku_idx = none_opt.index(sku_guess) if sku_guess in none_opt else 0
                sku_label = "SFU_v column" if role == "Bible" else "SFU_v column"
                sku_sel = st.selectbox(sku_label, none_opt, index=sku_idx, key=f"col_{role}_SKU")
                if sku_sel != "— auto —":
                    cmap["SKU"] = sku_sel if role != "Bible" else None
                    cmap["SFU_v"] = sku_sel  # always update SFU_v to the current selection
                else:
                    cmap.pop("SKU", None) if role != "Bible" else None
                    cmap.pop("SFU_v", None)

            # Stat_DB specific columns
            if role == "Stat_DB":
                st.markdown("**Stat DB Export Columns**")
                
                # SFU column
                sfu_guess = cmap.get("SFU") or _guess_col(cols_all, STAT_DB_SFU_COL, "SFU")
                sfu_idx = none_opt.index(sfu_guess) if sfu_guess in none_opt else 0
                sfu_sel = st.selectbox("SFU column", none_opt, index=sfu_idx, key=f"col_{role}_SFU")
                if sfu_sel != "— auto —":
                    cmap["SFU"] = sfu_sel
                else:
                    cmap.pop("SFU", None)
                
                # SFU Version column
                version_guess = cmap.get("SFU Version") or _guess_col(cols_all, STAT_DB_SFU_VERSION_COL, "SFU Version", "Version")
                version_idx = none_opt.index(version_guess) if version_guess in none_opt else 0
                version_sel = st.selectbox("SFU Version column", none_opt, index=version_idx, key=f"col_{role}_Version")
                if version_sel != "— auto —":
                    cmap["SFU Version"] = version_sel
                else:
                    cmap.pop("SFU Version", None)
                
                # Forecast From column
                from_guess = cmap.get("Forecast From") or _guess_col(cols_all, STAT_DB_FORECAST_FROM_COL, "Forecast From", "From")
                from_idx = none_opt.index(from_guess) if from_guess in none_opt else 0
                from_sel = st.selectbox("Forecast From column (date: dd-mm-yy)", none_opt, index=from_idx, key=f"col_{role}_From")
                if from_sel != "— auto —":
                    cmap["Forecast From"] = from_sel
                else:
                    cmap.pop("Forecast From", None)
                
                # Forecast To column
                to_guess = cmap.get("Forecast To") or _guess_col(cols_all, STAT_DB_FORECAST_TO_COL, "Forecast To", "To")
                to_idx = none_opt.index(to_guess) if to_guess in none_opt else 0
                to_sel = st.selectbox("Forecast To column (date: dd-mm-yy)", none_opt, index=to_idx, key=f"col_{role}_To")
                if to_sel != "— auto —":
                    cmap["Forecast To"] = to_sel
                else:
                    cmap.pop("Forecast To", None)
                
                st.caption("ℹ️ Date format should be dd-mm-yy (e.g., 15-01-26 for January 15, 2026)")


            # BB_ID column (SAS only)
            if role == "SAS":
                bb_guess = cmap.get("BB_ID") or _guess_col(cols_all, "Plan Name_Brand", "BB_ID", "building block", "bb_id")
                bb_opts = ["— generate —"] + cols_all
                bb_idx = bb_opts.index(bb_guess) if bb_guess in bb_opts else 0
                bb_sel = st.selectbox(
                    "Building Block ID column (you can also generate one automatically)",
                    bb_opts,
                    index=bb_idx,
                    key="col_SAS_BB_ID",
                    help="Select an existing column to use as the Building Block ID, or choose '— generate —' to auto-generate one from the hierarchy columns.",
                )
                cmap["BB_ID"] = bb_sel if bb_sel != "— generate —" else None

                # ── GBB Type column mapping (SAS only) ──────────────────────
                st.markdown("**GBB Type column**")
                # Auto-detect from known variants; allow user to override
                auto_gbb = cmap.get("GBB Type") or find_gbb_type_column(cols_all)
                gbb_opts = ["— not present —"] + cols_all
                gbb_idx = gbb_opts.index(auto_gbb) if auto_gbb in gbb_opts else 0
                gbb_sel = st.selectbox(
                    "GBB Type column",
                    gbb_opts,
                    index=gbb_idx,
                    key="col_SAS_GBB_Type",
                    help=(
                        "The column in the SAS sheet that contains the GBB Type "
                        "(e.g. 'Brand Building Activities', 'Promotions - Go To Market'). "
                        "This drives the default Split Level and Action for each Building Block in Step 3."
                    ),
                )
                if gbb_sel != "— not present —":
                    cmap[SAS_GBB_TYPE_COL] = gbb_sel
                    st.caption(f"✅ GBB Type mapped to column: `{gbb_sel}`")
                else:
                    cmap.pop(SAS_GBB_TYPE_COL, None)
                    st.caption(
                        "⚠️ No GBB Type column selected — you can assign GBB Types manually in Step 3."
                    )

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
        st.session_state.max_step = max(st.session_state.max_step, 3)
        st.rerun()


def _guess_col(cols: list[str], *keywords: str) -> str:
    for kw in keywords:
        for c in cols:
            if kw.lower() in c.lower():
                return c
    return "— auto —"


def _find_product_description_col(df: pd.DataFrame) -> str | None:
    """Return the best matching product-description column in a DataFrame."""
    if df is None or df.empty:
        return None

    cols = [str(c) for c in df.columns]
    normalized = {re.sub(r"[^a-z0-9]+", "", c.lower()): c for c in cols}

    preferred_exact = [
        "productdescription",
        "productdesc",
        "materialdescription",
        "materialdesc",
        "skudescription",
        "sfudescription",
    ]
    for key in preferred_exact:
        if key in normalized:
            return normalized[key]

    for c in cols:
        cl = c.lower()
        if "description" not in cl:
            continue
        if any(tok in cl for tok in ("product", "material", "sku", "sfu")):
            return c
    return None


def _latest_product_description_map() -> dict[str, str]:
    """Build SFU_v -> latest Product Description from loaded sheets.

    Latest is determined by the best available date-like column; if none exists,
    later row order is used.
    """
    sheets = st.session_state.get("sheets", {})
    if not sheets:
        return {}

    role_by_sheet = {
        str(sheet): str(role)
        for role, sheet in st.session_state.get("sheet_map", {}).items()
    }

    records: list[pd.DataFrame] = []
    order_col_candidates = [
        "Last Updated",
        "Updated At",
        "Updated On",
        "Effective Date",
        "Valid From",
        "As Of Date",
        "Snapshot Date",
        "Date",
        "Timestamp",
        "Forecast To",
        "Forecast From",
    ]

    for sheet_name, df in sheets.items():
        if df is None or df.empty:
            continue

        desc_col = _find_product_description_col(df)
        if not desc_col or desc_col not in df.columns:
            continue

        role = role_by_sheet.get(str(sheet_name))
        cmap = st.session_state.get("col_maps", {}).get(role, {}) if role else {}

        sfu_col = None
        if MONTHLY_SFU_VERSION_COL in df.columns:
            sfu_col = MONTHLY_SFU_VERSION_COL
        else:
            mapped_sfu = cmap.get("SFU_v") or cmap.get("SKU")
            if mapped_sfu in df.columns:
                sfu_col = mapped_sfu
            else:
                for cand in ["SFU_v", "SKU", BIBLE_SFU_V_COL, BIBLE_SALIENCE_SFU_COL]:
                    if cand in df.columns:
                        sfu_col = cand
                        break

        work = pd.DataFrame(index=df.index)
        if sfu_col:
            work["_sfu"] = df[sfu_col].astype(str).str.strip()
        elif STAT_DB_SFU_COL in df.columns and STAT_DB_SFU_VERSION_COL in df.columns:
            work["_sfu"] = (
                df[STAT_DB_SFU_COL].astype(str).str.strip()
                + "_"
                + df[STAT_DB_SFU_VERSION_COL].astype(str).str.strip()
            )
        else:
            continue

        work["_desc"] = df[desc_col].astype(str).str.strip()
        work = work[(work["_sfu"] != "") & (work["_desc"] != "")]
        if work.empty:
            continue

        parsed_order = pd.Series(pd.NaT, index=df.index)
        for col in order_col_candidates:
            if col in df.columns:
                parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                if parsed.notna().any():
                    parsed_order = parsed
                    break

        work["_order_ts"] = parsed_order
        work["_order_row"] = np.arange(len(work), dtype=int)
        records.append(work)

    if not records:
        return {}

    all_rows = pd.concat(records, ignore_index=True)
    all_rows["_order_ts"] = all_rows["_order_ts"].fillna(pd.Timestamp.min)
    all_rows = all_rows.sort_values(["_sfu", "_order_ts", "_order_row"]) 
    latest = all_rows.drop_duplicates(subset=["_sfu"], keep="last")
    return dict(zip(latest["_sfu"], latest["_desc"]))


def _format_sku_excl(sku_value: str, desc_map: dict[str, str] | None = None) -> str:
    """Format a SKU value with its product description for display."""
    if desc_map is None:
        desc_map = _latest_product_description_map()
    sku_norm = str(sku_value).strip()
    desc = str(desc_map.get(sku_norm, "")).strip()
    return f"{sku_norm} ({desc})" if desc else sku_norm


def _add_product_description(df: pd.DataFrame) -> pd.DataFrame:
    """Add Product Description to SFU_v tables for display purposes."""
    if df is None or df.empty or "Product Description" in df.columns:
        return df

    sfu_col = next(
        (c for c in [MONTHLY_SFU_VERSION_COL, "SFU_v", "SKU"] if c in df.columns),
        None,
    )
    if not sfu_col:
        return df

    desc_map = _latest_product_description_map()
    if not desc_map:
        return df

    out = df.copy()
    out["Product Description"] = out[sfu_col].astype(str).str.strip().map(desc_map).fillna("")
    cols = list(out.columns)
    cols.remove("Product Description")
    insert_at = cols.index(sfu_col) + 1
    cols.insert(insert_at, "Product Description")
    return out[cols]


# ──────────────────────────────────────────────────────────────────────────────
# SFU_v aggregate helper (BOP only)
# ──────────────────────────────────────────────────────────────────────────────
def _compute_sfuv_aggregates() -> pd.DataFrame | None:
    """Return a per-SFU_SFU Version summary table with three aggregate columns:

    * **Shipments (last 12 months)** — sum of the 12 most-recent past month
      columns in the Shipments measure DataFrame.
    * **Stat Forecast (future months)** — sum of all future month columns in the
      Statistical Forecast measure DataFrame.
    * **Final Fcst to Finance (future months)** — same for Final Fcst to Finance.

    "Past" = month whose first day < the first day of the current calendar month.
    "Future" = month whose first day >= the first day of the current calendar month.

    Returns None if no BOP data or if SFU_SFU Version column is not found.
    """
    if not st.session_state.get("_is_bop"):
        return None

    # ── Classify month columns as past / future ────────────────────────────────
    current_month_start = pd.Timestamp.now().normalize().replace(day=1)

    # Pull month list from whichever measure sheet is available first
    all_month_cols: list[str] = []
    for measure in MONTHLY_MEASURES:
        if measure in st.session_state.sheets:
            all_month_cols = detect_month_columns(st.session_state.sheets[measure])
            break

    if not all_month_cols:
        return None

    past_months, future_months = [], []
    for col in all_month_cols:
        try:
            ts = pd.to_datetime(col, format="%b-%y")
            (past_months if ts < current_month_start else future_months).append(col)
        except Exception:
            pass

    last_12 = past_months[-12:] if past_months else []

    # ── Groupby key: hierarchy + SFU_SFU Version ─────────────────────────────
    # Use the first available measure sheet to discover columns
    sample_df = st.session_state.sheets.get(MONTHLY_MEASURES[0], pd.DataFrame())
    hier_cols = [c for c in ["Country", "SMO Category", "Brand", "Sub Brand", "Form"]
                 if c in sample_df.columns]

    measure_months = {
        "Shipments":              last_12,
        "Statistical Forecast":   future_months,
        "Final Fcst to Finance":  future_months,
    }

    result: pd.DataFrame | None = None

    for measure, month_subset in measure_months.items():
        if measure not in st.session_state.sheets:
            continue
        df = st.session_state.sheets[measure]
        if MONTHLY_SFU_VERSION_COL not in df.columns:
            continue

        # Exclude grand-total rollup row identified by SFU_SFU Version == "_".
        df = df[df[MONTHLY_SFU_VERSION_COL].astype(str).str.strip() != "_"]
        if df.empty:
            continue

        available = [m for m in month_subset if m in df.columns]
        if not available:
            continue

        # Aggregate at SFU_SFU Version level across month columns
        group_cols = [c for c in hier_cols + [MONTHLY_SFU_VERSION_COL]
                      if c in df.columns]
        if not group_cols:
            continue

        tmp = df[group_cols].copy()
        numeric = df[available].apply(pd.to_numeric, errors="coerce")
        for m in available:
            tmp[m] = numeric[m].values
        agg = tmp.groupby(group_cols, dropna=False)[available].sum().reset_index()
        agg[measure] = agg[available].sum(axis=1)
        agg = agg[group_cols + [measure]]

        result = agg if result is None else result.merge(agg, on=group_cols, how="outer")

    return result if result is not None and not result.empty else None


# ──────────────────────────────────────────────────────────────────────────────
# STEP 3 – Filters & Split Level
# ──────────────────────────────────────────────────────────────────────────────
def _classify_months(months: list[str]) -> tuple[list[str], list[str]]:
    """Split month list into (past_months, future_months) relative to today."""
    current_month_start = pd.Timestamp.now().normalize().replace(day=1)
    past, future = [], []
    for m in months:
        try:
            ts = pd.to_datetime(m, format="%b-%y")
            (past if ts < current_month_start else future).append(m)
        except Exception:
            future.append(m)
    return past, future


def _validate_hierarchy_completeness(df: pd.DataFrame, hier_cols: list[str], df_name: str = "DataFrame") -> None:
    """Check for NaN values in hierarchy columns and warn user."""
    if df is None or df.empty:
        return
    
    for col in hier_cols:
        if col not in df.columns:
            continue
        nulls = df[col].isna().sum()
        if nulls > 0:
            pct = (nulls / len(df)) * 100
            st.warning(
                f"⚠️ **{df_name}**: Column `{col}` has **{nulls} null values** ({pct:.1f}%). "
                f"Rows with missing hierarchy may not match correctly during split."
            )


def _parse_forecast_date(date_raw: any, column_name: str, row_idx: int = None) -> pd.Timestamp | None:
    """Parse forecast date with explicit error reporting and multiple format support.
    
    Args:
        date_raw: Raw date value from DataFrame
        column_name: Name of the column (for logging)
        row_idx: Row index (for logging context)
    
    Returns:
        Parsed pd.Timestamp, or None if unparseable
    """
    if pd.isna(date_raw):
        return None
    
    # If already a timestamp, just return it
    if isinstance(date_raw, pd.Timestamp):
        return date_raw
    
    # Try multiple date formats
    formats = ["%d-%m-%y", "%d/%m/%y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%m-%d-%Y"]
    
    for fmt in formats:
        try:
            return pd.to_datetime(str(date_raw), format=fmt)
        except (ValueError, TypeError):
            continue
    
    # If all formats fail, log warning and return None
    ctx = f" (row {row_idx})" if row_idx is not None else ""
    st.warning(
        f"⚠️ Could not parse `{column_name}`: `{date_raw}`{ctx}. "
        f"Tried formats: dd-mm-yy, dd/mm/yy, dd-mm-yyyy, etc. This boundary will be ignored."
    )
    return None


def step3_filters():
    st.header("Step 3 — Filters & Split Level")

    sas_df = _get_df("SAS")
    if sas_df is None:
        st.error("SAS sheet not loaded.")
        return

    sas_cmap = st.session_state.col_maps.get("SAS", {})

    # Build BB_ID if needed
    if st.session_state.bb_id_col not in sas_df.columns:
        sas_df = sas_df.copy()
        if "Plan Name" in sas_df.columns and "Brand" in sas_df.columns:
            sas_df["Plan Name_Brand"] = (
                sas_df["Plan Name"].astype(str).str.strip()
                + "_"
                + sas_df["Brand"].astype(str).str.strip()
            )
            st.session_state.bb_id_col = "Plan Name_Brand"
        else:
            hier_for_hash = [sas_cmap.get(h) for h in LOGICAL_HIER if sas_cmap.get(h) and sas_cmap.get(h) in sas_df.columns]
            sas_df["BB_ID"] = _make_bb_id(sas_df, hier_for_hash)
            st.session_state.bb_id_col = "BB_ID"
        st.session_state.sheets[st.session_state.sheet_map["SAS"]] = sas_df

    # ---- Aggregation (One block = One total) ----
    hier_actual = [sas_cmap.get(h) for h in LOGICAL_HIER if sas_cmap.get(h) and sas_cmap.get(h) in sas_df.columns]
    bb_id_col = st.session_state.bb_id_col or "BB_ID"
    group_keys = [k for k in [bb_id_col] + hier_actual if k in sas_df.columns]

    if group_keys:
        month_cols = detect_month_columns(sas_df)
        if month_cols:
            month_col_set = set(month_cols)
            # Preserve GBB Type through aggregation (it can be non-object in some files).
            mapped_gbb_col = sas_cmap.get(SAS_GBB_TYPE_COL)
            gbb_col_for_group = (
                mapped_gbb_col
                if mapped_gbb_col in sas_df.columns
                else find_gbb_type_column(sas_df.columns)
            )
            # Include all non-month, non-numeric metadata columns in the groupby
            # so they are preserved after aggregation (e.g. GBB Type, Entry Type, Plan Name)
            meta_cols = [
                c for c in sas_df.columns
                if c not in month_col_set
                and c not in group_keys
                and sas_df[c].dtype == object  # string/categorical metadata
            ]
            if gbb_col_for_group and gbb_col_for_group not in group_keys and gbb_col_for_group not in meta_cols:
                meta_cols.append(gbb_col_for_group)
            full_group_keys = group_keys + meta_cols

            # Ensure all month columns are numeric for summation
            sas_df[month_cols] = sas_df[month_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
            # Group and sum — metadata columns are part of the key so they survive
            sas_df = sas_df.groupby(full_group_keys, as_index=False, dropna=False)[month_cols].sum()
            # C-903: SAS values are stored /1000 in the source file — multiply back
            # to real SU volume once per loaded dataset, not on every rerun.
            if not st.session_state.get("_sas_scaled_once", False):
                sas_df[month_cols] = sas_df[month_cols] * 1000
                st.session_state["_sas_scaled_once"] = True
            # Update session state with aggregated data
            st.session_state.sheets[st.session_state.sheet_map["SAS"]] = sas_df

    # ---- Month Selection ----
    st.subheader("Months")
    all_sas_months = sas_cmap.get("_months", detect_month_columns(sas_df))
    past_months, future_months = _classify_months(all_sas_months)

    # Actuals (past months): always auto-selected — show as read-only info
    if past_months:
        st.info(
            f"**Actual months (auto-selected — all {len(past_months)}):** "
            + "  |  ".join(past_months)
        )
    else:
        st.caption("No historical months found in SAS sheet.")

    # Forecast months: default = next month through July of the same year
    def _default_forecast_months(months: list[str], horizon_mode: str) -> list[str]:
        if not months:
            return []

        # Next 18 Months: use all future months available in the loaded data.
        if horizon_mode == "Next 18 Months":
            return months

        # Current FY: use future months up to the last month available in SAS deck.
        parsed = [pd.to_datetime(m, format="%b-%y") for m in months if parse_month_to_date(m) is not None]
        if not parsed:
            return months
        max_sas_month = max(parsed)
        result = []
        for m in months:
            try:
                ts = pd.to_datetime(m, format="%b-%y")
                if ts <= max_sas_month:
                    result.append(m)
            except Exception:
                continue
        return result or months

    horizon_mode = st.session_state.get("forecast_horizon_mode", "Current FY")
    prev_forecast = st.session_state.sas_forecast_months_selected or _default_forecast_months(future_months, horizon_mode)
    selected_forecast = st.multiselect(
        f"Select forecast months to split: Forecast horizon mode: {horizon_mode} —\nmonths are auto-picked",
        future_months,
        default=[m for m in prev_forecast if m in future_months],
        key="forecast_months_step3",
    )
    st.session_state.sas_forecast_months_selected = selected_forecast
    sas_months = past_months + selected_forecast
    st.session_state.sas_months_selected = sas_months

    # ---- Building Block list ----
    st.subheader("Building Blocks")
    hier_actual = [sas_cmap.get(h) for h in LOGICAL_HIER if sas_cmap.get(h) and sas_cmap.get(h) in sas_df.columns]
    # Only show selected forecast months (not historical) in the Building Block table
    forecast_month_cols_in_data = [m for m in selected_forecast if m in sas_df.columns]
    display_cols = hier_actual + forecast_month_cols_in_data
    if display_cols:
        bb_table = sas_df[display_cols].drop_duplicates().reset_index(drop=True)
        if forecast_month_cols_in_data:
            bb_table = bb_table.copy()
            bb_table["Total"] = bb_table[forecast_month_cols_in_data].apply(pd.to_numeric, errors="coerce").sum(axis=1)
        st.dataframe(bb_table, width='stretch', height=min(400, 40 + len(bb_table) * 35))
        st.caption(f"**{len(bb_table)}** Building Blocks")
    else:
        st.info("No hierarchy columns mapped — configure hierarchy columns in Step 2 to see Building Blocks here.")

    # ---- Diagnose SFU_v data availability ----
    sku_merged = _sku_merged()
    sku_problem = None
    if sku_merged is None:
        sku_sheets_mapped = [r for r in SKU_SHEETS if r in st.session_state.sheet_map]
        if not sku_sheets_mapped:
            sku_problem = (
                "No SFU_v sheets are mapped. Go back to **Step 1** and map at least one of: "
                "Shipments, Consumption, Retailing, or Statistical Forecast."
            )
        else:
            no_sku_col = [r for r in sku_sheets_mapped if not st.session_state.col_maps.get(r, {}).get("SKU") and not st.session_state.col_maps.get(r, {}).get("SFU_v")]
            no_hier = [
                r for r in sku_sheets_mapped
                if (st.session_state.col_maps.get(r, {}).get("SKU") or st.session_state.col_maps.get(r, {}).get("SFU_v"))
                and not any(st.session_state.col_maps.get(r, {}).get(h) for h in LOGICAL_HIER)
            ]
            if no_sku_col:
                sku_problem = (
                    f"The **SFU_v column** is not mapped for: {', '.join(no_sku_col)}. "
                    "Go back to **Step 2** and select the SFU_v column for those sheets."
                )
            elif no_hier:
                sku_problem = (
                    f"No hierarchy columns are mapped for: {', '.join(no_hier)}. "
                    "Go back to **Step 2** and map at least one hierarchy column."
                )
            else:
                sku_problem = (
                    "Could not load SFU_v data from mapped sheets. "
                    "Check that the SFU_v column and at least one hierarchy column are set in **Step 2**."
                )

    if sku_problem:
        st.error(sku_problem)
    else:
        st.info(f"**{len(sas_df)}** Building Blocks | **{len(sku_merged)}** SFU_v rows available")

    # ---- Per-BB Split Granularity ----
    st.subheader("Split Granularity per Building Block")
    
    st.info(
        "👉 **How to customize:** Click any cell in the **Split Level ▾** or **Action ▾** columns below to open a dropdown menu.\n\n"
        "- **Split Level**: Choose granularity (Country, SMO Category, Brand, Sub Brand, Form, or SFU_v)\n"
        "- **Action**: Choose 'split' (normal), 'exceptions' (route to exception list), or 'ignore' (exclude from split)"
    )

    bb_id_col = st.session_state.bb_id_col or "BB_ID"
    saved_bb_split_levels = st.session_state.get("bb_split_levels", {})

    # Detect GBB Type column in SAS.
    # Prefer the explicit Step 2 mapping, then fall back to known variants.
    mapped_gbb_col = sas_cmap.get(SAS_GBB_TYPE_COL)
    if mapped_gbb_col in sas_df.columns:
        _gbb_col = mapped_gbb_col
    else:
        _gbb_col = find_gbb_type_column(sas_df.columns)

    if _gbb_col:
        st.caption(
            f"**GBB Type** is pulled directly from the SAS sheet (column: `{_gbb_col}`) and is read-only. "
            "It drives the default **Split Level** and **Action** for each Building Block. "
            "You can still override **Split Level** for any row. "
            "Rows flagged as *exceptions* or *ignore* will be highlighted in Step 4."
        )
    else:
        st.caption(
            "No GBB Type column was found in the SAS sheet — you can manually assign a **GBB Type** per row. "
            "GBB Type drives the default **Split Level** and **Action** for each Building Block. "
            "Rows flagged as *exceptions* or *ignore* will be highlighted in Step 4."
        )

    # Auto-detect SFU_v split level: if SAS has a Specific SFU_v column with data for a BB row → use SFU_v level
    _sas_sfuv_col_in_data = SAS_SFU_V_COL if SAS_SFU_V_COL in sas_df.columns else None

    def _auto_split_level(bb_row_subset: pd.DataFrame) -> str:
        """Return 'SFU_v' if any row in this BB slice has a filled Specific SFU_v value."""
        if _sas_sfuv_col_in_data is None:
            return "Form"
        vals = bb_row_subset[_sas_sfuv_col_in_data].dropna().astype(str).str.strip()
        return "SFU_v" if (vals != "").any() else "Form"

    def _gbb_split_level(gbb_type: str, bid: str) -> str:
        """Derive split level from GBB Type rule, falling back to SFU_v auto-detect or Form."""
        rule = resolve_gbb_type_rule(gbb_type)
        if rule and not rule["user_defined"]:
            return rule["split_level"]
        # user_defined or unknown type → fall through to SFU_v check
        bb_rows = sas_df[sas_df[bb_id_col] == bid]
        return _auto_split_level(bb_rows)

    def _gbb_action(gbb_type: str) -> str:
        rule = resolve_gbb_type_rule(gbb_type)
        return rule["action"] if rule else "split"

    if hier_actual and bb_id_col in sas_df.columns:
        # Build base columns: hierarchy + BB_ID + optionally GBB Type
        base_cols = hier_actual + [bb_id_col]
        if _gbb_col:
            base_cols = hier_actual + [bb_id_col, _gbb_col]

        bb_editor_df = (
            sas_df[base_cols]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # Rename mapped GBB Type column to the standard app column name.
        if _gbb_col and _gbb_col != SAS_GBB_TYPE_COL:
            bb_editor_df = bb_editor_df.rename(columns={_gbb_col: SAS_GBB_TYPE_COL})

        # Fill missing GBB Type with empty string (only when not sourced from SAS)
        if SAS_GBB_TYPE_COL not in bb_editor_df.columns:
            bb_editor_df[SAS_GBB_TYPE_COL] = ""

        def _resolve_split_level(row):
            bid = row[bb_id_col]
            gbb = str(row.get("GBB Type", "") or "").strip()
            if bid in saved_bb_split_levels:
                return saved_bb_split_levels[bid]
            return _gbb_split_level(gbb, bid)

        bb_editor_df["Split Level"] = bb_editor_df.apply(_resolve_split_level, axis=1)
        bb_editor_df["Action"] = bb_editor_df["GBB Type"].apply(
            lambda g: _gbb_action(str(g).strip())
        )

        # Columns users may edit: Split Level and Action always; GBB Type only if NOT sourced from SAS
        editable_cols = ["Split Level", "Action"]
        if not _gbb_col:
            editable_cols.append("GBB Type")
        disabled_cols = [c for c in bb_editor_df.columns if c not in editable_cols]

        col_config = {
            "Split Level": st.column_config.SelectboxColumn(
                "Split Level ▾",
                options=list(SPLIT_KEYS.keys()),
                required=True,
                disabled=False,  # Explicitly enable editing
                help="Click a cell to open the dropdown and override the split granularity for this Building Block.",
            ),
            "Action": st.column_config.SelectboxColumn(
                "Action ▾",
                options=["split", "exceptions", "ignore"],
                required=True,
                disabled=False,  # Explicitly enable editing
                help="Click a cell to choose the action: 'split' = normal split using salience, 'exceptions' = route to exceptions list, 'ignore' = exclude from split.",
            ),
        }

        if _gbb_col:
            # GBB Type came from SAS — display read-only with a clear label
            col_config["GBB Type"] = st.column_config.TextColumn(
                "GBB Type (from SAS)",
                disabled=True,
                help=f"Read directly from the SAS sheet column '{_gbb_col}'. Edit in the source file to change.",
            )
        else:
            # GBB Type not in SAS — let user set it manually
            col_config["GBB Type"] = st.column_config.SelectboxColumn(
                "GBB Type (manual) ▾",
                options=[""] + list(GBB_TYPE_RULES.keys()),
                required=False,
                disabled=False,  # Explicitly enable editing when user-defined
                help="No GBB Type column found in SAS — click a cell and select from the dropdown to drive split level and action.",
            )

        edited_bb = st.data_editor(
            bb_editor_df,
            column_config=col_config,
            disabled=disabled_cols,
            width='stretch',
            key="bb_split_level_editor",
            height=min(450, 60 + len(bb_editor_df) * 35),
            hide_index=True,
        )
        st.caption(
            "💡 **Tip:** Click any cell in the **Split Level ▾** or **Action ▾** columns "
            "(or **GBB Type ▾** if shown) to open a dropdown and change the value."
        )
        
        # Show how many building blocks have been customized
        if saved_bb_split_levels:
            st.info(f"ℹ️ {len(saved_bb_split_levels)} building block(s) have custom split levels configured.")

        # Show legend
        with st.expander("📋 GBB Type rules reference (click to expand)", expanded=False):
            # Number prefixes as they appear in SAS files
            _gbb_prefix_map = {
                "Base": "0.",
                "Brand Building Activities": "1.",
                "Promotions - Go To Market": "2.",
                "New Channels": "3.",
                "Initiatives": "4.",
                "Pricing Strategy": "5.",
                "Market Trend": "6.",
                "Customer Inventory Strategy": "8.",
            }
            rule_rows = [
                {
                    "GBB Type (in SAS file)": f"{_gbb_prefix_map.get(k, '')} {k}".strip(),
                    "Default Split Level": v["split_level"],
                    "Action": v["action"],
                    "Notes": v["description"],
                }
                for k, v in GBB_TYPE_RULES.items()
            ]
            st.caption(
                "The **GBB Type** in your SAS file may include a leading number prefix "
                "(e.g. `1. Brand Building Activities`). The tool automatically strips the prefix "
                "and uses the SAS value to pick the correct Split Level and Action."
            )
            st.dataframe(pd.DataFrame(rule_rows), width='stretch', hide_index=True)

    else:
        # Fallback: single global split level radio when BB_ID not available
        edited_bb = None
        global_split_level = st.radio(
            "Split each Building Block at:",
            list(SPLIT_KEYS.keys()),
            index=list(SPLIT_KEYS.keys()).index(st.session_state.split_level),
            horizontal=True,
            key="split_level_radio_fallback",
        )
        st.session_state.split_level = global_split_level

    if st.button("Confirm Months & Split Level →", type="primary", disabled=bool(sku_problem)):
        # Save per-BB split levels
        if edited_bb is not None and bb_id_col in edited_bb.columns:
            new_bb_split_levels = {
                str(bid): lvl for bid, lvl in zip(edited_bb[bb_id_col], edited_bb["Split Level"])
            }
            st.session_state.bb_split_levels = new_bb_split_levels
            # Use the most common level as the global default (for display purposes)
            if new_bb_split_levels:
                from collections import Counter
                st.session_state.split_level = Counter(new_bb_split_levels.values()).most_common(1)[0][0]

            # Persist GBB action flags so Step 4 can surface them
            gbb_actions: dict[str, str] = {}
            gbb_types: dict[str, str] = {}
            if "Action" in edited_bb.columns and "GBB Type" in edited_bb.columns:
                for _, row in edited_bb.iterrows():
                    bid = str(row[bb_id_col])
                    gbb_actions[bid] = str(row.get("Action", "split"))
                    gbb_types[bid] = str(row.get("GBB Type", ""))
            st.session_state.gbb_actions = gbb_actions
            st.session_state.gbb_types = gbb_types
        else:
            new_bb_split_levels = {}
            st.session_state.bb_split_levels = {}
            st.session_state.gbb_actions = {}
            st.session_state.gbb_types = {}

        sas_out = sas_df.reset_index(drop=True)
        sku_out = sku_merged.reset_index(drop=True)

        # Canonicalize runtime SFU identifier to SFU_SFU Version across the app.
        sfu_source_col = next(
            (c for c in [MONTHLY_SFU_VERSION_COL, "SFU_v", "SKU"] if c in sku_out.columns),
            None,
        )
        if sfu_source_col and sfu_source_col != MONTHLY_SFU_VERSION_COL:
            sku_out[MONTHLY_SFU_VERSION_COL] = sku_out[sfu_source_col].astype(str).str.strip()

        # Exclude grand-total rollup row identified by SFU_SFU Version == "_".
        if MONTHLY_SFU_VERSION_COL in sku_out.columns:
            sku_out = sku_out[sku_out[MONTHLY_SFU_VERSION_COL].astype(str).str.strip() != "_"].copy()

        # Keep all SFU_vs for salience setup; basis can come from Shipments / Retailing (Sellout) / other metrics.
        # We still compute and show FFF coverage as an informational hint only.
        fff_df = _get_df("Final Fcst to Finance")
        if fff_df is not None and not fff_df.empty and MONTHLY_SFU_VERSION_COL in fff_df.columns:
            fff_months = detect_month_columns(fff_df)
            current_month_start_filter = pd.Timestamp.now().normalize().replace(day=1)
            future_fff_months = [
                m for m in fff_months
                if (parse_month_to_date(m) or pd.Timestamp.min) >= current_month_start_filter
            ]
            if future_fff_months and MONTHLY_SFU_VERSION_COL in sku_out.columns:
                fff_numeric = fff_df[future_fff_months].apply(pd.to_numeric, errors="coerce").fillna(0)
                fff_df = fff_df.copy()
                fff_df["_has_fff"] = fff_numeric.sum(axis=1) > 0
                sfus_with_fff = set(
                    fff_df.loc[fff_df["_has_fff"], MONTHLY_SFU_VERSION_COL].dropna().astype(str).tolist()
                )
                total_sfus = len(sku_out[MONTHLY_SFU_VERSION_COL].astype(str).unique())
                covered_sfus = len(set(sku_out[MONTHLY_SFU_VERSION_COL].astype(str).tolist()) & sfus_with_fff)
                st.info(
                    f"ℹ️ Future FFF coverage: **{covered_sfus}/{total_sfus}** SFU_vs have non-zero FFF. "
                    "All SFU_vs are kept for Step 5 salience configuration."
                )

        st.session_state.sas_df_filtered = sas_out
        st.session_state.sku_df_filtered = sku_out

        # Validate hierarchy completeness (check for NaN values)
        hcm = hier_col_map_from_state()
        sas_hier_cols = [sas_cmap.get(h) for h in LOGICAL_HIER if sas_cmap.get(h) and sas_cmap.get(h) in sas_out.columns]
        sku_hier_cols = [hcm.get(h) for h in LOGICAL_HIER if hcm.get(h) and hcm.get(h) in sku_out.columns]
        _validate_hierarchy_completeness(sas_out, sas_hier_cols, "SAS DataFrame")
        _validate_hierarchy_completeness(sku_out, sku_hier_cols, "SKU DataFrame")
        forecast_boundaries = _get_forecast_date_boundaries()
        st.session_state.forecast_boundaries = forecast_boundaries
        if forecast_boundaries:
            st.info(
                f"📅 Loaded forecast date boundaries for **{len(forecast_boundaries)}** SFU_v(s) from Stat DB export. "
                "Splits will respect Forecast From/To dates."
            )

        # Compute equal salience per unique split level used across BBs
        sku_col = hcm.get("SFU_v", hcm.get("SKU", "SKU"))
        global_exclusions = st.session_state.exc_store.global_exclusions

        unique_levels = set(new_bb_split_levels.values()) if new_bb_split_levels else {st.session_state.split_level}
        sal_parts = []
        for lvl in unique_levels:
            part, _ = compute_equal_salience(sku_out, lvl, sku_col, hcm, global_exclusions)
            if not part.empty:
                part["_split_level"] = lvl
                sal_parts.append(part)
        combined_sal = pd.concat(sal_parts, ignore_index=True) if sal_parts else pd.DataFrame()
        st.session_state.salience_df = combined_sal

        st.session_state.step = max(st.session_state.step, 4)
        st.session_state.max_step = max(st.session_state.max_step, 4)
        st.rerun()


def _build_exclusion_upload_template(sku_col: str, months: list[str]) -> bytes:
    """Build an Excel template for the exclusions upload (SKU column + month columns)."""
    import io
    template_months = months if months else ["Jan-26", "Feb-26", "Mar-26"]
    # Add a couple of example rows
    rows = []
    for i in range(2):
        row = {sku_col: f"1234567{i}"}
        for m in template_months:
            row[m] = 0.0
        rows.append(row)
    template_df = pd.DataFrame(rows, columns=[sku_col] + template_months)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        template_df.to_excel(writer, index=False, sheet_name="Exclusions")
    return buf.getvalue()


def render_detailed_exclusion_tools() -> None:
    """Detailed exclusion review table and exclusions upload workflow."""
    st.markdown("#### Detailed Global Exclusion Review")
    st.caption(
        "Review SFU_v exclusions in detail before salience is calculated. "
        "Excluded items receive salience 0 and do not participate in the split."
    )

    sku_df_excl = st.session_state.sku_df_filtered
    exc_store_sal: ExceptionStore = st.session_state.exc_store
    hcm_excl = hier_col_map_from_state()
    sku_col_excl = hcm_excl.get("SFU_v", MONTHLY_SFU_VERSION_COL)
    desc_map_excl = _latest_product_description_map()

    if sku_df_excl is not None:
        candidate_cols = [
            sku_col_excl,
            MONTHLY_SFU_VERSION_COL,
            "SFU_v",
            "SKU",
        ]
        sku_col_excl = next((c for c in candidate_cols if c and c in sku_df_excl.columns), None)

    current_month_start_excl = pd.Timestamp.now().normalize().replace(day=1)

    sparse_skus: set[str] = set()
    shipments_df = _get_df("Shipments")
    if shipments_df is not None and sku_col_excl in shipments_df.columns:
        ship_past_months = [
            m for m in detect_month_columns(shipments_df)
            if (parse_month_to_date(m) or pd.Timestamp.max) < current_month_start_excl
        ]
        if ship_past_months:
            for _sku_val, grp in shipments_df.groupby(sku_col_excl):
                nonzero_months = sum(
                    1 for m in ship_past_months
                    if m in grp.columns
                    and pd.to_numeric(grp[m], errors="coerce").sum() > 0
                )
                if nonzero_months < 3:
                    sparse_skus.add(str(_sku_val))

    if sparse_skus:
        sparse_list_display = ", ".join(f"`{_format_sku_excl(v)}`" for v in sorted(sparse_skus))
        st.warning(
            f"⚠️ **{len(sparse_skus)} SFU_v value(s) have fewer than 3 months of shipments** "
            "and are pre-checked for exclusion below. "
            "Please confirm whether these are **Initiatives** or new SFU_v values before saving exclusions: "
            f"{sparse_list_display}"
        )

    # ═════════════════════════════════════════════════════════════════════════════════
    # BLOCK 1: SELECT EXCLUSIONS
    # ═════════════════════════════════════════════════════════════════════════════════
    st.subheader("📋 Select Exclusions")
    st.caption("Review SFU_v exclusions in detail before salience is calculated.")
    
    if sku_df_excl is not None and not sku_df_excl.empty and sku_col_excl:
        excl_cols = []
        if MONTHLY_SFU_VERSION_COL in sku_df_excl.columns:
            excl_cols.append(MONTHLY_SFU_VERSION_COL)
        if sku_col_excl in sku_df_excl.columns and sku_col_excl != MONTHLY_SFU_VERSION_COL:
            excl_cols.append(sku_col_excl)
        for h in HIERARCHY_LEVELS:
            actual_h = hcm_excl.get(h)
            if actual_h and actual_h in sku_df_excl.columns and actual_h not in excl_cols:
                excl_cols.append(actual_h)

        if excl_cols:
            excl_display = (
                sku_df_excl[excl_cols]
                .drop_duplicates()
                .sort_values(excl_cols)
                .reset_index(drop=True)
            )
            excl_display = _add_product_description(excl_display)

            already_excl_set = exc_store_sal.global_exclusions
            excl_display["Exclude"] = excl_display[sku_col_excl].apply(
                lambda v: (str(v) in already_excl_set) or (str(v) in sparse_skus)
            ) if sku_col_excl in excl_display.columns else False

            excl_display["⚠️ < 3 months data"] = excl_display[sku_col_excl].apply(
                lambda v: "⚠️ Sparse" if str(v) in sparse_skus else ""
            ) if sku_col_excl in excl_display.columns else ""

            excl_display["📌 Status"] = excl_display[sku_col_excl].apply(
                lambda v: "Excluded ✓" if str(v) in already_excl_set else ""
            ) if sku_col_excl in excl_display.columns else ""

            control_cols = ["Exclude", "⚠️ < 3 months data", "📌 Status"]
            excl_ordered = control_cols + [c for c in excl_display.columns if c not in control_cols]
            excl_col_cfg: dict = {
                "Exclude": st.column_config.CheckboxColumn(
                    "Exclude",
                    help="Check to exclude this SFU_v value from salience calculation.",
                ),
                "⚠️ < 3 months data": st.column_config.TextColumn(
                    "< 3 months shipments",
                    disabled=True,
                    help="SFU_v values with fewer than 3 non-zero shipment months — likely Initiatives.",
                ),
                "📌 Status": st.column_config.TextColumn(
                    "Current Status",
                    disabled=True,
                    help="Whether this SFU_v value is currently in the global exclusions list.",
                ),
            }
            if MONTHLY_SFU_VERSION_COL in excl_display.columns:
                excl_col_cfg[MONTHLY_SFU_VERSION_COL] = st.column_config.TextColumn("SFU_v", disabled=True)
            if sku_col_excl in excl_display.columns:
                excl_col_cfg[sku_col_excl] = st.column_config.TextColumn("SFU_v", disabled=True)
            if "Product Description" in excl_display.columns:
                excl_col_cfg["Product Description"] = st.column_config.TextColumn("Product Description", disabled=True)

            # Use a signature-based editor key so checkbox defaults are refreshed
            # when sparse-SFU prechecks or current exclusion scope changes.
            sparse_sig = "|".join(sorted(str(v).strip() for v in sparse_skus))
            current_sig = "|".join(sorted(str(v).strip() for v in already_excl_set))
            editor_sig = hashlib.md5(
                f"{sku_col_excl}::{len(excl_display)}::{sparse_sig}::{current_sig}".encode("utf-8")
            ).hexdigest()[:12]
            editor_key = f"sfu_exclusion_editor_{editor_sig}"

            edited_excl = st.data_editor(
                excl_display[excl_ordered],
                column_config=excl_col_cfg,
                disabled=[c for c in excl_ordered if c != "Exclude"],
                width='stretch',
                key=editor_key,
                height=min(450, 60 + len(excl_display) * 35),
                num_rows="fixed",
            )

            if st.button("💾 Save Exclusions", key="save_excl_btn"):
                all_skus_in_table = set(
                    excl_display[sku_col_excl].dropna().astype(str).tolist()
                ) if sku_col_excl in excl_display.columns else set()
                exc_store_sal.global_exclusions -= all_skus_in_table
                newly_excluded = set()
                for _, erow in edited_excl.iterrows():
                    if erow.get("Exclude"):
                        sku_val = str(erow.get(sku_col_excl, ""))
                        if sku_val:
                            flag_note = " [< 3 months shipments — confirmed initiative/new SKU]" if sku_val in sparse_skus else ""
                            exc_store_sal.add_global_exclusion(sku_val, notes=f"Excluded before salience in Step 4{flag_note}")
                            newly_excluded.add(sku_val)
                st.session_state.exc_store = exc_store_sal
                if newly_excluded:
                    new_excl_display = ", ".join(_format_sku_excl(v) for v in sorted(newly_excluded))
                    st.success(f"✅ {len(newly_excluded)} SFU_v value(s) marked for exclusion: {new_excl_display}")
                else:
                    st.info("No exclusions set — all SFU_v values will be included in salience.")
                st.rerun()

            current_excl = exc_store_sal.global_exclusions & (
                set(excl_display[sku_col_excl].dropna().astype(str).tolist())
                if sku_col_excl in excl_display.columns else set()
            )
            if current_excl:
                current_excl_display = ", ".join(_format_sku_excl(v) for v in sorted(current_excl))
                st.warning(
                    f"⚠️ **{len(current_excl)} SFU_v value(s) currently excluded** "
                    f"(will be skipped in salience): {current_excl_display}"
                )
        else:
            st.info("No SFU_v data available — complete Step 3 first.")
    else:
        if sku_df_excl is not None and not sku_df_excl.empty and not sku_col_excl:
            st.warning(
                "Could not find a valid SKU/SFU column in the filtered data. "
                "Please check column mappings in Step 2."
            )
        else:
            st.info("No SFU_v data loaded yet — complete Step 3 first.")


def _compute_bop_salience_for_level(
    split_level: str,
    sfu_configs: dict,
    basis_overrides: dict[str, float],
    hcm: dict,
    sku_df: pd.DataFrame,
    current_month_start: pd.Timestamp,
) -> pd.DataFrame | None:
    """
    Compute BOP salience for a specific split level.
    
    Returns DataFrame with columns: group_keys + [sku_col, 'basis', 'salience', 'flag']
    """
    group_keys_logical = [k for k in SPLIT_KEYS[split_level] if k != "SFU_v"]
    group_keys = [hcm.get(k, k) for k in group_keys_logical]
    valid_group_keys = [k for k in group_keys if k in sku_df.columns]
    sku_col = hcm.get("SFU_v", MONTHLY_SFU_VERSION_COL)

    # Step 1: build {(group_vals..., SFU_version): total_SFU_basis}
    sfu_basis: dict[tuple, float] = {}

    def _is_fff_source(role: str) -> bool:
        role_n = str(role or "").strip().lower()
        return role_n in {"final fcst to finance", "fff", "final forecast to finance"}

    # Group SFUs by their configuration (source, mode, selected_months) to batch process
    config_to_sfus: dict[tuple, list] = {}
    for sfu_ver, cfg in sfu_configs.items():
        # Handle legacy simple role string or modern dict
        if isinstance(cfg, str):
            cfg = {"source": cfg, "mode": "last_3", "selected": []}
        
        # Tuple-ize the config to use as a key
        config_key = (cfg["source"], cfg["mode"], tuple(sorted(cfg.get("selected", []))))
        config_to_sfus.setdefault(config_key, []).append(sfu_ver)

    for (src_role, mode, selected_m), sfu_list in config_to_sfus.items():
        src_role = _role_from_display_name(str(src_role))
        src_df = _get_df(src_role)
        if src_df is None:
            continue

        # Ensure SFU_SFU Version column exists; for Retailing (Sellout) use the mapped SFU_v column
        if src_role == "Sellout":
            cmap_sellout = st.session_state.col_maps.get("Sellout", {})
            sfu_col = cmap_sellout.get("SFU_v") or cmap_sellout.get("SKU")
            if not sfu_col or sfu_col not in src_df.columns:
                continue
            src_df = src_df.copy()
            src_df[MONTHLY_SFU_VERSION_COL] = src_df[sfu_col].astype(str)
        elif MONTHLY_SFU_VERSION_COL not in src_df.columns:
            continue

        # Enrich any source with missing hierarchy columns from SKU/Bible mapping so
        # basis aggregation keys always match the split-level keys.
        missing_group_cols = [k for k in valid_group_keys if k not in src_df.columns]
        if missing_group_cols and MONTHLY_SFU_VERSION_COL in sku_df.columns:
            hier_lookup_cols = [MONTHLY_SFU_VERSION_COL] + [k for k in valid_group_keys if k in sku_df.columns]
            hier_lookup = sku_df[hier_lookup_cols].drop_duplicates()
            src_df = src_df.merge(hier_lookup, on=MONTHLY_SFU_VERSION_COL, how="left")

        cmap = st.session_state.col_maps.get(src_role, {})
        all_months = cmap.get("_months", detect_month_columns(src_df))
        
        # Determine basis columns based on mode.
        # For FFF sources, basis windows use future months; other sources use past months.
        if mode == "selected":
            basis_cols = [c for c in selected_m if c in src_df.columns]
        else:
            # Build month pools first
            past_m = []
            future_m = []
            for m in all_months:
                try:
                    ts = pd.to_datetime(m, format="%b-%y")
                    if ts < current_month_start:
                        past_m.append(m)
                    else:
                        future_m.append(m)
                except:
                    pass

            month_pool = future_m if _is_fff_source(src_role) else past_m
            
            if mode.startswith("last_"):
                n = int(mode.split("_")[1])
                basis_cols = month_pool[-n:] if len(month_pool) >= n else month_pool
            else:
                basis_cols = month_pool
        
        basis_cols = [c for c in basis_cols if c in src_df.columns]
        if not basis_cols:
            continue
            
        sfu_list_str = [str(v) for v in sfu_list]
        sub = src_df[src_df[MONTHLY_SFU_VERSION_COL].astype(str).isin(sfu_list_str)].copy()
        if sub.empty:
            continue
            
        # Compute row-level basis as mean of basis_cols
        sub["_row_basis"] = sub[basis_cols].apply(pd.to_numeric, errors="coerce").mean(axis=1)
        
        # Aggregate to (Group + SFU) level
        agg_keys = [k for k in valid_group_keys if k in sub.columns] + [MONTHLY_SFU_VERSION_COL]
        # We sum the bases of all rows within the SFU
        agg = sub.groupby(agg_keys, sort=False, dropna=False)["_row_basis"].sum(min_count=1).reset_index()
        
        for _, row in agg.iterrows():
            grp_vals = tuple(row[k] for k in agg_keys)
            val = row["_row_basis"]
            sfu_basis[grp_vals] = float(val) if not pd.isna(val) else 0.0

    # Apply optional manual Final Basis overrides from Live Preview.
    if basis_overrides:
        for sfu_key, basis_val in list(sfu_basis.items()):
            sfu_ver = str(sfu_key[-1]) if sfu_key else ""
            if sfu_ver in basis_overrides:
                sfu_basis[sfu_key] = float(basis_overrides[sfu_ver])

    n_gk = len(valid_group_keys)

    # Step 3: per split group → normalise SFU bases, compute one salience per SFU_v
    rows = []
    sfu_rows = sku_df[[k for k in valid_group_keys if k in sku_df.columns] + ([sku_col] if sku_col in sku_df.columns else [])].drop_duplicates()
    for grp_vals, grp in sfu_rows.groupby(valid_group_keys, sort=False, dropna=False):
        if not isinstance(grp_vals, tuple):
            grp_vals = (grp_vals,)
        group_id = dict(zip(valid_group_keys, grp_vals))
        # Total SFU basis for this split group (all SFU keys that start with grp_vals)
        total_group_basis = sum(v for k, v in sfu_basis.items() if k[:n_gk] == grp_vals)

        for _, row in grp.iterrows():
            sfu_ver = row.get(sku_col, "") if sku_col in grp.columns else ""
            sfu_key = grp_vals + (sfu_ver,)
            sfu_b = sfu_basis.get(sfu_key, 0.0)

            if total_group_basis > 0 and sfu_b > 0:
                sal = sfu_b / total_group_basis
                flag = "computed"
            else:
                sal = 0.0
                flag = "blocked"

            rows.append({**group_id, sku_col: sfu_ver, "basis": sfu_b, "salience": sal, "flag": flag})

    return pd.DataFrame(rows) if rows else None


def _compute_bop_salience(sfu_configs: dict, basis_overrides: dict[str, float] | None = None) -> pd.DataFrame | None:
    """
    Compute BOP salience at SFU_SFU Version level, respecting per-BB split levels.

    sfu_configs: dict {SFU_version: {"source": str, "mode": str, "selected": list[str]}}
    Returns DataFrame with _split_level column for each row.
    """
    sku_df = st.session_state.sku_df_filtered
    basis_overrides = basis_overrides or {}
    if sku_df is None or sku_df.empty:
        st.error("No SFU_v data — complete Step 3 first.")
        return None

    hcm = hier_col_map_from_state()
    bb_split_levels = st.session_state.get("bb_split_levels", {})
    global_split_level = st.session_state.split_level
    current_month_start = pd.Timestamp.now().normalize().replace(day=1)

    # Determine unique split levels (respect per-BB overrides)
    unique_levels = set(bb_split_levels.values()) if bb_split_levels else {global_split_level}

    sal_parts = []
    for level in sorted(unique_levels):  # Sort for deterministic order
        part = _compute_bop_salience_for_level(
            level, sfu_configs, basis_overrides, hcm, sku_df, current_month_start
        )
        if part is not None and not part.empty:
            part["_split_level"] = level
            sal_parts.append(part)

    if sal_parts:
        return pd.concat(sal_parts, ignore_index=True)
    else:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# STEP 4 – Exclusions
# ──────────────────────────────────────────────────────────────────────────────
def step4_exclusions():
    st.header("Step 4 — Exclusions & Exceptions")

    sku_filtered = st.session_state.sku_df_filtered
    sas_filtered = st.session_state.sas_df_filtered
    if sku_filtered is None or sku_filtered.empty or sas_filtered is None or sas_filtered.empty:
        st.error("No filtered SAS/SFU_v data — complete Step 3 first.")
        return

    exc_store = st.session_state.exc_store
    hcm = hier_col_map_from_state()
    sku_col_for_quick = hcm.get("SFU_v", MONTHLY_SFU_VERSION_COL)
    desc_map_step4 = _latest_product_description_map()

    def _format_sku_step4(sku_value: str) -> str:
        sku_norm = str(sku_value).strip()
        desc = str(desc_map_step4.get(sku_norm, "")).strip()
        return f"{sku_norm} ({desc})" if desc else sku_norm

    st.caption(
        "Review exclusions before salience setup. Global exclusions narrow the SFU_v list that appears in "
        "the live salience preview, while initiative and per-BB overrides stay available below."
    )

    if (
        MONTHLY_SFU_VERSION_COL in sku_filtered.columns
        and sku_col_for_quick in sku_filtered.columns
    ):
        quick_scope_cols = list(dict.fromkeys([MONTHLY_SFU_VERSION_COL, sku_col_for_quick]))
        quick_scope = sku_filtered[quick_scope_cols].copy()
        quick_scope[MONTHLY_SFU_VERSION_COL] = quick_scope[MONTHLY_SFU_VERSION_COL].astype(str).str.strip()
        if sku_col_for_quick != MONTHLY_SFU_VERSION_COL:
            quick_scope[sku_col_for_quick] = quick_scope[sku_col_for_quick].astype(str).str.strip()
        quick_scope = quick_scope[
            (quick_scope[MONTHLY_SFU_VERSION_COL] != "")
            & (quick_scope[sku_col_for_quick] != "")
        ]

        all_quick_skus = sorted(quick_scope[sku_col_for_quick].dropna().unique().tolist())
        current_excl = exc_store.global_exclusions
        quick_default = [s for s in all_quick_skus if s in current_excl]

        col_total, col_excluded, col_visible = st.columns(3)
        bb_included_skus = set()
        for _, bb_exc in exc_store.bb_exceptions.items():
            bb_included_skus.update(bb_exc.get("include", set()))
        visible_mask = ~quick_scope[sku_col_for_quick].isin(current_excl) | quick_scope[sku_col_for_quick].isin(bb_included_skus)
        visible_scope = quick_scope[visible_mask]
        col_total.metric("SFU_v values in scope", len(all_quick_skus))
        col_excluded.metric("Global exclusions", len(current_excl))
        col_visible.metric("Visible for salience", visible_scope[MONTHLY_SFU_VERSION_COL].nunique())

        st.markdown("#### Global SFU_v Exclusions")
        st.caption(
            "Use this list for broad exclusions such as fixed promo volumes or initiatives. "
            "BB-specific includes below can still bring an excluded SFU_v back for a particular building block. "
            "All initiative SKUs are treated as part of the global exclusion pool by default."
        )
        quick_selected = st.multiselect(
            "Exclude SFU_v values before salience setup",
            all_quick_skus,
            default=quick_default,
            key="quick_pre_basis_exclusions",
            format_func=_format_sku_step4,
        )

        if st.button("Save Global Exclusions", key="apply_quick_pre_basis_exclusions"):
            quick_scope_skus = set(all_quick_skus)
            exc_store.global_exclusions -= quick_scope_skus
            for _sku in quick_selected:
                exc_store.add_global_exclusion(
                    str(_sku),
                    notes="Excluded before salience setup (Step 4 quick filter)",
                )
            st.session_state.exc_store = exc_store
            if quick_selected:
                quick_selected_display = ", ".join(_format_sku_step4(v) for v in sorted(quick_selected))
                st.success(f"Saved {len(quick_selected)} global exclusion(s): {quick_selected_display}")
            else:
                st.success("Saved 0 global exclusions.")
            st.rerun()

        st.caption(
            f"Live Preview in Step 5 will show **{visible_scope[MONTHLY_SFU_VERSION_COL].nunique()}** visible SFU_v "
            f"out of **{quick_scope[MONTHLY_SFU_VERSION_COL].nunique()}** after exclusions."
        )
    else:
        st.warning(
            "Could not resolve the SFU_v column for quick exclusions. Verify your Step 2 mappings if the "
            "global exclusion list appears empty."
        )

    st.divider()
    render_detailed_exclusion_tools()

    st.divider()
    render_exceptions_panel()

    if st.button("Confirm Exclusions →", type="primary"):
        st.session_state.step = max(st.session_state.step, 5)
        st.session_state.max_step = max(st.session_state.max_step, 5)
        st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# STEP 5 – Basis & Salience
# ──────────────────────────────────────────────────────────────────────────────
def step4_salience():
    st.header("Step 5 — Review & Refine Salience")

    sku_filtered = st.session_state.sku_df_filtered
    if sku_filtered is None or sku_filtered.empty:
        st.error("No filtered SFU_v data — complete Step 3 first.")
        return

    sal_df = st.session_state.salience_df
    hcm = hier_col_map_from_state()
    split_level = st.session_state.split_level
    exc_store = st.session_state.exc_store

    # ── BOP Auto-Salience is the only supported salience method ──────────────
    bb_split_levels = st.session_state.get("bb_split_levels", {})

    if sal_df is not None and not sal_df.empty:
        display_sal = sal_df.drop(columns=["_split_level"], errors="ignore").copy()
        # Aggregate strictly to SFU_SFU Version level.
        if MONTHLY_SFU_VERSION_COL in display_sal.columns:
            group_disp = [MONTHLY_SFU_VERSION_COL]
            # Optionally add hierarchy columns if needed.
            for h in HIERARCHY_LEVELS:
                col = hcm.get(h)
                if col and col in display_sal.columns and col != hcm.get("SKU"):
                    group_disp.append(col)
            agg_disp = (
                display_sal.groupby(group_disp, dropna=False)
                .agg(salience=("salience", "sum"), basis=("basis", "first"), flag=("flag", "first"))
                .reset_index()
            )
            display_sal = agg_disp

        # Display one salience value per SFU_SFU Version (no monthly salience overrides).
        if "salience" in display_sal.columns:
            display_sal["Salience %"] = (pd.to_numeric(display_sal["salience"], errors="coerce") * 100).round(3)
            display_sal = display_sal.drop(columns=["salience"])
        elif "salience %" in display_sal.columns:
            display_sal = display_sal.rename(columns={"salience %": "Salience %"})
            display_sal["Salience %"] = pd.to_numeric(display_sal["Salience %"], errors="coerce").round(3)

        show_current_salience = st.checkbox(
            "Show Current Salience Table",
            value=False,
            key="show_current_salience_table",
            help="Optional QA view. Split logic is unchanged whether this table is shown or hidden.",
        )
        if show_current_salience:
            st.subheader(f"Current Salience Table — {len(display_sal)} rows (at SFU_SFU Version level)")
            st.caption("This salience is applied consistently across all selected SAS forecast months.")
            st.dataframe(
                _add_product_description(display_sal),
                width='stretch',
                height=300,
                column_config={
                    "Salience %": st.column_config.NumberColumn("Salience %", format="%.3f %%"),
                },
            )

    # ── BOP Auto-Salience from historical data (Shipments-based, SFU level) ──
    if st.session_state.get("_is_bop"):
        st.subheader("BOP Auto-Salience — Basis & Salience Configuration (SFU Level)")
        with st.container():
            st.caption(
                "Configure the **Basis / Metric** and **Basis Window** for each SFU Version. "
                "The table previews the last 6 historical months of that basis, "
                "computes the **Final Basis** (average over the window), and shows the resulting **Salience %**. "
                "Add optional **Remarks** per row."
            )

            # ── Discover available data ──────────────────────────────────────
            # Include MONTHLY_MEASURES (standard BOP measures) + Retailing (Sellout)
            available_basis_sources = [r for r in MONTHLY_MEASURES if r in st.session_state.sheets]
            if "Sellout" in st.session_state.sheets:
                available_basis_sources.append("Sellout")
            available_basis_sources_display = [_role_display_name(r) for r in available_basis_sources]

            def _is_fff_source(role: str) -> bool:
                role_n = str(role or "").strip().lower()
                return role_n in {"final fcst to finance", "fff", "final forecast to finance"}

            # Collect SFU versions from the filtered SKU dataset first (authoritative list)
            sku_for_versions = st.session_state.sku_df_filtered
            if sku_for_versions is not None and MONTHLY_SFU_VERSION_COL in sku_for_versions.columns:
                sfu_versions = sorted(
                    set(sku_for_versions[MONTHLY_SFU_VERSION_COL].dropna().astype(str).tolist())
                )
            else:
                # Fallback: union of SFU identifiers from available sources
                all_sfu_versions: list = []
                for _src in available_basis_sources:
                    _df_src = _get_df(_src)
                    if _df_src is not None:
                        if _src == "Sellout":
                            # Retailing (Sellout) sheet: get SFU_v from mapped column
                            _cmap_sellout = st.session_state.col_maps.get("Sellout", {})
                            _sfuv_col = _cmap_sellout.get("SFU_v") or _cmap_sellout.get("SKU")
                            if _sfuv_col and _sfuv_col in _df_src.columns:
                                all_sfu_versions.extend(_df_src[_sfuv_col].dropna().astype(str).tolist())
                        elif MONTHLY_SFU_VERSION_COL in _df_src.columns:
                            all_sfu_versions.extend(_df_src[MONTHLY_SFU_VERSION_COL].dropna().astype(str).tolist())
                sfu_versions = sorted(set(all_sfu_versions))

            # Apply exclusions from Step 4 before building the live preview.
            sku_col_for_quick = hcm.get("SFU_v", MONTHLY_SFU_VERSION_COL)
            sfu_versions_all = list(sfu_versions)
            if (
                sku_for_versions is not None
                and MONTHLY_SFU_VERSION_COL in sku_for_versions.columns
                and sku_col_for_quick in sku_for_versions.columns
            ):
                quick_scope_cols = list(dict.fromkeys([MONTHLY_SFU_VERSION_COL, sku_col_for_quick]))
                quick_scope = sku_for_versions[quick_scope_cols].copy()
                quick_scope[MONTHLY_SFU_VERSION_COL] = quick_scope[MONTHLY_SFU_VERSION_COL].astype(str).str.strip()
                if sku_col_for_quick != MONTHLY_SFU_VERSION_COL:
                    quick_scope[sku_col_for_quick] = quick_scope[sku_col_for_quick].astype(str).str.strip()
                quick_scope = quick_scope[(quick_scope[MONTHLY_SFU_VERSION_COL] != "") & (quick_scope[sku_col_for_quick] != "")]

                # Reduce visible SFU list to those not globally excluded.
                # Keep SFU_vs that have BB-specific includes, even if globally excluded elsewhere.
                exc_store_for_filter = st.session_state.exc_store
                bb_included_skus = set()
                for bb_id, bb_exc in exc_store_for_filter.bb_exceptions.items():
                    bb_included_skus.update(bb_exc.get("include", set()))
                
                # Keep SKU if: (1) not globally excluded, OR (2) has a BB-specific include
                mask = ~quick_scope[sku_col_for_quick].isin(exc_store_for_filter.global_exclusions) | \
                       quick_scope[sku_col_for_quick].isin(bb_included_skus)
                eligible_scope = quick_scope[mask]
                sfu_versions = sorted(eligible_scope[MONTHLY_SFU_VERSION_COL].dropna().unique().tolist())

                st.caption(
                    f"Visible in Live Preview: **{len(sfu_versions)}** / **{len(sfu_versions_all)}** SFU_v "
                    "after Step 4 exclusions."
                )

            if not sfu_versions or not available_basis_sources:
                st.info(
                    f"BOP auto-salience requires at least one measure sheet "
                    f"(Shipments / Consumption / Retailing) with a **{MONTHLY_SFU_VERSION_COL}** column. "
                    "Load a BOP Excel file to enable this feature."
                )
            else:
                # ── Build list of all past months available across all sources ──
                all_avail_months_set: set = set()
                for _src in available_basis_sources:
                    _df_tmp = _get_df(_src)
                    if _df_tmp is not None:
                        all_avail_months_set.update(detect_month_columns(_df_tmp))
                current_month_start_sal = pd.Timestamp.now().normalize().replace(day=1)
                all_past_months_sorted = sorted(
                    [m for m in all_avail_months_set
                     if (parse_month_to_date(m) or pd.Timestamp.max) < current_month_start_sal],
                    key=lambda x: parse_month_to_date(x) or pd.Timestamp.min,
                )
                # Last 6 past months for preview columns
                preview_months = all_past_months_sorted[-6:] if len(all_past_months_sorted) >= 6 else all_past_months_sorted
                # Friendly display labels with explicit month names, e.g. "P6M (Jan-26)"
                preview_labels = [
                    f"P{len(preview_months) - i}M ({m})"
                    for i, m in enumerate(preview_months)
                ]
                preview_col_map = dict(zip(preview_labels, preview_months))  # label → actual col name

                # ── Basis window options ─────────────────────────────────────
                basis_window_options = ["P3M", "P6M", "P9M", "P12M", "specific months"]
                basis_window_mode_map = {
                    "P3M": "last_3",
                    "P6M": "last_6",
                    "P9M": "last_9",
                    "P12M": "last_12",
                    "specific months": "selected",
                }

                # ── Restore saved configs ────────────────────────────────────
                saved_sfu_configs = st.session_state.get("sfu_basis_sources", {})
                saved_basis_overrides: dict[str, float] = st.session_state.get("sfu_basis_overrides", {})
                saved_remarks: dict = st.session_state.get("sfu_remarks", {})

                preview_meta_source = sku_for_versions if sku_for_versions is not None else pd.DataFrame()
                preview_brand_col = next(
                    (
                        col for col in (
                            st.session_state.col_maps.get("Bible", {}).get("Brand"),
                            hcm.get("Brand"),
                            "Brand",
                        )
                        if col and col in preview_meta_source.columns
                    ),
                    None,
                )
                preview_form_col = next(
                    (
                        col for col in (
                            st.session_state.col_maps.get("Bible", {}).get("Form"),
                            hcm.get("Form"),
                            "Form",
                        )
                        if col and col in preview_meta_source.columns
                    ),
                    None,
                )
                preview_meta_map: dict[str, dict[str, str]] = {}
                if not preview_meta_source.empty and MONTHLY_SFU_VERSION_COL in preview_meta_source.columns:
                    meta_keep = [MONTHLY_SFU_VERSION_COL]
                    if preview_brand_col:
                        meta_keep.append(preview_brand_col)
                    if preview_form_col and preview_form_col not in meta_keep:
                        meta_keep.append(preview_form_col)
                    preview_meta_df = (
                        preview_meta_source[meta_keep]
                        .copy()
                        .drop_duplicates(subset=[MONTHLY_SFU_VERSION_COL], keep="first")
                    )
                    preview_meta_df[MONTHLY_SFU_VERSION_COL] = (
                        preview_meta_df[MONTHLY_SFU_VERSION_COL].astype(str).str.strip()
                    )
                    for _, meta_row in preview_meta_df.iterrows():
                        sfu_ver = str(meta_row[MONTHLY_SFU_VERSION_COL]).strip()
                        if not sfu_ver:
                            continue
                        preview_meta_map[sfu_ver] = {
                            "Brand": str(meta_row.get(preview_brand_col, "") or "") if preview_brand_col else "",
                            "Form": str(meta_row.get(preview_form_col, "") or "") if preview_form_col else "",
                        }

                available_brands = sorted(
                    {
                        m.get("Brand", "")
                        for v, m in preview_meta_map.items()
                        if v in sfu_versions and str(m.get("Brand", "")).strip()
                    }
                )
                available_forms = sorted(
                    {
                        m.get("Form", "")
                        for v, m in preview_meta_map.items()
                        if v in sfu_versions and str(m.get("Form", "")).strip()
                    }
                )
                filter_col1, filter_col2 = st.columns(2)
                with filter_col1:
                    selected_brands = st.multiselect(
                        "Filter Brand",
                        options=available_brands,
                        key="step5_brand_filter",
                        help="Filter the Step 5 tables by Brand.",
                    )
                with filter_col2:
                    selected_forms = st.multiselect(
                        "Filter Form",
                        options=available_forms,
                        key="step5_form_filter",
                        help="Filter the Step 5 tables by Form.",
                    )

                filtered_sfu_versions: list[str] = []
                for v in sfu_versions:
                    meta = preview_meta_map.get(v, {})
                    brand_v = str(meta.get("Brand", "") or "")
                    form_v = str(meta.get("Form", "") or "")
                    if selected_brands and brand_v not in selected_brands:
                        continue
                    if selected_forms and form_v not in selected_forms:
                        continue
                    filtered_sfu_versions.append(v)

                # ── Helper: compute basis value for one SFU from a source df ─
                def _sfu_basis_with_reason(sfu_ver: str, src_role: str, mode_key: str, sel_months: list[str]) -> tuple[float, str]:
                    src_role = _role_from_display_name(str(src_role))
                    src_df = _get_df(src_role)
                    if src_df is None:
                        return float("nan"), "source not loaded"
                    sfu_ver_norm = str(sfu_ver).strip()
                    
                    # Handle Sellout sheet differently
                    if src_role == "Sellout":
                        cmap_sellout = st.session_state.col_maps.get("Sellout", {})
                        sfuv_col = cmap_sellout.get("SFU_v") or cmap_sellout.get("SKU")
                        if not sfuv_col or sfuv_col not in src_df.columns:
                            return float("nan"), "Retailing (Sellout) SFU column missing"
                        sfu_col = sfuv_col
                    else:
                        if MONTHLY_SFU_VERSION_COL not in src_df.columns:
                            return float("nan"), f"{MONTHLY_SFU_VERSION_COL} column missing"
                        sfu_col = MONTHLY_SFU_VERSION_COL
                    
                    cmap_s = st.session_state.col_maps.get(src_role, {})
                    all_m = cmap_s.get("_months", detect_month_columns(src_df))
                    past_m = [m for m in all_m
                               if (parse_month_to_date(m) or pd.Timestamp.max) < current_month_start_sal]
                    future_m = [m for m in all_m
                                 if (parse_month_to_date(m) or pd.Timestamp.min) >= current_month_start_sal]
                    if mode_key == "selected":
                        cols = [m for m in sel_months if m in src_df.columns]
                        if not sel_months:
                            return float("nan"), "no months selected"
                    elif mode_key.startswith("last_"):
                        n = int(mode_key.split("_")[1])
                        month_pool = future_m if _is_fff_source(src_role) else past_m
                        cols = month_pool[-n:] if len(month_pool) >= n else month_pool
                    else:
                        cols = future_m if _is_fff_source(src_role) else past_m
                    cols = [c for c in cols if c in src_df.columns]
                    if not cols:
                        return float("nan"), "no matching month columns"
                    sub = src_df[src_df[sfu_col].astype(str).str.strip() == sfu_ver_norm]
                    if sub.empty:
                        return float("nan"), "no SFU match in source"
                    num_vals = sub[cols].replace({",": ""}, regex=True).apply(pd.to_numeric, errors="coerce")
                    arr = num_vals.to_numpy(dtype=float)
                    valid = arr[np.isfinite(arr)]
                    if valid.size == 0:
                        return float("nan"), "no valid numeric data"
                    return float(valid.mean()), "ok"

                def _sfu_basis_value(sfu_ver: str, src_role: str, mode_key: str, sel_months: list[str]) -> float:
                    val, _ = _sfu_basis_with_reason(sfu_ver, src_role, mode_key, sel_months)
                    return val

                # ── Helper: get preview month value for one SFU ──────────────
                def _sfu_month_val(sfu_ver: str, src_role: str, month_col: str) -> float:
                    src_role = _role_from_display_name(str(src_role))
                    src_df = _get_df(src_role)
                    if src_df is None or month_col not in src_df.columns:
                        return float("nan")
                    sfu_ver_norm = str(sfu_ver).strip()
                    
                    # Handle Sellout sheet differently
                    if src_role == "Sellout":
                        cmap_sellout = st.session_state.col_maps.get("Sellout", {})
                        sfuv_col = cmap_sellout.get("SFU_v") or cmap_sellout.get("SKU")
                        if not sfuv_col or sfuv_col not in src_df.columns:
                            return float("nan")
                        sfu_col = sfuv_col
                    else:
                        if MONTHLY_SFU_VERSION_COL not in src_df.columns:
                            return float("nan")
                        sfu_col = MONTHLY_SFU_VERSION_COL
                    
                    sub = src_df[src_df[sfu_col].astype(str).str.strip() == sfu_ver_norm]
                    if sub.empty:
                        return float("nan")
                    return float(
                        pd.to_numeric(sub[month_col].replace({",": ""}, regex=True), errors="coerce").sum(min_count=1)
                    )

                # ── Live preview table ────────────────────────────────────────
                st.markdown("#### Live Preview — Basis Values and Salience")
                st.caption(
                    "Set **Basis / Metric** and **Basis Window** directly in this table. "
                    "The table shows actual historical values for the **last 6 months** (P6M→P1M) "
                    "pulled from the selected Basis / Metric sheet for each SFU Version, "
                    "plus computed **P3M Basis** and **P6M Basis** (averages using the selected Basis / Metric), "
                    "plus the **Final Basis** (avg over the chosen window) and the resulting **Salience %**. "
                    "Edit **Remarks** directly in the table. "
                    "When Basis / Metric is **Final Fcst to Finance (FFF)**, specific-month selection uses **future months**."
                )

                saved_specific: dict = st.session_state.get("sfu_specific_months", {})

                # Build per-SFU config dict from saved state (live edits are persisted below)
                preview_configs: dict[str, dict] = {}
                for v in sfu_versions:
                    cfg = saved_sfu_configs.get(v, {})
                    if isinstance(cfg, str):
                        cfg = {"source": cfg, "mode": "last_3", "selected": []}
                    default_src = "Shipments" if "Shipments" in available_basis_sources else available_basis_sources[0]
                    src = _role_from_display_name(str(cfg.get("source", default_src)))
                    if src not in available_basis_sources:
                        src = default_src
                    mode_val = str(cfg.get("mode", "last_3"))
                    preview_configs[v] = {
                        "source": src,
                        "mode": mode_val,
                        "selected": saved_specific.get(v, []) if mode_val == "selected" else [],
                    }

                # Compute Final Basis for each SFU for salience % calculation
                sfu_final_basis: dict[str, float] = {}
                sfu_basis_reason: dict[str, str] = {}
                for v, cfg_p in preview_configs.items():
                    basis_val, basis_reason = _sfu_basis_with_reason(v, cfg_p["source"], cfg_p["mode"], cfg_p["selected"])
                    sfu_final_basis[v] = basis_val
                    sfu_basis_reason[v] = basis_reason

                # Apply manual override values (if any) for preview and salience %.
                sfu_effective_basis: dict[str, float] = {}
                for v, auto_basis in sfu_final_basis.items():
                    ovr = pd.to_numeric(saved_basis_overrides.get(v), errors="coerce")
                    if pd.notna(ovr):
                        sfu_effective_basis[v] = float(ovr)
                    else:
                        sfu_effective_basis[v] = auto_basis

                # Normalize to salience % within each SFU version group
                # (each SFU's share = its basis / sum of all SFU bases)
                total_basis = sum(b for b in sfu_effective_basis.values() if not pd.isna(b))

                # Build preview rows
                preview_rows = []
                for v in filtered_sfu_versions:
                    cfg_p = preview_configs.get(v, {})
                    src_role = cfg_p["source"]
                    meta_row = preview_meta_map.get(v, {})
                    row_p: dict = {
                        "SFU_SFU Version": v,
                        "Brand": meta_row.get("Brand", ""),
                        "Form": meta_row.get("Form", ""),
                        "Basis / Metric": _role_display_name(src_role),
                        "Basis Window": next(
                            (k for k, mv in basis_window_mode_map.items() if mv == cfg_p["mode"]),
                            cfg_p["mode"],
                        ),
                        "P3M Basis": _sfu_basis_value(v, src_role, "last_3", []),
                        "P6M Basis": _sfu_basis_value(v, src_role, "last_6", []),
                    }
                    for lbl, actual_col in preview_col_map.items():
                        row_p[lbl] = _sfu_month_val(v, src_role, actual_col)
                    final_b = sfu_effective_basis.get(v, float("nan"))
                    ovr_val = pd.to_numeric(saved_basis_overrides.get(v), errors="coerce")
                    row_p["Final Basis"] = float(final_b) if not pd.isna(final_b) else np.nan
                    basis_status = sfu_basis_reason.get(v, "ok")
                    if pd.notna(ovr_val):
                        basis_status = f"overridden | {basis_status}"
                    if cfg_p.get("mode") == "selected":
                        sel_months = cfg_p.get("selected", []) or []
                        sel_txt = ", ".join(sel_months) if sel_months else "none"
                        if _is_fff_source(src_role):
                            basis_status = f"specific future months [{sel_txt}] | {basis_status}"
                        else:
                            basis_status = f"specific months [{sel_txt}] | {basis_status}"
                    row_p["Basis Status"] = basis_status
                    sal_pct = (float(final_b) / total_basis * 100) if (total_basis > 0 and not pd.isna(final_b)) else 0.0
                    row_p["Salience %"] = round(sal_pct, 3)
                    row_p["Remarks"] = saved_remarks.get(v, "")
                    preview_rows.append(row_p)

                preview_df = pd.DataFrame(preview_rows)
                required_preview_cols = [
                    "SFU_SFU Version",
                    "Brand",
                    "Form",
                    "Basis / Metric",
                    "Basis Window",
                    "P3M Basis",
                    "P6M Basis",
                    "Final Basis",
                    "Basis Status",
                    "Salience %",
                    "Remarks",
                ] + preview_labels
                for col in required_preview_cols:
                    if col not in preview_df.columns:
                        preview_df[col] = np.nan if col in (preview_labels + ["P3M Basis", "P6M Basis", "Final Basis", "Salience %"]) else ""

                # Build column config for preview table
                preview_col_cfg: dict = {
                    "SFU_SFU Version": st.column_config.TextColumn("SFU_SFU Version", disabled=True),
                    "Basis / Metric": st.column_config.SelectboxColumn(
                        "Basis / Metric ▾",
                        options=available_basis_sources_display,
                        required=True,
                        help="Select which historical data sheet to use as basis for this SFU version.",
                    ),
                    "Basis Window": st.column_config.SelectboxColumn(
                        "Basis Window ▾",
                        options=basis_window_options,
                        required=True,
                        help="P3M/P6M/P9M/P12M = average over latest past months. specific months = choose exact months below.",
                    ),
                    "P3M Basis": st.column_config.NumberColumn(
                        "P3M Basis",
                        format="%.2f",
                        disabled=True,
                        help="Average of last 3 months from the selected Basis / Metric for this SFU version.",
                    ),
                    "P6M Basis": st.column_config.NumberColumn(
                        "P6M Basis",
                        format="%.2f",
                        disabled=True,
                        help="Average of last 6 months from the selected Basis / Metric for this SFU version.",
                    ),
                    "Final Basis": st.column_config.NumberColumn(
                        "Final Basis",
                        format="%.2f",
                        help="Editable. Change this value to override the computed final basis for this SFU_v.",
                    ),
                    "Basis Status": st.column_config.TextColumn(
                        "Basis Status",
                        disabled=True,
                        help="Reason when Final Basis is N/A (for example: no months selected, no SFU match, no valid numeric data).",
                    ),
                    "Salience %": st.column_config.NumberColumn("Salience %", format="%.3f %%", disabled=True),
                    "Remarks": st.column_config.TextColumn("Remarks", help="Add any notes about this SFU version's basis choice."),
                }
                for lbl in preview_labels:
                    preview_col_cfg[lbl] = st.column_config.NumberColumn(
                        lbl,
                        format="%.2f",
                        disabled=True,
                        help=f"Actual value for {preview_col_map.get(lbl, lbl)} from the selected basis sheet.",
                    )

                # Column order: SFU_v, Basis, Window, P6M…P1M, Final, Salience, Remarks
                preview_df = _add_product_description(preview_df)
                ordered_cols = (
                    ["SFU_SFU Version", "Product Description", "Brand", "Form", "Basis / Metric", "Basis Window"]
                    + ["P3M Basis", "P6M Basis"]
                    + preview_labels
                    + ["Final Basis", "Basis Status", "Salience %", "Remarks"]
                )
                ordered_cols = [c for c in ordered_cols if c in preview_df.columns]

                edited_preview = st.data_editor(
                    preview_df[ordered_cols],
                    column_config=preview_col_cfg,
                    disabled=[c for c in ordered_cols if c not in ("Basis / Metric", "Basis Window", "Final Basis", "Remarks")],
                    width='stretch',
                    key="sfu_preview_editor",
                    height=min(500, 60 + len(preview_df) * 35),
                    num_rows="fixed",
                )

                st.download_button(
                    label="Download Live Preview CSV",
                    data=edited_preview.to_csv(index=False).encode("utf-8"),
                    file_name=f"step5_live_preview_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv",
                )

                # Persist live config edits from preview table so calculated columns refresh.
                edited_configs: dict[str, dict] = {}
                for _, r in edited_preview.iterrows():
                    v = str(r.get("SFU_SFU Version", ""))
                    if not v:
                        continue
                    window_lbl = str(r.get("Basis Window", "P3M"))
                    mode_val = basis_window_mode_map.get(window_lbl, "last_3")
                    edited_configs[v] = {
                        "source": _role_from_display_name(str(r.get("Basis / Metric", available_basis_sources_display[0]))),
                        "mode": mode_val,
                        "selected": saved_specific.get(v, []) if mode_val == "selected" else [],
                    }

                merged_configs = dict(preview_configs)
                merged_configs.update(edited_configs)
                if merged_configs != preview_configs:
                    st.session_state.sfu_basis_sources = merged_configs
                    st.rerun()

                # Persist manual final basis overrides from preview table edits.
                edited_basis_overrides: dict[str, float] = {}
                for _, r in edited_preview.iterrows():
                    v = str(r.get("SFU_SFU Version", ""))
                    if not v:
                        continue
                    edited_basis = pd.to_numeric(r.get("Final Basis"), errors="coerce")
                    auto_basis = pd.to_numeric(sfu_final_basis.get(v), errors="coerce")
                    if pd.notna(edited_basis):
                        if pd.isna(auto_basis) or abs(float(edited_basis) - float(auto_basis)) > 1e-9:
                            edited_basis_overrides[v] = float(edited_basis)

                merged_basis_overrides = dict(saved_basis_overrides)
                for v in edited_preview["SFU_SFU Version"].astype(str):
                    merged_basis_overrides.pop(v, None)
                merged_basis_overrides.update(edited_basis_overrides)

                if merged_basis_overrides != saved_basis_overrides:
                    st.session_state.sfu_basis_overrides = merged_basis_overrides
                    st.rerun()

                st.markdown("#### Editable Monthly Salience Matrix (SKU x Month)")
                split_months = list(st.session_state.get("sas_months_selected", []) or [])
                if not split_months:
                    st.info("No forecast months selected yet. Complete Step 3 month selection to edit monthly salience.")
                else:
                    # Load forecast boundaries (per SFU_v from Stat_DB)
                    _fcst_bounds: dict | None = _get_forecast_date_boundaries()

                    has_any_bounds = bool(_fcst_bounds)
                    st.caption(
                        "Edit month-level salience in % for each SFU Version. These values are used when BB monthly values are split in Step 6. "
                        "Within each BB-month, values are normalized across eligible SFU_v rows."
                        + (" Months outside an SFU Version's Forecast From / Forecast To date range are shown as empty and cannot be forecasted." if has_any_bounds else "")
                    )
                    saved_monthly_salience = st.session_state.get("sfu_monthly_salience", {})
                    row_sal_map = {
                        str(r.get("SFU_SFU Version", "")): float(pd.to_numeric(r.get("Salience %"), errors="coerce") or 0.0)
                        for _, r in preview_df.iterrows()
                    }
                    monthly_rows: list[dict] = []
                    # Track which (sfu_v, month) cells are out of forecast range so we can disable them.
                    _out_of_range_cells: dict[str, set[str]] = {}
                    for _, r in preview_df.iterrows():
                        sfu_v = str(r.get("SFU_SFU Version", "") or "").strip()
                        if not sfu_v:
                            continue
                        base_pct = row_sal_map.get(sfu_v, 0.0)
                        stored_for_sfu = saved_monthly_salience.get(sfu_v, {}) if isinstance(saved_monthly_salience.get(sfu_v, {}), dict) else {}
                        m_row = {
                            "SFU_SFU Version": sfu_v,
                            "Product Description": r.get("Product Description", ""),
                            "Brand": r.get("Brand", ""),
                            "Form": r.get("Form", ""),
                        }
                        # Resolve forecast boundaries for this SFU_v
                        sfu_from: pd.Timestamp | None = None
                        sfu_to: pd.Timestamp | None = None
                        if _fcst_bounds:
                            _bounds_entry = _fcst_bounds.get(sfu_v)
                            if _bounds_entry:
                                sfu_from, sfu_to = _bounds_entry
                        _oor: set[str] = set()
                        for m in split_months:
                            month_ts = parse_month_to_date(m)
                            is_out_of_range = False
                            if month_ts is not None and (sfu_from is not None or sfu_to is not None):
                                if sfu_from is not None and month_ts < sfu_from:
                                    is_out_of_range = True
                                if sfu_to is not None and month_ts > sfu_to:
                                    is_out_of_range = True
                            if is_out_of_range:
                                m_row[m] = None
                                _oor.add(m)
                            else:
                                stored_frac = pd.to_numeric(stored_for_sfu.get(m), errors="coerce")
                                if pd.notna(stored_frac):
                                    m_row[m] = float(stored_frac) * 100.0
                                else:
                                    m_row[m] = float(base_pct)
                        _out_of_range_cells[sfu_v] = _oor
                        monthly_rows.append(m_row)

                    monthly_df = pd.DataFrame(monthly_rows)
                    monthly_col_cfg: dict = {
                        "SFU_SFU Version": st.column_config.TextColumn("SFU_SFU Version", disabled=True),
                        "Product Description": st.column_config.TextColumn("Product Description", disabled=True),
                        "Brand": st.column_config.TextColumn("Brand", disabled=True),
                        "Form": st.column_config.TextColumn("Form", disabled=True),
                    }
                    for m in split_months:
                        monthly_col_cfg[m] = st.column_config.NumberColumn(f"{m} Salience %", format="%.3f")

                    monthly_editor_cols = [
                        c for c in ["SFU_SFU Version", "Product Description", "Brand", "Form"] + split_months
                        if c in monthly_df.columns
                    ]
                    if not monthly_rows:
                        st.info("No SFU_v rows available for the current Brand/Form filter.")
                        edited_monthly = pd.DataFrame(columns=monthly_editor_cols)
                    else:
                        edited_monthly = st.data_editor(
                            monthly_df[monthly_editor_cols],
                            column_config=monthly_col_cfg,
                            disabled=[c for c in monthly_editor_cols if c not in split_months],
                            width='stretch',
                            key="sfu_monthly_salience_editor",
                            height=min(500, 60 + len(monthly_df) * 35),
                            num_rows="fixed",
                        )

                    merged_monthly_salience = dict(saved_monthly_salience)
                    for _, mr in edited_monthly.iterrows():
                        sfu_v = str(mr.get("SFU_SFU Version", "") or "").strip()
                        if not sfu_v:
                            continue
                        oor_for_sfu = _out_of_range_cells.get(sfu_v, set())
                        month_map: dict[str, float] = {}
                        for m in split_months:
                            if m in oor_for_sfu:
                                continue  # Skip out-of-range months — not forecasted for this SFU_v
                            pct = pd.to_numeric(mr.get(m), errors="coerce")
                            if pd.notna(pct):
                                month_map[m] = max(0.0, float(pct)) / 100.0
                        if month_map:
                            merged_monthly_salience[sfu_v] = month_map
                        else:
                            merged_monthly_salience.pop(sfu_v, None)
                    st.session_state.sfu_monthly_salience = merged_monthly_salience

                    if not edited_monthly.empty:
                        st.download_button(
                            label="Download SKU x Month Salience CSV",
                            data=edited_monthly.to_csv(index=False).encode("utf-8"),
                            file_name=f"step5_sku_monthly_salience_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                            mime="text/csv",
                        )

                # ── Optional manual month selection ──────────────────────────
                specific_sfu_rows = [
                    r for _, r in edited_preview.iterrows()
                    if str(r.get("Basis Window", "")) == "specific months"
                ]

                if specific_sfu_rows:
                    st.markdown("#### Optional — Manual Month Selection per SFU Version")
                    st.caption(
                        "For each SFU Version set to *specific months*, pick the exact months "
                        "to use as basis. For **FFF** rows, this picker shows future months. "
                        "The live preview will refresh automatically."
                    )
                    new_specific: dict = {}
                    for r in specific_sfu_rows:
                        v = r[MONTHLY_SFU_VERSION_COL]
                        src_for_months = _role_from_display_name(str(r.get("Basis / Metric", available_basis_sources_display[0])))
                        src_df_m = _get_df(src_for_months)
                        if src_df_m is not None:
                            cmap_m = st.session_state.col_maps.get(src_for_months, {})
                            month_pool = cmap_m.get("_months", detect_month_columns(src_df_m))
                            if _is_fff_source(src_for_months):
                                src_months = sorted(
                                    [m for m in month_pool
                                     if (parse_month_to_date(m) or pd.Timestamp.min) >= current_month_start_sal],
                                    key=lambda x: parse_month_to_date(x) or pd.Timestamp.min,
                                )
                            else:
                                src_months = sorted(
                                    [m for m in month_pool
                                     if (parse_month_to_date(m) or pd.Timestamp.max) < current_month_start_sal],
                                    key=lambda x: parse_month_to_date(x) or pd.Timestamp.min,
                                )
                        else:
                            src_months = all_past_months_sorted
                        prev_sel = saved_specific.get(v, [])
                        valid_prev = [m for m in prev_sel if m in src_months]
                        with st.expander(f"📅 {v}  ({_role_display_name(src_for_months)})", expanded=(not valid_prev)):
                            chosen = st.multiselect(
                                f"Months for **{v}**:",
                                src_months,
                                default=valid_prev,
                                key=f"sfu_specific_{v}",
                            )
                        new_specific[v] = chosen
                    st.session_state.sfu_specific_months = new_specific
                else:
                    new_specific = st.session_state.get("sfu_specific_months", {})

                st.caption("Exclusion editing lives in Step 4. Step 5 uses those saved exclusions to scope the live salience preview.")

                # ── Compute button ───────────────────────────────────────────
                if st.button("✅ Compute BOP Salience (SFU Level)", type="primary"):
                    # Save remarks
                    new_remarks = dict(saved_remarks)
                    for _, pr in edited_preview.iterrows():
                        new_remarks[pr["SFU_SFU Version"]] = str(pr.get("Remarks", "") or "")
                    st.session_state.sfu_remarks = new_remarks

                    # Persist per-SFU specific month selections
                    st.session_state.sfu_specific_months = new_specific

                    # Save configs from the live preview table edits
                    new_sfu_configs: dict = dict(preview_configs)
                    for _, pr in edited_preview.iterrows():
                        v = str(pr.get("SFU_SFU Version", ""))
                        if not v:
                            continue
                        window_lbl = str(pr.get("Basis Window", "P3M"))
                        mode_val = basis_window_mode_map.get(window_lbl, "last_3")
                        new_sfu_configs[v] = {
                            "source": _role_from_display_name(str(pr.get("Basis / Metric", available_basis_sources_display[0]))),
                            "mode": mode_val,
                            "selected": new_specific.get(v, []) if mode_val == "selected" else [],
                        }
                    st.session_state.sfu_basis_sources = new_sfu_configs

                    # Warn (non-blocking) if SFUs within the same Form use mixed Basis / Metric.
                    form_col_name = preview_form_col
                    if form_col_name and sku_for_versions is not None and form_col_name in sku_for_versions.columns and MONTHLY_SFU_VERSION_COL in sku_for_versions.columns:
                        cfg_by_sfu = {
                            str(k): str(v.get("source", "")).strip()
                            for k, v in new_sfu_configs.items()
                            if isinstance(v, dict)
                        }
                        form_sources: dict[str, set[str]] = {}
                        form_sfus: dict[str, set[str]] = {}
                        for _, rr in sku_for_versions[[MONTHLY_SFU_VERSION_COL, form_col_name]].dropna(subset=[MONTHLY_SFU_VERSION_COL]).iterrows():
                            sfu_v = str(rr.get(MONTHLY_SFU_VERSION_COL, "") or "").strip()
                            form_v = str(rr.get(form_col_name, "") or "").strip()
                            if not sfu_v or not form_v:
                                continue
                            src_v = cfg_by_sfu.get(sfu_v, "")
                            if not src_v:
                                continue
                            form_sources.setdefault(form_v, set()).add(src_v)
                            form_sfus.setdefault(form_v, set()).add(sfu_v)

                        inconsistent = [
                            (form_v, sorted(list(srcs)), sorted(list(form_sfus.get(form_v, set()))))
                            for form_v, srcs in form_sources.items()
                            if len(srcs) > 1
                        ]
                        if inconsistent:
                            st.error("Mixed Basis / Metric detected within the same Form. Please align these rows.")
                            for form_v, srcs, sfu_list in inconsistent:
                                srcs_display = [_role_display_name(s) for s in srcs]
                                st.error(
                                    f"Form '{form_v}' uses multiple Basis / Metric values: {', '.join(srcs_display)}. "
                                    f"Impacted SFU_v count: {len(sfu_list)}."
                                )

                    # Save manual final basis overrides from the preview table edits.
                    new_basis_overrides: dict[str, float] = dict(saved_basis_overrides)
                    for sfu_v in edited_preview["SFU_SFU Version"].astype(str):
                        new_basis_overrides.pop(sfu_v, None)
                    for _, pr in edited_preview.iterrows():
                        v = str(pr.get("SFU_SFU Version", ""))
                        if not v:
                            continue
                        edited_basis = pd.to_numeric(pr.get("Final Basis"), errors="coerce")
                        auto_basis = pd.to_numeric(sfu_final_basis.get(v), errors="coerce")
                        if pd.notna(edited_basis):
                            if pd.isna(auto_basis) or abs(float(edited_basis) - float(auto_basis)) > 1e-9:
                                new_basis_overrides[v] = float(edited_basis)
                    st.session_state.sfu_basis_overrides = new_basis_overrides

                    with st.spinner("Computing BOP salience…"):
                        bop_sal = _compute_bop_salience(new_sfu_configs, basis_overrides=new_basis_overrides)
                    if bop_sal is not None and not bop_sal.empty:
                        st.session_state.salience_df = bop_sal
                        st.session_state.blocking_groups = []
                        src_rows = len(sku_filtered) if sku_filtered is not None else 0
                        src_unique_sfuv = (
                            sku_filtered[MONTHLY_SFU_VERSION_COL].astype(str).nunique()
                            if (sku_filtered is not None and MONTHLY_SFU_VERSION_COL in sku_filtered.columns)
                            else 0
                        )
                        st.success(
                            f"✅ BOP salience computed: {len(bop_sal):,} salience row(s) "
                            f"from {src_rows:,} input row(s), {src_unique_sfuv:,} unique SFU_v value(s)."
                        )
                        st.rerun()
                    else:
                        st.error(
                            "Could not compute BOP salience. "
                            "Ensure the selected source sheets have enough past months, "
                            "the data matches the SFU versions, "
                            "and per-BB split levels are properly configured."
                        )

    # ── Blocked groups ────────────────────────────────────────────────────────
    blocking = st.session_state.blocking_groups
    if sal_df is not None and blocking:
        st.error(f"⚠️ **{len(blocking)} blocked group(s)** with zero/missing basis — override required:")
        for i, bg in enumerate(blocking):
            with st.expander(f"Blocked group {i+1}: {bg['group']}"):
                st.write(f"Reason: {bg['reason']} | SKUs: {bg['n_skus']}")

        override_rows = sal_df[sal_df["flag"] == "blocked"].copy()
        if not override_rows.empty:
            sku_col_sal = hcm.get("SKU", "SKU")
            _ctx_cols = [c for c in override_rows.columns if c not in ("basis", "flag")]
            # Show salience as % for editing; convert back on save
            override_edit = override_rows[_ctx_cols].assign(
                **{"Salience %": (pd.to_numeric(override_rows["salience"].fillna(0.0), errors="coerce") * 100).round(3)}
            ).drop(columns=["salience"], errors="ignore")
            _override_disabled = [c for c in override_edit.columns if c != "Salience %"]
            edited = st.data_editor(
                override_edit,
                width='stretch',
                key="sal_override_editor",
                num_rows="fixed",
                disabled=_override_disabled,
                column_config={"Salience %": st.column_config.NumberColumn("Salience %", format="%.3f %%")},
            )
            if st.button("Apply Overrides"):
                _group_cols = [c for c in override_rows.columns if c not in (sku_col_sal, "basis", "salience", "flag")]
                for idx_row, (_, row) in enumerate(override_rows.iterrows()):
                    g_key = tuple(row[c] for c in _group_cols if c in row.index)
                    sku_val = row.get(sku_col_sal, "")
                    # Convert % back to fraction (0-1) for storage
                    st.session_state.sal_overrides[(g_key, sku_val)] = float(edited.iloc[idx_row]["Salience %"]) / 100.0
                st.success("Overrides saved. Click 'Compute Historical Salience' again to refresh.")

    if st.button("Confirm Salience →", type="primary", disabled=(sal_df is None)):
        st.session_state.step = max(st.session_state.step, 6)
        st.session_state.max_step = max(st.session_state.max_step, 6)
        st.rerun()


def hier_col_map_from_state() -> dict[str, str]:
    """Build logical->actual col map from session state."""
    hcm = {}
    for lh in LOGICAL_HIER + ["SFU_v", "SKU"]:
        for role in SKU_SHEETS + ["SAS"]:
            rc = st.session_state.col_maps.get(role, {}).get(lh)
            if rc:
                hcm[lh] = rc
                break

    # Best-effort fallback so split engine can always resolve SFU_v identifier.
    sku_df = st.session_state.get("sku_df_filtered")
    if sku_df is not None and not sku_df.empty and MONTHLY_SFU_VERSION_COL in sku_df.columns:
        hcm["SFU_v"] = MONTHLY_SFU_VERSION_COL

    # Fallback if canonical column is unavailable.
    if "SFU_v" not in hcm:
        if sku_df is not None and not sku_df.empty:
            for cand in [MONTHLY_SFU_VERSION_COL, "SFU_v", "SKU"]:
                if cand in sku_df.columns:
                    hcm["SFU_v"] = cand
                    break
    return hcm


def _detect_plan_name_col(sas_df: pd.DataFrame, sas_cmap: dict[str, str]) -> str | None:
    candidates = [
        sas_cmap.get("Plan Name"),
        "Plan Name",
        "Plan_Name",
    ]
    for col in candidates:
        if col and col in sas_df.columns:
            return col
    for col in sas_df.columns:
        c = str(col).lower().strip()
        if "plan" in c and "name" in c:
            return col
    return None


def _split_sas_last_ff_rows(sas_df: pd.DataFrame, sas_cmap: dict[str, str]) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    plan_col = _detect_plan_name_col(sas_df, sas_cmap)
    if not plan_col:
        return sas_df.copy(), sas_df.iloc[0:0].copy(), None

    plan_vals = sas_df[plan_col].astype(str).str.strip().str.lower()
    last_ff_mask = plan_vals.str.contains("last ff", na=False)
    alloc_df = sas_df.loc[~last_ff_mask].copy()
    ref_df = sas_df.loc[last_ff_mask].copy()
    return alloc_df, ref_df, plan_col


def _extract_bop_cycle_token(plan_name: object) -> str | None:
    """Extract cycle token like JUN'26 from a Plan Name value if present."""
    text = str(plan_name or "").strip()
    if not text:
        return None
    m = re.search(r"bop\s*cycle\s*\(\s*([A-Za-z]{3})\s*'\s*(\d{2})\s*\)", text, flags=re.IGNORECASE)
    if not m:
        return None
    mon = m.group(1).upper()
    yy = m.group(2)
    return f"{mon}'{yy}"


def _infer_active_bop_cycle_token(sas_months: list[str]) -> str:
    """Infer the active BOP cycle token as previous month of earliest selected SAS month."""
    parsed = [parse_month_to_date(m) for m in (sas_months or [])]
    parsed = [p for p in parsed if p is not None]
    if parsed:
        first_month = min(parsed).replace(day=1)
        active = first_month - pd.DateOffset(months=1)
    else:
        active = pd.Timestamp.now().normalize().replace(day=1)
    return active.strftime("%b").upper() + "'" + active.strftime("%y")


def _prepare_sas_split_and_topline_scopes(
    sas_df: pd.DataFrame,
    sas_cmap: dict[str, str],
    sas_months: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, str | None, str]:
    """Return split scope, topline scope, Last FF reference rows, prior-cycle excluded rows, and active token."""
    plan_col = _detect_plan_name_col(sas_df, sas_cmap)
    active_token = _infer_active_bop_cycle_token(sas_months)
    work_df = sas_df.copy()
    prior_cycle_df = sas_df.iloc[0:0].copy()

    if plan_col and plan_col in work_df.columns:
        plan_series = work_df[plan_col].astype(str)
        cycle_tokens = plan_series.map(_extract_bop_cycle_token)
        has_cycle = cycle_tokens.notna()
        in_active = cycle_tokens.fillna("").str.upper() == active_token.upper()
        prior_mask = has_cycle & (~in_active)
        prior_cycle_df = work_df.loc[prior_mask].copy()
        work_df = work_df.loc[~prior_mask].copy()

    alloc_df, last_ff_df, _ = _split_sas_last_ff_rows(work_df, sas_cmap)
    if not plan_col or plan_col not in alloc_df.columns:
        return alloc_df.copy(), alloc_df.iloc[0:0].copy(), last_ff_df.copy(), prior_cycle_df, plan_col, active_token

    plan_vals = alloc_df[plan_col].astype(str).str.strip().str.lower()
    topline_mask = plan_vals.str.contains("topline base", na=False)
    topline_df = alloc_df.loc[topline_mask].copy()
    split_df = alloc_df.loc[~topline_mask].copy()
    return split_df, topline_df, last_ff_df.copy(), prior_cycle_df, plan_col, active_token


def _resolve_monthly_hierarchy_columns(role: str) -> tuple[str | None, str | None, str | None]:
    """Resolve SKU/Country/Category columns for a monthly role DataFrame."""
    df = _get_df(role)
    if df is None or df.empty:
        return None, None, None
    cmap = st.session_state.col_maps.get(role, {})
    sku_col = next(
        (c for c in [cmap.get("SFU_v"), MONTHLY_SFU_VERSION_COL, "SFU_v", "SKU"] if c and c in df.columns),
        None,
    )
    country_col = next(
        (c for c in [cmap.get("Country"), "Country", "Reporting Country"] if c and c in df.columns),
        None,
    )
    category_col = next(
        (c for c in [cmap.get("SMO Category"), cmap.get("Category"), "SMO Category", "Category"] if c and c in df.columns),
        None,
    )
    return sku_col, country_col, category_col


def _build_country_category_lookup() -> dict[str, tuple[str, str]]:
    """Build SKU -> (Country, SMO Category) lookup using monthly sources."""
    role_order = [
        "Final Fcst to Finance",
        "Shipments",
        "Statistical Forecast",
        "Retailing",
        "Consumption",
    ]
    lookup: dict[str, tuple[str, str]] = {}
    for role in role_order:
        df = _get_df(role)
        if df is None or df.empty:
            continue
        sku_col, country_col, category_col = _resolve_monthly_hierarchy_columns(role)
        if not sku_col:
            continue
        sub_cols = [sku_col]
        if country_col:
            sub_cols.append(country_col)
        if category_col:
            sub_cols.append(category_col)
        sub = df[sub_cols].copy()
        sub[sku_col] = sub[sku_col].astype(str).str.strip()
        sub = sub[sub[sku_col] != ""]
        if country_col:
            sub[country_col] = sub[country_col].fillna("").astype(str).str.strip()
        if category_col:
            sub[category_col] = sub[category_col].fillna("").astype(str).str.strip()
        sub = sub.drop_duplicates(subset=[sku_col])
        for _, row in sub.iterrows():
            sku = str(row.get(sku_col, "")).strip()
            if not sku:
                continue
            if sku in lookup and lookup[sku] != ("", ""):
                continue
            lookup[sku] = (
                str(row.get(country_col, "")).strip() if country_col else "",
                str(row.get(category_col, "")).strip() if category_col else "",
            )
    return lookup


def _compute_bop_gap_artifacts(
    sas_topline_df: pd.DataFrame,
    output_wide: pd.DataFrame,
    sas_months: list[str],
    hcm: dict[str, str],
    salience_df: pd.DataFrame | None,
    fff_df: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compute country-category monthly gap and SKU-level gap/final matched artifacts."""
    if output_wide is None or output_wide.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    month_cols = [m for m in (sas_months or []) if m in output_wide.columns]
    if not month_cols:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    sfu_col = next((c for c in [MONTHLY_SFU_VERSION_COL, hcm.get("SFU_v"), "SFU_v", "SKU"] if c and c in output_wide.columns), None)
    if not sfu_col:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    split_rows = output_wide.copy()
    split_rows[sfu_col] = split_rows[sfu_col].astype(str).str.strip()
    split_rows = split_rows[split_rows[sfu_col] != ""]

    out_country_col = hcm.get("Country") if hcm.get("Country") in split_rows.columns else None
    out_category_col = hcm.get("SMO Category") if hcm.get("SMO Category") in split_rows.columns else None
    cc_lookup = _build_country_category_lookup()

    if out_country_col:
        split_rows["Country"] = split_rows[out_country_col].fillna("").astype(str).str.strip()
    else:
        split_rows["Country"] = ""
    if out_category_col:
        split_rows["SMO Category"] = split_rows[out_category_col].fillna("").astype(str).str.strip()
    else:
        split_rows["SMO Category"] = ""

    missing_cc = (split_rows["Country"] == "") | (split_rows["SMO Category"] == "")
    if missing_cc.any():
        fill_rows = split_rows.loc[missing_cc, [sfu_col]].copy()
        fill_rows[["Country", "SMO Category"]] = fill_rows[sfu_col].map(
            lambda s: cc_lookup.get(str(s).strip(), ("", ""))
        ).apply(pd.Series)
        split_rows.loc[missing_cc, "Country"] = fill_rows["Country"].fillna("")
        split_rows.loc[missing_cc, "SMO Category"] = fill_rows["SMO Category"].fillna("")

    split_by_sku = split_rows[[sfu_col, "Country", "SMO Category"] + month_cols].copy()
    for m in month_cols:
        split_by_sku[m] = pd.to_numeric(split_by_sku[m], errors="coerce").fillna(0.0)
    split_by_sku = split_by_sku.groupby([sfu_col, "Country", "SMO Category"], dropna=False)[month_cols].sum().reset_index()
    split_cc = split_by_sku.groupby(["Country", "SMO Category"], dropna=False)[month_cols].sum().reset_index()

    fff_by_sku = pd.DataFrame(columns=[sfu_col, "Country", "SMO Category"] + month_cols)
    fff_cc = pd.DataFrame(columns=["Country", "SMO Category"] + month_cols)
    if fff_df is not None and not fff_df.empty:
        fff_sku_col = next((c for c in [MONTHLY_SFU_VERSION_COL, hcm.get("SFU_v"), "SFU_v", "SKU"] if c and c in fff_df.columns), None)
        fff_country_col = next((c for c in [hcm.get("Country"), "Country", "Reporting Country"] if c and c in fff_df.columns), None)
        fff_cat_col = next((c for c in [hcm.get("SMO Category"), "SMO Category", "Category"] if c and c in fff_df.columns), None)
        if fff_sku_col:
            fff_cols = [fff_sku_col]
            if fff_country_col:
                fff_cols.append(fff_country_col)
            if fff_cat_col:
                fff_cols.append(fff_cat_col)
            fff_cols += [m for m in month_cols if m in fff_df.columns]
            fff_work = fff_df[fff_cols].copy()
            fff_work[fff_sku_col] = fff_work[fff_sku_col].astype(str).str.strip()
            fff_work = fff_work[fff_work[fff_sku_col] != ""]
            if fff_country_col:
                fff_work["Country"] = fff_work[fff_country_col].fillna("").astype(str).str.strip()
            else:
                fff_work["Country"] = fff_work[fff_sku_col].map(lambda s: cc_lookup.get(str(s).strip(), ("", ""))[0])
            if fff_cat_col:
                fff_work["SMO Category"] = fff_work[fff_cat_col].fillna("").astype(str).str.strip()
            else:
                fff_work["SMO Category"] = fff_work[fff_sku_col].map(lambda s: cc_lookup.get(str(s).strip(), ("", ""))[1])
            for m in month_cols:
                if m in fff_work.columns:
                    fff_work[m] = pd.to_numeric(fff_work[m], errors="coerce").fillna(0.0)
                else:
                    fff_work[m] = 0.0
            fff_by_sku = (
                fff_work[[fff_sku_col, "Country", "SMO Category"] + month_cols]
                .groupby([fff_sku_col, "Country", "SMO Category"], dropna=False)[month_cols]
                .sum()
                .reset_index()
                .rename(columns={fff_sku_col: sfu_col})
            )
            fff_cc = fff_by_sku.groupby(["Country", "SMO Category"], dropna=False)[month_cols].sum().reset_index()

    topline_cc = pd.DataFrame(columns=["Country", "SMO Category"] + month_cols)
    if sas_topline_df is not None and not sas_topline_df.empty:
        sas_country = st.session_state.col_maps.get("SAS", {}).get("Country") or "Country"
        sas_cat = st.session_state.col_maps.get("SAS", {}).get("SMO Category") or "SMO Category"
        if sas_country in sas_topline_df.columns and sas_cat in sas_topline_df.columns:
            top_cols = [sas_country, sas_cat] + [m for m in month_cols if m in sas_topline_df.columns]
            top = sas_topline_df[top_cols].copy()
            top["Country"] = top[sas_country].fillna("").astype(str).str.strip()
            top["SMO Category"] = top[sas_cat].fillna("").astype(str).str.strip()
            for m in month_cols:
                if m in top.columns:
                    top[m] = pd.to_numeric(top[m], errors="coerce").fillna(0.0)
                else:
                    top[m] = 0.0
            topline_cc = top[["Country", "SMO Category"] + month_cols].groupby(["Country", "SMO Category"], dropna=False)[month_cols].sum().reset_index()

    ccm_base = topline_cc.merge(fff_cc, on=["Country", "SMO Category"], how="outer", suffixes=("_topline", "_fff"))
    ccm_base = ccm_base.merge(split_cc, on=["Country", "SMO Category"], how="outer", suffixes=("", "_split"))
    ccm_base = ccm_base.fillna(0.0)

    gap_ccm_rows: list[dict] = []
    for _, row in ccm_base.iterrows():
        country = str(row.get("Country", "")).strip()
        category = str(row.get("SMO Category", "")).strip()
        for m in month_cols:
            topline_val = float(pd.to_numeric(row.get(f"{m}_topline", 0.0), errors="coerce") or 0.0)
            fff_val = float(pd.to_numeric(row.get(f"{m}_fff", 0.0), errors="coerce") or 0.0)
            split_val = float(pd.to_numeric(row.get(m, 0.0), errors="coerce") or 0.0)
            gap_val = topline_val - (fff_val + split_val)
            gap_ccm_rows.append(
                {
                    "Country": country,
                    "SMO Category": category,
                    "month": m,
                    "topline": topline_val,
                    "fff": fff_val,
                    "split": split_val,
                    "gap": gap_val,
                }
            )
    gap_ccm_df = pd.DataFrame(gap_ccm_rows)

    sal_sku_col = next((c for c in [MONTHLY_SFU_VERSION_COL, hcm.get("SFU_v"), "SFU_v", "SKU"] if salience_df is not None and c and c in salience_df.columns), None)
    sal_country_col = next((c for c in [hcm.get("Country"), "Country", "Reporting Country"] if salience_df is not None and c and c in salience_df.columns), None)
    sal_cat_col = next((c for c in [hcm.get("SMO Category"), "SMO Category", "Category"] if salience_df is not None and c and c in salience_df.columns), None)
    sal_lookup: dict[tuple[str, str, str], float] = {}
    if salience_df is not None and not salience_df.empty and sal_sku_col:
        sal_work = salience_df.copy()
        sal_work["__sku"] = sal_work[sal_sku_col].astype(str).str.strip()
        sal_work["__country"] = sal_work[sal_country_col].fillna("").astype(str).str.strip() if sal_country_col else ""
        sal_work["__cat"] = sal_work[sal_cat_col].fillna("").astype(str).str.strip() if sal_cat_col else ""
        sal_work["__sal"] = pd.to_numeric(sal_work.get("salience", 0.0), errors="coerce").fillna(0.0)
        sal_grouped = sal_work.groupby(["__country", "__cat", "__sku"], dropna=False)["__sal"].sum().reset_index()
        for _, r in sal_grouped.iterrows():
            sal_lookup[(str(r["__country"]), str(r["__cat"]), str(r["__sku"]))] = float(r["__sal"])

    gap_by_sku_rows: list[dict] = []
    final_rows: dict[tuple[str, str, str], dict] = {}

    split_long = split_by_sku.melt(
        id_vars=[sfu_col, "Country", "SMO Category"],
        value_vars=month_cols,
        var_name="month",
        value_name="split",
    )
    fff_long = fff_by_sku.melt(
        id_vars=[sfu_col, "Country", "SMO Category"],
        value_vars=month_cols,
        var_name="month",
        value_name="fff",
    ) if not fff_by_sku.empty else pd.DataFrame(columns=[sfu_col, "Country", "SMO Category", "month", "fff"])

    split_idx = {
        (str(r[sfu_col]), str(r["Country"]), str(r["SMO Category"]), str(r["month"])): float(pd.to_numeric(r["split"], errors="coerce") or 0.0)
        for _, r in split_long.iterrows()
    }
    fff_idx = {
        (str(r[sfu_col]), str(r["Country"]), str(r["SMO Category"]), str(r["month"])): float(pd.to_numeric(r["fff"], errors="coerce") or 0.0)
        for _, r in fff_long.iterrows()
    }

    for _, ccm in gap_ccm_df.iterrows():
        country = str(ccm.get("Country", "")).strip()
        category = str(ccm.get("SMO Category", "")).strip()
        month = str(ccm.get("month", "")).strip()
        gap_val = float(pd.to_numeric(ccm.get("gap", 0.0), errors="coerce") or 0.0)

        cc_skus = split_by_sku[
            (split_by_sku["Country"].astype(str).str.strip() == country)
            & (split_by_sku["SMO Category"].astype(str).str.strip() == category)
        ][sfu_col].astype(str).str.strip().unique().tolist()
        if not cc_skus:
            continue

        weights = []
        for sku in cc_skus:
            w = sal_lookup.get((country, category, sku), sal_lookup.get(("", "", sku), 0.0))
            weights.append(max(float(w), 0.0))
        w_sum = float(sum(weights))
        if w_sum > 0:
            shares = [w / w_sum for w in weights]
        else:
            shares = [1.0 / len(cc_skus)] * len(cc_skus)

        for sku, share in zip(cc_skus, shares):
            gap_sku_val = float(gap_val * share)
            split_val = split_idx.get((sku, country, category, month), 0.0)
            fff_val = fff_idx.get((sku, country, category, month), 0.0)
            final_val = fff_val + split_val + gap_sku_val
            gap_by_sku_rows.append(
                {
                    sfu_col: sku,
                    "Country": country,
                    "SMO Category": category,
                    "month": month,
                    "gap": gap_sku_val,
                }
            )
            final_rows[(sku, country, category, month)] = {
                sfu_col: sku,
                "Country": country,
                "SMO Category": category,
                "month": month,
                "fff": fff_val,
                "split": split_val,
                "gap": gap_sku_val,
                "final_matched": final_val,
            }

    gap_sku_long = pd.DataFrame(gap_by_sku_rows)
    final_long = pd.DataFrame(final_rows.values()) if final_rows else pd.DataFrame()
    if gap_sku_long.empty:
        return gap_ccm_df, pd.DataFrame(), pd.DataFrame()

    gap_sku_wide = (
        gap_sku_long.pivot_table(
            index=[sfu_col, "Country", "SMO Category"],
            columns="month",
            values="gap",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
    )
    final_wide = pd.DataFrame()
    if not final_long.empty:
        final_wide = (
            final_long.pivot_table(
                index=[sfu_col, "Country", "SMO Category"],
                columns="month",
                values="final_matched",
                aggfunc="sum",
                fill_value=0.0,
            )
            .reset_index()
        )

    ordered_gap_cols = [c for c in [sfu_col, "Country", "SMO Category"] + month_cols if c in gap_sku_wide.columns]
    gap_sku_wide = gap_sku_wide[ordered_gap_cols]
    if not final_wide.empty:
        ordered_final_cols = [c for c in [sfu_col, "Country", "SMO Category"] + month_cols if c in final_wide.columns]
        final_wide = final_wide[ordered_final_cols]

    return gap_ccm_df, gap_sku_wide, final_wide


def _build_last_bop_matching_tables(
    last_bop_df: pd.DataFrame | None,
    output_wide: pd.DataFrame | None,
    shared_month_candidates: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    if output_wide is None or output_wide.empty or last_bop_df is None or last_bop_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), []
    if MONTHLY_SFU_VERSION_COL not in output_wide.columns or MONTHLY_SFU_VERSION_COL not in last_bop_df.columns:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), []

    lb_df = last_bop_df.copy()
    if MONTHLY_MEASURE_COL in lb_df.columns:
        lb_df = lb_df[lb_df[MONTHLY_MEASURE_COL] == "Final Fcst to Finance"].copy()
    if lb_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), []

    lb_months = detect_month_columns(lb_df)
    shared_months = [m for m in shared_month_candidates if m in lb_months and m in output_wide.columns]
    if not shared_months:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), []

    lb_agg = lb_df[[MONTHLY_SFU_VERSION_COL] + shared_months].copy()
    for m in shared_months:
        lb_agg[m] = pd.to_numeric(lb_agg[m], errors="coerce").fillna(0.0)
    lb_agg = lb_agg.groupby(MONTHLY_SFU_VERSION_COL, dropna=False)[shared_months].sum().reset_index()

    adj_agg = output_wide[[MONTHLY_SFU_VERSION_COL] + shared_months].copy()
    for m in shared_months:
        adj_agg[m] = pd.to_numeric(adj_agg[m], errors="coerce").fillna(0.0)
    adj_agg = adj_agg.groupby(MONTHLY_SFU_VERSION_COL, dropna=False)[shared_months].sum().reset_index()

    merged = lb_agg.merge(adj_agg, on=MONTHLY_SFU_VERSION_COL, how="outer", suffixes=("_lb", "_adj")).fillna(0.0)
    new_total = pd.DataFrame({MONTHLY_SFU_VERSION_COL: merged[MONTHLY_SFU_VERSION_COL]})
    for m in shared_months:
        new_total[m] = merged.get(f"{m}_lb", 0.0) + merged.get(f"{m}_adj", 0.0)

    return lb_agg, adj_agg, new_total, shared_months


def _apply_planner_redistribution(
    base_output_wide: pd.DataFrame,
    planner_final_df: pd.DataFrame,
    allocation_trace_df: pd.DataFrame,
    sas_df_split_input: pd.DataFrame,
    bb_id_col: str,
    sas_months: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if base_output_wide is None or base_output_wide.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if allocation_trace_df is None or allocation_trace_df.empty:
        return base_output_wide.copy(), pd.DataFrame(), pd.DataFrame()
    if planner_final_df is None or planner_final_df.empty:
        return base_output_wide.copy(), pd.DataFrame(), pd.DataFrame()

    sfu_col = next((c for c in [MONTHLY_SFU_VERSION_COL, "SFU_v", "SKU"] if c in base_output_wide.columns), None)
    if not sfu_col or MONTHLY_SFU_VERSION_COL not in planner_final_df.columns:
        return base_output_wide.copy(), pd.DataFrame(), pd.DataFrame()

    month_cols = [m for m in sas_months if m in base_output_wide.columns and m in planner_final_df.columns]
    if not month_cols:
        return base_output_wide.copy(), pd.DataFrame(), pd.DataFrame()

    base_sku = (
        base_output_wide[[sfu_col] + month_cols]
        .groupby(sfu_col, dropna=False)[month_cols]
        .sum()
        .reset_index()
        .rename(columns={sfu_col: MONTHLY_SFU_VERSION_COL})
    )

    planner = planner_final_df[[MONTHLY_SFU_VERSION_COL] + month_cols].copy()
    for m in month_cols:
        planner[m] = pd.to_numeric(planner[m], errors="coerce").fillna(0.0)

    base_m = base_sku.merge(planner, on=MONTHLY_SFU_VERSION_COL, how="outer", suffixes=("_base", "_final")).fillna(0.0)

    trace = allocation_trace_df.copy()
    if "alloc" not in trace.columns:
        return base_output_wide.copy(), pd.DataFrame(), pd.DataFrame()
    trace["alloc"] = pd.to_numeric(trace["alloc"], errors="coerce").fillna(0.0)
    trace["adj_alloc"] = trace["alloc"].astype(float)
    trace["lock_cell"] = False
    if "is_fixed" not in trace.columns:
        trace["is_fixed"] = False

    audit_rows: list[dict] = []
    tol = 1e-9

    for _, row in base_m.iterrows():
        sfuv = str(row.get(MONTHLY_SFU_VERSION_COL, ""))
        if not sfuv:
            continue
        for m in month_cols:
            base_val = float(row.get(f"{m}_base", 0.0))
            final_val = float(row.get(f"{m}_final", 0.0))
            delta = final_val - base_val
            if abs(delta) <= tol:
                continue

            cell_mask = (trace["sfuv"].astype(str) == sfuv) & (trace["month"].astype(str) == str(m))
            if not cell_mask.any():
                audit_rows.append({
                    "type": "unmapped_planner_delta",
                    "sfuv": sfuv,
                    "month": m,
                    "delta": delta,
                    "detail": "Planner delta could not be mapped to BB trace rows.",
                })
                continue

            idxs = trace.index[cell_mask]
            base_parts = trace.loc[idxs, "adj_alloc"].astype(float)
            part_total = float(base_parts.sum())
            if abs(part_total) > tol:
                ratios = base_parts / part_total
            else:
                ratios = pd.Series([1.0 / len(idxs)] * len(idxs), index=idxs)

            trace.loc[idxs, "adj_alloc"] = base_parts + (delta * ratios)
            trace.loc[idxs, "lock_cell"] = True

    sas_target_rows: list[dict] = []
    if bb_id_col in sas_df_split_input.columns:
        for m in month_cols:
            if m not in sas_df_split_input.columns:
                continue
            s = pd.to_numeric(sas_df_split_input[m], errors="coerce").fillna(0.0)
            tmp = pd.DataFrame({"bb_id": sas_df_split_input[bb_id_col].astype(str), "month": m, "target": s})
            sas_target_rows.append(tmp)

    unresolved_rows: list[dict] = []
    if sas_target_rows:
        sas_targets = pd.concat(sas_target_rows, ignore_index=True)
        sas_targets = sas_targets.groupby(["bb_id", "month"], dropna=False)["target"].sum().reset_index()

        trace_group = trace.groupby(["bb_id", "month"], dropna=False)["adj_alloc"].sum().reset_index(name="current_total")
        residual_df = sas_targets.merge(trace_group, on=["bb_id", "month"], how="left").fillna(0.0)
        residual_df["residual"] = residual_df["target"] - residual_df["current_total"]

        for _, r in residual_df.iterrows():
            bb = str(r["bb_id"])
            month = str(r["month"])
            residual = float(r["residual"])
            if abs(residual) <= tol:
                continue

            bbm_mask = (trace["bb_id"].astype(str) == bb) & (trace["month"].astype(str) == month)
            remaining = trace[bbm_mask & (~trace["lock_cell"]) & (~trace["is_fixed"].astype(bool))]
            if remaining.empty:
                unresolved_rows.append({
                    "bb_id": bb,
                    "month": month,
                    "residual": residual,
                    "detail": "No remaining SKUs for redistribution in this BB-month.",
                })
                continue

            rem_idx = remaining.index
            rem_vals = trace.loc[rem_idx, "adj_alloc"].astype(float)
            w_sum = float(rem_vals.sum())
            if abs(w_sum) > tol:
                weights = rem_vals / w_sum
            else:
                weights = pd.Series([1.0 / len(rem_idx)] * len(rem_idx), index=rem_idx)
            trace.loc[rem_idx, "adj_alloc"] = rem_vals + residual * weights

    final_sfu_month = trace.groupby(["sfuv", "month"], dropna=False)["adj_alloc"].sum().reset_index()

    out_final = base_output_wide.copy()
    out_final_months = [m for m in month_cols if m in out_final.columns]
    for m in out_final_months:
        out_final[m] = pd.to_numeric(out_final[m], errors="coerce").fillna(0.0)

    for m in out_final_months:
        month_map = final_sfu_month[final_sfu_month["month"].astype(str) == str(m)].set_index("sfuv")["adj_alloc"].to_dict()
        for sfuv, target_total in month_map.items():
            row_mask = out_final[sfu_col].astype(str) == str(sfuv)
            idxs = out_final.index[row_mask]
            if len(idxs) == 0:
                continue
            current_vals = pd.to_numeric(out_final.loc[idxs, m], errors="coerce").fillna(0.0)
            cur_sum = float(current_vals.sum())
            if abs(cur_sum) > tol:
                ratios = current_vals / cur_sum
            else:
                ratios = pd.Series([1.0 / len(idxs)] * len(idxs), index=idxs)
            out_final.loc[idxs, m] = float(target_total) * ratios

    audit_df = pd.DataFrame(audit_rows) if audit_rows else pd.DataFrame(columns=["type", "sfuv", "month", "delta", "detail"])
    residual_out = pd.DataFrame(unresolved_rows) if unresolved_rows else pd.DataFrame(columns=["bb_id", "month", "residual", "detail"])
    return out_final, audit_df, residual_out


# ──────────────────────────────────────────────────────────────────────────────
# Exceptions panel (merged into Step 4)
# ──────────────────────────────────────────────────────────────────────────────
def render_exceptions_panel():
    st.markdown("#### Step 4 Exception Controls")
    exc_store: ExceptionStore = st.session_state.exc_store
    exc_store_sal = exc_store  # Alias for compatibility with moved code
    hcm = hier_col_map_from_state()
    sku_col = hcm.get("SFU_v") or hcm.get("SKU") or MONTHLY_SFU_VERSION_COL
    sku_col_excl = hcm.get("SFU_v", MONTHLY_SFU_VERSION_COL)  # For compatibility with moved code
    sas_cmap = st.session_state.col_maps.get("SAS", {})
    bb_split_levels = st.session_state.get("bb_split_levels", {})
    split_level_default = st.session_state.get("split_level", "Form")
    bb_id_col = st.session_state.bb_id_col or "BB_ID"

    sku_filtered = st.session_state.sku_df_filtered
    sas_filtered = st.session_state.sas_df_filtered
    all_skus: list[str] = []
    resolved_sku_col: str | None = None
    if sku_filtered is not None and not sku_filtered.empty:
        sku_col_candidates = [
            sku_col,
            hcm.get("SFU_v"),
            MONTHLY_SFU_VERSION_COL,
            "SFU_v",
            "SKU",
        ]
        resolved_sku_col = next((c for c in sku_col_candidates if c and c in sku_filtered.columns), None)
        if resolved_sku_col:
            all_skus = sorted(sku_filtered[resolved_sku_col].dropna().astype(str).unique().tolist())

    if resolved_sku_col:
        st.caption(f"Exceptions status: resolved SFU_v column = {resolved_sku_col} ({len(all_skus)} unique values)")
    else:
        st.caption("Exceptions status: no SFU_v column resolved from filtered data")

    desc_map = _latest_product_description_map()

    def _format_sku_option(sku_value: str) -> str:
        sku_norm = str(sku_value).strip()
        desc = str(desc_map.get(sku_norm, "")).strip()
        return f"{sku_norm} ({desc})" if desc else sku_norm

    def _match_skus_for_bb(bb_id: str) -> list[str]:
        if (
            not bb_id
            or sku_filtered is None
            or sku_filtered.empty
            or sas_filtered is None
            or sas_filtered.empty
            or not resolved_sku_col
            or resolved_sku_col not in sku_filtered.columns
            or bb_id_col not in sas_filtered.columns
        ):
            return []

        bb_rows = sas_filtered[sas_filtered[bb_id_col].astype(str) == str(bb_id)]
        if bb_rows.empty:
            return []
        bb_row = bb_rows.iloc[0]

        specific_sfuv_col = sas_cmap.get("SFU_v") or sas_cmap.get("SKU")
        if specific_sfuv_col not in sas_filtered.columns:
            specific_sfuv_col = None

        pinned_sfuv = str(bb_row.get(specific_sfuv_col, "")).strip() if specific_sfuv_col else ""
        if pinned_sfuv and pinned_sfuv.lower() != "nan":
            candidate_cols = [
                resolved_sku_col,
                MONTHLY_SFU_VERSION_COL,
                hcm.get("SFU_v"),
                hcm.get("SKU"),
                "SFU_v",
                "SKU",
            ]
            candidate_cols = [c for c in dict.fromkeys(candidate_cols) if c and c in sku_filtered.columns]
            if not candidate_cols:
                return []
            mask = pd.Series([False] * len(sku_filtered), index=sku_filtered.index)
            for col in candidate_cols:
                mask |= sku_filtered[col].astype(str).str.strip() == pinned_sfuv
            matched_rows = sku_filtered[mask]
        else:
            bb_level = bb_split_levels.get(str(bb_id), split_level_default)
            bb_group_keys_logical = [k for k in SPLIT_KEYS.get(bb_level, []) if k != "SFU_v"]
            bb_sas_keys = [sas_cmap.get(k, k) for k in bb_group_keys_logical]
            bb_sku_keys = [hcm.get(k, k) for k in bb_group_keys_logical]

            if any(col not in sku_filtered.columns for col in bb_sku_keys):
                return []

            mask = pd.Series([True] * len(sku_filtered), index=sku_filtered.index)
            for sas_col, sku_col_name in zip(bb_sas_keys, bb_sku_keys):
                mask &= sku_filtered[sku_col_name].astype(str).str.strip() == str(bb_row.get(sas_col, "")).strip()
            matched_rows = sku_filtered[mask]

        if matched_rows.empty:
            return []

        return sorted(matched_rows[resolved_sku_col].dropna().astype(str).str.strip().unique().tolist())

    def _exception_log_key(row: pd.Series) -> str:
        parts = [
            str(row.get("timestamp", "")),
            str(row.get("scope", "")),
            str(row.get("bb_id", "")),
            str(row.get("sku", "")),
            str(row.get("exc_type", "")),
            str(row.get("old_value", "")),
            str(row.get("new_value", "")),
            str(row.get("notes", "")),
        ]
        return "|".join(parts)

    # ── GBB action banners ────────────────────────────────────────────────────
    gbb_actions: dict[str, str] = st.session_state.get("gbb_actions", {})
    gbb_types: dict[str, str] = st.session_state.get("gbb_types", {})
    exception_bbs = [bid for bid, act in gbb_actions.items() if act == "exceptions"]
    ignore_bbs = [bid for bid, act in gbb_actions.items() if act == "ignore"]

    st.markdown("#### Initiative Exclusion")
    if ignore_bbs:
        with st.expander("⚠️ Initiative Exclusion — click to review", expanded=True):
            st.error(
                f"**{len(ignore_bbs)} Building Block(s) flagged as *Ignore* (Initiatives):** "
                "Select which SFU_v values should still be included for BB split handling. "
                f"BBs: `{'`, `'.join(ignore_bbs)}`"
            )
            if all_skus:
                sel_ignore_bb = st.selectbox(
                    "Select Initiative BB",
                    ignore_bbs,
                    key="ignore_bb_selector",
                )
                matched_ignore_skus = _match_skus_for_bb(sel_ignore_bb)
                cur_ignore_inc = sorted([
                    s for s in exc_store.bb_exceptions.get(sel_ignore_bb, {}).get("include", set())
                    if s in matched_ignore_skus
                ])
                if matched_ignore_skus:
                    st.caption(
                        f"Showing **{len(matched_ignore_skus)}** SFU_v values that match this BB's split granularity. "
                        "All of them remain globally excluded by default."
                    )
                else:
                    st.warning("No matching SFU_v values were found for this Initiative BB at its current split granularity.")
                selected_ignore_include_skus = st.multiselect(
                    "SFU_v values to include for this Initiative BB split",
                    matched_ignore_skus,
                    default=cur_ignore_inc,
                    key="ignore_bb_sku_selector",
                    format_func=_format_sku_option,
                    help="Selected SFU_v values are saved as BB-specific includes for this Initiative BB while all initiative SKUs stay globally excluded by default.",
                )
                if st.button("Save Initiative Exclusion", key="gbb_add_ignore"):
                    note = (
                        f"Initiatives BB — include for BB split handling. "
                        f"GBB Type: {gbb_types.get(sel_ignore_bb, 'Initiatives')}"
                    )
                    exc_store.remove_bb_exclude(sel_ignore_bb, ExceptionStore.ALL_SKU_SENTINEL)
                    for sku in matched_ignore_skus:
                        exc_store.remove_bb_exclude(sel_ignore_bb, sku)
                        exc_store.remove_bb_include(sel_ignore_bb, sku)
                        exc_store.add_global_exclusion(sku, notes="Initiative SKU — globally excluded by default")

                    for sku in selected_ignore_include_skus:
                        exc_store.add_bb_include(sel_ignore_bb, sku, notes=note)

                    st.success(
                        f"Saved {len(selected_ignore_include_skus)} initiative include SFU_v value(s) for BB {sel_ignore_bb}."
                    )
                    st.rerun()
            else:
                st.warning(
                    "No SFU_v column resolved in exceptions data. Please verify SFU_v mapping in Step 2 "
                    "to select initiative values."
                )
    else:
        st.caption("No initiative exclusions require review for the current filtered dataset.")

    st.divider()
    override_count = sum(
        1
        for values in exc_store.bb_exceptions.values()
        if values.get("include") or values.get("exclude")
    )
    st.markdown(f"#### Manage Per-BB Exceptions ({override_count} BB(s) with overrides)")
    if exception_bbs:
        st.warning(
            f"**{len(exception_bbs)} Building Block(s) require SFU_v selection** "
            f"(Promotions / New Channels): `{'`, `'.join(exception_bbs)}`"
        )

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

            matched_bb_skus = _match_skus_for_bb(selected_bb)
            if matched_bb_skus:
                st.caption(f"Showing **{len(matched_bb_skus)}** SFU_v values that match this BB's split granularity.")
            else:
                st.warning("No matching SFU_v values were found for this Building Block at its current split granularity.")

            exc_bb = exc_store.bb_exceptions.get(selected_bb, {})
            cur_include = [s for s in exc_bb.get("include", set()) if s in matched_bb_skus]
            cur_exclude = [
                s for s in exc_bb.get("exclude", set())
                if s != ExceptionStore.ALL_SKU_SENTINEL and s in matched_bb_skus
            ]

            c1, c2 = st.columns(2)

            with c1:
                st.markdown("**Force Include SFU_vs**")
                new_include = st.multiselect(
                    "Include",
                    matched_bb_skus,
                    default=cur_include,
                    key=f"inc_{selected_bb}",
                    format_func=_format_sku_option,
                )
            with c2:
                st.markdown("**Force Exclude SFU_vs**")
                new_exclude = st.multiselect(
                    "Exclude",
                    matched_bb_skus,
                    default=cur_exclude,
                    key=f"exc_{selected_bb}",
                    format_func=_format_sku_option,
                )

            notes = st.text_input("Notes (optional)", key=f"exc_notes_{selected_bb}")

            if st.button("Save BB Exceptions", key=f"save_bb_{selected_bb}"):
                old_include = set(exc_store.bb_exceptions.get(selected_bb, {}).get("include", set()))
                for sku in set(new_include) - old_include:
                    exc_store.add_bb_include(selected_bb, sku, notes)
                for sku in old_include - set(new_include):
                    exc_store.remove_bb_include(selected_bb, sku)

                old_exclude = {
                    s for s in set(exc_store.bb_exceptions.get(selected_bb, {}).get("exclude", set()))
                    if s != ExceptionStore.ALL_SKU_SENTINEL
                }
                for sku in set(new_exclude) - old_exclude:
                    exc_store.add_bb_exclude(selected_bb, sku, notes)
                for sku in old_exclude - set(new_exclude):
                    exc_store.remove_bb_exclude(selected_bb, sku)

                st.success("BB exceptions saved.")

    st.divider()
    st.markdown("#### Exception Log")
    st.caption("Review the saved exception actions below and add optional remarks before moving to Step 5.")
    log_df = exc_store.log_as_df()
    if log_df.empty:
        st.info("No exceptions logged yet.")
    else:
        log_df = log_df.copy()
        if "sku" in log_df.columns:
            desc_map_log = _latest_product_description_map()
            log_df["Product Description"] = (
                log_df["sku"].astype(str).str.strip().map(desc_map_log).fillna("")
            )
        log_remarks = st.session_state.get("exception_log_remarks", {})
        log_df["Remarks"] = log_df.apply(
            lambda row: log_remarks.get(_exception_log_key(row), ""),
            axis=1,
        )
        edited_log = st.data_editor(
            log_df,
            width='stretch',
            num_rows="fixed",
            key="exception_log_editor",
            disabled=[c for c in log_df.columns if c != "Remarks"],
            column_config={
                "Remarks": st.column_config.TextColumn(
                    "Remarks",
                    help="Optional user notes for remembering why this exception was captured.",
                ),
                "notes": st.column_config.TextColumn("Notes", disabled=True),
            },
        )
        st.session_state.exception_log_remarks = {
            _exception_log_key(row): str(edited_log.iloc[idx].get("Remarks", ""))
            for idx, (_, row) in enumerate(log_df.iterrows())
        }

    # ═════════════════════════════════════════════════════════════════════════════════
    # BLOCK 2: UPLOAD & CONFIGURE EXCLUSION VOLUMES
    # ═════════════════════════════════════════════════════════════════════════════════
    st.divider()
    st.subheader("📤 Upload & Configure Exclusion Volumes")
    st.caption("Configure volumes for excluded SFU_v values using upload or direct input.")
    
    # Sub-tabs for upload methods
    tab_upload_file, tab_direct_input = st.tabs(["📤 Upload File", "✏️ Direct Input"])
    
    # ═════════════════════════════════════════════════════════════════════════════════
    # TAB 1: UPLOAD FILE
    # ═════════════════════════════════════════════════════════════════════════════════
    with tab_upload_file:
        st.markdown(
            "Upload an **Excel file** with excluded SFU_v values and their volumes. "
            "The file must have:\n"
            "- A column named **`SFU_v`** (or the mapped SFU_v column name for your data)\n"
            "- Month columns in `Mmm-YY` format (e.g. `Jan-26`, `Feb-26`) with the volumes to allocate"
        )
        st.download_button(
            label="📥 Download template",
            data=_build_exclusion_upload_template(sku_col_excl, st.session_state.get("sas_months_selected", [])),
            file_name="exclusions_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        excl_upload_file = st.file_uploader(
            "Upload exclusions Excel",
            type=["xlsx", "xls"],
            key="excl_upload_file",
        )
        
        if excl_upload_file is not None:
            try:
                excl_up_df = pd.read_excel(excl_upload_file)
                sku_col_candidates = [sku_col_excl, MONTHLY_SFU_VERSION_COL, "SFU_v", "SKU"]
                found_sku_col = next((c for c in sku_col_candidates if c in excl_up_df.columns), None)
                if found_sku_col is None:
                    st.error(f"Could not find a SKU column in the uploaded file. Expected one of: {sku_col_candidates}")
                else:
                    month_cols_up = [
                        c for c in excl_up_df.columns
                        if c != found_sku_col and parse_month_to_date(c) is not None
                    ]
                    if not month_cols_up:
                        st.warning("No month columns detected in the uploaded file. Ensure columns are in `Mmm-YY` format.")
                    else:
                        st.success(
                            f"Detected **{len(excl_up_df)} SKU row(s)** and "
                            f"**{len(month_cols_up)} month column(s)**: {', '.join(month_cols_up)}"
                        )
                        
                        # ═══════════════════════════════════════════════════════════════════
                        # REVIEW EXCLUSION VOLUMES BLOCK
                        # ═══════════════════════════════════════════════════════════════════
                        st.divider()
                        st.subheader("🔍 Review Exclusion Volumes")
                        st.markdown(
                            "Below are the SFU values and their allocated volumes for exclusion. "
                            "Review the details and confirm before applying."
                        )
                        
                        # Build review DataFrame with SFU, Product Description, and volumes
                        review_cols = [found_sku_col] + month_cols_up
                        review_df = excl_up_df[review_cols].copy()
                        review_df = _add_product_description(review_df)
                        
                        # Calculate total volume per SFU for quick review
                        review_df["Total Volume"] = review_df[month_cols_up].sum(axis=1)
                        
                        # Reorder columns: SFU_v, Product Description, months, Total Volume
                        display_cols = [found_sku_col, "Product Description"] + month_cols_up + ["Total Volume"]
                        display_cols = [c for c in display_cols if c in review_df.columns]
                        review_display = review_df[display_cols]
                        
                        # Configure column display
                        col_config_review = {
                            found_sku_col: st.column_config.TextColumn("SFU_v", disabled=True),
                            "Product Description": st.column_config.TextColumn("Product Description", disabled=True),
                            "Total Volume": st.column_config.NumberColumn("Total Volume", format="%.2f", disabled=True),
                        }
                        for mc in month_cols_up:
                            col_config_review[mc] = st.column_config.NumberColumn(f"{mc}", format="%.2f", disabled=True)
                        
                        st.dataframe(
                            review_display.head(20),
                            column_config=col_config_review,
                            width='stretch',
                            height=min(450, 60 + len(review_display) * 35),
                        )

                        if st.button("✅ Apply Uploaded Exclusions + Volumes", key="apply_excl_upload"):
                            applied_skus = set()
                            for _, urow in excl_up_df.iterrows():
                                sku_v = str(urow[found_sku_col])
                                if not sku_v or sku_v == "nan":
                                    continue
                                exc_store_sal.add_global_exclusion(
                                    sku_v,
                                    notes="Excluded via uploaded exclusions Excel (Step 4)"
                                )
                                applied_skus.add(sku_v)
                                for mc in month_cols_up:
                                    qty_val = pd.to_numeric(urow.get(mc, 0), errors="coerce")
                                    if not pd.isna(qty_val) and qty_val != 0:
                                        exc_store_sal.set_fixed_qty(
                                            "__uploaded__",
                                            sku_v,
                                            mc,
                                            float(qty_val),
                                            notes="Volume from uploaded exclusions Excel",
                                        )
                            st.session_state.exc_store = exc_store_sal
                            applied_display = ", ".join(_format_sku_excl(v) for v in sorted(applied_skus))
                            st.success(
                                f"✅ {len(applied_skus)} SKU(s) excluded and volumes loaded: "
                                f"{applied_display}"
                            )
                            st.rerun()
            except Exception as _exc_up_err:
                st.error(f"Error reading uploaded file: {_exc_up_err}")
    
    # ═════════════════════════════════════════════════════════════════════════════════
    # TAB 2: DIRECT INPUT
    # ═════════════════════════════════════════════════════════════════════════════════
    with tab_direct_input:
        st.markdown(
            "Enter exclusion volumes directly for each excluded SFU_v. "
            "Volumes will be saved as fixed allocations in the exception store."
        )
        
        excluded_skus_list = sorted(exc_store_sal.global_exclusions)
        sas_months_for_excl = st.session_state.get("sas_months_selected", [])
        
        if excluded_skus_list and sas_months_for_excl:
            # Build editable DataFrame with excluded SKUs and months
            excl_edit_data = []
            for sku in excluded_skus_list:
                row = {"SFU_v": sku}
                for month in sas_months_for_excl:
                    # Get existing fixed quantity for __uploaded__ or default to 0
                    existing_qty = exc_store_sal.get_fixed_qty("__uploaded__", sku, month)
                    row[month] = existing_qty if existing_qty is not None else 0.0
                excl_edit_data.append(row)
            
            excl_edit_df = pd.DataFrame(excl_edit_data)
            excl_edit_df = _add_product_description(excl_edit_df)
            
            # Reorder: SFU_v, Product Description, months
            month_cols_edit = sas_months_for_excl
            display_cols_edit = ["SFU_v", "Product Description"] + month_cols_edit
            display_cols_edit = [c for c in display_cols_edit if c in excl_edit_df.columns]
            
            # Configure columns for editing
            col_config_edit = {
                "SFU_v": st.column_config.TextColumn("SFU_v", disabled=True),
                "Product Description": st.column_config.TextColumn("Product Description", disabled=True),
            }
            for mc in month_cols_edit:
                col_config_edit[mc] = st.column_config.NumberColumn(
                    f"{mc}",
                    format="%.2f",
                    min_value=0.0,
                    help=f"Volume for {mc}",
                )
            
            # Editable data editor
            edited_excl_vols = st.data_editor(
                excl_edit_df[display_cols_edit],
                column_config=col_config_edit,
                disabled=["SFU_v", "Product Description"],
                width='stretch',
                key="excl_direct_volume_editor",
                height=min(450, 60 + len(excl_edit_df) * 35),
                num_rows="fixed",
            )
            
            # Save button for direct input
            if st.button("💾 Save Direct Volume Input", key="save_direct_excl_volumes"):
                for _, erow in edited_excl_vols.iterrows():
                    sku_v = str(erow.get("SFU_v", "")).strip()
                    if not sku_v:
                        continue
                    for month in month_cols_edit:
                        qty_val = pd.to_numeric(erow.get(month, 0), errors="coerce")
                        qty_val = float(qty_val) if not pd.isna(qty_val) else 0.0
                        if qty_val != 0:
                            exc_store_sal.set_fixed_qty(
                                "__uploaded__",
                                sku_v,
                                month,
                                qty_val,
                                notes="Direct volume input in Step 4",
                            )
                st.session_state.exc_store = exc_store_sal
                st.success(f"✅ Exclusion volumes saved for {len(excluded_skus_list)} SFU_v value(s)")
                st.rerun()
        elif excluded_skus_list and not sas_months_for_excl:
            st.info("No SAS months selected yet — complete Step 3 to select months for volume input.")
        elif not excluded_skus_list:
            st.info("No excluded SKUs yet — exclude SFU_v values from the table above first.")

    # ═════════════════════════════════════════════════════════════════════════════════
    # EXCLUSION VOLUMES REVIEW
    # ═════════════════════════════════════════════════════════════════════════════════
    st.divider()
    st.markdown("#### 📊 Exclusion Volumes Review")
    st.caption("Review the SFU values, product descriptions, and allocated volumes across months before moving to Step 5.")
    
    # Collect all fixed quantities from exception store
    excl_volumes_data = []
    for bb_id, bb_exc in exc_store.bb_exceptions.items():
        fixed_qty_dict = bb_exc.get("fixed_qty", {})
        for sku, month_dict in fixed_qty_dict.items():
            row_data = {"SFU_v": sku}
            for month, qty in month_dict.items():
                if qty != 0:  # Only include non-zero volumes
                    row_data[month] = qty
            if len(row_data) > 1:  # Only add if there's at least one volume
                excl_volumes_data.append(row_data)
    
    if excl_volumes_data:
        # Create DataFrame and fill NaN with 0 for display
        excl_vol_df = pd.DataFrame(excl_volumes_data).fillna(0.0)
        
        # Add product descriptions
        desc_map_excl = _latest_product_description_map()
        excl_vol_df["Product Description"] = excl_vol_df["SFU_v"].astype(str).str.strip().map(desc_map_excl).fillna("")
        
        # Identify month columns
        month_cols_excl = [c for c in excl_vol_df.columns if c not in ["SFU_v", "Product Description"] and parse_month_to_date(c) is not None]
        month_cols_excl = sorted(month_cols_excl, key=lambda x: parse_month_to_date(x) if parse_month_to_date(x) else datetime.datetime.min)
        
        # Calculate total volume
        excl_vol_df["Total Volume"] = excl_vol_df[month_cols_excl].sum(axis=1)
        
        # Reorder columns: SFU_v, Product Description, months, Total Volume
        display_cols_excl = ["SFU_v", "Product Description"] + month_cols_excl + ["Total Volume"]
        display_cols_excl = [c for c in display_cols_excl if c in excl_vol_df.columns]
        excl_vol_display = excl_vol_df[display_cols_excl]
        
        # Configure columns
        col_config_excl_vol = {
            "SFU_v": st.column_config.TextColumn("SFU_v", disabled=True),
            "Product Description": st.column_config.TextColumn("Product Description", disabled=True),
            "Total Volume": st.column_config.NumberColumn("Total Volume", format="%.2f", disabled=True),
        }
        for mc in month_cols_excl:
            col_config_excl_vol[mc] = st.column_config.NumberColumn(f"{mc}", format="%.2f", disabled=True)
        
        st.dataframe(
            excl_vol_display,
            column_config=col_config_excl_vol,
            width='stretch',
            height=min(450, 60 + len(excl_vol_display) * 35),
        )
    else:
        st.info("No exclusion volumes configured yet.")
    
    # ═════════════════════════════════════════════════════════════════════════════════
    # VALIDATION: CHECK FOR EXCLUDED SKUs WITHOUT VOLUMES
    # ═════════════════════════════════════════════════════════════════════════════════
    if exc_store.global_exclusions:
        # Collect all SKUs with fixed quantities
        skus_with_volumes = set()
        for bb_exc in exc_store.bb_exceptions.values():
            fixed_qty_dict = bb_exc.get("fixed_qty", {})
            for sku, month_dict in fixed_qty_dict.items():
                if any(qty != 0 for qty in month_dict.values()):
                    skus_with_volumes.add(sku)
        
        # Find excluded SKUs without any volumes
        excluded_without_volumes = exc_store.global_exclusions - skus_with_volumes
        
        if excluded_without_volumes:
            st.divider()
            excl_no_vol_list = ", ".join(_format_sku_excl(v) for v in sorted(excluded_without_volumes))
            st.warning(
                f"⚠️ **{len(excluded_without_volumes)} SFU_v value(s) are excluded but have NO volumes configured:**\n\n"
                f"{excl_no_vol_list}\n\n"
                f"**Action required:** Upload exclusion volumes for these SFU_v values OR remove them from exclusions before proceeding to Step 5."
            )

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
        issues.append("No SFU_v data — complete Steps 1-3")
    if st.session_state.salience_df is None:
        issues.append("Salience not computed — complete Step 5")
    if not st.session_state.sas_months_selected:
        issues.append("No SAS months selected — complete Step 3")

    if issues:
        for iss in issues:
            st.error(f"• {iss}")
        return

    sas_df = st.session_state.sas_df_filtered
    sku_df = st.session_state.sku_df_filtered
    sal_df = st.session_state.salience_df
    sas_months = st.session_state.sas_months_selected
    split_level = st.session_state.split_level
    bb_split_levels = st.session_state.get("bb_split_levels", {})
    bb_id_col = st.session_state.bb_id_col or "BB_ID"
    exc_store = st.session_state.exc_store
    hcm = hier_col_map_from_state()
    sas_cmap = st.session_state.col_maps.get("SAS", {})
    (
        sas_df_alloc,
        sas_df_topline,
        sas_df_last_ff,
        sas_df_prior_cycle,
        plan_name_col,
        active_cycle_token,
    ) = _prepare_sas_split_and_topline_scopes(sas_df, sas_cmap, sas_months)

    if plan_name_col is None:
        st.info("Plan Name column not detected in SAS. Last FF reference rows cannot be separated.")
    elif sas_df_last_ff.empty:
        st.caption("No Last FF reference rows found in SAS Plan Name. All rows will be used for split allocation.")
    else:
        st.info(
            f"Detected {len(sas_df_last_ff)} Last FF reference row(s) using column '{plan_name_col}'. "
            "These rows will be ignored for split allocation and shown in reasonability reference checks."
        )

    if plan_name_col and not sas_df_prior_cycle.empty:
        st.info(
            f"Excluded {len(sas_df_prior_cycle)} prior-cycle row(s) from split/topline scope using active cycle token "
            f"{active_cycle_token}."
        )

    if plan_name_col:
        st.caption(
            f"Topline rows detected by Plan Name containing 'Topline Base': {len(sas_df_topline)} "
            f"(used only for topline matching)."
        )

    if sas_df_alloc.empty:
        st.error("No allocatable SAS rows remain after excluding Last FF reference rows.")
        return

    # Summary
    per_bb_note = f"per-BB ({len(bb_split_levels)} configured)" if bb_split_levels else split_level
    st.markdown(f"""
| Parameter | Value |
|---|---|
| Building Blocks | {len(sas_df_alloc)} allocatable ({len(sas_df_topline)} Topline Base, {len(sas_df_last_ff)} Last FF reference, {len(sas_df_prior_cycle)} prior-cycle excluded) |
| SKU rows | {len(sku_df)} |
| Split level | **{per_bb_note}** |
| SAS months | {', '.join(sas_months)} |
| Active BOP cycle | **{active_cycle_token}** |
| Global exclusions | {len(exc_store.global_exclusions)} |
| BB-specific exceptions | {len(exc_store.bb_exceptions)} |
    """)

    # Diagnostic Check Button
    with st.expander("🔍 Diagnostic Check - Troubleshoot Matching Issues", expanded=False):
        st.markdown("""
        Use this diagnostic to identify which building blocks might fail to match with SFU_v rows.
        This will help you understand **why** you're getting "No matching SFU_vs found" errors.
        """)
        
        if st.button("Run Diagnostic Check"):
            with st.spinner("Analyzing building block matches..."):
                # Determine which sheet(s) provide the SFU_v / SKU data
                _sku_source_role = "Bible" if "Bible" in st.session_state.sheet_map else next(
                    (r for r in SKU_SHEETS if r in st.session_state.sheet_map and r != "Bible"), None
                )
                _sku_source_sheet = (
                    st.session_state.sheet_map.get(_sku_source_role, _sku_source_role)
                    if _sku_source_role else "SFU_v data"
                )
                sku_cmap = st.session_state.col_maps.get(_sku_source_role, {}) if _sku_source_role else {}

                sfuv_id_col = hcm.get("SFU_v", hcm.get("SKU", "SKU"))
                if sfuv_id_col not in sku_df.columns:
                    sfuv_id_col = sku_cmap.get("SFU_v") or sku_cmap.get("SKU") or sfuv_id_col
                specific_sfuv_col = sas_cmap.get("SFU_v") or sas_cmap.get("SKU")

                def _salience_hit_count(
                    eligible_ids: list[str],
                    bb_level: str,
                    bb_sku_keys: list[str] | None = None,
                    bb_group_vals: list[str] | None = None,
                    matched_rows: pd.DataFrame | None = None,
                ) -> int:
                    """Return count of eligible IDs with positive salience for this BB context.

                    Primary check: do any of the eligible SFU_v IDs appear in the salience
                    table with salience > 0?  Group-hierarchy filtering is attempted first
                    but falls back to the full salience table when it produces an empty slice,
                    so column-name or value mismatches never cause a false NO SALIENCE.
                    """
                    if sal_df is None or sal_df.empty:
                        return 0

                    eligible_norm = {str(x).strip() for x in eligible_ids if str(x).strip() != ""}
                    if not eligible_norm:
                        return 0

                    # Candidate identifier columns (priority order).
                    candidate_id_cols = [
                        sfuv_id_col,
                        MONTHLY_SFU_VERSION_COL,
                        hcm.get("SFU_v"),
                        hcm.get("SKU"),
                        "SFU_v",
                        "SKU",
                    ]
                    candidate_id_cols = [c for c in dict.fromkeys(candidate_id_cols) if c]

                    # Expand eligible_norm with aliases from matched SKU rows so that
                    # SFU_v vs "SFU_SFU Version" column-name differences don't block hits.
                    if matched_rows is not None and not matched_rows.empty:
                        alias_cols = [c for c in candidate_id_cols if c in matched_rows.columns]
                        if alias_cols:
                            eligible_rows = matched_rows[
                                matched_rows[alias_cols[0]].astype(str).str.strip().isin(eligible_norm)
                            ]
                            for col in alias_cols:
                                eligible_norm.update(
                                    str(v).strip()
                                    for v in eligible_rows[col].dropna().tolist()
                                    if str(v).strip() != ""
                                )

                    if "salience" not in sal_df.columns:
                        return 0

                    def _hits_in_slice(sal_slice: pd.DataFrame) -> int:
                        """Count eligible IDs that have salience > 0 in this slice."""
                        sal_id_cols = [c for c in candidate_id_cols if c in sal_slice.columns]
                        sal_lookup: dict[str, float] = {}
                        for _, srow in sal_slice.iterrows():
                            sval = float(pd.to_numeric(srow.get("salience", 0), errors="coerce") or 0.0)
                            for col in sal_id_cols:
                                sid = str(srow.get(col, "")).strip()
                                if sid:
                                    sal_lookup[sid] = max(sal_lookup.get(sid, 0.0), sval)
                        return sum(1 for sid in eligible_norm if sal_lookup.get(sid, 0.0) > 0)

                    # --- Attempt 1: level + group filtered slice --------------------
                    sal_sub = sal_df.copy()
                    if "_split_level" in sal_sub.columns:
                        sal_sub = sal_sub[sal_sub["_split_level"].astype(str) == str(bb_level)]

                    if bb_sku_keys and bb_group_vals:
                        for col, val in zip(bb_sku_keys, bb_group_vals):
                            if col in sal_sub.columns:
                                sal_sub = sal_sub[
                                    sal_sub[col].astype(str).str.strip() == str(val).strip()
                                ]

                    if not sal_sub.empty:
                        hits = _hits_in_slice(sal_sub)
                        if hits > 0:
                            return hits
                        # Group slice exists but IDs didn't match — check if any rows in
                        # this slice have positive salience (catches formatting differences).
                        sal_pos = pd.to_numeric(sal_sub["salience"], errors="coerce").fillna(0) > 0
                        if sal_pos.any():
                            return int(sal_pos.sum())

                    # --- Attempt 2: full salience table (no group filter) ------------
                    # Used when group filtering emptied the slice OR produced 0 hits.
                    hits_full = _hits_in_slice(sal_df)
                    return hits_full

                diagnostic_results = []
                for bb_idx, bb_row in sas_df.iterrows():
                    bb_id = str(bb_row.get(bb_id_col, f"BB_{bb_idx}"))
                    bb_level = bb_split_levels.get(bb_id, split_level)
                    
                    # Check if there's a pinned SFU_v
                    pinned_sfuv = str(bb_row.get(specific_sfuv_col, "")).strip() if specific_sfuv_col and specific_sfuv_col in bb_row.index else ""
                    
                    if pinned_sfuv and pinned_sfuv != "" and pinned_sfuv.lower() != "nan":
                        # Check if pinned value exists
                        mask = sku_df[sfuv_id_col].astype(str) == pinned_sfuv
                        match_count = mask.sum()
                        
                        # Check eligibility after exceptions
                        matched_rows = sku_df[mask] if match_count > 0 else sku_df.iloc[0:0]
                        matched_sfuvs_list = matched_rows[sfuv_id_col].astype(str).tolist() if match_count > 0 else []
                        eligible = exc_store.get_eligible_skus(bb_id, matched_sfuvs_list)
                        eligible_count = len(eligible)
                        
                        # Check salience coverage
                        salience_count = _salience_hit_count(eligible, bb_level, matched_rows=matched_rows)
                        
                        # Determine status
                        if match_count == 0:
                            status = "❌ NO MATCH"
                        elif eligible_count == 0:
                            status = f"⚠️ EXCLUDED (all {match_count} matched SKUs are globally excluded)"
                        elif salience_count == 0:
                            status = f"⚠️ NO SALIENCE ({eligible_count} eligible, but no salience data)"
                        else:
                            status = f"✅ OK ({eligible_count} eligible, {salience_count} with salience)"
                        
                        diagnostic_results.append({
                            "BB_ID": bb_id,
                            "Split_Level": bb_level,
                            "Match_Type": "Pinned SFU_v",
                            "Match_Criteria": f"SFU_v={pinned_sfuv}",
                            "Matches_Found": match_count,
                            "Eligible": eligible_count,
                            "With_Salience": salience_count,
                            "Status": status
                        })
                    else:
                        # Check hierarchical matching
                        bb_group_keys_logical = [k for k in SPLIT_KEYS[bb_level] if k != "SFU_v"]
                        bb_sas_keys = [sas_cmap.get(k, k) for k in bb_group_keys_logical]
                        bb_sku_keys = [hcm.get(k, k) for k in bb_group_keys_logical]
                        bb_group_vals = [str(bb_row.get(c, "")) for c in bb_sas_keys]
                        
                        # Build match criteria string
                        criteria_parts = [f"{col}={val}" for col, val in zip(bb_sku_keys, bb_group_vals)]
                        criteria_str = ", ".join(criteria_parts)
                        
                        # Find matching rows
                        mask = pd.Series([True] * len(sku_df), index=sku_df.index)
                        for col, val in zip(bb_sku_keys, bb_group_vals):
                            if col in sku_df.columns:
                                mask &= sku_df[col].astype(str) == str(val)
                            else:
                                mask &= False  # Column doesn't exist
                        
                        match_count = mask.sum()
                        
                        # Check for missing columns
                        missing_cols = [col for col in bb_sku_keys if col not in sku_df.columns]
                        if missing_cols:
                            status = f"❌ MISSING COLS in '{_sku_source_sheet}': {', '.join(missing_cols)}"
                            diagnostic_results.append({
                                "BB_ID": bb_id,
                                "Split_Level": bb_level,
                                "Match_Type": "Hierarchical",
                                "Match_Criteria": criteria_str,
                                "Matches_Found": 0,
                                "Eligible": 0,
                                "With_Salience": 0,
                                "Status": status
                            })
                        elif match_count == 0:
                            status = "❌ NO MATCH"
                            diagnostic_results.append({
                                "BB_ID": bb_id,
                                "Split_Level": bb_level,
                                "Match_Type": "Hierarchical",
                                "Match_Criteria": criteria_str,
                                "Matches_Found": 0,
                                "Eligible": 0,
                                "With_Salience": 0,
                                "Status": status
                            })
                        else:
                            # Check eligibility after exceptions
                            matched_rows = sku_df[mask]
                            matched_sfuvs_list = matched_rows[sfuv_id_col].astype(str).tolist()
                            eligible = exc_store.get_eligible_skus(bb_id, matched_sfuvs_list)
                            eligible_count = len(eligible)
                            
                            # Check salience coverage
                            salience_count = _salience_hit_count(
                                eligible,
                                bb_level,
                                bb_sku_keys=bb_sku_keys,
                                bb_group_vals=bb_group_vals,
                                matched_rows=matched_rows,
                            )
                            
                            # Determine status
                            if eligible_count == 0:
                                status = f"⚠️ EXCLUDED (all {match_count} matched SKUs are globally excluded)"
                            elif salience_count == 0:
                                status = f"⚠️ NO SALIENCE ({eligible_count} eligible, but no salience data)"
                            else:
                                status = f"✅ OK ({eligible_count} eligible, {salience_count} with salience)"
                            
                            diagnostic_results.append({
                                "BB_ID": bb_id,
                                "Split_Level": bb_level,
                                "Match_Type": "Hierarchical",
                                "Match_Criteria": criteria_str,
                                "Matches_Found": match_count,
                                "Eligible": eligible_count,
                                "With_Salience": salience_count,
                                "Status": status
                            })
                
                # Display results
                diag_df = pd.DataFrame(diagnostic_results)
                
                # Show statistics
                total_bbs = len(diag_df)
                ok_bbs = len(diag_df[diag_df["Status"].str.contains("✅")])
                failed_bbs = total_bbs - ok_bbs
                
                if failed_bbs > 0:
                    st.error(f"⚠️ **{failed_bbs} of {total_bbs} building blocks will fail or have issues!**")
                else:
                    st.success(f"✅ All {total_bbs} building blocks are ready to split!")
                
                # Filter options
                col1, col2 = st.columns(2)
                with col1:
                    show_filter = st.selectbox(
                        "Filter results:",
                        ["Show All", "Show Only Problems", "Show Only OK"],
                        key="diag_filter"
                    )
                
                if show_filter == "Show Only Problems":
                    diag_df = diag_df[~diag_df["Status"].str.contains("✅")]
                elif show_filter == "Show Only OK":
                    diag_df = diag_df[diag_df["Status"].str.contains("✅")]
                
                st.dataframe(diag_df, width='stretch', height=400)
                
                # Helpful suggestions
                if failed_bbs > 0:
                    st.markdown("### 💡 Troubleshooting Suggestions")
                    
                    has_missing_cols = any("MISSING COLS" in result["Status"] for result in diagnostic_results)
                    has_no_match = any(result["Status"] == "❌ NO MATCH" for result in diagnostic_results)
                    has_pinned = any(result["Match_Type"] == "Pinned SFU_v" and "❌" in result["Status"] for result in diagnostic_results)
                    has_excluded = any("EXCLUDED" in result["Status"] for result in diagnostic_results)
                    has_no_salience = any("NO SALIENCE" in result["Status"] for result in diagnostic_results)
                    
                    if has_excluded:
                        st.warning("""
                        **All Matched SKUs Are Excluded:**
                        - Your matching SFU_vs exist but are ALL in the global exclusion list
                        - Check the **Advanced Exceptions** section: Are you excluding too many SKUs?
                        - For Initiative BBs: make sure you've selected SKUs to **include** in the Initiative BB section
                        - If intentional, you may need to adjust your exclusion strategy
                        """)
                    
                    if has_no_salience:
                        st.warning("""
                        **No Salience Data for Matched SKUs:**
                        - Your SKUs match hierarchically and are eligible, but have zero salience values
                        - Check the **Live Preview** in Step 5: Do these SFU_vs have "N/A" or 0 Final Basis?
                        - If Final Basis is N/A, select a different **Basis / Metric** or **Basis Window**
                        - Check the **Basis Status** column for specific failure reasons
                        """)
                    
                    if has_missing_cols:
                        st.warning("""
                        **Missing Columns Issue:**
                        - Some hierarchy columns in your SAS data don't exist in your SFU_v data
                        - Go back to **Step 2** and verify your column mappings
                        - Make sure the same hierarchy columns are mapped for both SAS and SFU_v sheets
                        """)
                    
                    if has_no_match:
                        st.warning("""
                        **No Matching Values Issue:**
                        - The hierarchy values in your SAS building blocks don't match any SFU_v rows
                        - Check for data type differences (e.g., "1.0" vs "1", "US" vs " US")
                        - Verify that your filters in **Step 3** aren't excluding all matching SFU_v rows
                        - Check for leading/trailing spaces or case sensitivity issues
                        """)
                    
                    if has_pinned:
                        st.warning("""
                        **Pinned SFU_v Issue:**
                        - Your SAS data has specific SFU_v values that don't exist in your SFU_v dataset
                        - Check the SFU_v column in your SAS data for typos or outdated values
                        - Verify that your SFU_v data includes all the SKUs referenced in SAS
                        """)
                
                st.markdown("---")

    if st.button("▶ Run Split Now", type="primary"):
        with st.spinner("Running split…"):
            try:
                shipments_df = st.session_state.sheets.get("Shipments")
                fff_df = st.session_state.sheets.get("Final Fcst to Finance")
                all_sas_months_for_fy = sas_cmap.get("_months", detect_month_columns(sas_df_alloc))
                monthly_salience_by_month: dict[str, dict[str, float]] = {}
                for sfu_v, month_vals in st.session_state.get("sfu_monthly_salience", {}).items():
                    sfu_key = str(sfu_v).strip()
                    if not sfu_key or not isinstance(month_vals, dict):
                        continue
                    for month, frac in month_vals.items():
                        val = pd.to_numeric(frac, errors="coerce")
                        if pd.isna(val):
                            continue
                        monthly_salience_by_month.setdefault(str(month), {})[sfu_key] = float(val)

                output_wide, validation_df, split_trace_df = run_split(
                    sas_df=sas_df_alloc,
                    sfuv_df=sku_df,
                    salience_df=sal_df,
                    sas_months=sas_months,
                    split_level=split_level,
                    bb_split_levels=bb_split_levels,
                    bb_id_col=bb_id_col,
                    sfuv_col=hcm.get("SFU_v", hcm.get("SKU", "SKU")),
                    sas_sfuv_col=st.session_state.col_maps.get("SAS", {}).get("SFU_v"),
                    hier_col_map=hcm,
                    sas_hier_col_map=st.session_state.col_maps.get("SAS", {}),
                    exc_store=exc_store,
                    forecast_boundaries=st.session_state.get("forecast_boundaries"),
                    shipments_df=shipments_df,
                    fff_df=fff_df,
                    all_sas_months=all_sas_months_for_fy,
                    monthly_salience=monthly_salience_by_month,
                )
                lb_agg, adj_agg, new_total_df, _shared = _build_last_bop_matching_tables(
                    _get_df("Last BOP"),
                    output_wide,
                    sas_months,
                )
                gap_ccm_df, gap_sku_df, final_matched_df = _compute_bop_gap_artifacts(
                    sas_topline_df=sas_df_topline,
                    output_wide=output_wide,
                    sas_months=sas_months,
                    hcm=hcm,
                    salience_df=sal_df,
                    fff_df=fff_df,
                )

                st.session_state.output_wide = output_wide
                st.session_state.output_wide_final = None
                st.session_state.validation_df = validation_df
                st.session_state.split_trace_df = split_trace_df
                st.session_state.sas_df_split_input = sas_df_alloc.copy()
                st.session_state.sas_df_topline_scope = sas_df_topline.copy()
                st.session_state.sas_df_prior_cycle_excluded = sas_df_prior_cycle.copy()
                st.session_state.bop_cycle_active_token = active_cycle_token
                st.session_state.last_ff_reference_df = sas_df_last_ff.copy()
                st.session_state.last_ff_plan_col = plan_name_col
                st.session_state.matching_last_bop_df = lb_agg
                st.session_state.matching_split_adj_df = adj_agg
                st.session_state.matching_new_total_df = new_total_df
                st.session_state.bop_gap_ccm_df = gap_ccm_df
                st.session_state.bop_gap_sku_df = gap_sku_df
                st.session_state.bop_final_matched_sku_df = final_matched_df
                st.session_state.planner_adjustment_audit_df = None
                st.session_state.planner_residual_df = None
                st.session_state.run_settings = {
                    "run_timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
                    "split_level": split_level,
                    "basis_source": st.session_state.basis_source,
                    "basis_mode": st.session_state.basis_mode,
                    "sas_months": ", ".join(sas_months),
                    "global_exclusions": ", ".join(exc_store.global_exclusions),
                    "n_bb": len(sas_df_alloc),
                    "n_topline_rows": len(sas_df_topline),
                    "n_last_ff_refs": len(sas_df_last_ff),
                    "n_prior_cycle_excluded": len(sas_df_prior_cycle),
                    "active_bop_cycle": active_cycle_token,
                    "n_sku_rows_input": len(sku_df),
                    "n_sku_rows_output": len(output_wide),
                    "n_bop_gap_sku_rows": len(gap_sku_df),
                }
            except Exception as e:
                st.error(f"Split failed: {e}")
                import traceback
                st.code(traceback.format_exc())
                return

        st.success(f"Split complete! **{len(output_wide)}** output rows.")

        if not validation_df.empty:
            st.warning(f"**{len(validation_df)} validation issue(s)** — review before downloading.")
            st.dataframe(validation_df, width='stretch')
        else:
            st.success("No validation issues.")

        st.session_state.step = max(st.session_state.step, 7)
        st.session_state.max_step = max(st.session_state.max_step, 7)

    # Preview if already run
    if st.session_state.output_wide is not None:
        output_wide = st.session_state.output_wide
        st.subheader("Output Preview (first 20 rows)")
        st.dataframe(_add_product_description(output_wide).head(20), width='stretch')

        if st.button("Proceed to Reasonability Check →", type="primary"):
            st.session_state.step = max(st.session_state.step, 7)
            st.session_state.max_step = max(st.session_state.max_step, 7)
            st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# STEP 7 – Reasonability Check
# ──────────────────────────────────────────────────────────────────────────────
def step7_reasonability():
    st.header("Step 7 — Reasonability Check")
    st.caption(
        "Review the split output against historical actuals. "
        "Cells highlighted in **🔵 blue** are above the upper tolerance; "
        "cells in **🟡 yellow** are below the lower tolerance."
    )

    output_wide = st.session_state.output_wide_final if st.session_state.output_wide_final is not None else st.session_state.output_wide
    if output_wide is None or output_wide.empty:
        st.warning("No split output yet — complete Step 6 first.")
        return

    # ── Controls ──────────────────────────────────────────────────────────────
    col_tol1, col_tol2, col_window, col_metric = st.columns(4)
    lower_pct = col_tol1.number_input("Lower tolerance (%)", min_value=0, max_value=100, value=80, step=5, key="rb_lower")
    upper_pct = col_tol2.number_input("Upper tolerance (%)", min_value=100, max_value=500, value=120, step=5, key="rb_upper")
    baseline_window = col_window.selectbox("Baseline window", ["Past 3 months", "Past 6 months"], key="rb_window")
    baseline_metric = col_metric.selectbox("Baseline metric", ["Shipments", "Retailing"], key="rb_metric")
    enable_zscore = st.checkbox("Enable z-score outlier flag (|z| >= 3)", value=True, key="rb_zscore")

    n_baseline = 3 if "3" in baseline_window else 6

    # ── Get baseline data ─────────────────────────────────────────────────────
    baseline_df = _get_df(baseline_metric)
    if baseline_df is None:
        baseline_df = _get_df("Shipments")  # fallback
    if baseline_df is None:
        st.warning(f"No {baseline_metric} sheet loaded — cannot compute baseline.")
        return

    baseline_months_all = detect_month_columns(baseline_df)
    current_month_start_rb = pd.Timestamp.now().normalize().replace(day=1)
    past_baseline_months = [
        m for m in baseline_months_all
        if (parse_month_to_date(m) or pd.Timestamp.max) < current_month_start_rb
    ]
    selected_baseline_months = past_baseline_months[-n_baseline:] if len(past_baseline_months) >= n_baseline else past_baseline_months

    if not selected_baseline_months:
        st.warning("No past months found in baseline sheet.")
        return

    # Aggregate baseline by SFU_SFU Version
    if MONTHLY_SFU_VERSION_COL not in baseline_df.columns:
        st.warning(f"`{MONTHLY_SFU_VERSION_COL}` column not found in {baseline_metric} sheet.")
        return

    baseline_numeric = baseline_df[[MONTHLY_SFU_VERSION_COL] + [m for m in selected_baseline_months if m in baseline_df.columns]].copy()
    for m in selected_baseline_months:
        if m in baseline_numeric.columns:
            baseline_numeric[m] = pd.to_numeric(baseline_numeric[m], errors="coerce").fillna(0)

    month_cols_avail = [m for m in selected_baseline_months if m in baseline_numeric.columns]
    baseline_agg = (
        baseline_numeric.groupby(MONTHLY_SFU_VERSION_COL, dropna=False)[month_cols_avail]
        .sum()
        .reset_index()
    )
    baseline_agg["_baseline_avg"] = baseline_agg[month_cols_avail].mean(axis=1)
    baseline_avg_map = baseline_agg.set_index(MONTHLY_SFU_VERSION_COL)["_baseline_avg"].to_dict()

    # ── Build the output pivot table at SFU_SFU Version level ─────────────────
    future_months = st.session_state.sas_months_selected or []
    future_months = [m for m in future_months if (parse_month_to_date(m) or pd.Timestamp.min) >= current_month_start_rb]

    if MONTHLY_SFU_VERSION_COL not in output_wide.columns:
        st.warning(f"`{MONTHLY_SFU_VERSION_COL}` column not found in split output.")
        return

    available_future = [m for m in future_months if m in output_wide.columns]
    if not available_future:
        st.warning("No future month columns found in the split output.")
        return

    lower_mult = lower_pct / 100.0
    upper_mult = upper_pct / 100.0

    # ── Build FFF pivot table ─────────────────────────────────────────────────
    fff_df_rb = _get_df("Final Fcst to Finance")
    if fff_df_rb is not None and not fff_df_rb.empty and MONTHLY_SFU_VERSION_COL in fff_df_rb.columns:
        fff_months_avail = [m for m in available_future if m in fff_df_rb.columns]
    else:
        fff_months_avail = []

    if fff_months_avail:
        fff_agg_rb = (
            fff_df_rb[[MONTHLY_SFU_VERSION_COL] + fff_months_avail]
            .copy()
            .assign(**{m: lambda df, m=m: pd.to_numeric(df[m], errors="coerce").fillna(0.0) for m in fff_months_avail})
            .groupby(MONTHLY_SFU_VERSION_COL, dropna=False)[fff_months_avail]
            .sum()
            .reset_index()
        )
        fff_agg_rb["_baseline_avg"] = fff_agg_rb[MONTHLY_SFU_VERSION_COL].map(baseline_avg_map).fillna(0)
        pivot_df = fff_agg_rb
        display_months = fff_months_avail
    else:
        # Fallback to split output when FFF is not available
        pivot_df = (
            output_wide[[c for c in [MONTHLY_SFU_VERSION_COL] + available_future if c in output_wide.columns]]
            .groupby(MONTHLY_SFU_VERSION_COL, dropna=False)[available_future]
            .sum()
            .reset_index()
        )
        pivot_df["_baseline_avg"] = pivot_df[MONTHLY_SFU_VERSION_COL].map(baseline_avg_map).fillna(0)
        display_months = available_future

    display_pivot = pivot_df.set_index(MONTHLY_SFU_VERSION_COL)
    baseline_series = display_pivot.pop("_baseline_avg")

    # ── Add hierarchy labels (Form, Brand, Product Description) ───────────────
    label_cols: list[str] = []
    sku_ref = st.session_state.get("sku_df_filtered")
    if sku_ref is not None and not sku_ref.empty and MONTHLY_SFU_VERSION_COL in sku_ref.columns:
        for hier_col in ["Form", "Brand"]:
            if hier_col in sku_ref.columns:
                lookup = sku_ref[[MONTHLY_SFU_VERSION_COL, hier_col]].drop_duplicates(subset=MONTHLY_SFU_VERSION_COL).set_index(MONTHLY_SFU_VERSION_COL)[hier_col]
                display_pivot.insert(len(label_cols), hier_col, display_pivot.index.map(lookup).fillna(""))
                label_cols.append(hier_col)
    desc_map = _latest_product_description_map()
    if desc_map:
        display_pivot.insert(len(label_cols), "Product Description", display_pivot.index.map(desc_map).fillna(""))
        label_cols.append("Product Description")

    numeric_cols = [c for c in display_pivot.columns if c not in label_cols]

    # ── Apply conditional styling ─────────────────────────────────────────────
    def _style_cell(val, baseline):
        if pd.isna(val) or baseline == 0:
            return ""
        if not isinstance(val, (int, float)):
            return ""
        if val > baseline * upper_mult:
            return "background-color: #cce5ff; color: #003366"  # blue
        if val < baseline * lower_mult:
            return "background-color: #fff3cd; color: #664d00"  # yellow
        return ""

    def _style_row(row):
        bl = baseline_series.get(row.name, 0)
        return [_style_cell(v, bl) for v in row]

    styled = (
        display_pivot.style
        .apply(_style_row, axis=1)
        .format("{:,.1f}", subset=numeric_cols)
    )

    st.caption(
        f"Baseline: **{baseline_metric}** avg over **{len(selected_baseline_months)} months** "
        f"({', '.join(selected_baseline_months)}). "
        f"Tolerance: {lower_pct}% – {upper_pct}%."
    )

    st.dataframe(
        styled,
        width='stretch',
        height=min(600, 60 + len(display_pivot) * 35),
    )

    n_high = sum(
        1 for sfu in display_pivot.index
        for m in display_months
        if m in display_pivot.columns and (bl := baseline_series.get(sfu, 0)) > 0 and display_pivot.at[sfu, m] > bl * upper_mult
    )
    n_low = sum(
        1 for sfu in display_pivot.index
        for m in display_months
        if m in display_pivot.columns and (bl := baseline_series.get(sfu, 0)) > 0 and display_pivot.at[sfu, m] < bl * lower_mult
    )

    if n_high or n_low:
        st.warning(
            f"**{n_high}** cell(s) above {upper_pct}% of baseline (blue) · "
            f"**{n_low}** cell(s) below {lower_pct}% of baseline (yellow)"
        )
    else:
        st.success("All cells are within tolerance. ✅")

    if enable_zscore and not display_pivot.empty:
        z_outliers = 0
        split_only_cols = [m for m in display_months if m in display_pivot.columns]
        for _, row in display_pivot[split_only_cols].iterrows():
            vals = pd.to_numeric(row, errors="coerce").dropna().astype(float)
            if len(vals) < 2:
                continue
            sd = float(vals.std(ddof=0))
            if sd <= 0:
                continue
            mean = float(vals.mean())
            z = (vals - mean) / sd
            z_outliers += int((z.abs() >= 3.0).sum())
        if z_outliers > 0:
            st.warning(f"{z_outliers} cell(s) flagged by z-score outlier check (non-blocking warning).")

    # ── BOP topline matching (FFF + split + gap) ───────────────────────────
    gap_ccm_df = st.session_state.get("bop_gap_ccm_df")
    gap_sku_df = st.session_state.get("bop_gap_sku_df")
    final_matched_df = st.session_state.get("bop_final_matched_sku_df")
    if gap_ccm_df is not None and not gap_ccm_df.empty:
        st.markdown("#### BOP Topline Matching (Country + Category + Month)")
        bal_view = gap_ccm_df.copy()
        for c in ["topline", "fff", "split", "gap"]:
            if c in bal_view.columns:
                bal_view[c] = pd.to_numeric(bal_view[c], errors="coerce").fillna(0.0)
        bal_view["final_matched"] = bal_view["fff"] + bal_view["split"] + bal_view["gap"]
        bal_view["closure_delta"] = bal_view["topline"] - bal_view["final_matched"]

        tol = 1e-6
        bad_cells = int((bal_view["closure_delta"].abs() > tol).sum())
        c1, c2, c3 = st.columns(3)
        c1.metric("Country-Category-Month rows", len(bal_view))
        c2.metric("Rows balanced", int((bal_view["closure_delta"].abs() <= tol).sum()))
        c3.metric("Rows with mismatch", bad_cells)

        with st.expander("BOP Closure Table (topline vs FFF + split + gap)", expanded=(bad_cells > 0)):
            st.dataframe(
                bal_view[["Country", "SMO Category", "month", "topline", "fff", "split", "gap", "final_matched", "closure_delta"]]
                .sort_values(["Country", "SMO Category", "month"]),
                width="stretch",
                height=350,
            )

        if bad_cells > 0:
            st.warning("BOP closure mismatch detected for some Country + Category + Month rows. Review DA type 9 gap values before download.")
        else:
            st.success("BOP closure check passed: topline equals FFF + split + gap for all Country + Category + Month rows.")

        if gap_sku_df is not None and not gap_sku_df.empty:
            with st.expander("DA Type 9 Payload Preview (SKU-level gap only)", expanded=False):
                st.caption("These are the gap-only values that will be exported as Level 1 = 9. BOP Adjustment.")
                st.dataframe(gap_sku_df.head(300), width="stretch", height=300)

        if final_matched_df is not None and not final_matched_df.empty:
            with st.expander("Final Matched SKU View (FFF + split + gap)", expanded=False):
                st.dataframe(final_matched_df.head(300), width="stretch", height=300)

    # ── Last BOP + adjustments view ───────────────────────────────────────────
    last_bop_df = _get_df("Last BOP")
    lb_agg, adj_agg, new_total_df, shared_months = _build_last_bop_matching_tables(last_bop_df, output_wide, available_future)

    # ── Metric trend chart (SKU-level time series) ───────────────────────────
    st.markdown("#### SKU Metric Trend (Monthly Time Series)")
    st.caption(
        "Compare historical and current-plan signals for a selected SKU (SFU_v). "
        "Choose one SKU and any combination of metrics."
    )

    def _metric_long_from_df(metric_df: pd.DataFrame | None, metric_name: str) -> pd.DataFrame:
        if metric_df is None or metric_df.empty or MONTHLY_SFU_VERSION_COL not in metric_df.columns:
            return pd.DataFrame(columns=[MONTHLY_SFU_VERSION_COL, "month", "value", "metric"])

        month_cols = [m for m in detect_month_columns(metric_df) if m in metric_df.columns]
        if not month_cols:
            return pd.DataFrame(columns=[MONTHLY_SFU_VERSION_COL, "month", "value", "metric"])

        metric_agg = metric_df[[MONTHLY_SFU_VERSION_COL] + month_cols].copy()
        for m in month_cols:
            metric_agg[m] = pd.to_numeric(metric_agg[m], errors="coerce").fillna(0.0)
        metric_agg = (
            metric_agg
            .groupby(MONTHLY_SFU_VERSION_COL, dropna=False)[month_cols]
            .sum()
            .reset_index()
        )

        metric_long = metric_agg.melt(
            id_vars=[MONTHLY_SFU_VERSION_COL],
            value_vars=month_cols,
            var_name="month",
            value_name="value",
        )
        metric_long["metric"] = metric_name
        return metric_long

    metric_sources: dict[str, pd.DataFrame] = {}
    retailing_ts = _metric_long_from_df(_get_df("Retailing"), "Retailing")
    sellout_ts = _metric_long_from_df(_get_df("Sellout"), "Retailing")
    shipments_ts = _metric_long_from_df(_get_df("Shipments"), "Shipments")
    stat_ts = _metric_long_from_df(_get_df("Statistical Forecast"), "Stat")

    retailing_frames: list[pd.DataFrame] = []
    if not retailing_ts.empty:
        retailing_frames.append(retailing_ts)
    if not sellout_ts.empty:
        retailing_frames.append(sellout_ts)
    if retailing_frames:
        # Retailing and Sellout represent the same metric; merge into one series.
        retailing_merged = pd.concat(retailing_frames, ignore_index=True)
        retailing_merged = retailing_merged.drop_duplicates(
            subset=[MONTHLY_SFU_VERSION_COL, "month", "value"]
        )
        retailing_merged = (
            retailing_merged
            .groupby([MONTHLY_SFU_VERSION_COL, "month", "metric"], dropna=False)["value"]
            .sum()
            .reset_index()
        )
        metric_sources["Retailing"] = retailing_merged
    if not shipments_ts.empty:
        metric_sources["Shipments"] = shipments_ts
    if not stat_ts.empty:
        metric_sources["Stat"] = stat_ts

    old_fff_source_df = pd.DataFrame()
    new_fff_source_df = pd.DataFrame()
    old_new_months = list(shared_months)

    if not lb_agg.empty and not new_total_df.empty and shared_months:
        old_fff_source_df = lb_agg.copy()
        new_fff_source_df = new_total_df.copy()
    else:
        # Fallback: derive Old/New FFF from input FFF + split adjustment when Last BOP is not used.
        if fff_df_rb is not None and not fff_df_rb.empty and MONTHLY_SFU_VERSION_COL in fff_df_rb.columns:
            old_new_months = [m for m in available_future if m in fff_df_rb.columns and m in output_wide.columns]
            if old_new_months:
                old_fff_source_df = (
                    fff_df_rb[[MONTHLY_SFU_VERSION_COL] + old_new_months]
                    .copy()
                    .assign(**{m: lambda df, m=m: pd.to_numeric(df[m], errors="coerce").fillna(0.0) for m in old_new_months})
                    .groupby(MONTHLY_SFU_VERSION_COL, dropna=False)[old_new_months]
                    .sum()
                    .reset_index()
                )
                # Use Planner Final values if they were applied; otherwise use original split output
                output_for_adjustment = (
                    st.session_state.output_wide_final 
                    if st.session_state.output_wide_final is not None 
                    else output_wide
                )
                split_adj_df = (
                    output_for_adjustment[[MONTHLY_SFU_VERSION_COL] + old_new_months]
                    .copy()
                    .assign(**{m: lambda df, m=m: pd.to_numeric(df[m], errors="coerce").fillna(0.0) for m in old_new_months})
                    .groupby(MONTHLY_SFU_VERSION_COL, dropna=False)[old_new_months]
                    .sum()
                    .reset_index()
                )
                old_new_merged = old_fff_source_df.merge(
                    split_adj_df,
                    on=MONTHLY_SFU_VERSION_COL,
                    how="outer",
                    suffixes=("_old", "_adj"),
                ).fillna(0.0)

                new_fff_source_df = pd.DataFrame({MONTHLY_SFU_VERSION_COL: old_new_merged[MONTHLY_SFU_VERSION_COL]})
                for m in old_new_months:
                    new_fff_source_df[m] = old_new_merged.get(f"{m}_old", 0.0) + old_new_merged.get(f"{m}_adj", 0.0)

    if not old_fff_source_df.empty and old_new_months:
        old_fff_long = old_fff_source_df[[MONTHLY_SFU_VERSION_COL] + old_new_months].melt(
            id_vars=[MONTHLY_SFU_VERSION_COL],
            value_vars=old_new_months,
            var_name="month",
            value_name="value",
        )
        old_fff_long["metric"] = "Old FFF"
        metric_sources["Old FFF"] = old_fff_long

    if not new_fff_source_df.empty and old_new_months:
        new_fff_long = new_fff_source_df[[MONTHLY_SFU_VERSION_COL] + old_new_months].melt(
            id_vars=[MONTHLY_SFU_VERSION_COL],
            value_vars=old_new_months,
            var_name="month",
            value_name="value",
        )
        new_fff_long["metric"] = "New FFF"
        metric_sources["New FFF"] = new_fff_long

    if metric_sources:
        trend_df = pd.concat(metric_sources.values(), ignore_index=True)
        trend_df[MONTHLY_SFU_VERSION_COL] = trend_df[MONTHLY_SFU_VERSION_COL].astype(str)
        desc_map = _latest_product_description_map()

        sku_options = sorted(
            s for s in trend_df[MONTHLY_SFU_VERSION_COL].dropna().unique().tolist()
            if str(s).strip()
        )
        default_metrics = [m for m in ["Retailing", "Old FFF", "New FFF", "Shipments", "Stat"] if m in metric_sources]
        if not default_metrics:
            default_metrics = list(metric_sources.keys())
        sku_display_map = {
            s: (f"{s} - {str(desc_map.get(s, '')).strip()}" if str(desc_map.get(s, "")).strip() else s)
            for s in sku_options
        }

        trend_col1, trend_col2 = st.columns([1, 2])
        selected_sku = trend_col1.selectbox(
            "Select SKU (SFU_v)",
            sku_options,
            format_func=lambda s: sku_display_map.get(s, s),
            key="step7_trend_sku",
        )
        selected_metrics = trend_col2.multiselect(
            "Select metrics",
            options=list(metric_sources.keys()),
            default=default_metrics,
            key="step7_trend_metrics",
        )

        missing_fff_metrics = [m for m in ["Old FFF", "New FFF"] if m not in metric_sources]
        if missing_fff_metrics:
            st.caption(
                "FFF trend lines are unavailable because no usable FFF month overlap was found with the Step 3 forecast months."
            )

        if selected_metrics:
            sku_trend = trend_df[
                (trend_df[MONTHLY_SFU_VERSION_COL] == selected_sku)
                & (trend_df["metric"].isin(selected_metrics))
            ].copy()
            sku_trend["month_dt"] = sku_trend["month"].map(parse_month_to_date)
            sku_trend["month_sort"] = sku_trend["month_dt"].fillna(pd.Timestamp.max)
            sku_trend = sku_trend.sort_values(["month_sort", "month", "metric"])

            if sku_trend.empty:
                st.info("No trend data found for the selected SKU/metric combination.")
            else:
                plot_df = (
                    sku_trend
                    .pivot_table(index="month", columns="metric", values="value", aggfunc="sum")
                    .fillna(0.0)
                )

                ordered_months = (
                    sku_trend[["month", "month_sort"]]
                    .drop_duplicates()
                    .sort_values(["month_sort", "month"]) ["month"]
                    .tolist()
                )
                plot_df = plot_df.reindex(ordered_months)

                st.line_chart(plot_df, height=360)
                st.caption(
                    f"Showing {', '.join(selected_metrics)} for SKU {sku_display_map.get(selected_sku, selected_sku)}."
                )
        else:
            st.info("Select at least one metric to render the line chart.")
    else:
        st.info(
            "No metric time-series data is available yet. "
            "Load Retailing/Shipments/Statistical Forecast or Last BOP data to enable this chart."
        )

    if not new_total_df.empty:
        with st.expander("📋 Last BOP + Split Adjustments → New Total", expanded=False):
            st.caption(
                "**Last BOP FFF** is the previous-week Final Fcst to Finance – BOP per SFU_v. "
                "**Adjustment** is the split output from the current session. "
                "**New Total = Last BOP + Adjustment.**"
            )

            merged = lb_agg.merge(adj_agg, on=MONTHLY_SFU_VERSION_COL, how="outer", suffixes=("_lb", "_adj")).fillna(0.0)
            display_rows: dict[str, dict] = {}
            for _, row in merged.iterrows():
                sfuv = row[MONTHLY_SFU_VERSION_COL]
                display_rows[sfuv] = {}
                for m in shared_months:
                    lb_val = row.get(f"{m}_lb", 0.0)
                    adj_val = row.get(f"{m}_adj", 0.0)
                    display_rows[sfuv][f"{m} · Last BOP"] = lb_val
                    display_rows[sfuv][f"{m} · Adjustment"] = adj_val
                    display_rows[sfuv][f"{m} · New Total"] = lb_val + adj_val

            lb_display = pd.DataFrame.from_dict(display_rows, orient="index")
            lb_display.index.name = MONTHLY_SFU_VERSION_COL

            new_total_cols = [c for c in lb_display.columns if c.endswith("· New Total")]

            def _style_negative_new_total(v):
                if pd.isna(v):
                    return ""
                try:
                    return "background-color: #f8d7da; color: #842029" if float(v) < 0 else ""
                except Exception:
                    return ""

            st.dataframe(
                lb_display.style.map(_style_negative_new_total, subset=new_total_cols).format("{:,.1f}"),
                width="stretch",
                height=min(600, 60 + len(lb_display) * 35),
            )

            n_missing_lb = (lb_agg.set_index(MONTHLY_SFU_VERSION_COL)[shared_months].sum(axis=1) == 0).sum()
            n_missing_adj = (adj_agg.set_index(MONTHLY_SFU_VERSION_COL)[shared_months].sum(axis=1) == 0).sum()
            if n_missing_lb:
                st.caption(f"ℹ️ {n_missing_lb} SFU_v(s) have zero Last BOP volumes (not in Last BOP sheet or all-zero).")
            if n_missing_adj:
                st.caption(f"ℹ️ {n_missing_adj} SFU_v(s) have zero adjustment (not touched by current split).")

    # ── Planner Final Number Adjustment (always shown, with or without Last BOP) ──
    st.markdown("#### Planner Final Number Adjustment")
    st.caption(
        "Edit Final Number at SFU_v × month. Gap to SAS BB topline will be redistributed to remaining SKUs "
        "within each BB-month proportionally to existing split share."
    )
    
    # Use Last BOP-derived data if available; otherwise fall back to output_wide
    planner_months = shared_months if shared_months else available_future
    if not new_total_df.empty:
        planner_input = new_total_df.copy()
    else:
        # Fallback: create from output_wide when Last BOP unavailable
        planner_input = output_wide[[MONTHLY_SFU_VERSION_COL] + planner_months].copy() if output_wide is not None else pd.DataFrame()
    
    for m in planner_months:
        if m in planner_input.columns:
            planner_input[m] = pd.to_numeric(planner_input[m], errors="coerce").fillna(0.0)

    if not planner_input.empty:
        planner_edited = st.data_editor(
            planner_input,
            width="stretch",
            height=min(650, 80 + len(planner_input) * 28),
            key="planner_final_editor",
            num_rows="fixed",
        )

        if st.button("Apply Planner Adjustments and Re-split Residuals", type="secondary"):
            base_output = st.session_state.output_wide
            trace_df = st.session_state.get("split_trace_df")
            sas_input = st.session_state.get("sas_df_split_input")
            bb_id_col = st.session_state.bb_id_col or "BB_ID"
            final_output, audit_df, residual_df = _apply_planner_redistribution(
                base_output_wide=base_output,
                planner_final_df=planner_edited,
                allocation_trace_df=trace_df,
                sas_df_split_input=sas_input,
                bb_id_col=bb_id_col,
                sas_months=planner_months,
            )
            st.session_state.output_wide_final = final_output
            st.session_state.planner_adjustment_audit_df = audit_df
            st.session_state.planner_residual_df = residual_df
            if residual_df is not None and not residual_df.empty:
                st.warning(f"Planner adjustments applied with {len(residual_df)} unresolved BB-month residual(s).")
            else:
                st.success("Planner adjustments applied and BB-month residuals redistributed.")

        audit_df = st.session_state.get("planner_adjustment_audit_df")
        residual_df = st.session_state.get("planner_residual_df")
        if audit_df is not None and not audit_df.empty:
            with st.expander("Planner adjustment mapping warnings", expanded=False):
                st.dataframe(audit_df, width="stretch")
        if residual_df is not None and not residual_df.empty:
            with st.expander("Unresolved BB-month residuals", expanded=True):
                st.dataframe(residual_df, width="stretch")

    # Last FF reference panel (informational only)
    last_ff_df = st.session_state.get("last_ff_reference_df")
    if last_ff_df is not None and not last_ff_df.empty:
        with st.expander("SAS Last FF Top-line Reference (ignored for split)", expanded=False):
            plan_col = st.session_state.get("last_ff_plan_col") or "Plan Name"
            st.caption(
                "Rows detected from Plan Name containing 'Last FF'. These values are shown only as reference and "
                "are excluded from split allocation."
            )
            show_cols = [c for c in [plan_col, st.session_state.bb_id_col or "BB_ID"] if c in last_ff_df.columns]
            month_cols = [m for m in (st.session_state.sas_months_selected or []) if m in last_ff_df.columns]
            for m in month_cols:
                last_ff_df[m] = pd.to_numeric(last_ff_df[m], errors="coerce").fillna(0.0)
            total_ref = last_ff_df[month_cols].sum().sum() if month_cols else 0.0
            st.write(f"Reference rows: {len(last_ff_df)} | Aggregate reference topline (selected months): {total_ref:,.2f}")
            st.dataframe(last_ff_df[show_cols + month_cols], width="stretch")
    elif last_bop_df is None:
        st.caption("_Last BOP sheet not loaded — upload a file containing a 'Last BOP' sheet to see the Last BOP + adjustments view._")

    if st.button("Proceed to Download →", type="primary"):
        st.session_state.step = max(st.session_state.step, 8)
        st.session_state.max_step = max(st.session_state.max_step, 8)
        st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# DA Export Builder
# ──────────────────────────────────────────────────────────────────────────────
def build_da_csv_export(
    output_wide: pd.DataFrame,
    sas_df: pd.DataFrame,
    sku_df: pd.DataFrame,
    sas_months: list[str],
    bb_split_levels: dict,
    hcm: dict,
    gbb_types: dict,
    bop_gap_sku_df: pd.DataFrame | None = None,
) -> bytes:
    """Build DA CSV export in the required format.
    
    Exports:
    - Split output rows (FFF + split) with original GBB type mappings
    - Gap-only rows as DA type 9 (BOP Adjustment) when gap artifact is provided
    """
    if output_wide is None or output_wide.empty or sas_df is None or sas_df.empty:
        return b""

    has_gap_artifact = bop_gap_sku_df is not None and not bop_gap_sku_df.empty

    sfu_col = next(
        (c for c in [MONTHLY_SFU_VERSION_COL, "SFU_v", "SKU"] if c in output_wide.columns),
        None,
    )
    if not sfu_col:
        return b""

    # Keep month order aligned to the selected SAS forecast months.
    month_cols = [m for m in sas_months if m in output_wide.columns and parse_month_to_date(m) is not None]
    if not month_cols:
        return b""

    if len(month_cols) > 24:
        st.warning(
            f"⚠️ **{len(month_cols)} forecast months detected** — the DA CSV format supports only 24 periods. "
            "Using the first 24 periods."
        )
        month_cols = month_cols[:24]

    start_ts = parse_month_to_date(month_cols[0])
    end_month_ts = parse_month_to_date(month_cols[-1])
    if start_ts is None or end_month_ts is None:
        return b""

    start_ts = start_ts.replace(day=1)
    end_ts = (end_month_ts.replace(day=1) + pd.offsets.MonthEnd(0))
    n_periods = len(month_cols)

    today_mmyy = datetime.datetime.now().strftime("%m%y")
    desc_month_label = start_ts.strftime("%b-%y")

    def _norm(v: object) -> str:
        if pd.isna(v):
            return ""
        return str(v).strip()

    # Allowed Level 1 values for downstream upload template.
    allowed_level1_values = [
        "Brand Building Activities",
        "Promotions - Go to Market",
        "New Channels",
        "Initiatives",
        "Pricing Strategy",
        "Market Trend",
        "Competitive Activities",
        "Customer Inventory Strategy",
        "BOP Adjustment",
        "Balancing DA",
        "Free Goods",
    ]

    # Mapping from Level 1 value to numbered prefix (for DA export format)
    level1_number_map = {
        "Brand Building Activities": "1.",
        "Promotions - Go to Market": "2.",
        "New Channels": "3.",
        "Initiatives": "4.",
        "Pricing Strategy": "5.",
        "Market Trend": "6.",
        "Competitive Activities": "7.",
        "Customer Inventory Strategy": "8.",
        "BOP Adjustment": "9.",
        "Balancing DA": "10.",
        "Free Goods": "11.",
    }

    def _lvl1_key(v: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", _norm(v).lower())

    allowed_by_key = {_lvl1_key(v): v for v in allowed_level1_values}
    level1_aliases = {
        _lvl1_key("Promotions - Go To Market"): "Promotions - Go to Market",
        _lvl1_key("Promotion - Go to Market"): "Promotions - Go to Market",
        _lvl1_key("Promotion - Go To Market"): "Promotions - Go to Market",
        _lvl1_key("Base"): "BOP Adjustment",
    }
    unknown_level1_raw: set[str] = set()

    def _to_allowed_level1(raw_gbb: str) -> str:
        candidate = normalize_gbb_type(raw_gbb) or _norm(raw_gbb)
        key = _lvl1_key(candidate)
        if key in allowed_by_key:
            return allowed_by_key[key]
        if key in level1_aliases:
            return level1_aliases[key]
        unknown_level1_raw.add(_norm(raw_gbb) or "<blank>")
        return "BOP Adjustment"

    def _to_numbered_level1(level1_value: str) -> str:
        """Convert Level 1 value to numbered format (e.g., '1. Brand Building Activities')."""
        if not level1_value:
            return ""
        prefix = level1_number_map.get(level1_value, "")
        if prefix:
            return f"{prefix} {level1_value}"
        return level1_value

    def _level1_number_only(level1_value: str) -> str:
        """Return only the Level 1 numeric prefix (e.g., '1.')."""
        if not level1_value:
            return ""
        return level1_number_map.get(level1_value, "")

    # Monthly lookup source for Country/Category against SFU_v.
    monthly_src_role = next(
        (r for r in ["Shipments", "Statistical Forecast", "Final Fcst to Finance", "Retailing", "Consumption"] if _get_df(r) is not None),
        None,
    )
    monthly_src_df = _get_df(monthly_src_role) if monthly_src_role else None
    monthly_src_map = st.session_state.col_maps.get(monthly_src_role, {}) if monthly_src_role else {}

    monthly_sfu_col = None
    monthly_country_col = None
    monthly_category_col = None
    if monthly_src_df is not None and not monthly_src_df.empty:
        monthly_sfu_col = next(
            (c for c in [monthly_src_map.get("SFU_v"), monthly_src_map.get("SKU"), MONTHLY_SFU_VERSION_COL, "SFU_v", "SKU"] if c and c in monthly_src_df.columns),
            None,
        )
        monthly_country_col = next(
            (c for c in [monthly_src_map.get("Country"), "Country", "Reporting Country"] if c and c in monthly_src_df.columns),
            None,
        )
        monthly_category_col = next(
            (c for c in [monthly_src_map.get("SMO Category"), monthly_src_map.get("Category"), "SMO Category", "Category"] if c and c in monthly_src_df.columns),
            None,
        )

    # Bible sheet lookup for Category (authoritative source if available)
    bible_df = _get_df("Bible")
    bible_map = st.session_state.col_maps.get("Bible", {}) if bible_df is not None else {}
    bible_sfu_col = next(
        (c for c in [bible_map.get("SFU_v"), BIBLE_SFU_V_COL, "SFU_v"] if c and c in bible_df.columns),
        None,
    ) if bible_df is not None and not bible_df.empty else None
    bible_category_col = next(
        (c for c in ["Category", bible_map.get("Category"), bible_map.get("SMO Category"), "SMO Category"] if c and c in bible_df.columns),
        None,
    ) if bible_df is not None and not bible_df.empty else None

    country_out_col = hcm.get("Country") if hcm.get("Country") in output_wide.columns else None
    category_out_col = hcm.get("SMO Category") if hcm.get("SMO Category") in output_wide.columns else None
    if export_gap_only:
        country_out_col = "Country" if "Country" in export_df.columns else country_out_col
        category_out_col = "SMO Category" if "SMO Category" in export_df.columns else category_out_col

    def _lookup_country_category(sfuv: str, fallback_country: str, fallback_category: str) -> tuple[str, str]:
        out_country = fallback_country
        out_category = fallback_category

        # First, try Bible sheet (authoritative source for Category)
        if bible_df is not None and bible_sfu_col and bible_category_col:
            bible_sub = bible_df[bible_df[bible_sfu_col].astype(str).str.strip() == sfuv]
            if not bible_sub.empty:
                vals = bible_sub[bible_category_col].dropna().astype(str).str.strip()
                if not vals.empty:
                    out_category = vals.iloc[0]

        # Then, try monthly source (for Country and Category fallback)
        if monthly_src_df is None or not monthly_sfu_col:
            return out_country, out_category

        sub = monthly_src_df[monthly_src_df[monthly_sfu_col].astype(str).str.strip() == sfuv]
        if sub.empty:
            return out_country, out_category

        # If output row already has a country, keep it stable and find matching category.
        if fallback_country and monthly_country_col:
            sub_country = sub[sub[monthly_country_col].astype(str).str.strip() == fallback_country]
            if not sub_country.empty:
                sub = sub_country

        if monthly_country_col and not out_country:
            vals = sub[monthly_country_col].dropna().astype(str).str.strip()
            out_country = vals.iloc[0] if not vals.empty else ""

        if monthly_category_col and not out_category:
            vals = sub[monthly_category_col].dropna().astype(str).str.strip()
            out_category = vals.iloc[0] if not vals.empty else ""

        return out_country, out_category

    sas_cmap = st.session_state.col_maps.get("SAS", {})
    bb_id_col = st.session_state.bb_id_col or "BB_ID"
    gbb_col = sas_cmap.get(SAS_GBB_TYPE_COL)
    if not gbb_col or gbb_col not in sas_df.columns:
        gbb_col = find_gbb_type_column(sas_df.columns)

    plan_col = "Plan Name" if "Plan Name" in sas_df.columns else None
    specific_sfuv_col = sas_cmap.get("SFU_v") or sas_cmap.get("SKU")
    if specific_sfuv_col not in sas_df.columns:
        specific_sfuv_col = None

    levels = ["Country", "SMO Category", "Brand", "Sub Brand", "Form"]
    level_match_pairs: list[tuple[str, str]] = []
    for lv in levels:
        sas_lv = sas_cmap.get(lv)
        out_lv = hcm.get(lv)
        if sas_lv in sas_df.columns and out_lv in output_wide.columns:
            level_match_pairs.append((sas_lv, out_lv))

    sas_records = []
    for _, srow in sas_df.iterrows():
        bb_id = _norm(srow.get(bb_id_col, ""))
        raw_gbb = _norm(srow.get(gbb_col, "")) if gbb_col else _norm(gbb_types.get(bb_id, ""))
        gbb_level1 = _to_allowed_level1(raw_gbb)
        plan_name = _norm(srow.get(plan_col, "")) if plan_col else bb_id
        pinned_sfuv = _norm(srow.get(specific_sfuv_col, "")) if specific_sfuv_col else ""
        match_vals = {sas_col: _norm(srow.get(sas_col, "")) for sas_col, _ in level_match_pairs}
        sas_records.append(
            {
                "bb_id": bb_id,
                "gbb": gbb_level1,
                "plan": plan_name,
                "pinned": pinned_sfuv,
                "vals": match_vals,
            }
        )

    def _resolve_sas_context(out_row: pd.Series, sfuv: str) -> tuple[str, str, str]:
        out_sfuv = _norm(sfuv)
        best_score = -1
        best_bb_id = ""
        best_gbb = ""
        best_plan = ""
        out_vals = {out_col: _norm(out_row.get(out_col, "")) for _, out_col in level_match_pairs}

        for rec in sas_records:
            score = 0
            if rec["pinned"]:
                if rec["pinned"] == out_sfuv:
                    score += 100
                else:
                    continue

            for sas_col, out_col in level_match_pairs:
                left = rec["vals"].get(sas_col, "")
                right = out_vals.get(out_col, "")
                if left and right and left == right:
                    score += 1

            if score > best_score:
                best_score = score
                best_bb_id = rec["bb_id"]
                best_gbb = rec["gbb"]
                best_plan = rec["plan"]

        return best_bb_id, best_gbb, best_plan

    # FM numbers are scoped within each DA (Country + Level 1), then grouped by FM Description.
    fm_numbers_by_group: dict[tuple[str, str, str], int] = {}
    next_fm_no_by_da: dict[tuple[str, str], int] = {}
    da_rows: list[dict[str, str]] = []

    # Export split rows first with original GBB type mappings
    for _, out_row in output_wide.iterrows():
        sfuv = _norm(out_row.get(sfu_col, ""))
        if not sfuv:
            continue

        fallback_country = _norm(out_row.get(country_out_col, "")) if country_out_col else ""
        fallback_category = _norm(out_row.get(category_out_col, "")) if category_out_col else ""
        country, category = _lookup_country_category(sfuv, fallback_country, fallback_category)

        _, level1, fm_desc = _resolve_sas_context(out_row, sfuv)
        if not level1:
            level1 = "Unmapped"
        if not fm_desc:
            fm_desc = ""

        da_key = (country, level1)
        fm_key = (country, level1, fm_desc)
        if fm_key not in fm_numbers_by_group:
            next_fm_no = next_fm_no_by_da.get(da_key, 1001)
            fm_numbers_by_group[fm_key] = next_fm_no
            next_fm_no_by_da[da_key] = next_fm_no + 1
        fm_no = fm_numbers_by_group[fm_key]

        level1_number = _level1_number_only(level1)
        da_name = f"BOP {country} {today_mmyy} {level1_number}".strip()
        da_description = f"BOP Upload for the {desc_month_label}".strip()

        period_vals: dict[int, float] = {}
        for i in range(1, 25):
            if i <= len(month_cols):
                raw_v = pd.to_numeric(out_row.get(month_cols[i - 1], 0), errors="coerce")
                period_vals[i] = round(float(raw_v) if not pd.isna(raw_v) else 0.0, 2)
            else:
                period_vals[i] = 0.0

        total_volume = round(sum(period_vals.values()), 2)

        row = {
            "Action type": "NEW",
            "DA Name": da_name,
            "DA Status": "In Forecast",
            "DA Grouping ID": "",
            "DA Description": da_description,
            "DA Long Description": "",
            "FM #": str(fm_no),
            "FM Description": fm_desc,
            "Customization ID": "",
            "Level 1": _to_numbered_level1(level1),
            "Level 2": "",
            "Level 3": "",
            "Periodicity": "Monthly",
            "Category": category,
            "Country": country,
            "Org": "R",
            "Product Dimension": "SFU",
            "Product": sfuv,
            "GTIN Split": "",
            "GTIN UoM": "",
            "Customer Dimension": "Customer Group",
            "Customer Name": "ALL OTHERS",
            "Location": "",
            "Vol Preserved": "",
            "TL Flag": "",
            "TL Exception": "",
            "SFU Aggreg Flag": "",
            "GC Loc Split Flag": "",
            "Start Date": start_ts.strftime("%d-%m-%y"),
            "End Date": end_ts.strftime("%d-%m-%y"),
            "No. of periods": str(n_periods),
            "Customer Start Date": "",
            "Customer End Date": "",
            "UoM": "SU",
            "Total Volume": f"{total_volume:.2f}",
            "DA Comments": "",
            "FM Comments": "",
        }

        for i in range(1, 25):
            row[f"Tot Period {i}"] = f"{period_vals[i]:.2f}"

        da_rows.append(row)

    # Export gap rows as DA type 9 (BOP Adjustment)
    if has_gap_artifact:
        gap_sfu_col = next(
            (c for c in [MONTHLY_SFU_VERSION_COL, "SFU_v", "SKU"] if c in bop_gap_sku_df.columns),
            None,
        )
        if gap_sfu_col:
            gap_country_col = "Country" if "Country" in bop_gap_sku_df.columns else None
            gap_cat_col = "SMO Category" if "SMO Category" in bop_gap_sku_df.columns else None

            for _, gap_row in bop_gap_sku_df.iterrows():
                gap_sfuv = _norm(gap_row.get(gap_sfu_col, ""))
                if not gap_sfuv:
                    continue

                gap_country = _norm(gap_row.get(gap_country_col, "")) if gap_country_col else ""
                gap_category = _norm(gap_row.get(gap_cat_col, "")) if gap_cat_col else ""
                if not gap_country or not gap_category:
                    gap_country, gap_category = _lookup_country_category(gap_sfuv, gap_country, gap_category)

                level1_gap = "BOP Adjustment"
                fm_desc_gap = "BOP Gap Adjustment"

                da_key_gap = (gap_country, level1_gap)
                fm_key_gap = (gap_country, level1_gap, fm_desc_gap)
                if fm_key_gap not in fm_numbers_by_group:
                    next_fm_no_gap = next_fm_no_by_da.get(da_key_gap, 1001)
                    fm_numbers_by_group[fm_key_gap] = next_fm_no_gap
                    next_fm_no_by_da[da_key_gap] = next_fm_no_gap + 1
                fm_no_gap = fm_numbers_by_group[fm_key_gap]

                level1_number_gap = _level1_number_only(level1_gap)
                da_name_gap = f"BOP {gap_country} {today_mmyy} {level1_number_gap}".strip()
                da_description_gap = f"BOP Upload for the {desc_month_label}".strip()

                gap_period_vals: dict[int, float] = {}
                for i in range(1, 25):
                    if i <= len(month_cols):
                        raw_gap = pd.to_numeric(gap_row.get(month_cols[i - 1], 0), errors="coerce")
                        gap_period_vals[i] = round(float(raw_gap) if not pd.isna(raw_gap) else 0.0, 2)
                    else:
                        gap_period_vals[i] = 0.0

                gap_total_volume = round(sum(gap_period_vals.values()), 2)

                gap_row_dict = {
                    "Action type": "NEW",
                    "DA Name": da_name_gap,
                    "DA Status": "In Forecast",
                    "DA Grouping ID": "",
                    "DA Description": da_description_gap,
                    "DA Long Description": "",
                    "FM #": str(fm_no_gap),
                    "FM Description": fm_desc_gap,
                    "Customization ID": "",
                    "Level 1": _to_numbered_level1(level1_gap),
                    "Level 2": "",
                    "Level 3": "",
                    "Periodicity": "Monthly",
                    "Category": gap_category,
                    "Country": gap_country,
                    "Org": "R",
                    "Product Dimension": "SFU",
                    "Product": gap_sfuv,
                    "GTIN Split": "",
                    "GTIN UoM": "",
                    "Customer Dimension": "Customer Group",
                    "Customer Name": "ALL OTHERS",
                    "Location": "",
                    "Vol Preserved": "",
                    "TL Flag": "",
                    "TL Exception": "",
                    "SFU Aggreg Flag": "",
                    "GC Loc Split Flag": "",
                    "Start Date": start_ts.strftime("%d-%m-%y"),
                    "End Date": end_ts.strftime("%d-%m-%y"),
                    "No. of periods": str(n_periods),
                    "Customer Start Date": "",
                    "Customer End Date": "",
                    "UoM": "SU",
                    "Total Volume": f"{gap_total_volume:.2f}",
                    "DA Comments": "",
                    "FM Comments": "",
                }

                for i in range(1, 25):
                    gap_row_dict[f"Tot Period {i}"] = f"{gap_period_vals[i]:.2f}"

                da_rows.append(gap_row_dict)

    if not da_rows:
        return b""

    if unknown_level1_raw:
        sample = ", ".join(sorted(list(unknown_level1_raw))[:5])
        st.warning(
            "⚠️ Some GBB Type values were not in the allowed Level 1 list and were exported as 'BOP Adjustment'. "
            f"Examples: {sample}"
        )

    da_df = pd.DataFrame(da_rows)

    # Ensure column order matches expected schema.
    column_order = [
        "Action type", "DA Name", "DA Status", "DA Grouping ID", "DA Description",
        "DA Long Description", "FM #", "FM Description", "Customization ID",
        "Level 1", "Level 2", "Level 3", "Periodicity", "Category", "Country", "Org",
        "Product Dimension", "Product", "GTIN Split", "GTIN UoM", "Customer Dimension",
        "Customer Name", "Location", "Vol Preserved", "TL Flag", "TL Exception",
        "SFU Aggreg Flag", "GC Loc Split Flag", "Start Date", "End Date", "No. of periods",
        "Customer Start Date", "Customer End Date", "UoM", "Total Volume",
    ] + [f"Tot Period {i}" for i in range(1, 25)] + [
        "DA Comments", "FM Comments"
    ]
    
    # Reorder and ensure all columns exist.
    for col in column_order:
        if col not in da_df.columns:
            da_df[col] = ""

    da_df = da_df[column_order]

    csv_string = da_df.to_csv(index=False)
    return csv_string.encode("utf-8")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 8 – Download
# ──────────────────────────────────────────────────────────────────────────────
def step7_download():
    st.header("Step 8 — Download Results")

    if st.session_state.output_wide is None and st.session_state.output_wide_final is None:
        st.warning("No output yet — complete Step 6.")
        return

    output_wide = st.session_state.output_wide_final if st.session_state.output_wide_final is not None else st.session_state.output_wide
    sal_df = st.session_state.salience_df if st.session_state.salience_df is not None else pd.DataFrame()
    exc_log = st.session_state.exc_store.log_as_df()
    val_df = st.session_state.validation_df if st.session_state.validation_df is not None else pd.DataFrame()
    run_settings = st.session_state.run_settings
    planner_residual_df = st.session_state.get("planner_residual_df")

    col1, col2 = st.columns(2)
    col1.metric("Output SFU_v rows", len(output_wide))
    col2.metric("Validation issues", len(val_df))
    if planner_residual_df is not None and not planner_residual_df.empty:
        st.warning(f"Planner redistribution has {len(planner_residual_df)} unresolved BB-month residual(s).")

    with st.spinner("Building Excel file…"):
        # Export configuration: apply rounding and validation
        # Identify numeric columns for export validation (exclude identifiers)
        numeric_cols = output_wide.select_dtypes(include=['number']).columns
        # Define value bounds: forecast volumes should be >= 0
        value_bounds = {col: (0.0, None) for col in numeric_cols if col not in ['horizon']}
        
        xlsx_bytes = build_excel_output(
            output_wide=output_wide,
            salience_df=sal_df,
            exception_log=exc_log,
            validation_df=val_df,
            run_settings=run_settings,
            round_to_decimals=2,
            critical_cols=None,  # Data already validated in split engine
            value_bounds=value_bounds,
        )

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    st.download_button(
        label="📥 Download BOP_Split_Output.xlsx",
        data=xlsx_bytes,
        file_name=f"BOP_Split_Output_{ts}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        width='stretch',
    )

    # ── DA CSV Export ──────────────────────────────────────────────────────────
    st.markdown("#### DA CSV Export")
    if st.button("Generate DA CSV"):
        with st.spinner("Building DA CSV…"):
            sas_df = st.session_state.sas_df_filtered
            sku_df = st.session_state.sku_df_filtered
            sas_months = st.session_state.sas_months_selected or []
            bb_split_levels = st.session_state.get("bb_split_levels", {})
            hcm = hier_col_map_from_state()
            gbb_types = st.session_state.get("gbb_types", {})
            
            da_csv_bytes = build_da_csv_export(
                output_wide=output_wide,
                sas_df=sas_df,
                sku_df=sku_df,
                sas_months=sas_months,
                bb_split_levels=bb_split_levels,
                hcm=hcm,
                gbb_types=gbb_types,
                bop_gap_sku_df=st.session_state.get("bop_gap_sku_df"),
            )

        if da_csv_bytes:
            st.session_state["da_csv_bytes"] = da_csv_bytes
            st.success("DA CSV generated successfully.")
        else:
            st.session_state.pop("da_csv_bytes", None)
            st.error("Failed to generate DA CSV — no data available.")

    if st.session_state.get("da_csv_bytes"):
        st.download_button(
            label="📥 Download DA_Export.csv",
            data=st.session_state["da_csv_bytes"],
            file_name=f"DA_Export_{ts}.csv",
            mime="text/csv",
            width='stretch',
        )

    st.divider()
    tab_out, tab_exc, tab_val = st.tabs(["Split Forecast", "Exception Log", "Validation"])

    with tab_out:
        st.dataframe(_add_product_description(output_wide), width='stretch', height=400)
    with tab_exc:
        if exc_log.empty:
            st.info("No exceptions logged.")
        else:
            st.dataframe(exc_log, width='stretch')
    with tab_val:
        if val_df.empty:
            st.success("No validation issues.")
        else:
            st.dataframe(val_df, width='stretch')


# ──────────────────────────────────────────────────────────────────────────────
# Main router
# ──────────────────────────────────────────────────────────────────────────────
def main():
    step = st.session_state.step

    if step == 1:
        step1_upload()
    elif step == 2:
        step2_columns()
    elif step == 3:
        step3_filters()
    elif step == 4:
        step4_exclusions()
    elif step == 5:
        step4_salience()
    elif step == 6:
        step6_run()
    elif step == 7:
        step7_reasonability()
    elif step >= 8:
        step7_download()

    # Auto-save session state so a browser refresh restores progress
    _save_session(_sid)


if __name__ == "__main__":
    main()
