"""BOP Splitter – Streamlit application."""
from __future__ import annotations

import datetime
import hashlib
import json
import re
from copy import deepcopy

import numpy as np
import pandas as pd
import streamlit as st

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
    _match_gbb_type,
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
        "sfu_basis_sources": {},      # SFU_SFU Version -> basis config (source, mode, selected)
        "sfu_manual_months": [],      # Global manual month selector for SFU basis (legacy)
        "sfu_specific_months": {},    # SFU_SFU Version -> list[str] of manually chosen months
        "sfu_remarks": {},            # SFU_SFU Version -> user remarks string
        "filters": {},
        "step": 1,
        "max_step": 1,
        "_loaded_file_id": None,
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
    "7. Reasonability Check",
    "8. Download",
]

with st.sidebar:
    st.title("🔀 BOP Splitter")
    st.caption("Building Block → SFU_v Forecast Splitter")
    st.divider()
    for i, label in enumerate(STEPS, 1):
        icon = "▶️" if i == st.session_state.step else ("✅" if i <= st.session_state.max_step else "⬜")
        st.markdown(f"{icon} {label}")
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
                    st.session_state.max_step = 7
                    st.success("Profile loaded! Upload your new Excel file and all previous settings will be pre-filled.")
                    with st.expander("What was loaded", expanded=False):
                        st.text("\n".join(load_notes))
                    st.rerun()
                except ValueError as _ve:
                    st.error(f"Invalid profile file: {_ve}")
                except Exception as _le:
                    st.error(f"Could not apply profile: {_le}")

    st.divider()
    if st.button("↺ Reset All", width='stretch'):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
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
LOGICAL_HIER = HIERARCHY_LEVELS  # Ctry, SMO Category, Brand, Sub Brand, Form
ALL_LOGICAL = LOGICAL_HIER + ["SFU_v"]


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
    """Merge all SFU_v sheets into a single deduplicated DataFrame.
    
    Priority order:
    1. Bible sheet (if available) - authoritative SKU/SFU hierarchy mapping
    2. Other SKU sheets (Shipments, Consumption, etc.)
    """
    frames = []
    hier_cols_actual = []
    
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
        sku_actual = cmap.get("SKU") or cmap.get("SFU_v")
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
    
    for _, row in stat_db_df.iterrows():
        sfu = str(row.get(sfu_col, "")).strip()
        version = str(row.get(version_col, "")).strip()
        
        if not sfu or not version:
            continue
        
        # Create composite SFU_v key (matching MONTHLY_SFU_VERSION_COL format)
        sfu_v = f"{sfu}_{version}"
        
        # Parse dates (format: dd-mm-yy or dd/mm/yy)
        from_date_raw = row.get(from_col)
        to_date_raw = row.get(to_col)
        
        from_date = None
        to_date = None
        
        # Try parsing Forecast From
        if pd.notna(from_date_raw):
            if isinstance(from_date_raw, pd.Timestamp):
                from_date = from_date_raw
            else:
                try:
                    # Try dd-mm-yy format
                    from_date = pd.to_datetime(str(from_date_raw), format="%d-%m-%y", errors="coerce")
                    if pd.isna(from_date):
                        # Try dd/mm/yy format
                        from_date = pd.to_datetime(str(from_date_raw), format="%d/%m/%y", errors="coerce")
                    if pd.isna(from_date):
                        # Try standard parsing as fallback
                        from_date = pd.to_datetime(str(from_date_raw), errors="coerce")
                except:
                    pass
        
        # Try parsing Forecast To
        if pd.notna(to_date_raw):
            if isinstance(to_date_raw, pd.Timestamp):
                to_date = to_date_raw
            else:
                try:
                    # Try dd-mm-yy format
                    to_date = pd.to_datetime(str(to_date_raw), format="%d-%m-%y", errors="coerce")
                    if pd.isna(to_date):
                        # Try dd/mm/yy format
                        to_date = pd.to_datetime(str(to_date_raw), format="%d/%m/%y", errors="coerce")
                    if pd.isna(to_date):
                        # Try standard parsing as fallback
                        to_date = pd.to_datetime(str(to_date_raw), errors="coerce")
                except:
                    pass
        
        # Normalize to start of month for comparison
        if pd.notna(from_date):
            from_date = from_date.replace(day=1).normalize()
        else:
            from_date = None
            
        if pd.notna(to_date):
            to_date = to_date.replace(day=1).normalize()
        else:
            to_date = None
        
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

    # Check for Bible, Stat_DB, and Sellout sheets
    has_bible = "Bible" in st.session_state.sheet_map
    has_stat_db = "Stat_DB" in st.session_state.sheet_map
    has_sellout = "Sellout" in st.session_state.sheet_map
    
    if has_bible:
        st.info(
            "📚 **Bible sheet detected!** This sheet will be used as the authoritative source for "
            "SFU hierarchy mappings (SMO Category, Brand, Sub Brand, Form)."
        )
    
    if has_stat_db:
        st.info(
            "📅 **Stat DB export detected!** This sheet will be used to enforce forecast date boundaries. "
            "Forecasts will only be created within the date range specified for each SFU."
        )
    
    if has_sellout:
        st.info(
            "📊 **Sellout sheet detected!** This sheet can be used as a historical basis for salience "
            "weighting in Step 4."
        )

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
            n_sku = mdf[MONTHLY_SFU_V_COL].nunique() if MONTHLY_SFU_V_COL in mdf.columns else "?"
            m_cols[min(i, 3)].metric(measure, f"{len(mdf):,} rows")
            m_cols[min(i, 3)].caption(f"{n_sku:,} unique SFU_vs")

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
            "Monthly column → renamed to SAS name": MONTHLY_SFU_V_COL,
        })

        # GBB Type — detect which variant is present in the SAS sheet
        gbb_col_found = next(
            (c for c in SAS_GBB_TYPE_VARIANTS if c in sas_df.columns), None
        )
        mapping_rows.append({
            "Logical Level": "GBB Type",
            "SAS column": gbb_col_found if gbb_col_found else "⚠️ not found in SAS sheet",
            "Monthly column → renamed to SAS name": "— (SAS only)",
        })

        st.dataframe(pd.DataFrame(mapping_rows), width='stretch', hide_index=True)

        if gbb_col_found:
            st.success(
                f"✅ **GBB Type** column detected in SAS sheet as `{gbb_col_found}` — "
                "Split Level and Action will be auto-derived from it in Step 3."
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
            gbb_preview_col = next(
                (c for c in SAS_GBB_TYPE_VARIANTS if c in sas_df.columns), None
            )
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
            sel = cols[i % 3].selectbox(f"**{role}**", none_opt, index=idx, key=f"sheet_map_{role}")
            if sel != "— none —":
                new_map[role] = sel

        if new_map:
            preview_role = st.selectbox("Preview sheet", list(new_map.keys()), key="preview_role")
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
                f"{role}{required_tag}",
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
                    with st.spinner(f"Fetching **{role}** from `{tbl}`…"):
                        try:
                            df = fetch_databricks_table(
                                db_host, db_http, db_token, tbl.strip(), int(row_limit)
                            )
                            loaded_sheets[role] = df
                            st.success(f"✓ **{role}**: {len(df):,} rows loaded")
                        except ImportError as exc:
                            st.error(str(exc))
                            has_error = True
                            break
                        except Exception as exc:
                            st.error(f"Failed to load **{role}** from `{tbl}`: {exc}")
                            has_error = True

                if not has_error and loaded_sheets:
                    # Store data and auto-map roles (role name = "sheet" name)
                    st.session_state.sheets = loaded_sheets
                    st.session_state.sheet_map = {r: r for r in loaded_sheets}
                    st.session_state["_loaded_file_id"] = None
                    st.session_state.data_source = "databricks"
                    st.session_state.step = max(st.session_state.step, 2)
                    st.session_state.max_step = max(st.session_state.max_step, 2)
                    st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# Mapping profile helpers (save / load column configuration as JSON)
# ──────────────────────────────────────────────────────────────────────────────
def _export_profile() -> dict:
    """Serialise the current sheet + column mappings to a JSON-safe dict.

    Month columns are intentionally excluded — they are auto-detected from the
    data and vary across files.  Everything else (sheet names, hierarchy columns,
    SFU_v column, BB_ID column) is included.
    """
    return {
        "version": 1,
        "created": datetime.datetime.now().isoformat(timespec="seconds"),
        "sheet_map": dict(st.session_state.sheet_map),
        "col_maps": {
            role: {k: v for k, v in cmap.items() if not k.startswith("_") and v}
            for role, cmap in st.session_state.col_maps.items()
        },
    }


def _fuzzy_match(name: str, candidates: list[str]) -> str | None:
    """Return the first candidate that is a sub/super-string of *name* (case-insensitive)."""
    nl = name.lower()
    for c in candidates:
        cl = c.lower()
        if nl in cl or cl in nl:
            return c
    return None


def _apply_profile(profile: dict) -> tuple[int, list[str]]:
    """Apply a loaded profile dict to session state.

    Matches profile column names against columns that are actually present in the
    currently-loaded sheets.  Near-miss names are resolved via fuzzy matching.

    Returns:
        (number of mappings applied, list of warning messages)
    """
    applied = 0
    warnings: list[str] = []

    # --- Sheet map -----------------------------------------------------------
    available_sheets = list(st.session_state.sheets.keys())
    for role, sheet_name in profile.get("sheet_map", {}).items():
        if sheet_name in available_sheets:
            target = sheet_name
        else:
            target = _fuzzy_match(sheet_name, available_sheets)
            if target:
                warnings.append(
                    f"Sheet **{sheet_name}** not found → using **{target}** for *{role}*."
                )
            else:
                warnings.append(f"Sheet **{sheet_name}** (*{role}*) not found in workbook — skipped.")
                continue
        st.session_state.sheet_map[role] = target
        # Push value into widget state so the selectbox renders the right option
        st.session_state[f"sheet_map_{role}"] = target
        applied += 1

    # --- Column maps ---------------------------------------------------------
    for role, cmap_profile in profile.get("col_maps", {}).items():
        role_df = _get_df(role)
        available_cols = list(role_df.columns) if role_df is not None else []
        if role not in st.session_state.col_maps:
            st.session_state.col_maps[role] = {}

        for logical, actual in cmap_profile.items():
            if logical.startswith("_"):
                continue  # skip internal keys (_months, etc.)
            if actual in available_cols:
                target_col = actual
            else:
                target_col = _fuzzy_match(actual, available_cols)
                if target_col:
                    warnings.append(
                        f"Column **{actual}** not found in *{role}* → using **{target_col}** for *{logical}*."
                    )
                else:
                    warnings.append(
                        f"Column **{actual}** (*{role} → {logical}*) not found — skipped."
                    )
                    continue
            st.session_state.col_maps[role][logical] = target_col
            # Also update widget state so selectboxes render the correct option
            widget_key = f"col_{role}_{logical}" if logical != "SKU" else f"col_{role}_SKU"
            st.session_state[widget_key] = target_col
            applied += 1

    return applied, warnings


# ──────────────────────────────────────────────────────────────────────────────
# STEP 2 – Column Mapping
# ──────────────────────────────────────────────────────────────────────────────
def step2_columns():
    st.header("Step 2 — Column Mapping")

    # ── Mapping Profile ───────────────────────────────────────────────────────
    with st.expander("💾 Mapping Profile — save or load column configuration", expanded=False):
        st.caption(
            "A **Mapping Profile** stores your sheet-to-role and column-to-logical-field "
            "assignments as a JSON file. Save once, reload on any future run to skip manual "
            "selection. Month columns are excluded (they are auto-detected each time)."
        )
        col_load, col_save = st.columns(2)

        with col_load:
            st.markdown("**Load a saved profile**")
            profile_file = st.file_uploader(
                "Upload profile (.json)", type=["json"], key="profile_upload",
                help="Select a profile JSON previously exported from this app.",
            )
            if profile_file is not None:
                try:
                    profile_data = json.loads(profile_file.read())
                    if profile_data.get("version") != 1:
                        st.warning("Unrecognised profile version — attempting to apply anyway.")
                    n_applied, warns = _apply_profile(profile_data)
                    for w in warns:
                        st.warning(w)
                    if n_applied:
                        st.success(
                            f"✅ Profile applied — **{n_applied}** mapping(s) pre-filled. "
                            "Scroll down to review, then click **Confirm**."
                        )
                        st.rerun()
                    else:
                        st.error("No mappings could be applied. Check that the profile matches this workbook's column names.")
                except Exception as exc:
                    st.error(f"Failed to load profile: {exc}")

        with col_save:
            st.markdown("**Save the current profile**")
            if st.session_state.col_maps or st.session_state.sheet_map:
                profile_json = json.dumps(_export_profile(), indent=2)
                st.download_button(
                    label="⬇ Download bop_profile.json",
                    data=profile_json,
                    file_name="bop_profile.json",
                    mime="application/json",
                    help="Save your current sheet and column mappings to reuse on future runs.",
                )
                with st.expander("Preview profile JSON", expanded=False):
                    st.code(profile_json, language="json")
            else:
                st.info("Configure at least one column mapping below, then save the profile.")

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

        with st.expander(f"**{role}** — {st.session_state.sheet_map[role]}", expanded=(role == "SAS")):
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
                    for _m in detected_months:
                        _ts = parse_month_to_date(_m)
                        if _ts is not None and _ts > current_month_start:
                            _sas_default.append(_m)
                    _sas_month_default = _sas_default  # strictly future months only
                else:
                    _sas_month_default = detected_months
            else:
                _sas_month_default = cmap.get("_months", detected_months)
            
            # Show month columns selector (skip for Stat_DB and Bible)
            if role not in ("Stat_DB", "Bible") and detected_months:
                confirmed_months = st.multiselect(
                    "Month columns",
                    cols_all,
                    default=[m for m in _sas_month_default if m in cols_all],
                    key=f"months_{role}",
                )
                cmap["_months"] = confirmed_months
            else:
                cmap["_months"] = []

            # Hierarchy columns (skip for Stat_DB - it doesn't use hierarchy)
            if role != "Stat_DB":
                hier_ui = st.columns(len(LOGICAL_HIER))
                for j, lh in enumerate(LOGICAL_HIER):
                    if lh == "Ctry":
                        guess = cmap.get(lh) or _guess_col(cols_all, "Ctry", "Country")
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
                    # Sellout sheet: prefer explicit SFU_v column if present
                    sku_guess = (
                        cmap.get("SFU_v")
                        or _guess_col(cols_all, "SFU_v", MONTHLY_SFU_VERSION_COL, MONTHLY_SFU_V_COL, "Salience SFU", "Material", "SKU")
                    )
                elif role in ("Shipments", "Statistical Forecast", "Final Fcst to Finance", "Stat", "FFF"):
                    # For these BOP roles, prioritize MONTHLY_SFU_VERSION_COL ("SFU_SFU Version") as default
                    sku_guess = cmap.get("SKU") or (MONTHLY_SFU_VERSION_COL if MONTHLY_SFU_VERSION_COL in cols_all else _guess_col(cols_all, "SFU_SFU Version", "APO Product", "SFU_v", "SKU"))
                else:
                    sku_guess = cmap.get("SKU") or cmap.get("SFU_v") or _guess_col(cols_all, "APO Product", "SFU_v", "SKU")
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
                auto_gbb = cmap.get("GBB Type") or next(
                    (c for c in SAS_GBB_TYPE_VARIANTS if c in cols_all), None
                )
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

    # ── Groupby key: hierarchy + SFU_SFU Version + APO Product ────────────────
    # Use the first available measure sheet to discover columns
    sample_df = st.session_state.sheets.get(MONTHLY_MEASURES[0], pd.DataFrame())
    hier_cols = [c for c in ["Ctry", "SMO Category", "Brand", "Sub Brand", "Form"]
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

        available = [m for m in month_subset if m in df.columns]
        if not available:
            continue

        # Aggregate at SFU_SFU Version level (not APO Product) across month columns
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
            # Include all non-month, non-numeric metadata columns in the groupby
            # so they are preserved after aggregation (e.g. GBB Type, Entry Type, Plan Name)
            meta_cols = [
                c for c in sas_df.columns
                if c not in month_col_set
                and c not in group_keys
                and sas_df[c].dtype == object  # string/categorical metadata
            ]
            full_group_keys = group_keys + meta_cols

            # Ensure all month columns are numeric for summation
            sas_df[month_cols] = sas_df[month_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
            # Group and sum — metadata columns are part of the key so they survive
            sas_df = sas_df.groupby(full_group_keys, as_index=False, dropna=False)[month_cols].sum()
            # C-903: SAS values are stored /1000 in the source file — multiply back to real SU volume
            sas_df[month_cols] = sas_df[month_cols] * 1000
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
    def _default_forecast_months(months: list[str]) -> list[str]:
        next_month = (pd.Timestamp.now().normalize().replace(day=1) + pd.DateOffset(months=1))
        july_of_year = next_month.replace(month=7)
        # If next month is already past July, target July of next year
        if next_month.month > 7:
            july_of_year = july_of_year + pd.DateOffset(years=1)
        result = []
        for m in months:
            try:
                ts = pd.to_datetime(m, format="%b-%y")
                if next_month <= ts <= july_of_year:
                    result.append(m)
            except Exception:
                pass
        return result or months  # fall back to all if nothing matched

    prev_forecast = st.session_state.sas_forecast_months_selected or _default_forecast_months(future_months)
    selected_forecast = st.multiselect(
        "Select forecast months to split:",
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
        "- **Split Level**: Choose granularity (Ctry, SMO Category, Brand, Sub Brand, Form, or SFU_v)\n"
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
        _gbb_col = next(
            (c for c in SAS_GBB_TYPE_VARIANTS if c in sas_df.columns),
            None,
        )

    if _gbb_col:
        st.caption(
            f"**GBB Type** is pulled directly from the SAS sheet (column: `{_gbb_col}`) and is read-only. "
            "It drives the default **Split Level** and **Action** for each Building Block. "
            "You can still override **Split Level** for any row. "
            "Rows flagged as *exceptions* or *ignore* will be highlighted in Step 5."
        )
    else:
        st.caption(
            "No GBB Type column was found in the SAS sheet — you can manually assign a **GBB Type** per row. "
            "GBB Type drives the default **Split Level** and **Action** for each Building Block. "
            "Rows flagged as *exceptions* or *ignore* will be highlighted in Step 5."
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
        canonical = _match_gbb_type(gbb_type)
        rule = GBB_TYPE_RULES.get(canonical) if canonical else None
        if rule and not rule["user_defined"]:
            return rule["split_level"]
        # user_defined or unknown type → fall through to SFU_v check
        bb_rows = sas_df[sas_df[bb_id_col] == bid]
        return _auto_split_level(bb_rows)

    def _gbb_action(gbb_type: str) -> str:
        canonical = _match_gbb_type(gbb_type)
        rule = GBB_TYPE_RULES.get(canonical) if canonical else None
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

        # Rename GBB Type col to canonical name for consistent handling
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
            use_container_width=True,
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
                    "Canonical Name": k,
                    "Default Split Level": v["split_level"],
                    "Action": v["action"],
                    "Notes": v["description"],
                }
                for k, v in GBB_TYPE_RULES.items()
            ]
            st.caption(
                "The **GBB Type** in your SAS file may include a leading number prefix "
                "(e.g. `1. Brand Building Activities`). The tool automatically strips the prefix "
                "and matches to the canonical name to pick the correct Split Level and Action."
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
            new_bb_split_levels = dict(zip(edited_bb[bb_id_col], edited_bb["Split Level"]))
            st.session_state.bb_split_levels = new_bb_split_levels
            # Use the most common level as the global default (for display purposes)
            if new_bb_split_levels:
                from collections import Counter
                st.session_state.split_level = Counter(new_bb_split_levels.values()).most_common(1)[0][0]

            # Persist GBB action flags so Step 5 can surface them
            gbb_actions: dict[str, str] = {}
            gbb_types: dict[str, str] = {}
            if "Action" in edited_bb.columns and "GBB Type" in edited_bb.columns:
                for _, row in edited_bb.iterrows():
                    bid = row[bb_id_col]
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

        # C-302: Only retain SFU_vs that have at least one non-zero FFF value in future months
        fff_df = _get_df("Final Fcst to Finance")
        if fff_df is not None and not fff_df.empty and MONTHLY_SFU_VERSION_COL in fff_df.columns:
            fff_months = detect_month_columns(fff_df)
            current_month_start_filter = pd.Timestamp.now().normalize().replace(day=1)
            future_fff_months = [
                m for m in fff_months
                if (parse_month_to_date(m) or pd.Timestamp.min) >= current_month_start_filter
            ]
            if future_fff_months:
                fff_numeric = fff_df[future_fff_months].apply(pd.to_numeric, errors="coerce").fillna(0)
                fff_df = fff_df.copy()
                fff_df["_has_fff"] = fff_numeric.sum(axis=1) > 0
                sfus_with_fff = set(
                    fff_df.loc[fff_df["_has_fff"], MONTHLY_SFU_VERSION_COL].dropna().astype(str).tolist()
                )
                if sfus_with_fff and MONTHLY_SFU_VERSION_COL in sku_out.columns:
                    original_count = len(sku_out[MONTHLY_SFU_VERSION_COL].unique())
                    sku_out = sku_out[sku_out[MONTHLY_SFU_VERSION_COL].astype(str).isin(sfus_with_fff)].reset_index(drop=True)
                    filtered_count = len(sku_out[MONTHLY_SFU_VERSION_COL].unique())
                    if filtered_count < original_count:
                        st.info(
                            f"ℹ️ Filtered to **{filtered_count}** SFU_vs with future FFF "
                            f"(removed {original_count - filtered_count} with no future Final Forecast to Finance)."
                        )

        st.session_state.sas_df_filtered = sas_out
        st.session_state.sku_df_filtered = sku_out

        # Load forecast date boundaries from Stat_DB (if available)
        forecast_boundaries = _get_forecast_date_boundaries()
        st.session_state.forecast_boundaries = forecast_boundaries
        if forecast_boundaries:
            st.info(
                f"📅 Loaded forecast date boundaries for **{len(forecast_boundaries)}** SFU_v(s) from Stat DB export. "
                "Splits will respect Forecast From/To dates."
            )

        # Compute equal salience per unique split level used across BBs
        hcm = hier_col_map_from_state()
        sku_col = hcm.get("SKU", "SKU")
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
    template_df = pd.DataFrame(columns=[sku_col] + template_months)
    # Add a couple of example rows
    for i in range(2):
        row = {sku_col: f"1234567{i}"}
        for m in template_months:
            row[m] = 0.0
        template_df = pd.concat([template_df, pd.DataFrame([row])], ignore_index=True)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        template_df.to_excel(writer, index=False, sheet_name="Exclusions")
    return buf.getvalue()


def _compute_bop_salience(sfu_configs: dict) -> pd.DataFrame | None:
    """
    Compute BOP salience at SFU_SFU Version level.

    sfu_configs: dict {SFU_version: {"source": str, "mode": str, "selected": list[str]}}
    """
    sku_df = st.session_state.sku_df_filtered
    if sku_df is None or sku_df.empty:
        st.error("No SFU_v data — complete Step 3 first.")
        return None

    hcm = hier_col_map_from_state()
    split_level = st.session_state.split_level
    group_keys_logical = [k for k in SPLIT_KEYS[split_level] if k != "SKU"]
    group_keys = [hcm.get(k, k) for k in group_keys_logical]
    valid_group_keys = [k for k in group_keys if k in sku_df.columns]
    sku_col = hcm.get("SKU", MONTHLY_SFU_V_COL)

    current_month_start = pd.Timestamp.now().normalize().replace(day=1)

    # Step 1: build {(group_vals..., SFU_version): total_SFU_basis}
    sfu_basis: dict[tuple, float] = {}

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
        src_df = _get_df(src_role)
        if src_df is None:
            continue

        # Ensure SFU_SFU Version column exists; for Sellout use the mapped SFU_v column
        if src_role == "Sellout":
            cmap_sellout = st.session_state.col_maps.get("Sellout", {})
            sfu_col = cmap_sellout.get("SFU_v") or cmap_sellout.get("SKU")
            if not sfu_col or sfu_col not in src_df.columns:
                continue
            src_df = src_df.copy()
            src_df[MONTHLY_SFU_VERSION_COL] = src_df[sfu_col].astype(str)

            # Enrich Sellout with hierarchy columns from SKU/Bible data when missing
            missing_group_cols = [k for k in valid_group_keys if k not in src_df.columns]
            if missing_group_cols and sku_df is not None and MONTHLY_SFU_VERSION_COL in sku_df.columns:
                hier_lookup = (
                    sku_df[[MONTHLY_SFU_VERSION_COL] + [k for k in valid_group_keys if k in sku_df.columns]]
                    .drop_duplicates()
                )
                src_df = src_df.merge(hier_lookup, on=MONTHLY_SFU_VERSION_COL, how="left")
        elif MONTHLY_SFU_VERSION_COL not in src_df.columns:
            continue

        cmap = st.session_state.col_maps.get(src_role, {})
        all_months = cmap.get("_months", detect_month_columns(src_df))
        
        # Determine basis columns based on mode
        if mode == "selected":
            basis_cols = [c for c in selected_m if c in src_df.columns]
        else:
            # Filter to past months first
            past_m = []
            for m in all_months:
                try:
                    if pd.to_datetime(m, format="%b-%y") < current_month_start:
                        past_m.append(m)
                except:
                    pass
            
            if mode.startswith("last_"):
                n = int(mode.split("_")[1])
                basis_cols = past_m[-n:] if len(past_m) >= n else past_m
            else:
                basis_cols = past_m
        
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

    # Step 2: count APO Products per (group + SFU) in sku_df
    sfu_n: dict[tuple, int] = {}
    if sku_col in sku_df.columns and MONTHLY_SFU_VERSION_COL in sku_df.columns:
        agg_keys_sku = [k for k in valid_group_keys if k in sku_df.columns] + [MONTHLY_SFU_VERSION_COL]
        for grp_vals, grp in sku_df.groupby(agg_keys_sku, sort=False, dropna=False):
            if not isinstance(grp_vals, tuple):
                grp_vals = (grp_vals,)
            sfu_n[grp_vals] = max(1, grp[sku_col].nunique())

    n_gk = len(valid_group_keys)

    # Step 3: per split group → normalise SFU bases, compute per-APO salience
    rows = []
    for grp_vals, grp in sku_df.groupby(valid_group_keys, sort=False, dropna=False):
        if not isinstance(grp_vals, tuple):
            grp_vals = (grp_vals,)
        group_id = dict(zip(valid_group_keys, grp_vals))
        # Total SFU basis for this split group (all SFU keys that start with grp_vals)
        total_group_basis = sum(v for k, v in sfu_basis.items() if k[:n_gk] == grp_vals)

        for _, row in grp.iterrows():
            sku_val = str(row.get(sku_col, "")) if sku_col in grp.columns else ""
            sfu_ver = row.get(MONTHLY_SFU_VERSION_COL, "") if MONTHLY_SFU_VERSION_COL in grp.columns else ""
            sfu_key = grp_vals + (sfu_ver,)
            sfu_b = sfu_basis.get(sfu_key, 0.0)
            n = sfu_n.get(sfu_key, 1)

            if total_group_basis > 0 and sfu_b > 0:
                sal = sfu_b / (total_group_basis * n)
                flag = "computed"
            else:
                sal = 0.0
                flag = "blocked"

            rows.append({**group_id, MONTHLY_SFU_VERSION_COL: sfu_ver, sku_col: sku_val, "basis": sfu_b, "salience": sal, "flag": flag})

    return pd.DataFrame(rows) if rows else None


# ──────────────────────────────────────────────────────────────────────────────
# STEP 4 – Basis & Salience
# ──────────────────────────────────────────────────────────────────────────────
def step4_salience():
    st.header("Step 4 — Review & Refine Salience")

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
        # Aggregate strictly to SFU_SFU Version level (drop SKU/APO Product from group keys)
        if MONTHLY_SFU_VERSION_COL in display_sal.columns:
            group_disp = [MONTHLY_SFU_VERSION_COL]
            # Optionally add hierarchy columns if needed (but not SKU/APO Product)
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

        # Build pivot table: SFU_SFU_version × months
        # Per-month salience columns (named by month) in salience_df take precedence over
        # the scalar 'salience' column; users can override any cell independently.
        sas_months = st.session_state.get("sas_months_selected", [])
        sfuv_col = MONTHLY_SFU_VERSION_COL if MONTHLY_SFU_VERSION_COL in display_sal.columns else None
        if sfuv_col and sas_months:
            scalar_pct = (pd.to_numeric(display_sal["salience"], errors="coerce") * 100).round(2)
            pivot_rows = []
            for row_idx, row in display_sal.iterrows():
                default_pct = round(float(scalar_pct.at[row_idx]), 2)
                entry = {"SFU_SFU_version": row[sfuv_col]}
                for m in sas_months:
                    # Use existing per-month column if already set (value stored as 0-1 fraction)
                    if m in display_sal.columns:
                        entry[m] = round(float(pd.to_numeric(row[m], errors="coerce")) * 100, 2)
                    else:
                        entry[m] = default_pct
                pivot_rows.append(entry)
            pivot_df = pd.DataFrame(pivot_rows).set_index("SFU_SFU_version")
            pivot_df = pivot_df[~pivot_df.index.duplicated(keep="first")]
            st.subheader(f"Current Salience Table — {len(pivot_df)} SFU_SFU versions (editable per month)")
            st.caption(
                "Salience % per SFU_SFU version × month. "
                "Default is the same weight across all months — edit any cell to override a specific month. "
                "Click **Apply** to save changes."
            )
            col_cfg = {m: st.column_config.NumberColumn(m, format="%.2f %%") for m in sas_months}
            edited_pivot = st.data_editor(
                pivot_df,
                width='stretch',
                height=min(400, 50 + 35 * len(pivot_df)),
                column_config=col_cfg,
                num_rows="fixed",
            )
            # Write per-month salience back into salience_df as fractional columns
            if st.button("Apply Salience Overrides from Table"):
                updated_sal = st.session_state.salience_df.copy()
                for m in sas_months:
                    updated_sal[m] = updated_sal[MONTHLY_SFU_VERSION_COL].map(
                        lambda v, _m=m: float(edited_pivot.at[v, _m]) / 100.0
                        if v in edited_pivot.index else np.nan
                    )
                st.session_state.salience_df = updated_sal
                st.success("Per-month salience saved — will be used in the split.")
                st.rerun()
        else:
            # Fallback: long-format display (no SAS months available yet)
            if "salience" in display_sal.columns:
                display_sal["Salience %"] = (pd.to_numeric(display_sal["salience"], errors="coerce") * 100).round(2)
                display_sal = display_sal.drop(columns=["salience"])
            elif "salience %" in display_sal.columns:
                display_sal = display_sal.rename(columns={"salience %": "Salience %"})
                display_sal["Salience %"] = pd.to_numeric(display_sal["Salience %"], errors="coerce").round(2)
            st.subheader(f"Current Salience Table — {len(display_sal)} rows (at SFU_SFU Version level)")
            st.dataframe(
                display_sal,
                width='stretch',
                height=300,
                column_config={
                    "Salience %": st.column_config.NumberColumn("Salience %", format="%.2f %%"),
                },
            )

    # ── BOP Auto-Salience from historical data (Shipments-based, SFU level) ──
    if st.session_state.get("_is_bop"):
        with st.expander("BOP Auto-Salience — Basis & Salience Configuration (SFU Level)", expanded=True):
            st.caption(
                "Configure the **Basis / Metric** and **Basis Window** for each SFU Version. "
                "The table previews the last 6 historical months of that basis, "
                "computes the **Final Basis** (average over the window), and shows the resulting **Salience %**. "
                "Add optional **Remarks** per row."
            )

            # ── Discover available data ──────────────────────────────────────
            # Include MONTHLY_MEASURES (standard BOP measures) + Sellout
            available_basis_sources = [r for r in MONTHLY_MEASURES if r in st.session_state.sheets]
            if "Sellout" in st.session_state.sheets:
                available_basis_sources.append("Sellout")

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
                            # Sellout sheet: get SFU_v from mapped column or first column
                            _cmap_sellout = st.session_state.col_maps.get("Sellout", {})
                            _sfuv_col = _cmap_sellout.get("SFU_v") or _cmap_sellout.get("SKU")
                            if _sfuv_col and _sfuv_col in _df_src.columns:
                                all_sfu_versions.extend(_df_src[_sfuv_col].dropna().astype(str).tolist())
                        elif MONTHLY_SFU_VERSION_COL in _df_src.columns:
                            all_sfu_versions.extend(_df_src[MONTHLY_SFU_VERSION_COL].dropna().astype(str).tolist())
                sfu_versions = sorted(set(all_sfu_versions))

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
                # Friendly display labels P6M … P1M (oldest → newest)
                preview_labels = [f"P{len(preview_months) - i}M" for i in range(len(preview_months))]
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
                saved_remarks: dict = st.session_state.get("sfu_remarks", {})

                # ── Helper: compute basis value for one SFU from a source df ─
                def _sfu_basis_value(sfu_ver: str, src_role: str, mode_key: str, sel_months: list[str]) -> float:
                    src_df = _get_df(src_role)
                    if src_df is None:
                        return float("nan")
                    
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
                    
                    cmap_s = st.session_state.col_maps.get(src_role, {})
                    all_m = cmap_s.get("_months", detect_month_columns(src_df))
                    past_m = [m for m in all_m
                               if (parse_month_to_date(m) or pd.Timestamp.max) < current_month_start_sal]
                    if mode_key == "selected":
                        cols = [m for m in sel_months if m in src_df.columns]
                    elif mode_key.startswith("last_"):
                        n = int(mode_key.split("_")[1])
                        cols = past_m[-n:] if len(past_m) >= n else past_m
                    else:
                        cols = past_m
                    cols = [c for c in cols if c in src_df.columns]
                    if not cols:
                        return float("nan")
                    sub = src_df[src_df[sfu_col] == sfu_ver]
                    if sub.empty:
                        return float("nan")
                    return float(sub[cols].apply(pd.to_numeric, errors="coerce").values.mean())

                # ── Helper: get preview month value for one SFU ──────────────
                def _sfu_month_val(sfu_ver: str, src_role: str, month_col: str) -> float:
                    src_df = _get_df(src_role)
                    if src_df is None or month_col not in src_df.columns:
                        return float("nan")
                    
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
                    
                    sub = src_df[src_df[sfu_col] == sfu_ver]
                    if sub.empty:
                        return float("nan")
                    return float(pd.to_numeric(sub[month_col], errors="coerce").sum(min_count=1))

                # ── Build config table rows ──────────────────────────────────
                sfu_table_rows = []
                for v in sfu_versions:
                    cfg = saved_sfu_configs.get(v, {})
                    if isinstance(cfg, str):
                        cfg = {"source": cfg, "mode": "last_3", "selected": []}
                    default_src = "Shipments" if "Shipments" in available_basis_sources else available_basis_sources[0]
                    src = cfg.get("source", default_src)
                    raw_mode = cfg.get("mode", "last_3")
                    # Reverse-map internal mode → display label
                    window_label = next((k for k, mv in basis_window_mode_map.items() if mv == raw_mode), "P3M")
                    row_d: dict = {
                        MONTHLY_SFU_VERSION_COL: v,
                        "Basis / Metric": src,
                        "Basis Window": window_label,
                    }
                    sfu_table_rows.append(row_d)

                sfu_config_df = pd.DataFrame(sfu_table_rows)

                # ── Step 1: editable config table ───────────────────────────
                st.markdown("#### Step 1 — Set Basis / Metric and Window per SFU Version")
                edited_sfu = st.data_editor(
                    sfu_config_df,
                    column_config={
                        MONTHLY_SFU_VERSION_COL: st.column_config.TextColumn("SFU_SFU Version", disabled=True),
                        "Basis / Metric": st.column_config.SelectboxColumn(
                            "Basis / Metric ▾",
                            options=available_basis_sources,
                            required=True,
                            help="Click a cell to open the dropdown and select which historical data sheet to use as the basis for this SFU version.",
                        ),
                        "Basis Window": st.column_config.SelectboxColumn(
                            "Basis Window ▾",
                            options=basis_window_options,
                            required=True,
                            help="Click a cell to open the dropdown. P3M = avg last 3 months, P6M = avg last 6 months, etc. 'specific months' = manual pick below.",
                        ),
                    },
                    disabled=[MONTHLY_SFU_VERSION_COL],
                    width='stretch',
                    key="sfu_basis_editor",
                    height=min(450, 60 + len(sfu_config_df) * 35),
                )
                st.caption("💡 **Tip:** Click any cell in the **Basis / Metric ▾** or **Basis Window ▾** columns to open a dropdown and change the value.")

                # ── Step 2: per-SFU manual month selection (only for "specific months" rows) ──
                specific_sfu_rows = [
                    r for _, r in edited_sfu.iterrows()
                    if str(r.get("Basis Window", "")) == "specific months"
                ]
                saved_specific: dict = st.session_state.get("sfu_specific_months", {})

                if specific_sfu_rows:
                    st.markdown("#### Step 2 — Manual Month Selection per SFU Version")
                    st.caption(
                        "For each SFU Version set to *specific months*, pick the exact months "
                        "to use as basis. Expand a row to configure it."
                    )
                    # Render one expander per SFU that needs manual months
                    new_specific: dict = {}
                    for r in specific_sfu_rows:
                        v = r[MONTHLY_SFU_VERSION_COL]
                        src_for_months = str(r.get("Basis / Metric", available_basis_sources[0]))
                        # Offer months from the SFU's own chosen source sheet first, fallback to all
                        src_df_m = _get_df(src_for_months)
                        if src_df_m is not None:
                            cmap_m = st.session_state.col_maps.get(src_for_months, {})
                            src_months = sorted(
                                [m for m in cmap_m.get("_months", detect_month_columns(src_df_m))
                                 if (parse_month_to_date(m) or pd.Timestamp.max) < current_month_start_sal],
                                key=lambda x: parse_month_to_date(x) or pd.Timestamp.min,
                            )
                        else:
                            src_months = all_past_months_sorted
                        prev_sel = saved_specific.get(v, [])
                        # Filter previous selection to only valid months for this source
                        valid_prev = [m for m in prev_sel if m in src_months]
                        with st.expander(f"📅 {v}  ({src_for_months})", expanded=(not valid_prev)):
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

                # ── Step 3: live preview table ───────────────────────────────
                st.markdown("#### Step 3 — Preview: Basis Values and Salience")
                st.caption(
                    "The table below shows the actual historical values for the **last 6 months** (P6M→P1M) "
                    "pulled from the selected Basis / Metric sheet for each SFU Version, "
                    "plus the **Final Basis** (avg over the chosen window) and the resulting **Salience %**. "
                    "Edit **Remarks** directly in the table."
                )

                # Build per-SFU config dict from edited table
                preview_configs: dict[str, dict] = {}
                for _, r in edited_sfu.iterrows():
                    v = r[MONTHLY_SFU_VERSION_COL]
                    window_lbl = str(r.get("Basis Window", "P3M"))
                    preview_configs[v] = {
                        "source": str(r.get("Basis / Metric", available_basis_sources[0])),
                        "mode": basis_window_mode_map.get(window_lbl, "last_3"),
                        "selected": new_specific.get(v, []) if window_lbl == "specific months" else [],
                    }

                # Compute Final Basis for each SFU for salience % calculation
                sfu_final_basis: dict[str, float] = {}
                for v, cfg_p in preview_configs.items():
                    sfu_final_basis[v] = _sfu_basis_value(v, cfg_p["source"], cfg_p["mode"], cfg_p["selected"])

                # Normalize to salience % within each SFU version group
                # (each SFU's share = its basis / sum of all SFU bases)
                total_basis = sum(b for b in sfu_final_basis.values() if not pd.isna(b))

                # Build preview rows
                preview_rows = []
                for v, cfg_p in preview_configs.items():
                    src_role = cfg_p["source"]
                    row_p: dict = {
                        "SFU_SFU Version": v,
                        "Basis / Metric": src_role,
                        "Basis Window": next(
                            (k for k, mv in basis_window_mode_map.items() if mv == cfg_p["mode"]),
                            cfg_p["mode"],
                        ),
                    }
                    for lbl, actual_col in preview_col_map.items():
                        row_p[lbl] = _sfu_month_val(v, src_role, actual_col)
                    final_b = sfu_final_basis.get(v, float("nan"))
                    final_b = 0.0 if pd.isna(final_b) else float(final_b)
                    row_p["Final Basis"] = round(final_b, 2)
                    sal_pct = (final_b / total_basis * 100) if total_basis > 0 else 0.0
                    row_p["Salience %"] = round(sal_pct, 2)
                    row_p["Remarks"] = saved_remarks.get(v, "")
                    preview_rows.append(row_p)

                preview_df = pd.DataFrame(preview_rows)

                # Build column config for preview table
                preview_col_cfg: dict = {
                    "SFU_SFU Version": st.column_config.TextColumn("SFU_SFU Version", disabled=True),
                    "Basis / Metric": st.column_config.TextColumn("Basis / Metric", disabled=True),
                    "Basis Window": st.column_config.TextColumn("Basis Window", disabled=True),
                    "Final Basis": st.column_config.NumberColumn("Final Basis Calculated", format="%.2f", disabled=True),
                    "Salience %": st.column_config.NumberColumn("Salience %", format="%.2f %%", disabled=True),
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
                ordered_cols = (
                    ["SFU_SFU Version", "Basis / Metric", "Basis Window"]
                    + preview_labels
                    + ["Final Basis", "Salience %", "Remarks"]
                )
                ordered_cols = [c for c in ordered_cols if c in preview_df.columns]

                edited_preview = st.data_editor(
                    preview_df[ordered_cols],
                    column_config=preview_col_cfg,
                    width='stretch',
                    key="sfu_preview_editor",
                    height=min(500, 60 + len(preview_df) * 35),
                    num_rows="fixed",
                )

                # ── Step 4: Exclusions before salience ──────────────────────
                st.markdown("#### Step 4 — Exclusions (Fixed Promo volumes, Initiatives)")
                st.caption(
                    "Select any **SFU Versions or APO Products** to exclude before salience is calculated. "
                    "Excluded items will receive a salience weight of **0** and will not participate in the split. "
                    "SKUs with fewer than 3 months of shipments are flagged automatically and pre-checked for exclusion."
                )

                sku_df_excl = st.session_state.sku_df_filtered
                exc_store_sal: ExceptionStore = st.session_state.exc_store
                hcm_excl = hier_col_map_from_state()
                sku_col_excl = hcm_excl.get("SKU", MONTHLY_SFU_V_COL)
                if sku_df_excl is not None:
                    candidate_cols = [
                        sku_col_excl,
                        MONTHLY_SFU_VERSION_COL,
                        MONTHLY_SFU_V_COL,
                        "SFU_v",
                        "SKU",
                        "APO Product",
                    ]
                    sku_col_excl = next((c for c in candidate_cols if c and c in sku_df_excl.columns), None)

                # Try to get product description column from Monthly sheet
                monthly_df = None
                for sheet_name in ["Shipments", "Consumption", "Monthly"]:
                    df = _get_df(sheet_name)
                    if df is not None and not df.empty:
                        monthly_df = df
                        break
                desc_col = None
                if monthly_df is not None:
                    for cand in ["Product Description", "Description", "Desc"]:
                        for c in monthly_df.columns:
                            if cand.lower() in c.lower():
                                desc_col = c
                                break
                        if desc_col:
                            break

                # ── Detect SKUs with < 3 months of shipments ─────────────────
                sparse_skus: set[str] = set()
                shipments_df = _get_df("Shipments")
                if shipments_df is not None and sku_col_excl in (shipments_df.columns if shipments_df is not None else []):
                    ship_past_months = [
                        m for m in detect_month_columns(shipments_df)
                        if (parse_month_to_date(m) or pd.Timestamp.max) < current_month_start_sal
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
                    st.warning(
                        f"⚠️ **{len(sparse_skus)} APO Product(s) have fewer than 3 months of shipments** "
                        "and are pre-checked for exclusion below. "
                        "Please confirm whether these are **Initiatives** or new SKUs before saving exclusions: "
                        f"`{'`, `'.join(sorted(sparse_skus))}`"
                    )

                # ── Sub-tabs: SKU table exclusions | Upload exclusions Excel ─
                tab_excl_table, tab_excl_upload = st.tabs(["📋 Select Exclusions", "📤 Upload Exclusions + Volumes"])

                with tab_excl_table:
                    if sku_df_excl is not None and not sku_df_excl.empty and sku_col_excl:
                        # Build a display table of all APO Products with their SFU Version
                        excl_cols = []
                        if MONTHLY_SFU_VERSION_COL in sku_df_excl.columns:
                            excl_cols.append(MONTHLY_SFU_VERSION_COL)
                        if sku_col_excl in sku_df_excl.columns and sku_col_excl != MONTHLY_SFU_VERSION_COL:
                            excl_cols.append(sku_col_excl)
                        # Add hierarchy cols for context
                        for h in HIERARCHY_LEVELS:
                            actual_h = hcm_excl.get(h)
                            if actual_h and actual_h in sku_df_excl.columns and actual_h not in excl_cols:
                                excl_cols.append(actual_h)

                        # Add product description if available
                        if (
                            desc_col
                            and monthly_df is not None
                            and sku_col_excl in monthly_df.columns
                            and sku_col_excl in sku_df_excl.columns
                        ):
                            desc_map = monthly_df.drop_duplicates(subset=[sku_col_excl]).set_index(sku_col_excl)[desc_col].to_dict()
                            sku_df_excl = sku_df_excl.copy()
                            sku_df_excl["Product Description"] = sku_df_excl[sku_col_excl].map(desc_map)
                            if "Product Description" not in excl_cols:
                                excl_cols.append("Product Description")

                        if excl_cols:
                            excl_display = (
                                sku_df_excl[excl_cols]
                                .drop_duplicates()
                                .sort_values(excl_cols)
                                .reset_index(drop=True)
                            )

                            # "Exclude" checkbox — pre-checked if already excluded OR has < 3 months shipments
                            already_excl_set = exc_store_sal.global_exclusions
                            excl_display["Exclude"] = excl_display[sku_col_excl].apply(
                                lambda v: (str(v) in already_excl_set) or (str(v) in sparse_skus)
                            ) if sku_col_excl in excl_display.columns else False

                            # "Flagged (< 3 months shipments)" indicator column
                            excl_display["⚠️ < 3 months data"] = excl_display[sku_col_excl].apply(
                                lambda v: "⚠️ Sparse" if str(v) in sparse_skus else ""
                            ) if sku_col_excl in excl_display.columns else ""

                            # "Currently Excluded" status column
                            excl_display["📌 Status"] = excl_display[sku_col_excl].apply(
                                lambda v: "Excluded ✓" if str(v) in already_excl_set else ""
                            ) if sku_col_excl in excl_display.columns else ""

                            # Column order: Exclude, status, flag, ID cols
                            excl_ordered = (
                                ["Exclude", "⚠️ < 3 months data", "📌 Status"]
                                + [c for c in excl_cols if c in excl_display.columns]
                            )
                            excl_col_cfg: dict = {
                                "Exclude": st.column_config.CheckboxColumn(
                                    "Exclude",
                                    help="Check to exclude this APO Product from salience calculation.",
                                ),
                                "⚠️ < 3 months data": st.column_config.TextColumn(
                                    "< 3 months shipments",
                                    disabled=True,
                                    help="SKUs with fewer than 3 non-zero shipment months — likely Initiatives.",
                                ),
                                "📌 Status": st.column_config.TextColumn(
                                    "Current Status",
                                    disabled=True,
                                    help="Whether this SKU is currently in the global exclusions list.",
                                ),
                            }
                            if MONTHLY_SFU_VERSION_COL in excl_display.columns:
                                excl_col_cfg[MONTHLY_SFU_VERSION_COL] = st.column_config.TextColumn("SFU Version", disabled=True)
                            if sku_col_excl in excl_display.columns:
                                excl_col_cfg[sku_col_excl] = st.column_config.TextColumn("APO Product", disabled=True)
                            if "Product Description" in excl_display.columns:
                                excl_col_cfg["Product Description"] = st.column_config.TextColumn("Product Description", disabled=True)

                            edited_excl = st.data_editor(
                                excl_display[excl_ordered],
                                column_config=excl_col_cfg,
                                disabled=[c for c in excl_ordered if c != "Exclude"],
                                width='stretch',
                                key="sfu_exclusion_editor",
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
                                    st.success(f"✅ {len(newly_excluded)} APO Product(s) marked for exclusion: {', '.join(sorted(newly_excluded))}")
                                else:
                                    st.info("No exclusions set — all APO Products will be included in salience.")
                                st.rerun()

                            # Summary of currently excluded SKUs
                            current_excl = exc_store_sal.global_exclusions & (
                                set(excl_display[sku_col_excl].dropna().astype(str).tolist())
                                if sku_col_excl in excl_display.columns else set()
                            )
                            if current_excl:
                                st.warning(
                                    f"⚠️ **{len(current_excl)} APO Product(s) currently excluded** "
                                    f"(will be skipped in salience): "
                                    f"{', '.join(sorted(current_excl))}"
                                )
                        else:
                            st.info("No APO Product data available — complete Step 3 first.")
                    else:
                        if sku_df_excl is not None and not sku_df_excl.empty and not sku_col_excl:
                            st.warning(
                                "Could not find a valid SKU/SFU column in the filtered data. "
                                "Please check column mappings in Step 2."
                            )
                        else:
                            st.info("No SFU_v data loaded yet — complete Step 3 first.")

                with tab_excl_upload:
                    st.markdown(
                        "Upload an **Excel file** with excluded SKUs and their volumes. "
                        "The file must have:\n"
                        "- A column named **`APO Product`** (or the SKU column name for your data)\n"
                        "- Month columns in `Mmm-YY` format (e.g. `Jan-26`, `Feb-26`) with the volumes to allocate\n\n"
                        "These SKUs will be **globally excluded from the salience split** and their volumes will be "
                        "loaded as **fixed allocations** in the exception store."
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
                            # Normalise SKU column name
                            sku_col_candidates = [sku_col_excl, "APO Product", "SKU", "GCAS", "FPC"]
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
                                    st.dataframe(excl_up_df[[found_sku_col] + month_cols_up].head(20), width='stretch')

                                    if st.button("✅ Apply Uploaded Exclusions + Volumes", key="apply_excl_upload"):
                                        applied_skus = set()
                                        for _, urow in excl_up_df.iterrows():
                                            sku_v = str(urow[found_sku_col])
                                            if not sku_v or sku_v == "nan":
                                                continue
                                            # Globally exclude this SKU
                                            exc_store_sal.add_global_exclusion(
                                                sku_v,
                                                notes="Excluded via uploaded exclusions Excel (Step 4)"
                                            )
                                            applied_skus.add(sku_v)
                                            # Load fixed volumes per month (stored under a special bb_id = "__uploaded__")
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
                                        st.success(
                                            f"✅ {len(applied_skus)} SKU(s) excluded and volumes loaded: "
                                            f"{', '.join(sorted(applied_skus))}"
                                        )
                                        st.rerun()
                        except Exception as _exc_up_err:
                            st.error(f"Error reading uploaded file: {_exc_up_err}")

                # ── Compute button ───────────────────────────────────────────
                if st.button("✅ Compute BOP Salience (SFU Level)", type="primary"):
                    # Save remarks
                    new_remarks = {}
                    for _, pr in edited_preview.iterrows():
                        new_remarks[pr["SFU_SFU Version"]] = str(pr.get("Remarks", "") or "")
                    st.session_state.sfu_remarks = new_remarks

                    # Persist per-SFU specific month selections
                    st.session_state.sfu_specific_months = new_specific

                    # Save configs
                    new_sfu_configs: dict = {}
                    for v, cfg_p in preview_configs.items():
                        new_sfu_configs[v] = cfg_p
                    st.session_state.sfu_basis_sources = new_sfu_configs

                    with st.spinner("Computing BOP salience…"):
                        bop_sal = _compute_bop_salience(new_sfu_configs)
                    if bop_sal is not None and not bop_sal.empty:
                        st.session_state.salience_df = bop_sal
                        st.session_state.blocking_groups = []
                        st.success(f"✅ BOP salience computed — {len(bop_sal):,} rows.")
                        st.rerun()
                    else:
                        st.error(
                            "Could not compute BOP salience. "
                            "Ensure the selected source sheets have enough past months "
                            "and the data matches the SFU versions."
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
                **{"Salience %": (pd.to_numeric(override_rows["salience"].fillna(0.0), errors="coerce") * 100).round(2)}
            ).drop(columns=["salience"], errors="ignore")
            _override_disabled = [c for c in override_edit.columns if c != "Salience %"]
            edited = st.data_editor(
                override_edit,
                width='stretch',
                key="sal_override_editor",
                num_rows="fixed",
                disabled=_override_disabled,
                column_config={"Salience %": st.column_config.NumberColumn("Salience %", format="%.2f %%")},
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
        st.session_state.step = max(st.session_state.step, 5)
        st.session_state.max_step = max(st.session_state.max_step, 5)
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

    # ── GBB action banners ────────────────────────────────────────────────────
    gbb_actions: dict[str, str] = st.session_state.get("gbb_actions", {})
    gbb_types: dict[str, str] = st.session_state.get("gbb_types", {})
    exception_bbs = [bid for bid, act in gbb_actions.items() if act == "exceptions"]
    ignore_bbs = [bid for bid, act in gbb_actions.items() if act == "ignore"]

    if exception_bbs or ignore_bbs:
        with st.expander("⚠️ GBB Type actions required — click to review", expanded=True):
            if ignore_bbs:
                st.error(
                    f"**{len(ignore_bbs)} Building Block(s) flagged as *Ignore* (Initiatives):** "
                    "These BBs will not be split. Add them to the exception list with a note or provide manual inputs. "
                    f"BBs: `{'`, `'.join(ignore_bbs)}`"
                )
                if st.button("Add Initiatives BBs to exception log", key="gbb_add_ignore"):
                    for bid in ignore_bbs:
                        note = f"Initiatives BB — excluded from split. GBB Type: {gbb_types.get(bid, 'Initiatives')}"
                        for sku in all_skus:
                            exc_store.add_bb_exclude(bid, sku, notes=note)
                    st.success(f"{len(ignore_bbs)} BB(s) added to exception log with all SKUs excluded.")
                    st.rerun()

            if exception_bbs:
                st.warning(
                    f"**{len(exception_bbs)} Building Block(s) require SKU selection** "
                    f"(Promotions / New Channels): `{'`, `'.join(exception_bbs)}`  \n"
                    "Use the **Per-BB Exceptions** tab below to specify which SKUs should receive allocation."
                )

    tab_global, tab_bb, tab_log = st.tabs(["Global Exclusions", "Per-BB Exceptions", "Exception Log"])

    # ---- Global exclusions ----
    with tab_global:
        st.subheader("Globally Excluded SFU_vs")
        st.caption("These SFU_vs will never receive allocation in any split.")
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
            st.success(f"Global exclusions updated: {len(exc_store.global_exclusions)} SFU_v(s)")

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
                    st.markdown("**Force Include SFU_vs**")
                    new_include = st.multiselect("Include", all_skus, default=cur_include, key=f"inc_{selected_bb}")
                with c2:
                    st.markdown("**Force Exclude SFU_vs**")
                    new_exclude = st.multiselect("Exclude", all_skus, default=cur_exclude, key=f"exc_{selected_bb}")

                # Fixed quantities
                st.markdown("**Fixed Quantity Allocations** (by SFU_v × Month)")
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
                        edited_fq = st.data_editor(fq_df, width='stretch', key=f"fq_editor_{selected_bb}", num_rows="fixed")
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
            st.dataframe(log_df, width='stretch')

    if st.button("Proceed to Split →", type="primary"):
        st.session_state.step = max(st.session_state.step, 6)
        st.session_state.max_step = max(st.session_state.max_step, 6)
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
        issues.append("No SFU_v data — complete Steps 1-3")
    if st.session_state.salience_df is None:
        issues.append("Salience not computed — complete Step 4")
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

    # Summary
    per_bb_note = f"per-BB ({len(bb_split_levels)} configured)" if bb_split_levels else split_level
    st.markdown(f"""
| Parameter | Value |
|---|---|
| Building Blocks | {len(sas_df)} |
| SKU rows | {len(sku_df)} |
| Split level | **{per_bb_note}** |
| SAS months | {', '.join(sas_months)} |
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
                sfuv_id_col = hcm.get("SFU_v", hcm.get("SKU", "SKU"))
                specific_sfuv_col = hcm.get("SFU_v") if "SFU_v" in hcm else None

                # Determine which sheet(s) provide the SFU_v / SKU data
                _sku_source_role = "Bible" if "Bible" in st.session_state.sheet_map else next(
                    (r for r in SKU_SHEETS if r in st.session_state.sheet_map and r != "Bible"), None
                )
                _sku_source_sheet = (
                    st.session_state.sheet_map.get(_sku_source_role, _sku_source_role)
                    if _sku_source_role else "SFU_v data"
                )

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
                        
                        diagnostic_results.append({
                            "BB_ID": bb_id,
                            "Split_Level": bb_level,
                            "Match_Type": "Pinned SFU_v",
                            "Match_Criteria": f"SFU_v={pinned_sfuv}",
                            "Matches_Found": match_count,
                            "Status": "✅ OK" if match_count > 0 else "❌ NO MATCH"
                        })
                    else:
                        # Check hierarchical matching
                        bb_group_keys_logical = [k for k in SPLIT_KEYS[bb_level] if k != "SFU_v"]
                        bb_sas_keys = [hcm.get(k, k) for k in bb_group_keys_logical]
                        bb_group_vals = [str(bb_row.get(c, "")) for c in bb_sas_keys]
                        
                        # Build match criteria string
                        criteria_parts = [f"{col}={val}" for col, val in zip(bb_sas_keys, bb_group_vals)]
                        criteria_str = ", ".join(criteria_parts)
                        
                        # Find matching rows
                        mask = pd.Series([True] * len(sku_df), index=sku_df.index)
                        for col, val in zip(bb_sas_keys, bb_group_vals):
                            if col in sku_df.columns:
                                mask &= sku_df[col].astype(str) == str(val)
                            else:
                                mask &= False  # Column doesn't exist
                        
                        match_count = mask.sum()
                        
                        # Check for missing columns
                        missing_cols = [col for col in bb_sas_keys if col not in sku_df.columns]
                        if missing_cols:
                            status = f"❌ MISSING COLS in '{_sku_source_sheet}': {', '.join(missing_cols)}"
                        elif match_count == 0:
                            status = "❌ NO MATCH"
                        else:
                            status = "✅ OK"
                        
                        diagnostic_results.append({
                            "BB_ID": bb_id,
                            "Split_Level": bb_level,
                            "Match_Type": "Hierarchical",
                            "Match_Criteria": criteria_str,
                            "Matches_Found": match_count,
                            "Status": status
                        })
                
                # Display results
                diag_df = pd.DataFrame(diagnostic_results)
                
                # Show statistics
                total_bbs = len(diag_df)
                ok_bbs = len(diag_df[diag_df["Status"].str.contains("✅")])
                failed_bbs = total_bbs - ok_bbs
                
                if failed_bbs > 0:
                    st.error(f"⚠️ **{failed_bbs} of {total_bbs} building blocks will fail to match!**")
                else:
                    st.success(f"✅ All {total_bbs} building blocks have matching SFU_v rows!")
                
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
                
                st.dataframe(diag_df, use_container_width=True, height=400)
                
                # Helpful suggestions
                if failed_bbs > 0:
                    st.markdown("### 💡 Troubleshooting Suggestions")
                    
                    has_missing_cols = any("MISSING COLS" in result["Status"] for result in diagnostic_results)
                    has_no_match = any(result["Status"] == "❌ NO MATCH" for result in diagnostic_results)
                    has_pinned = any(result["Match_Type"] == "Pinned SFU_v" and "❌" in result["Status"] for result in diagnostic_results)
                    
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
                output_wide, validation_df = run_split(
                    sas_df=sas_df,
                    sfuv_df=sku_df,
                    salience_df=sal_df,
                    sas_months=sas_months,
                    split_level=split_level,
                    bb_split_levels=bb_split_levels,
                    bb_id_col=bb_id_col,
                    sfuv_col=hcm.get("SKU", "SKU"),
                    hier_col_map=hcm,
                    exc_store=exc_store,
                    forecast_boundaries=st.session_state.get("forecast_boundaries"),
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
            st.dataframe(validation_df, width='stretch')
        else:
            st.success("No validation issues.")

        st.session_state.step = max(st.session_state.step, 7)
        st.session_state.max_step = max(st.session_state.max_step, 7)

    # Preview if already run
    if st.session_state.output_wide is not None:
        st.subheader("Output Preview (first 20 rows)")
        st.dataframe(st.session_state.output_wide.head(20), width='stretch')

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

    output_wide = st.session_state.output_wide
    if output_wide is None or output_wide.empty:
        st.warning("No split output yet — complete Step 6 first.")
        return

    # ── Controls ──────────────────────────────────────────────────────────────
    col_tol1, col_tol2, col_window, col_metric = st.columns(4)
    lower_pct = col_tol1.number_input("Lower tolerance (%)", min_value=0, max_value=100, value=80, step=5, key="rb_lower")
    upper_pct = col_tol2.number_input("Upper tolerance (%)", min_value=100, max_value=500, value=120, step=5, key="rb_upper")
    baseline_window = col_window.selectbox("Baseline window", ["Past 3 months", "Past 6 months"], key="rb_window")
    baseline_metric = col_metric.selectbox("Baseline metric", ["Shipments", "Retailing"], key="rb_metric")

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

    pivot_cols = [MONTHLY_SFU_VERSION_COL] + available_future
    pivot_df = (
        output_wide[[c for c in pivot_cols if c in output_wide.columns]]
        .groupby(MONTHLY_SFU_VERSION_COL, dropna=False)[available_future]
        .sum()
        .reset_index()
    )
    pivot_df["_baseline_avg"] = pivot_df[MONTHLY_SFU_VERSION_COL].map(baseline_avg_map).fillna(0)
    lower_mult = lower_pct / 100.0
    upper_mult = upper_pct / 100.0

    # ── Apply conditional styling ─────────────────────────────────────────────
    def _style_cell(val, baseline):
        if pd.isna(val) or baseline == 0:
            return ""
        if val > baseline * upper_mult:
            return "background-color: #cce5ff; color: #003366"  # blue
        if val < baseline * lower_mult:
            return "background-color: #fff3cd; color: #664d00"  # yellow
        return ""

    display_pivot = pivot_df.set_index(MONTHLY_SFU_VERSION_COL)
    baseline_series = display_pivot.pop("_baseline_avg")

    def _style_row(row):
        bl = baseline_series.get(row.name, 0)
        return [_style_cell(v, bl) for v in row]

    styled = display_pivot.style.apply(_style_row, axis=1).format("{:,.1f}")

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
        for m in available_future
        if (bl := baseline_series.get(sfu, 0)) > 0 and display_pivot.at[sfu, m] > bl * upper_mult
    )
    n_low = sum(
        1 for sfu in display_pivot.index
        for m in available_future
        if (bl := baseline_series.get(sfu, 0)) > 0 and display_pivot.at[sfu, m] < bl * lower_mult
    )

    if n_high or n_low:
        st.warning(
            f"**{n_high}** cell(s) above {upper_pct}% of baseline (blue) · "
            f"**{n_low}** cell(s) below {lower_pct}% of baseline (yellow)"
        )
    else:
        st.success("All cells are within tolerance. ✅")

    if st.button("Proceed to Download →", type="primary"):
        st.session_state.step = max(st.session_state.step, 8)
        st.session_state.max_step = max(st.session_state.max_step, 8)
        st.rerun()


# ──────────────────────────────────────────────────────────────────────────────
# STEP 8 – Download
# ──────────────────────────────────────────────────────────────────────────────
def step7_download():
    st.header("Step 8 — Download Results")

    if st.session_state.output_wide is None:
        st.warning("No output yet — complete Step 6.")
        return

    output_wide = st.session_state.output_wide
    sal_df = st.session_state.salience_df if st.session_state.salience_df is not None else pd.DataFrame()
    exc_log = st.session_state.exc_store.log_as_df()
    val_df = st.session_state.validation_df if st.session_state.validation_df is not None else pd.DataFrame()
    run_settings = st.session_state.run_settings

    col1, col2, col3 = st.columns(3)
    col1.metric("Output SFU_v rows", len(output_wide))
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
        width='stretch',
    )

    st.divider()
    tab_out, tab_sal, tab_exc, tab_val = st.tabs(["Split Forecast", "Salience Table", "Exception Log", "Validation"])

    with tab_out:
        st.dataframe(output_wide, width='stretch', height=400)
    with tab_sal:
        # Always show salience as % — drop raw fraction column, add Salience %
        sal_display = sal_df.copy()
        if not sal_display.empty and "salience" in sal_display.columns:
            sal_display["Salience %"] = (pd.to_numeric(sal_display["salience"], errors="coerce") * 100).round(2)
            sal_display = sal_display.drop(columns=["salience"])
            # Move Salience % right after the last hierarchy / ID column
            non_sal_cols = [c for c in sal_display.columns if c != "Salience %"]
            sal_display = sal_display[non_sal_cols + ["Salience %"]]
        st.dataframe(
            sal_display,
            width='stretch',
            height=400,
            column_config={"Salience %": st.column_config.NumberColumn("Salience %", format="%.2f %%")},
        )
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

    # Allow jumping back via sidebar nav
    with st.sidebar:
        st.divider()
        st.subheader("Jump to Step")
        max_step = st.session_state.max_step
        jump = st.radio("Navigate to step", STEPS[:max_step], index=min(step - 1, max_step - 1), label_visibility="collapsed")
        if jump:
            target = STEPS.index(jump) + 1
            if target != step:
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
    elif step == 7:
        step7_reasonability()
    elif step >= 8:
        step7_download()


if __name__ == "__main__":
    main()
