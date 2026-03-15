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
        "split_level": "Form",        # default global split level
        "bb_split_levels": {},        # bb_id -> split_level string (per-BB override)
        "basis_source": "Consumption",
        "basis_mode": "last_3",
        "basis_months_selected": [],
        "sas_months_selected": [],
        "sas_forecast_months_selected": [],  # user-selected future forecast months
        "sfu_basis_sources": {},      # SFU_SFU Version -> basis source sheet role
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
    st.caption("Building Block → SFU_v Forecast Splitter")
    st.divider()
    for i, label in enumerate(STEPS, 1):
        icon = "▶️" if i == st.session_state.step else ("✅" if i <= st.session_state.max_step else "⬜")
        st.markdown(f"{icon} {label}")
    st.divider()
    if st.button("↺ Reset All", use_container_width=True):
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
]
SKU_SHEETS = [
    "Shipments", "Consumption", "Retailing", "Statistical Forecast",
    "Final Fcst to Finance",   # BOP: derived from Monthly sheet
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
    """Merge all SFU_v sheets into a single deduplicated DataFrame."""
    frames = []
    hier_cols_actual = []
    for role in SKU_SHEETS:
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
        st.dataframe(pd.DataFrame(mapping_rows), use_container_width=True, hide_index=True)

    # SAS preview
    if not sas_df.empty:
        with st.expander("Preview SAS (Building Blocks) — first 5 rows"):
            st.dataframe(sas_df.head(5), use_container_width=True)

    # Monthly preview per measure
    for measure in MONTHLY_MEASURES:
        if measure in sheets:
            with st.expander(f"Preview Monthly · {measure} — first 5 rows"):
                st.dataframe(sheets[measure].head(5), use_container_width=True)


# ──────────────────────────────────────────────────────────────────────────────
# STEP 1 – Upload & Sheet Mapping  (Excel OR Azure Databricks)
# ──────────────────────────────────────────────────────────────────────────────
def step1_upload():
    st.header("Step 1 — Load Data")

    # ── Already-loaded summary (navigated back) ────────────────────────────────
    if st.session_state.sheets and st.session_state.sheet_map:
        src = st.session_state.data_source
        src_label = {"excel": "Excel file", "databricks": "Azure Databricks"}.get(src, "file")
        total_rows = sum(len(v) for v in st.session_state.sheets.values())
        st.success(
            f"**{src_label}** data already loaded — "
            f"{', '.join(st.session_state.sheets.keys())} "
            f"({total_rows:,} rows total). "
            "Use the button below to continue, or load new data."
        )
        if st.button("Continue with current data →", type="primary"):
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
            if st.button("Proceed to Column Mapping →", type="primary"):
                st.session_state.step = max(st.session_state.step, 2)
                st.session_state.max_step = max(st.session_state.max_step, 2)
                st.rerun()
            return

        # ── Generic path: manual sheet-to-role mapping ─────────────────────────
        st.success(f"Loaded **{len(sheets)}** sheet(s): {', '.join(sheets.keys())}")
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

        if new_map:
            preview_role = st.selectbox("Preview sheet", list(new_map.keys()), key="preview_role")
            if preview_role:
                st.dataframe(sheets[new_map[preview_role]].head(5), use_container_width=True)

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

            # Auto-detect months
            detected_months = detect_month_columns(df)
            
            if "_months" not in cmap:
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
            confirmed_months = st.multiselect(
                "Month columns",
                cols_all,
                default=[m for m in _sas_month_default if m in cols_all],
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

            # SFU_v column (not for SAS)
            if role != "SAS":
                if role in ("Shipments", "Statistical Forecast", "Final Fcst to Finance", "Stat", "FFF"):
                    # For these BOP roles, prioritize MONTHLY_SFU_VERSION_COL ("SFU_SFU Version") as default
                    sku_guess = cmap.get("SKU") or (MONTHLY_SFU_VERSION_COL if MONTHLY_SFU_VERSION_COL in cols_all else _guess_col(cols_all, "SFU_SFU Version", "APO Product", "SFU_v", "SKU"))
                else:
                    sku_guess = cmap.get("SKU") or cmap.get("SFU_v") or _guess_col(cols_all, "APO Product", "SFU_v", "SKU")
                sku_idx = none_opt.index(sku_guess) if sku_guess in none_opt else 0
                sku_sel = st.selectbox("SFU_v column", none_opt, index=sku_idx, key=f"col_{role}_SKU")
                if sku_sel != "— auto —":
                    cmap["SKU"] = sku_sel
                else:
                    cmap.pop("SKU", None)

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
        st.dataframe(bb_table, use_container_width=True, height=min(400, 40 + len(bb_table) * 35))
        st.caption(f"**{len(bb_table)}** Building Blocks")
    else:
        st.info("No hierarchy columns mapped — configure hierarchy columns in Step 2 to see Building Blocks here.")

    # ---- SFU_v Actuals & Forecasts (BOP only) ----
    if st.session_state.get("_is_bop"):
        st.subheader("SFU_v Actuals & Forecasts")
        current_month_start = pd.Timestamp.now().normalize().replace(day=1)
        st.caption(
            f"Grouped by hierarchy + SFU_SFU Version (aggregated across APO Products). "
            f"Shipments = last 12 months before **{current_month_start.strftime('%b-%Y')}**; "
            f"forecasts = **{current_month_start.strftime('%b-%Y')}** onward."
        )
        with st.spinner("Aggregating…"):
            sfuv_agg = _compute_sfuv_aggregates()

        if sfuv_agg is not None:
            display_col_map = {
                "Shipments":             "Shipments — last 12 months",
                "Statistical Forecast":  "Stat Forecast — future months",
                "Final Fcst to Finance": "Final Fcst to Finance — future months",
            }
            display_df = sfuv_agg.rename(columns=display_col_map)
            st.dataframe(
                display_df,
                use_container_width=True,
                height=min(500, 60 + len(display_df) * 35),
            )
            st.caption(f"**{len(display_df):,}** rows")
        else:
            st.info(
                f"SFU_v aggregate data unavailable — verify that the "
                f"**{MONTHLY_SFU_VERSION_COL}** column exists in the Monthly sheet "
                f"and that at least one measure was loaded."
            )

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
    st.caption(
        "Set the hierarchy level at which each Building Block is split to SFU_vs. "
        "Default is **Form** (finest level). If SAS has specific SFU_v data for a BB, it is auto-set to **SFU_v**. "
        "Edit the 'Split Level' column below."
    )

    bb_id_col = st.session_state.bb_id_col or "BB_ID"
    saved_bb_split_levels = st.session_state.get("bb_split_levels", {})

    # Auto-detect SFU_v split level: if SAS has a Specific SFU_v column with data for a BB row → use SFU_v level
    _sas_sfuv_col_in_data = SAS_SFU_V_COL if SAS_SFU_V_COL in sas_df.columns else None

    def _auto_split_level(bb_row_subset: pd.DataFrame) -> str:
        """Return 'SFU_v' if any row in this BB slice has a filled Specific SFU_v value."""
        if _sas_sfuv_col_in_data is None:
            return "Form"
        vals = bb_row_subset[_sas_sfuv_col_in_data].dropna().astype(str).str.strip()
        return "SFU_v" if (vals != "").any() else "Form"

    if hier_actual and bb_id_col in sas_df.columns:
        bb_editor_df = (
            sas_df[hier_actual + [bb_id_col]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        def _resolve_split_level(bid):
            # User override takes precedence; otherwise auto-detect from SAS SFU_v column
            if bid in saved_bb_split_levels:
                return saved_bb_split_levels[bid]
            bb_rows = sas_df[sas_df[bb_id_col] == bid]
            return _auto_split_level(bb_rows)

        bb_editor_df["Split Level"] = bb_editor_df[bb_id_col].map(_resolve_split_level)

        edited_bb = st.data_editor(
            bb_editor_df,
            column_config={
                "Split Level": st.column_config.SelectboxColumn(
                    "Split Level",
                    options=list(SPLIT_KEYS.keys()),
                    required=True,
                )
            },
            disabled=[c for c in bb_editor_df.columns if c != "Split Level"],
            use_container_width=True,
            key="bb_split_level_editor",
            height=min(450, 60 + len(bb_editor_df) * 35),
        )
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
        else:
            new_bb_split_levels = {}
            st.session_state.bb_split_levels = {}

        sas_out = sas_df.reset_index(drop=True)
        sku_out = sku_merged.reset_index(drop=True)
        st.session_state.sas_df_filtered = sas_out
        st.session_state.sku_df_filtered = sku_out

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


def _compute_bop_salience(sfu_basis_sources: dict) -> pd.DataFrame | None:
    """
    Compute BOP salience at SFU_SFU Version level.

    Algorithm per split group:
      1. For each SFU_SFU Version, compute total basis = avg of last 3 past months
         summed across all APO Products in that SFU (from the SFU's designated source).
      2. Each APO Product within the SFU shares the SFU total basis equally
         (SFU_basis / n_APO_Products_in_SFU).
      3. Normalise within split group so saliences sum to 1.
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

    # Invert sfu_basis_sources: source_role -> [SFU versions using that source]
    source_to_sfus: dict[str, list] = {}
    for sfu_ver, src_role in sfu_basis_sources.items():
        source_to_sfus.setdefault(src_role, []).append(sfu_ver)

    # Step 1: build {(group_vals..., SFU_version): total_SFU_basis}
    sfu_basis: dict[tuple, float] = {}
    for src_role, sfu_list in source_to_sfus.items():
        src_df = _get_df(src_role)
        if src_df is None or MONTHLY_SFU_VERSION_COL not in src_df.columns:
            continue
        cmap = st.session_state.col_maps.get(src_role, {})
        all_m = cmap.get("_months", detect_month_columns(src_df))
        past_m = []
        for m in all_m:
            try:
                if pd.to_datetime(m, format="%b-%y") < current_month_start:
                    past_m.append(m)
            except Exception:
                pass
        last_3 = past_m[-3:] if past_m else []
        basis_cols = [c for c in last_3 if c in src_df.columns]
        if not basis_cols:
            continue
        sub = src_df[src_df[MONTHLY_SFU_VERSION_COL].isin(sfu_list)].copy()
        if sub.empty:
            continue
        sub["_row_basis"] = sub[basis_cols].apply(pd.to_numeric, errors="coerce").mean(axis=1)
        agg_keys = [k for k in valid_group_keys if k in sub.columns] + [MONTHLY_SFU_VERSION_COL]
        for grp_vals, grp in sub.groupby(agg_keys, sort=False, dropna=False):
            if not isinstance(grp_vals, tuple):
                grp_vals = (grp_vals,)
            total = pd.to_numeric(grp["_row_basis"], errors="coerce").sum(min_count=1)
            sfu_basis[grp_vals] = float(total) if not pd.isna(total) else 0.0

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

    # ── Auto salience status ──────────────────────────────────────────────────
    bb_split_levels = st.session_state.get("bb_split_levels", {})
    if bb_split_levels:
        level_counts = {}
        for lvl in bb_split_levels.values():
            level_counts[lvl] = level_counts.get(lvl, 0) + 1
        level_summary = ", ".join(f"{cnt} BB(s) at **{lvl}**" for lvl, cnt in level_counts.items())
        st.info(f"**Equal split applied per Building Block** — {level_summary}.")
    else:
        st.info(
            f"**Equal split applied automatically** — each Building Block's value is divided equally "
            f"among the SFU_vs that match its **{split_level}** criteria "
            f"({' + '.join(SPLIT_KEYS[split_level])})."
        )

    if sal_df is not None and not sal_df.empty:
        display_sal = sal_df.drop(columns=["_split_level"], errors="ignore").copy()
        # Aggregate to SFU_SFU Version level (sum salience across APO Products per SFU)
        sku_col_display = hcm.get("SKU", MONTHLY_SFU_V_COL)
        if MONTHLY_SFU_VERSION_COL in display_sal.columns:
            group_disp = [c for c in display_sal.columns
                          if c not in (sku_col_display, "salience", "basis", "flag")]
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
            col_cfg = {m: st.column_config.NumberColumn(m, format="%.2f") for m in sas_months}
            edited_pivot = st.data_editor(
                pivot_df,
                use_container_width=True,
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
                use_container_width=True,
                height=300,
                column_config={
                    "Salience %": st.column_config.NumberColumn("Salience %", format="%.2f %%"),
                },
            )

    # ── BOP Auto-Salience from historical data (Shipments-based, SFU level) ──
    if st.session_state.get("_is_bop"):
        with st.expander("BOP Auto-Salience — Shipments-based (SFU Level)", expanded=True):
            st.caption(
                "Automatically compute salience from historical data aggregated at **SFU_SFU Version** level. "
                "Each APO Product's weight = its SFU's total past shipments ÷ number of APO Products in that SFU. "
                "Select a different basis source per SFU version below."
            )

            # Discover available SFU versions from Shipments sheet
            ship_df = _get_df("Shipments")
            available_basis_sources = [r for r in MONTHLY_MEASURES if r in st.session_state.sheets]

            if ship_df is not None and MONTHLY_SFU_VERSION_COL in ship_df.columns and available_basis_sources:
                sfu_versions = sorted(ship_df[MONTHLY_SFU_VERSION_COL].dropna().unique().tolist())
                saved_sfu_sources = st.session_state.get("sfu_basis_sources", {})

                sfu_table = pd.DataFrame({
                    MONTHLY_SFU_VERSION_COL: sfu_versions,
                    "Basis Source": [
                        saved_sfu_sources.get(v, "Shipments") if "Shipments" in available_basis_sources
                        else available_basis_sources[0]
                        for v in sfu_versions
                    ],
                })

                edited_sfu = st.data_editor(
                    sfu_table,
                    column_config={
                        "Basis Source": st.column_config.SelectboxColumn(
                            "Basis Source",
                            options=available_basis_sources,
                            required=True,
                        )
                    },
                    disabled=[MONTHLY_SFU_VERSION_COL],
                    use_container_width=True,
                    key="sfu_basis_editor",
                    height=min(400, 60 + len(sfu_table) * 35),
                )

                if st.button("Compute BOP Salience (SFU Level)", type="primary"):
                    new_sfu_sources = dict(zip(
                        edited_sfu[MONTHLY_SFU_VERSION_COL],
                        edited_sfu["Basis Source"],
                    ))
                    st.session_state.sfu_basis_sources = new_sfu_sources
                    with st.spinner("Computing BOP salience…"):
                        bop_sal = _compute_bop_salience(new_sfu_sources)
                    if bop_sal is not None and not bop_sal.empty:
                        st.session_state.salience_df = bop_sal
                        st.session_state.blocking_groups = []
                        st.success(f"BOP salience computed — {len(bop_sal):,} rows.")
                        st.rerun()
                    else:
                        st.error(
                            "Could not compute BOP salience. Ensure the Shipments sheet has past months "
                            f"and the **{MONTHLY_SFU_VERSION_COL}** column is present."
                        )
            else:
                st.info(
                    f"BOP auto-salience requires the **Shipments** sheet with a **{MONTHLY_SFU_VERSION_COL}** column. "
                    "Load a BOP Excel file to enable this feature."
                )

    # ── Optional: override with historical basis ───────────────────────────────
    BASIS_SOURCES = [r for r in SKU_SHEETS if r in st.session_state.sheet_map]
    if BASIS_SOURCES:
        with st.expander("Override with Historical Basis (optional)", expanded=False):
            st.caption(
                "Use actual sales data to weight SFU_vs by their historical share instead of an equal split."
            )
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
                basis_df_tmp = _get_df(basis_source)
                cmap_tmp = st.session_state.col_maps.get(basis_source, {})
                avail_months = cmap_tmp.get("_months", detect_month_columns(basis_df_tmp) if basis_df_tmp is not None else [])
                basis_months_selected = st.multiselect(
                    "Select months for basis", avail_months,
                    default=st.session_state.basis_months_selected, key="basis_months_sel",
                )

            if st.button("Compute Historical Salience", type="secondary"):
                basis_df_raw = _get_df(basis_source)
                if basis_df_raw is None:
                    st.error("Basis source sheet not found.")
                else:
                    cmap = st.session_state.col_maps.get(basis_source, {})
                    month_cols = cmap.get("_months", detect_month_columns(basis_df_raw))

                    basis_df_slim = basis_df_raw.copy()
                    basis_vals = compute_basis(basis_df_slim, month_cols, basis_mode, basis_months_selected)
                    basis_df_slim["_basis_val"] = basis_vals

                    merge_keys = [
                        v for k, v in hcm.items()
                        if k != "SKU" and v in sku_filtered.columns and v in basis_df_slim.columns
                    ]
                    sku_key_col = hcm.get("SKU")
                    if sku_key_col and sku_key_col in sku_filtered.columns and sku_key_col in basis_df_slim.columns:
                        merge_keys = merge_keys + [sku_key_col]

                    if merge_keys:
                        basis_slim = basis_df_slim[merge_keys + ["_basis_val"]].drop_duplicates(subset=merge_keys)
                        sku_work = sku_filtered.merge(basis_slim, on=merge_keys, how="left")
                    else:
                        sku_work = sku_filtered.copy()
                        sku_work["_basis_val"] = np.nan

                    basis_series = sku_work["_basis_val"] if "_basis_val" in sku_work.columns else pd.Series(np.nan, index=sku_work.index)

                    new_sal, blocking = compute_salience(
                        sfuv_df=sku_work,
                        basis=basis_series,
                        split_level=split_level,
                        sfuv_col=hcm.get("SKU", "SKU"),
                        hier_col_map=hcm,
                        global_exclusions=exc_store.global_exclusions,
                        overrides=st.session_state.sal_overrides,
                    )
                    st.session_state.salience_df = new_sal
                    st.session_state.blocking_groups = blocking
                    st.session_state.basis_source = basis_source
                    st.session_state.basis_mode = basis_mode
                    st.session_state.basis_months_selected = basis_months_selected
                    st.rerun()

    # ── Blocked groups (only relevant after historical basis) ─────────────────
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
            edited = st.data_editor(
                override_rows[_ctx_cols].assign(salience=override_rows["salience"].fillna(0.0)),
                use_container_width=True,
                key="sal_override_editor",
                num_rows="fixed",
                disabled=[c for c in _ctx_cols if c != "salience"],
            )
            if st.button("Apply Overrides"):
                _group_cols = [c for c in override_rows.columns if c not in (sku_col_sal, "basis", "salience", "flag")]
                for idx_row, (_, row) in enumerate(override_rows.iterrows()):
                    g_key = tuple(row[c] for c in _group_cols if c in row.index)
                    sku_val = row.get(sku_col_sal, "")
                    st.session_state.sal_overrides[(g_key, sku_val)] = float(edited.iloc[idx_row]["salience"])
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
        st.session_state.max_step = max(st.session_state.max_step, 7)

    # Preview if already run
    if st.session_state.output_wide is not None:
        st.subheader("Output Preview (first 20 rows)")
        st.dataframe(st.session_state.output_wide.head(20), use_container_width=True)

        if st.button("Proceed to Download →", type="primary"):
            st.session_state.step = max(st.session_state.step, 7)
            st.session_state.max_step = max(st.session_state.max_step, 7)
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
        max_step = st.session_state.max_step
        jump = st.radio("", STEPS[:max_step], index=min(step - 1, max_step - 1), label_visibility="collapsed")
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
    elif step >= 7:
        step7_download()


if __name__ == "__main__":
    main()
