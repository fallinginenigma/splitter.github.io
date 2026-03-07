"""Excel file loading utilities — supports both generic workbooks and the
standard SAP BEx/HANA BOP export format (SAS + Monthly sheets).
"""
from __future__ import annotations

import io
import re
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Generic month-label pattern  (Jan-26, Jan 26, January-2026, …)
# ---------------------------------------------------------------------------
MONTH_PATTERN = re.compile(
    r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[- ]?\d{2,4}$",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# SAP BEx BOP-specific constants
# ---------------------------------------------------------------------------

# "NOV 2025" / "JAN 2026" format used in the Monthly sheet
MONTHLY_MONTH_RE = re.compile(
    r"^(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+\d{4}$",
    re.IGNORECASE,
)

# Logical name → actual column name in the SAS (Building Block) sheet
SAS_HIERARCHY_MAP: dict[str, str] = {
    "Ctry":         "Ctry",
    "SMO Category": "SMO Category",
    "Brand":        "Brand",
    "Sub Brand":    "Sub Brand",
    "Form":         "Form",
}
# Optional pinned-SKU column in SAS (only populated when BB is pegged to 1 SKU)
SAS_SKU_COL = "Specific SKU"

# Logical name → actual column name in the Monthly sheet (before renaming)
MONTHLY_HIERARCHY_MAP: dict[str, str] = {
    "Ctry":         "Reporting Country",
    "SMO Category": "Category",
    "Brand":        "Brand",          # same name in both sheets
    "Sub Brand":    "Family Name 1",
    "Form":         "Family Name 2",
}
MONTHLY_SKU_COL = "APO Product"         # SKU identifier in Monthly

MONTHLY_MEASURE_COL = "Calendar Year/Month"
MONTHLY_MEASURES = [
    "Shipments",
    "Statistical Forecast",
    "Final Fcst to Finance",
]
MONTHLY_HEADER_ROW = 13                 # 0-indexed row that contains column headers


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def is_bop_file(sheet_names: list[str]) -> bool:
    """Return True when the workbook looks like a standard SAP BEx BOP export
    (i.e. it contains both a ``SAS`` sheet and a ``Monthly`` sheet)."""
    return "SAS" in sheet_names and "Monthly" in sheet_names


def detect_bop_col_maps(sheets: dict[str, pd.DataFrame]) -> tuple[dict, dict]:
    """Build auto ``sheet_map`` and ``col_maps`` for a BOP file.

    Call this after :func:`load_excel` has detected a BOP workbook and
    returned the derived per-measure DataFrames.

    Returns:
        ``(sheet_map, col_maps)`` dicts ready to drop straight into session state.
    """
    sheet_map: dict[str, str] = {}
    col_maps: dict[str, dict] = {}

    # SAS ---------------------------------------------------------------
    if "SAS" in sheets:
        sas_df = sheets["SAS"]
        sheet_map["SAS"] = "SAS"
        month_cols = detect_month_columns(sas_df)
        col_maps["SAS"] = {
            **{k: v for k, v in SAS_HIERARCHY_MAP.items() if v in sas_df.columns},
            "BB_ID": None,          # will be generated in Step 3
            "_months": month_cols,
        }

    # Measure-derived sheets (Shipments, Statistical Forecast, …) -------
    for measure in MONTHLY_MEASURES:
        if measure not in sheets:
            continue
        mdf = sheets[measure]
        sheet_map[measure] = measure
        # After renaming, hierarchy cols already use the SAS/logical names.
        hier_map = {k: k for k in SAS_HIERARCHY_MAP if k in mdf.columns}
        month_cols = detect_month_columns(mdf)
        entry = {
            **hier_map,
            "_months": month_cols,
        }
        if MONTHLY_SKU_COL in mdf.columns:
            entry["SKU"] = MONTHLY_SKU_COL
        col_maps[measure] = entry

    return sheet_map, col_maps


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def load_excel(file_obj) -> dict[str, pd.DataFrame]:
    """Load an uploaded Excel workbook.

    For standard SAP BEx BOP exports (contains both ``SAS`` and ``Monthly``
    sheets) the loader applies special handling:

    * ``SAS`` is read with ``header=0``; Timestamp column headers are
      normalised to ``'Nov-25'`` string labels.
    * ``Monthly`` is read with ``header=13``; hierarchy columns are renamed
      to match the SAS sheet; month headers are normalised to ``'Nov-25'``
      format; the sheet is split into one DataFrame per measure.

    For every other workbook all sheets are loaded with ``header=0`` and
    column names are converted to strings (existing behaviour).
    """
    name = getattr(file_obj, "name", "")
    if hasattr(file_obj, "seek"):
        file_obj.seek(0)
    data = file_obj.read() if hasattr(file_obj, "read") else file_obj
    buf = io.BytesIO(data)

    if name.lower().endswith(".xlsb"):
        return _load_xlsb(buf)
    else:
        return _load_openpyxl(buf)


# ---------------------------------------------------------------------------
# Internal loaders
# ---------------------------------------------------------------------------

def _load_openpyxl(buf: io.BytesIO) -> dict[str, pd.DataFrame]:
    xl = pd.ExcelFile(buf, engine="openpyxl")
    if is_bop_file(xl.sheet_names):
        return _load_bop_openpyxl(xl)
    # Generic fallback -------------------------------------------------------
    sheets: dict[str, pd.DataFrame] = {}
    for sheet in xl.sheet_names:
        df = xl.parse(sheet, header=0)
        df.columns = [str(c) for c in df.columns]
        sheets[sheet] = df
    return sheets


def _load_bop_openpyxl(xl: pd.ExcelFile) -> dict[str, pd.DataFrame]:
    """Load a BOP workbook and return per-measure DataFrames.

    Returned keys:  ``"SAS"``, ``"Shipments"``, ``"Statistical Forecast"``,
    ``"Final Fcst to Finance"``  (the latter three only when data rows exist
    for those measures in the Monthly sheet).
    """
    sheets: dict[str, pd.DataFrame] = {}

    # --- SAS ----------------------------------------------------------------
    sas_df = xl.parse("SAS", header=0)
    sas_df = _normalize_sas_month_cols(sas_df)   # Timestamp → "Nov-25"
    sas_df.columns = [str(c) for c in sas_df.columns]
    sheets["SAS"] = sas_df

    # --- Monthly ------------------------------------------------------------
    if "Monthly" not in xl.sheet_names:
        return sheets

    monthly_raw = xl.parse("Monthly", header=MONTHLY_HEADER_ROW)

    # Drop rows where the measure discriminator is blank (SAP metadata rows)
    if MONTHLY_MEASURE_COL in monthly_raw.columns:
        monthly_raw = monthly_raw.dropna(subset=[MONTHLY_MEASURE_COL])

    # Rename Monthly hierarchy columns to match SAS names so the join works
    hier_rename = {
        v: k
        for k, v in MONTHLY_HIERARCHY_MAP.items()
        if v != k and v in monthly_raw.columns
    }
    # e.g. {"Reporting Country": "Ctry", "Category": "SMO Category",
    #        "Family Name 1": "Sub Brand", "Family Name 2": "Form"}
    monthly_raw = monthly_raw.rename(columns=hier_rename)

    # Normalise month column names from "NOV 2025" → "Nov-25"
    month_col_rename = {
        col: _normalize_monthly_month_label(col)
        for col in monthly_raw.columns
        if isinstance(col, str) and MONTHLY_MONTH_RE.match(col.strip())
        and _normalize_monthly_month_label(col) != col
    }
    if month_col_rename:
        monthly_raw = monthly_raw.rename(columns=month_col_rename)

    monthly_raw.columns = [str(c) for c in monthly_raw.columns]

    # Split into one DataFrame per measure
    if MONTHLY_MEASURE_COL in monthly_raw.columns:
        for measure in MONTHLY_MEASURES:
            mdf = monthly_raw[monthly_raw[MONTHLY_MEASURE_COL] == measure].copy()
            if not mdf.empty:
                sheets[measure] = mdf.reset_index(drop=True)
    else:
        # Measure column absent — store everything under "Shipments"
        sheets["Shipments"] = monthly_raw.reset_index(drop=True)

    return sheets


def _load_xlsb(buf: io.BytesIO) -> dict[str, pd.DataFrame]:
    from pyxlsb import open_workbook

    sheets: dict[str, pd.DataFrame] = {}
    buf.seek(0)
    with open_workbook(buf) as wb:
        for sheet_name in wb.sheets:
            with wb.get_sheet(sheet_name) as sheet:
                rows = []
                for row in sheet.rows():
                    rows.append([cell.v for cell in row])
            if rows:
                df = pd.DataFrame(
                    rows[1:],
                    columns=[str(c) if c is not None else "" for c in rows[0]],
                )
                sheets[sheet_name] = df
            else:
                sheets[sheet_name] = pd.DataFrame()
    return sheets


# ---------------------------------------------------------------------------
# BOP normalisation helpers (internal)
# ---------------------------------------------------------------------------

def _normalize_sas_month_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Rename ``pd.Timestamp`` column headers to ``'Nov-25'`` string format."""
    rename = {
        col: col.strftime("%b-%y")
        for col in df.columns
        if isinstance(col, pd.Timestamp)
    }
    return df.rename(columns=rename) if rename else df


def _normalize_monthly_month_label(col: str) -> str:
    """Convert ``'NOV 2025'`` → ``'Nov-25'`` for consistency with SAS labels."""
    try:
        ts = pd.to_datetime(col.strip(), format="%b %Y")
        return ts.strftime("%b-%y")
    except Exception:
        return col


# ---------------------------------------------------------------------------
# Generic detection helpers (used by app.py)
# ---------------------------------------------------------------------------

def detect_month_columns(df: pd.DataFrame) -> list[str]:
    """Return column names that look like month labels (e.g. ``'Nov-25'``)."""
    return [c for c in df.columns if MONTH_PATTERN.match(str(c).strip())]


def detect_hierarchy_columns(df: pd.DataFrame, month_cols: list[str]) -> list[str]:
    """Return non-month columns (potential hierarchy / metadata columns)."""
    month_set = set(month_cols)
    return [c for c in df.columns if c not in month_set]


def normalize_month_label(label: str) -> str:
    """Normalise a month label to Title-case + dash, e.g. ``'jan 26'`` → ``'Jan-26'``."""
    label = label.strip()
    m = MONTH_PATTERN.match(label)
    if not m:
        return label
    parts = re.split(r"[- ]+", label, maxsplit=1)
    if len(parts) == 2:
        month, year = parts
        if len(year) == 2:
            year = "20" + year
        return f"{month.capitalize()}-{year[-2:]}"
    return label


def parse_month_to_date(label: str) -> pd.Timestamp | None:
    """Convert a month label like ``'Jan-26'`` to a :class:`pandas.Timestamp`."""
    label = normalize_month_label(label)
    try:
        return pd.to_datetime(label, format="%b-%y")
    except Exception:
        try:
            return pd.to_datetime(label)
        except Exception:
            return None
