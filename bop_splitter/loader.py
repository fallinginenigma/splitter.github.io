"""Excel file loading utilities."""
from __future__ import annotations

import io
import re
from pathlib import Path

import pandas as pd

MONTH_PATTERN = re.compile(
    r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[- ]?\d{2,4}$",
    re.IGNORECASE,
)


def load_excel(file_obj) -> dict[str, pd.DataFrame]:
    """Load all sheets from an uploaded Excel file (.xlsx/.xlsm/.xlsb)."""
    name = getattr(file_obj, "name", "")
    if hasattr(file_obj, "seek"):
        file_obj.seek(0)
    data = file_obj.read() if hasattr(file_obj, "read") else file_obj
    buf = io.BytesIO(data)

    if name.lower().endswith(".xlsb"):
        return _load_xlsb(buf)
    else:
        return _load_openpyxl(buf)


def _load_openpyxl(buf: io.BytesIO) -> dict[str, pd.DataFrame]:
    xl = pd.ExcelFile(buf, engine="openpyxl")
    sheets = {}
    for sheet in xl.sheet_names:
        df = xl.parse(sheet, header=0)
        df.columns = [str(c) for c in df.columns]
        sheets[sheet] = df
    return sheets


def _load_xlsb(buf: io.BytesIO) -> dict[str, pd.DataFrame]:
    from pyxlsb import open_workbook

    sheets = {}
    buf.seek(0)
    with open_workbook(buf) as wb:
        for sheet_name in wb.sheets:
            with wb.get_sheet(sheet_name) as sheet:
                rows = []
                for row in sheet.rows():
                    rows.append([cell.v for cell in row])
            if rows:
                df = pd.DataFrame(rows[1:], columns=[str(c) if c is not None else "" for c in rows[0]])
                sheets[sheet_name] = df
            else:
                sheets[sheet_name] = pd.DataFrame()
    return sheets


def detect_month_columns(df: pd.DataFrame) -> list[str]:
    """Return column names that look like month labels (e.g. Jan-26)."""
    return [c for c in df.columns if MONTH_PATTERN.match(str(c).strip())]


def detect_hierarchy_columns(df: pd.DataFrame, month_cols: list[str]) -> list[str]:
    """Return non-month columns (potential hierarchy / metadata columns)."""
    month_set = set(month_cols)
    return [c for c in df.columns if c not in month_set]


def normalize_month_label(label: str) -> str:
    """Normalize month label to Title-case with dash separator, e.g. 'jan 26' -> 'Jan-26'."""
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
    """Convert a month label like 'Jan-26' to a pandas Timestamp."""
    label = normalize_month_label(label)
    try:
        return pd.to_datetime(label, format="%b-%y")
    except Exception:
        try:
            return pd.to_datetime(label)
        except Exception:
            return None
