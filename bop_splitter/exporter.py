"""Excel export utilities."""
from __future__ import annotations

import io
import logging
import math
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def validate_and_sanitize_data(
    df: pd.DataFrame,
    critical_cols: list[str] | None = None,
    replace_inf_with: float = 0.0,
) -> tuple[pd.DataFrame, list[dict]]:
    """
    Validate and sanitize DataFrame before export.
    
    Performs:
    - Removes rows with NaN in critical columns
    - Replaces infinite values with specified value
    - Logs data quality issues
    
    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame to sanitize
    critical_cols : list[str] | None
        Columns where NaN values trigger row removal. If None, no NaN removal.
    replace_inf_with : float
        Value to replace np.inf / -np.inf with (default 0.0)
    
    Returns
    -------
    tuple[pd.DataFrame, list[dict]]
        Sanitized DataFrame and list of issues found
    """
    issues = []
    df = df.copy()
    
    # Track initial row count
    initial_rows = len(df)
    
    # Remove rows with NaN in critical columns
    if critical_cols:
        for col in critical_cols:
            if col in df.columns:
                nan_count = df[col].isna().sum()
                if nan_count > 0:
                    issues.append({
                        "type": "nan_in_critical_column",
                        "column": col,
                        "count": int(nan_count),
                        "action": "removed rows",
                    })
                    df = df.dropna(subset=[col])
    
    # Replace infinite values in numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        inf_count = np.isinf(df[col]).sum()
        if inf_count > 0:
            issues.append({
                "type": "infinite_values",
                "column": col,
                "count": int(inf_count),
                "action": f"replaced with {replace_inf_with}",
            })
            df[col] = df[col].replace([np.inf, -np.inf], replace_inf_with)
    
    rows_removed = initial_rows - len(df)
    if rows_removed > 0:
        logger.warning(f"Removed {rows_removed} rows with NaN in critical columns during sanitization")
    
    return df, issues


def round_and_convert_types(
    df: pd.DataFrame,
    round_to_decimals: int = 2,
    int_columns: list[str] | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    """
    Apply rounding and selective type conversion to DataFrame.
    
    Performs:
    - Rounds all float columns to specified decimals
    - Converts specified columns to integer
    - Validates type conversions
    
    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame
    round_to_decimals : int
        Number of decimal places for rounding (default 2)
    int_columns : list[str] | None
        Columns to convert to integer type. If None, no conversion.
    
    Returns
    -------
    tuple[pd.DataFrame, list[dict]]
        Processed DataFrame and list of conversion issues
    """
    issues = []
    df = df.copy()
    
    # Round all float columns
    float_cols = df.select_dtypes(include=[np.floating]).columns
    for col in float_cols:
        df[col] = df[col].round(round_to_decimals)
    
    if len(float_cols) > 0:
        logger.info(f"Rounded {len(float_cols)} float columns to {round_to_decimals} decimals")
    
    # Convert specified columns to integer
    if int_columns:
        for col in int_columns:
            if col in df.columns and df[col].dtype != np.int64 and df[col].dtype != int:
                try:
                    # Check for non-integer values before conversion
                    non_int_count = (df[col] != df[col].astype(int)).sum()
                    if non_int_count > 0:
                        issues.append({
                            "type": "non_integer_in_int_column",
                            "column": col,
                            "count": int(non_int_count),
                            "action": "truncated to integer",
                        })
                    df[col] = df[col].astype(int)
                except (ValueError, TypeError) as e:
                    issues.append({
                        "type": "int_conversion_failed",
                        "column": col,
                        "error": str(e),
                        "action": "column kept as-is",
                    })
                    logger.warning(f"Failed to convert {col} to int: {e}")
    
    return df, issues


def check_value_bounds(
    df: pd.DataFrame,
    bounds: dict[str, tuple[float | None, float | None]] | None = None,
) -> list[dict]:
    """
    Check that numeric values stay within specified bounds.
    
    Bounds are specified as {column: (min_value, max_value)} where None means no bound.
    
    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame
    bounds : dict[str, tuple[float | None, float | None]] | None
        Mapping of column names to (min, max) tuples. None for no limit on that side.
    
    Returns
    -------
    list[dict]
        List of bound violations found
    """
    issues = []
    
    if not bounds:
        return issues
    
    for col, (min_val, max_val) in bounds.items():
        if col not in df.columns:
            issues.append({
                "type": "bounds_column_not_found",
                "column": col,
                "detail": f"Column not in DataFrame",
            })
            continue
        
        if min_val is not None:
            below_min = (df[col] < min_val).sum()
            if below_min > 0:
                issues.append({
                    "type": "values_below_minimum",
                    "column": col,
                    "bound": min_val,
                    "count": int(below_min),
                    "min_value_found": float(df[col].min()),
                })
        
        if max_val is not None:
            above_max = (df[col] > max_val).sum()
            if above_max > 0:
                issues.append({
                    "type": "values_above_maximum",
                    "column": col,
                    "bound": max_val,
                    "count": int(above_max),
                    "max_value_found": float(df[col].max()),
                })
    
    return issues


def build_excel_output(
    output_wide: pd.DataFrame,
    salience_df: pd.DataFrame,
    exception_log: pd.DataFrame,
    validation_df: pd.DataFrame,
    run_settings: dict | None = None,
    round_to_decimals: int = 2,
    int_columns: list[str] | None = None,
    critical_cols: list[str] | None = None,
    value_bounds: dict[str, tuple[float | None, float | None]] | None = None,
) -> bytes:
    """
    Build a multi-sheet .xlsx file and return as bytes.
    
    Parameters
    ----------
    output_wide : pd.DataFrame
        Main forecast output
    salience_df : pd.DataFrame
        Salience lookup table
    exception_log : pd.DataFrame
        Exception records
    validation_df : pd.DataFrame
        Validation report
    run_settings : dict | None
        Optional run configuration to include
    round_to_decimals : int
        Decimal places for rounding (default 2)
    int_columns : list[str] | None
        Columns to convert to integer type
    critical_cols : list[str] | None
        Columns where NaN values trigger row removal
    value_bounds : dict[str, tuple[float | None, float | None]] | None
        Value bounds to enforce {column: (min, max)}
    
    Returns
    -------
    bytes
        Excel file as bytes
    """
    # Pre-export validation and sanitization
    all_validation_issues = []
    
    # Sanitize main output
    output_wide, san_issues = validate_and_sanitize_data(
        output_wide,
        critical_cols=critical_cols,
        replace_inf_with=0.0,
    )
    all_validation_issues.extend(san_issues)
    
    # Apply rounding and type conversion
    output_wide, round_issues = round_and_convert_types(
        output_wide,
        round_to_decimals=round_to_decimals,
        int_columns=int_columns,
    )
    all_validation_issues.extend(round_issues)
    
    # Check value bounds
    if value_bounds:
        bound_issues = check_value_bounds(output_wide, bounds=value_bounds)
        all_validation_issues.extend(bound_issues)
    
    # Sanitize other sheets (no type conversion needed)
    salience_df, _ = validate_and_sanitize_data(salience_df, replace_inf_with=0.0)
    exception_log, _ = validate_and_sanitize_data(exception_log, replace_inf_with=0.0)
    
    # Append export validation issues to validation report
    if all_validation_issues:
        export_issues_df = pd.DataFrame(all_validation_issues)
        validation_df = pd.concat([validation_df, export_issues_df], ignore_index=True)
    
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        wb = writer.book

        # Formats
        header_fmt = wb.add_format({"bold": True, "bg_color": "#1F4E79", "font_color": "white", "border": 1})
        num_fmt = wb.add_format({"num_format": "#,##0.00", "border": 1})
        pct_fmt = wb.add_format({"num_format": "0.00%", "border": 1})
        warn_fmt = wb.add_format({"bg_color": "#FFE699", "border": 1})

        def write_sheet(df: pd.DataFrame, sheet_name: str, pct_cols: list[str] | None = None):
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]
            pct_cols = pct_cols or []
            for col_num, col_name in enumerate(df.columns):
                ws.set_column(col_num, col_num, max(15, len(str(col_name)) + 2))
                ws.write(0, col_num, col_name, header_fmt)
            for row_num in range(len(df)):
                for col_num, col_name in enumerate(df.columns):
                    val = df.iloc[row_num, col_num]
                    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                        ws.write(row_num + 1, col_num, None)
                    elif col_name in pct_cols and isinstance(val, (int, float)):
                        ws.write(row_num + 1, col_num, val, pct_fmt)
                    elif isinstance(val, (int, float)) and not isinstance(val, bool):
                        ws.write(row_num + 1, col_num, val, num_fmt)

        write_sheet(output_wide, "BOP_Split_Forecast")
        write_sheet(salience_df, "Salience_Table", pct_cols=["salience"])
        write_sheet(exception_log, "Exception_Log")

        if not validation_df.empty:
            write_sheet(validation_df, "Validation_Report")
        else:
            ok_df = pd.DataFrame([{"status": "OK", "detail": "No validation issues found."}])
            write_sheet(ok_df, "Validation_Report")

        if run_settings:
            settings_df = pd.DataFrame(list(run_settings.items()), columns=["Setting", "Value"])
            write_sheet(settings_df, "Run_Settings")

    return buf.getvalue()
