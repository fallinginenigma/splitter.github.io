"""Excel export utilities."""
from __future__ import annotations

import io
import math
import pandas as pd


def build_excel_output(
    output_wide: pd.DataFrame,
    salience_df: pd.DataFrame,
    exception_log: pd.DataFrame,
    validation_df: pd.DataFrame,
    run_settings: dict | None = None,
) -> bytes:
    """Build a multi-sheet .xlsx file and return as bytes."""
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
