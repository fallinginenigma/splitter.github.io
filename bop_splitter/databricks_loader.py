"""Azure Databricks connectivity utilities for BOP Splitter.

Requires the optional dependency:
    pip install databricks-sql-connector
"""
from __future__ import annotations

import pandas as pd


def fetch_table(
    server_hostname: str,
    http_path: str,
    access_token: str,
    table_name: str,
    row_limit: int = 100_000,
) -> pd.DataFrame:
    """Fetch a Databricks table as a DataFrame.

    Args:
        server_hostname: Databricks workspace host, e.g. ``adb-xxx.azuredatabricks.net``.
        http_path: SQL Warehouse HTTP path, e.g. ``/sql/1.0/warehouses/xxx``.
        access_token: Personal access token (PAT) or AAD token.
        table_name: Fully-qualified table name, e.g. ``catalog.schema.table``.
        row_limit: Maximum rows to fetch (default 100 000).

    Returns:
        DataFrame containing the table data.

    Raises:
        ImportError: If ``databricks-sql-connector`` is not installed.
        Exception: On connection or query errors.
    """
    try:
        from databricks import sql as dbsql  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "The 'databricks-sql-connector' package is required for Azure Databricks "
            "connectivity.\n\nInstall it with:\n    pip install databricks-sql-connector"
        ) from exc

    with dbsql.connect(
        server_hostname=server_hostname.strip(),
        http_path=http_path.strip(),
        access_token=access_token.strip(),
    ) as conn:
        with conn.cursor() as cur:
            # table_name is a user-supplied, fully-qualified catalog.schema.table
            cur.execute(f"SELECT * FROM {table_name} LIMIT {row_limit}")  # noqa: S608
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    return pd.DataFrame(rows, columns=columns)


def test_connection(server_hostname: str, http_path: str, access_token: str) -> tuple[bool, str]:
    """Verify credentials by running a trivial query.

    Returns:
        (success, message) tuple.
    """
    try:
        from databricks import sql as dbsql  # type: ignore[import]
    except ImportError:
        return False, "databricks-sql-connector is not installed."

    try:
        with dbsql.connect(
            server_hostname=server_hostname.strip(),
            http_path=http_path.strip(),
            access_token=access_token.strip(),
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True, "Connection successful."
    except Exception as e:
        return False, str(e)
