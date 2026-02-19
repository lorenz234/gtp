import json
import os
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from google.oauth2 import service_account
import gcsfs
import polars as pl
from sqlalchemy import text

from src.db_connector import DbConnector


DEFAULT_TABLES = [
    "imx_deposits",
    "imx_fees",
    "imx_mints",
    "imx_orders",
    "imx_trades",
    "imx_transfers",
    "imx_withdrawals",
]


TIMESTAMP_CANDIDATES = [
    "timestamp",
    "updated_timestamp",
]


def _detect_timestamp_column(conn: DbConnector, table_name: str) -> str:
    query = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :table_name
        """
    )
    with conn.engine.begin() as connection:
        rows = connection.execute(query, {"table_name": table_name}).fetchall()
    columns = [row[0] for row in rows]
    if not columns:
        raise ValueError(f"No columns found for table {table_name}")

    lower_map = {col.lower(): col for col in columns}
    for candidate in TIMESTAMP_CANDIDATES:
        if candidate in lower_map:
            return lower_map[candidate]

    raise ValueError(
        f"No timestamp column found in {table_name}. "
        f"Looked for {TIMESTAMP_CANDIDATES}. Available: {columns}"
    )


def _get_min_max_timestamp(conn: DbConnector, table_name: str, ts_col: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    query = text(f"SELECT MIN({ts_col}) AS min_ts, MAX({ts_col}) AS max_ts FROM {table_name}")
    with conn.engine.begin() as connection:
        row = connection.execute(query).fetchone()
    if not row or row[0] is None or row[1] is None:
        return None, None
    min_ts, max_ts = row[0], row[1]
    if isinstance(min_ts, date) and not isinstance(min_ts, datetime):
        min_ts = datetime.combine(min_ts, datetime.min.time())
    if isinstance(max_ts, date) and not isinstance(max_ts, datetime):
        max_ts = datetime.combine(max_ts, datetime.min.time())
    if isinstance(min_ts, str):
        min_ts = datetime.fromisoformat(min_ts)
    if isinstance(max_ts, str):
        max_ts = datetime.fromisoformat(max_ts)
    return min_ts, max_ts


def _build_windows(min_ts: datetime, max_ts: datetime, chunk: timedelta) -> List[Tuple[datetime, datetime]]:
    windows = []
    max_inclusive = max_ts + timedelta(microseconds=1)
    start = min_ts
    while start < max_inclusive:
        end = start + chunk
        if end > max_inclusive:
            end = max_inclusive
        windows.append((start, end))
        start = end
    return windows


def _date_partition_key(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d")


def _export_window(
    conn: DbConnector,
    fs: gcsfs.GCSFileSystem,
    bucket: str,
    prefix: str,
    table_name: str,
    ts_col: str,
    start: datetime,
    end: datetime,
    part_id: int,
) -> int:
    query = f"""
        SELECT *
        FROM {table_name}
        WHERE {ts_col} >= '{start.isoformat()}'
          AND {ts_col} < '{end.isoformat()}'
        ORDER BY {ts_col} ASC
    """
    df = pl.read_database_uri(query=query, uri=conn.uri)
    if df.is_empty():
        return 0

    date_key = _date_partition_key(start)
    filename = f"part_{part_id}.parquet"
    path = f"gs://{bucket}/{prefix}/{table_name}/date={date_key}/{filename}"

    with fs.open(path, "wb") as f:
        df.write_parquet(f, compression="snappy")

    return df.shape[0]


def export_tables(
    tables: List[str],
    bucket: str,
    prefix: str,
    chunk_days: int,
    chunk_hours: Optional[int],
) -> None:
    if chunk_hours is not None and chunk_hours <= 0:
        raise ValueError("chunk_hours must be > 0")
    if chunk_hours is None and chunk_days <= 0:
        raise ValueError("chunk_days must be > 0")
    
    credentials_info = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
    credentials = service_account.Credentials.from_service_account_info(credentials_info)

    conn = DbConnector()
    fs = gcsfs.GCSFileSystem(token=credentials_info)

    if chunk_hours is not None:
        chunk = timedelta(hours=chunk_hours)
        chunk_desc = f"{chunk_hours}h"
    else:
        chunk = timedelta(days=chunk_days)
        chunk_desc = f"{chunk_days}d"

    print(f"Exporting {len(tables)} tables to gs://{bucket}/{prefix} using chunk={chunk_desc}")

    for table_name in tables:
        print(f"\n=== {table_name} ===")
        ts_col = _detect_timestamp_column(conn, table_name)
        print(f"Timestamp column: {ts_col}")

        min_ts, max_ts = _get_min_max_timestamp(conn, table_name, ts_col)
        if not min_ts or not max_ts:
            print(f"No data found for {table_name}. Skipping.")
            continue

        print(f"Range: {min_ts} -> {max_ts}")
        windows = _build_windows(min_ts, max_ts, chunk)
        print(f"Windows: {len(windows)}")

        total_rows = 0
        for idx, (start, end) in enumerate(windows, start=1):
            rows = _export_window(conn, fs, bucket, prefix, table_name, ts_col, start, end, idx)
            total_rows += rows
            print(f"  {idx}/{len(windows)} {start} -> {end}: {rows} rows")

        print(f"Done {table_name}: {total_rows} rows exported")


if __name__ == "__main__":
    export_tables(
        tables=DEFAULT_TABLES,
        bucket="gtp-archive",
        prefix="db_txt/imx",
        chunk_days=1,
        chunk_hours=None
    )
