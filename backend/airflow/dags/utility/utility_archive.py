import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import gcsfs
import polars as pl
from airflow.decorators import dag, task
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import inspect

from src.db_connector import DbConnector
from src.main_config import get_main_config
from src.misc.airflow_utils import alert_via_webhook
from src.misc.helper_functions import send_discord_message

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
def save_to_gcs(df: pl.DataFrame, path: str, fs: Optional[gcsfs.GCSFileSystem]) -> None:
    """Write a Polars DataFrame to GCS."""
    assert fs is not None, "GCS filesystem not initialised"
    with fs.open(path, "wb") as f:
        df.write_parquet(f, compression="snappy")
    logger.info("Saved to GCS: %s", path)


def get_bq_max_block(client: bigquery.Client, table_name: str) -> Optional[int]:
    """Fetch the current max block in BQ for the table; return None if missing."""
    query = f"""
    SELECT 
        MAX(block_number) AS max_block
    FROM `growthepie.gtp_archive.{table_name}` 
    WHERE date > '2020-01-01'
    """
    try:
        results = client.query(query).result()
        df_bq = results.to_dataframe()
        return int(df_bq["max_block"][0]) if not df_bq.empty else None
    except Exception as exc:  # broad catch: table may not exist yet
        logger.warning("Could not read max block for %s from BQ: %s", table_name, exc)
        send_discord_message(f"Archival DAG: Could not read max block for {table_name} from BQ: {exc}")
        return None


def get_eligible_chains() -> List[Dict[str, str]]:
    """Return chains flagged for archiving and with an existing *_tx table."""
    main_conf = get_main_config()
    db_connector = DbConnector()
    inspector = inspect(db_connector.engine)
    existing_tables = set(inspector.get_table_names())
    #skip_tables = {"rhino_tx"}  # safeguard against wrong start block noted in script

    eligible = []
    for chain in main_conf:
        if not chain.runs_archive_raw_tx:
            continue
        table_name = f"{chain.origin_key}_tx"
        # if table_name in skip_tables:
        #     logger.info("Skipping %s (listed in skip tables)", table_name)
        #     continue
        if table_name not in existing_tables:
            logger.info("Skipping %s: table %s not found in DB", chain.origin_key, table_name)
            continue
        eligible.append({"origin_key": chain.origin_key, "table_name": table_name})
    return eligible


def get_eligible_archive_tables() -> List[Dict[str, str]]:
    """Return chain/table pairs flagged in archive_tables and present in the DB."""
    main_conf = get_main_config()
    db_connector = DbConnector()
    inspector = inspect(db_connector.engine)
    existing_tables = set(inspector.get_table_names())

    eligible = []
    for chain in main_conf:
        if not chain.archive_tables:
            continue
        for table_name in chain.archive_tables:
            if table_name not in existing_tables:
                logger.info("Skipping %s: table %s not found in DB", chain.origin_key, table_name)
                continue
            eligible.append({"origin_key": chain.origin_key, "table_name": table_name})
    return eligible


def get_archive_dates(
    db_connector: DbConnector,
    table_name: str,
    origin_key: str,
    archival_date: date,
) -> List[date]:
    query = f"""
        SELECT DISTINCT date
        FROM {table_name}
        WHERE origin_key = '{origin_key}'
          AND date <= '{archival_date}'
        ORDER BY date
    """
    df = pl.read_database_uri(query=query, uri=db_connector.uri)
    if df.is_empty():
        return []
    if df.schema["date"] == pl.Utf8:
        df = df.with_columns(pl.col("date").str.strptime(pl.Date, strict=False))
    return [d for d in df["date"].to_list() if d is not None]


def archive_chain(table_name: str, bucket_name: str, keep_postgres_days: int, chunk_size: int) -> None:
    """Archive a single chain's *_tx table to GCS/BQ."""
    credentials_info = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    bq_client = bigquery.Client(credentials=credentials)

    db_connector = DbConnector()
    fs = gcsfs.GCSFileSystem(token=credentials_info)

    archival_date = datetime.now().date() - timedelta(days=keep_postgres_days)
    logger.info("Archival date cutoff: %s", archival_date)

    start_block = (get_bq_max_block(bq_client, table_name) or -1) + 1
    logger.info("Starting from block %s for table %s", start_block, table_name)

    part = 0
    date_part_counter: Dict[str, int] = {}
    reached_end = False

    while not reached_end:
        query = f"""
            SELECT * 
            FROM {table_name} 
            WHERE block_number >= {start_block} 
            ORDER BY block_number ASC 
            LIMIT {chunk_size}
        """

        df = pl.read_database_uri(query=query, uri=db_connector.uri)

        if df.is_empty():
            logger.info("No more rows to fetch for %s.", table_name)
            break

        logger.info(
            "Loaded %s rows. Min block: %s, Max block: %s",
            df.shape[0],
            df["block_number"].min(),
            df["block_number"].max(),
        )

        # Ensure timestamp is datetime
        if df.schema["block_timestamp"] == pl.Utf8:
            df = df.with_columns(pl.col("block_timestamp").str.strptime(pl.Datetime, strict=False))

        df = df.with_columns(pl.col("block_timestamp").dt.date().alias("block_date"))

        unique_dates = [x for x in df.select("block_date").unique().to_series().to_list() if x is not None]

        if len(unique_dates) == 1:
            date = unique_dates[0]
            logger.info("Single date batch: %s", date)
            max_block = df["block_number"].max()
            df = df.filter(df["block_number"] < max_block)
            logger.info("Filtered rows after removing max block: %s", df.shape[0])

            if df.is_empty():
                logger.info("%s is done. Max block was %s.", table_name, max_block)
                break

            year, month, day = date.year, f"{date.month:02}", f"{date.day:02}"

            if date > archival_date:
                logger.info("DONE: date %s is greater than archival date %s.", date, archival_date)
                reached_end = True
                break

            date_key = str(date)
            date_part_counter[date_key] = date_part_counter.get(date_key, 0) + 1
            part_id = date_part_counter[date_key]

            block_range = f"{df['block_number'].min()}_{df['block_number'].max()}"
            filename = f"part_{part_id}_{block_range}.parquet"
            gcs_path = f"{bucket_name}/db_tx/{table_name}/date={year}-{month}-{day}/{filename}"

            save_to_gcs(df, f"gs://{gcs_path}", fs)

        else:
            logger.info("Multiple dates in batch %s, dropping latest date.", unique_dates)
            latest_date = max(unique_dates)
            df = df.filter(df["block_date"] < latest_date)
            max_block = df["block_number"].max() + 1
            logger.info("Filtered rows: %s", df.shape[0])

            if df.is_empty():
                logger.info("%s is done. Max block was %s.", table_name, max_block)
                break

            for date in sorted(set(df["block_date"].to_list())):
                group = df.filter(df["block_date"] == date)
                year, month, day = date.year, f"{date.month:02}", f"{date.day:02}"

                if date > archival_date:
                    logger.info("DONE: date %s is greater than archival date %s.", date, archival_date)
                    reached_end = True
                    break

                date_key = str(date)
                date_part_counter[date_key] = date_part_counter.get(date_key, 0) + 1
                part_id = date_part_counter[date_key]

                block_range = f"{group['block_number'].min()}_{group['block_number'].max()}"
                filename = f"part_{part_id}_{block_range}.parquet"
                gcs_path = f"{bucket_name}/db_tx/{table_name}/date={year}-{month}-{day}/{filename}"

                save_to_gcs(group, f"gs://{gcs_path}", fs)

        start_block = max_block
        part += 1


def archive_table_by_date(
    table_name: str,
    origin_key: str,
    bucket_name: str,
    keep_postgres_days: int,
) -> None:
    """Archive non-tx tables by date for a single origin_key."""
    credentials_info = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
    fs = gcsfs.GCSFileSystem(token=credentials_info)
    db_connector = DbConnector()

    archival_date = datetime.now().date() - timedelta(days=keep_postgres_days)
    logger.info("Archival date cutoff: %s", archival_date)

    dates = get_archive_dates(db_connector, table_name, origin_key, archival_date)
    if not dates:
        logger.info("No dates to archive for %s (%s).", table_name, origin_key)
        return

    for archive_date in dates:
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE origin_key = '{origin_key}'
              AND date = '{archive_date}'
        """
        df = pl.read_database_uri(query=query, uri=db_connector.uri)
        if df.is_empty():
            logger.info("No data for %s %s on %s.", table_name, origin_key, archive_date)
            continue

        year, month, day = archive_date.year, f"{archive_date.month:02}", f"{archive_date.day:02}"
        filename = "part_1.parquet"
        gcs_path = (
            f"{bucket_name}/db_other/{table_name}/origin_key={origin_key}/date={year}-{month}-{day}/{filename}"
        )
        save_to_gcs(df, f"gs://{gcs_path}", fs)


def build_task_id(prefix: str, origin_key: str, table_name: str) -> str:
    safe_name = re.sub(r"[^a-zA-Z0-9_]+", "_", f"{origin_key}_{table_name}")
    return f"{prefix}_{safe_name}"


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #
eligible_chains = get_eligible_chains()
eligible_archive_tables = get_eligible_archive_tables()
bucket_default = os.getenv("UTILITY_ARCHIVE_BUCKET", "gtp-archive")
keep_days_default = int(os.getenv("UTILITY_ARCHIVE_KEEP_POSTGRES_DAYS", "30"))
chunk_size_default = int(os.getenv("UTILITY_ARCHIVE_CHUNK_SIZE", "1000000"))


@dag(
    default_args={
        "owner": "mseidl",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="utility_archive",
    description="Archive raw *_tx tables and configured archive_tables to GCS/BQ.",
    tags=["utility", "archive"],
    start_date=datetime(2024, 1, 1),
    schedule="15 16 * * *",
)
def utility_archive():
    for chain in eligible_chains:
        origin_key = chain["origin_key"]
        table_name = chain["table_name"]

        @task(task_id=f"archive_{origin_key}", execution_timeout=timedelta(hours=2))
        def run_archive(
            table: str = table_name,
            bucket: str = bucket_default,
            keep_days: int = keep_days_default,
            chunk_size: int = chunk_size_default
        ):
            logger.info("Starting archive task for %s (table: %s)", origin_key, table)
            archive_chain(table, bucket, keep_days, chunk_size)

        run_archive()

    for chain_table in eligible_archive_tables:
        origin_key = chain_table["origin_key"]
        table_name = chain_table["table_name"]

        @task(task_id=build_task_id("archive_table", origin_key, table_name), execution_timeout=timedelta(hours=2))
        def run_archive_table(
            table: str = table_name,
            origin: str = origin_key,
            bucket: str = bucket_default,
            keep_days: int = keep_days_default,
        ):
            logger.info("Starting archive task for %s (table: %s)", origin, table)
            archive_table_by_date(table, origin, bucket, keep_days)

        run_archive_table()


utility_archive_dag = utility_archive()
