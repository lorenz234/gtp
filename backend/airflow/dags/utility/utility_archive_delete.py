import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from airflow.decorators import dag, task
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import inspect, text

from src.db_connector import DbConnector
from src.main_config import get_main_config
from src.misc.airflow_utils import alert_via_webhook
from src.misc.helper_functions import send_discord_message

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
def get_bq_client() -> bigquery.Client:
    credentials_raw = os.getenv("GOOGLE_CREDENTIALS")
    if not credentials_raw:
        raise ValueError("Missing GOOGLE_CREDENTIALS")
    credentials_info = json.loads(credentials_raw)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return bigquery.Client(credentials=credentials)


def get_eligible_chains() -> List[Dict[str, str]]:
    """Return chains flagged for delete and with an existing *_tx table."""
    main_conf = get_main_config()
    db_connector = DbConnector()
    inspector = inspect(db_connector.engine)
    existing_tables = set(inspector.get_table_names())

    eligible = []
    for chain in main_conf:
        if not chain.runs_delete_archived_raw_tx:
            continue
        table_name = f"{chain.origin_key}_tx"
        if table_name not in existing_tables:
            logger.info("Skipping %s: table %s not found in DB", chain.origin_key, table_name)
            continue
        eligible.append({"origin_key": chain.origin_key, "table_name": table_name})
    return eligible


def get_archive_window(
    db_connector: DbConnector,
    keep_postgres_days: int,
    table_name: str,
    query_start_override: Optional[str],
) -> Tuple[datetime.date, datetime.date]:
    """
        Return archival date and query start date.
        Archival date: today - keep_postgres_days (everything before this date can be deleted)
        Query start date: either from override or min DB date (everything after this date is compared with BQ)
    """
    
    today = datetime.now().date()
    archival_date = today - timedelta(days=keep_postgres_days)
    
    if query_start_override:
        query_start_date = datetime.strptime(query_start_override, "%Y-%m-%d").date()
    else:
        min_db_date = get_db_min_date(db_connector, table_name)
        if min_db_date is None:
            raise ValueError(f"No min DB date found for {table_name}")
        query_start_date = min_db_date
    
    return archival_date, query_start_date


def get_bq_count(
    client: bigquery.Client,
    table_name: str,
    archival_date: datetime.date,
    query_start_date: datetime.date,
) -> int:
    """
        Return count of rows in BQ table between archival_date and query_start_date.
    """
    
    query = f"""
    SELECT 
        COUNT(*) AS row_count
    FROM `growthepie.gtp_archive.{table_name}` 
    WHERE date <= '{archival_date}'
      AND date >= '{query_start_date}'
    """
    results = client.query(query).result()
    df_bq = results.to_dataframe()
    if df_bq.empty:
        return 0
    return int(df_bq["row_count"][0])


def get_db_count(
    db_connector: DbConnector,
    table_name: str,
    archival_date: datetime.date,
    query_start_date: datetime.date,
) -> int:
    """
        Return count of rows in DB table between archival_date and query_start_date.
    """
    
    query = f""" 
    SELECT 
        COUNT(*)
    FROM {table_name}
    WHERE block_date <= '{archival_date}'
      AND block_date >= '{query_start_date}'
    """
    with db_connector.engine.connect() as connection:
        result = connection.execute(text(query))
        return int(result.scalar() or 0)


def get_bq_max_block(
    client: bigquery.Client,
    table_name: str,
    archival_date: datetime.date,
) -> Optional[int]:
    """
        Return max block number in BQ table up to archival_date.
        Return None if table does not exist or no rows found.
        Why? To know up to which block we can delete in the DB.
    """
    
    query = f"""
    SELECT 
        MAX(block_number) AS max_block
    FROM `growthepie.gtp_archive.{table_name}` 
    WHERE date <= '{archival_date}'
    """
    try:
        results = client.query(query).result()
        df_bq = results.to_dataframe()
        return int(df_bq["max_block"][0]) if not df_bq.empty else None
    except Exception as exc:  # broad catch: table may not exist yet
        logger.warning("Could not read max block for %s from BQ: %s", table_name, exc)
        send_discord_message(f"Archive delete DAG: Could not read max block for {table_name} from BQ: {exc}")
        return None

def get_db_min_block(
    db_connector: DbConnector,
    table_name: str,
) -> Optional[int]:
    """
        Return min block number in DB table.
        Return None if no rows found.
        Why? To know from which block we can start deleting.
    """
    
    query = f""" 
    SELECT 
        MIN(block_number)
    FROM {table_name}
    """
    with db_connector.engine.connect() as connection:
        result = connection.execute(text(query))
        min_block = result.scalar()
        return int(min_block) if min_block is not None else None
    
def get_db_min_date(
    db_connector: DbConnector,
    table_name: str,
) -> Optional[datetime.date]:
    """
        Return min block date in DB table.
        Return None if no rows found.
        Why? To know from which date we can start comparing with BQ.
    """
    
    query = f"""  
    SELECT  
        MIN(block_date) 
    FROM {table_name} 
    """ 
    with db_connector.engine.connect() as connection: 
        result = connection.execute(text(query)) 
        min_date = result.scalar() 
        return min_date if min_date is not None else None

def delete_blocks(
    db_connector: DbConnector,
    table_name: str,
    start_block: int,
    max_block: int,
    batch_size: int,
) -> None:
    """
        Delete rows in DB table from start_block up to max_block in batches.
    """
    
    if max_block <= start_block:
        logger.info("No deletion needed for %s (start_block=%s, max_block=%s)", table_name, start_block, max_block)
        return

    end_block = min(start_block + batch_size, max_block)
    while end_block <= max_block:
        logger.info(
            "Deleting block_number <= %s out of %s for %s",
            end_block,
            max_block,
            table_name,
        )
        query = f"""
            DELETE FROM {table_name}
            WHERE block_number <= {end_block};
        """
        with db_connector.engine.begin() as connection:
            connection.execute(text(query))

        if end_block == max_block:
            logger.info("Done deleting for %s up to %s.", table_name, max_block)
            break

        end_block = min(end_block + batch_size, max_block)


def run_check_and_delete(
    table_name: str,
    keep_postgres_days: int,
    query_start_override: Optional[str],
    diff_threshold: int,
    batch_size: int,
) -> None:
    """
        Run count check between DB and BQ, and delete rows from DB if within threshold.
    """
    
    bq_client = get_bq_client()
    db_connector = DbConnector()

    archival_date, query_start_date = get_archive_window(
        db_connector, keep_postgres_days, table_name, query_start_override
    )

    logger.info("Archival date (before here stuff gets deleted): %s", archival_date)
    logger.info("Query window start (from here on we start comparing -> current min block date in DB): %s", query_start_date)

    bq_count = get_bq_count(bq_client, table_name, archival_date, query_start_date)
    db_count = get_db_count(db_connector, table_name, archival_date, query_start_date)

    diff = abs(db_count - bq_count)
    logger.info("Count check for %s: DB=%s, BQ=%s, diff=%s", table_name, db_count, bq_count, diff)

    if diff > diff_threshold:
        message = (
            f"Archive delete DAG: mismatch for {table_name} "
            f"(DB={db_count}, BQ={bq_count}, diff={diff})"
        )
        logger.warning(message)
        send_discord_message(message)
        return

    max_block = get_bq_max_block(bq_client, table_name, archival_date)
    if max_block is None:
        logger.info("No max block found for %s; skipping deletion.", table_name)
        return

    start_block = get_db_min_block(db_connector, table_name)
    logger.info("Starting delete from block for %s: %s", table_name, start_block)
    if start_block is None:
        start_block = 0

    delete_blocks(db_connector, table_name, start_block, max_block, batch_size)


# --------------------------------------------------------------------------- #
# DAG definition
# --------------------------------------------------------------------------- #
@dag(
    default_args={
        "owner": "mseidl",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="utility_archive_delete",
    description="Validate archived raw tx and delete archived rows for flagged chains.",
    tags=["utility", "archive"],
    start_date=datetime(2024, 1, 1),
    schedule="45 16 * * *",
)
def utility_archive_delete():
    eligible_chains = get_eligible_chains() # chains flagged for delete and with existing table
    keep_days_default = 35 # keep this many days in Postgres
    query_start_override_default = None # e.g. '2024-01-01' to override query start date
    diff_threshold_default = 10 # max allowed difference between BQ and Postgres counts
    batch_size_default = 10_000 # delete this many blocks at once

    for chain in eligible_chains:
        origin_key = chain["origin_key"]
        table_name = chain["table_name"]
        
        @task(task_id=f"delete_archived_{origin_key}", execution_timeout=timedelta(hours=12))
        def run_delete(
            table: str = table_name,
            keep_days: int = keep_days_default,
            query_start_override: Optional[str] = query_start_override_default,
            diff_threshold: int = diff_threshold_default,
            batch_size: int = batch_size_default,
        ):
            logger.info("Starting delete task for table: %s", table)
            run_check_and_delete(
                table,
                keep_days,
                query_start_override,
                diff_threshold,
                batch_size,
            )

        run_delete()


utility_archive_delete_dag = utility_archive_delete()
