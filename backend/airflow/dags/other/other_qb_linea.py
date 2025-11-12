import sys
import getpass

sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd
from airflow.decorators import dag, task
from src.adapters.adapter_logs import AdapterLogs
from src.db_connector import DbConnector
from src.misc.airflow_utils import alert_via_webhook
from web3 import Web3

RPC_URL = "https://linea.drpc.org"
CHAIN_NAME = "linea"
CHUNK_SIZE = 10_000
ORIGIN_KEY = "linea"

INVOICE_TOPIC = "0xc6da70dbb809f46821a66136527962f1e93ac500c61f1878c39417b2fa8c35a6"
BURN_TOPIC = "0x0e2419f2e998f267b7ebbe863f0c8144b9bd6741dafa3386eae42e2a460f0167"

INVOICE_LOOKBACK_DAYS = 3
BURN_LOOKBACK_DAYS = 3

def _build_logs_adapter() -> Tuple[AdapterLogs, Web3]:
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    return AdapterLogs(w3), w3


def _resolve_from_block(db_connector: DbConnector, lookback_days: int) -> int:
    target_date = datetime.utcnow().date() - timedelta(days=lookback_days)
    from_block = db_connector.get_first_block_of_the_day(CHAIN_NAME, target_date)
    if from_block is None:
        raise ValueError(f"Failed to resolve from_block for {CHAIN_NAME} with lookback {lookback_days} days")
    return from_block


def _format_invoice_logs(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df

    df = raw_df[raw_df["topics"].apply(lambda x: isinstance(x, (list, tuple)) and len(x) >= 4)].copy()
    if df.empty:
        return df

    df["date"] = df["topics"].apply(lambda x: int(x[2], 16))
    df["date"] = pd.to_datetime(df["date"], unit="s").dt.strftime("%Y-%m-%d")
    df["qb_amountPaid_eth"] = df["data"].apply(lambda x: int(x[:66], 16) / 10**18)
    df["qb_amountRequested_eth"] = df["data"].apply(lambda x: int(x[66:], 16) / 10**18)
    df["origin_key"] = ORIGIN_KEY

    melted = df[["origin_key", "date", "qb_amountPaid_eth", "qb_amountRequested_eth"]].melt(
        id_vars=["origin_key", "date"],
        value_vars=["qb_amountPaid_eth", "qb_amountRequested_eth"],
        var_name="metric_key",
        value_name="value",
    )

    return melted.set_index(["origin_key", "metric_key", "date"])


def _block_numbers_to_dates(block_numbers: pd.Series, w3: Web3) -> pd.Series:
    if block_numbers.empty:
        return block_numbers

    block_numbers_int = block_numbers.astype(int)
    unique_blocks = block_numbers_int.unique().tolist()
    timestamp_cache = {}
    for block in unique_blocks:
        block_data = w3.eth.get_block(block)
        timestamp_cache[block] = pd.to_datetime(block_data["timestamp"], unit="s").strftime("%Y-%m-%d")

    return block_numbers_int.map(timestamp_cache)


def _format_burn_logs(raw_df: pd.DataFrame, w3: Web3) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df

    df = raw_df.copy()
    df["qb_ethBurnt_eth"] = df["data"].apply(lambda x: int(x[:66], 16) / 10**18)
    df["qb_lineaTokensBridged_linea"] = df["data"].apply(lambda x: int(x[66:], 16) / 10**18)
    df["date"] = _block_numbers_to_dates(df["blockNumber"], w3)
    df["origin_key"] = ORIGIN_KEY

    grouped = (
        df[["origin_key", "date", "qb_ethBurnt_eth", "qb_lineaTokensBridged_linea"]]
        .groupby(["origin_key", "date"], as_index=False)
        .sum()
    )

    melted = grouped.melt(
        id_vars=["origin_key", "date"],
        value_vars=["qb_ethBurnt_eth", "qb_lineaTokensBridged_linea"],
        var_name="metric_key",
        value_name="value",
    )

    return melted.set_index(["origin_key", "metric_key", "date"])


@dag(
    default_args={
        "owner": "lorenz",
        "retries": 1,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="other_qb_linea",
    description="Linea chain burns and invoices",
    tags=["other"],
    start_date=datetime(2025, 11, 11),
    schedule="11 2 * * *",  # Every day at 02:11
)
def run_dag():

    @task(task_id="process_invoice_logs")
    def process_invoice_logs():
        adapter, w3 = _build_logs_adapter()
        db_connector = DbConnector()
        from_block = _resolve_from_block(db_connector, INVOICE_LOOKBACK_DAYS)
        to_block = w3.eth.block_number

        extract_params = {
            "from_block": from_block,
            "to_block": to_block,
            "chunk_size": CHUNK_SIZE,
            "topics": [
                INVOICE_TOPIC,
                None,
                None,
                None,
            ],
        }

        logs = adapter.extract(extract_params)
        df = adapter.turn_logs_into_df(logs)
        if df.empty:
            print("No invoice logs found for the current window.")
            return

        formatted_df = _format_invoice_logs(df)
        if formatted_df.empty:
            print("Invoice logs could not be formatted; skipping upsert.")
            return

        db_connector.upsert_table("fact_kpis", formatted_df)
        print(f"{len(formatted_df)} invoice logs processed and upserted successfully.")

    @task(task_id="process_burn_bridge_logs")
    def process_burn_bridge_logs():
        adapter, w3 = _build_logs_adapter()
        db_connector = DbConnector()
        from_block = _resolve_from_block(db_connector, BURN_LOOKBACK_DAYS)
        to_block = w3.eth.block_number

        extract_params = {
            "from_block": from_block,
            "to_block": to_block,
            "chunk_size": CHUNK_SIZE,
            "topics": [BURN_TOPIC],
        }

        logs = adapter.extract(extract_params)
        df = adapter.turn_logs_into_df(logs)
        if df.empty:
            print("No burn/bridge logs found for the current window.")
            return

        formatted_df = _format_burn_logs(df, w3)
        if formatted_df.empty:
            print("Burn/bridge logs could not be formatted; skipping upsert.")
            return

        db_connector.upsert_table("fact_kpis", formatted_df)
        print(f"{len(formatted_df)} burn logs processed and upserted successfully.")

    process_invoice_logs() >> process_burn_bridge_logs()


run_dag()
