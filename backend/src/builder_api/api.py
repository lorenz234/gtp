from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException, Query


DEFAULT_BASE_API_URL = os.getenv("BUILDER_BLOCKSCOUT_API_URL", "https://eth.blockscout.com/api")
LABELS_URL = os.getenv(
    "BUILDER_LABELS_URL",
    "https://api.growthepie.com/v1/oli/project_labels.parquet",
)
LABELS_SOURCE = os.getenv("BUILDER_LABELS_SOURCE", "parquet").lower()
LABELS_REFRESH_SECONDS = int(os.getenv("BUILDER_LABELS_REFRESH_SECONDS", "86400"))

_LABELS_CACHE: Optional[pd.DataFrame] = None
_LABELS_LOADED_AT = 0.0


def _lower_addr(a: Optional[str]) -> Optional[str]:
    if not a:
        return None
    a = a.strip()
    return a.lower()


def fetch_blockscout_txlist(
    wallet: str,
    base_api_url: str,
    *,
    keep_inbound_tx: bool = False,
    startblock: Optional[int] = None,
    endblock: Optional[int] = None,
    sort: str = "asc",
    offset: int = 1000,
    max_txs: int = 10_000,
    sleep_s: float = 0.2,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch up to max_txs normal transactions from Blockscout txlist.

    Blockscout txlist supports pagination via page+offset and is capped at 10k results.
    """
    wallet = _lower_addr(wallet)
    if not wallet or not wallet.startswith("0x") or len(wallet) != 42:
        raise ValueError(f"Invalid wallet address: {wallet}")

    if offset <= 0 or offset > 10_000:
        raise ValueError("--offset should be 1..10000 (practically 100..2000 is sane)")

    all_rows: List[Dict[str, Any]] = []
    page = 1

    while len(all_rows) < max_txs:
        params = {
            "module": "account",
            "action": "txlist",
            "address": wallet,
            "page": page,
            "offset": min(offset, max_txs - len(all_rows)),
            "sort": sort,
        }
        if startblock is not None:
            params["startblock"] = int(startblock)
        if endblock is not None:
            params["endblock"] = int(endblock)
        if api_key:
            params["apikey"] = api_key

        resp = requests.get(base_api_url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        result = data.get("result")

        if not isinstance(result, list) or len(result) == 0:
            break

        all_rows.extend(result)

        if len(result) < params["offset"]:
            break

        page += 1
        time.sleep(sleep_s)

        if len(all_rows) >= 10_000:
            break

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    for col in ["to", "from", "contractAddress", "gasUsed", "timeStamp", "hash", "isError"]:
        if col not in df.columns:
            df[col] = None

    df["gasUsed"] = pd.to_numeric(df["gasUsed"], errors="coerce").fillna(0).astype("int64")

    df["to_effective"] = df["to"].where(df["to"].astype(str).str.len() > 0, df["contractAddress"])
    df["to_effective"] = df["to_effective"].apply(_lower_addr)

    df = df[df["to_effective"].notna()].copy()

    if not keep_inbound_tx:
        df = df[df["from"].apply(_lower_addr) == wallet].copy()

    return df


def aggregate_interactions(tx_df: pd.DataFrame) -> pd.DataFrame:
    if tx_df.empty:
        return pd.DataFrame(columns=["to_address", "tx_count", "total_gas_used"])

    agg = (
        tx_df.groupby("to_effective", as_index=False)
        .agg(tx_count=("hash", "count"), total_gas_used=("gasUsed", "sum"))
        .rename(columns={"to_effective": "to_address"})
    )

    agg = agg.sort_values(["tx_count", "total_gas_used"], ascending=[False, False]).reset_index(drop=True)
    return agg


def _load_contract_labels_from_parquet() -> pd.DataFrame:
    contracts_df = pd.read_parquet(LABELS_URL)
    contracts_df["address"] = contracts_df["address"].apply(_lower_addr)
    return contracts_df


def _load_contract_labels_from_db() -> pd.DataFrame:
    from src.db_connector import DbConnector

    db_connector = DbConnector()
    query = """
        SELECT concat('0x',encode(address, 'hex')) as address, origin_key, caip2, contract_name, owner_project, usage_category
        FROM public.vw_oli_label_pool_gold_pivoted_v2
        where owner_project is not null
    """

    contracts_df = db_connector.execute_query(query, load_df=True)
    contracts_df["address"] = contracts_df["address"].apply(_lower_addr)
    return contracts_df


def load_contract_labels(force_reload: bool = False) -> pd.DataFrame:
    global _LABELS_CACHE, _LABELS_LOADED_AT

    if not force_reload and _LABELS_CACHE is not None:
        if (time.time() - _LABELS_LOADED_AT) < LABELS_REFRESH_SECONDS:
            return _LABELS_CACHE

    if LABELS_SOURCE == "db":
        contracts_df = _load_contract_labels_from_db()
    else:
        contracts_df = _load_contract_labels_from_parquet()

    _LABELS_CACHE = contracts_df
    _LABELS_LOADED_AT = time.time()
    return contracts_df


def _records_for_json(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df.empty:
        return []
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


app = FastAPI(title="Builder API")


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/project-interactions")
def builder_interactions(
    wallet: str = Query(..., description="Wallet address to analyze"),
    base_api_url: str = Query(DEFAULT_BASE_API_URL, description="Blockscout API base URL"),
    keep_inbound_tx: bool = Query(False, description="Include inbound transactions"),
    startblock: Optional[int] = Query(None, description="Start block"),
    endblock: Optional[int] = Query(None, description="End block"),
    sort: str = Query("asc", description="Blockscout sort order"),
    offset: int = Query(1000, description="Blockscout page size"),
    max_txs: int = Query(10_000, description="Maximum transactions to fetch"),
    sleep_s: float = Query(0.2, description="Sleep between page requests"),
    api_key: Optional[str] = Query(None, description="Blockscout API key"),
) -> Dict[str, Any]:
    try:
        contracts_df = load_contract_labels()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to load labels: {exc}") from exc

    try:
        tx_list = fetch_blockscout_txlist(
            wallet,
            base_api_url,
            keep_inbound_tx=keep_inbound_tx,
            startblock=startblock,
            endblock=endblock,
            sort=sort,
            offset=offset,
            max_txs=max_txs,
            sleep_s=sleep_s,
            api_key=api_key,
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Blockscout request failed: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    tx_agg = aggregate_interactions(tx_list)
    if tx_agg.empty:
        return {"wallet": _lower_addr(wallet), "count": 0, "data": []}

    tx_agg_labeled = (
        tx_agg.merge(
            contracts_df,
            how="left",
            left_on="to_address",
            right_on="address",
        )
        .drop(columns=["address"])
        .sort_values(["tx_count", "total_gas_used"], ascending=[False, False])
        .reset_index(drop=True)
    )

    data_records = _records_for_json(tx_agg_labeled)
    return {"wallet": _lower_addr(wallet), "count": len(data_records), "data": data_records}


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)
