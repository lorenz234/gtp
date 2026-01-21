from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security, status
from fastapi.security.api_key import APIKeyHeader


DEFAULT_BASE_API_URL = os.getenv("BUILDER_BLOCKSCOUT_API_URL", "https://eth.blockscout.com/api")
DEFAULT_CHAIN_KEY = "ethereum"
API_KEY_HEADER = "x-api-key"
API_KEY_PREFIX_NS = "builder_"
API_KEY_PEPPER = os.getenv("BUILDER_KEY_PEPPER")
API_KEY_JSON = os.getenv("BUILDER_API_KEYS_JSON", "")

BLOCKSCOUT_CHAINS: Dict[str, Dict[str, Any]] = {
    "ethereum": {
        "key": "ethereum",
        "chain_id": 1,
        "name": "Ethereum",
        "explorer_url": "https://eth.blockscout.com/",
    },
    "arbitrum": {
        "key": "arbitrum",
        "chain_id": 42161,
        "name": "Arbitrum One",
        "explorer_url": "https://arbitrum.blockscout.com/",
    },
    "optimism": {
        "key": "optimism",
        "chain_id": 10,
        "name": "OP Mainnet",
        "explorer_url": "https://explorer.optimism.io/",
    },
    "megaeth": {
        "key": "megaeth",
        "chain_id": 4326,
        "name": "MegaETH",
        "explorer_url": "https://megaeth.blockscout.com/",
    },
    "linea": {
        "key": "linea",
        "chain_id": 59144,
        "name": "Linea",
        "explorer_url": "https://explorer.linea.build/",
    },
    "taiko": {
        "key": "taiko",
        "chain_id": 167000,
        "name": "Taiko",
        "explorer_url": "https://blockscout.mainnet.taiko.xyz/",
    },
    "base": {
        "key": "base",
        "chain_id": 8453,
        "name": "Base",
        "explorer_url": "https://base.blockscout.com/",
    },
    "zksync_era": {
        "key": "zksync_era",
        "chain_id": 324,
        "name": "zkSync Era",
        "explorer_url": "https://zksync.blockscout.com/",
    },
    "scroll": {
        "key": "scroll",
        "chain_id": 534352,
        "name": "Scroll",
        "explorer_url": "https://scroll.blockscout.com/",
    },
    "arbitrum_nova": {
        "key": "arbitrum_nova",
        "chain_id": 42170,
        "name": "Arbitrum Nova",
        "explorer_url": "https://arbitrum-nova.blockscout.com/",
    },
}
LABELS_URL = os.getenv(
    "BUILDER_LABELS_URL",
    "https://api.growthepie.com/v1/oli/project_labels.parquet",
)
LABELS_SOURCE = os.getenv("BUILDER_LABELS_SOURCE", "parquet").lower()
LABELS_REFRESH_SECONDS = int(os.getenv("BUILDER_LABELS_REFRESH_SECONDS", "86400"))

_LABELS_CACHE: Optional[pd.DataFrame] = None
_LABELS_LOADED_AT = 0.0

def _parse_api_key_json(raw: str) -> Dict[str, str]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"BUILDER_API_KEYS_JSON is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError("BUILDER_API_KEYS_JSON must be a JSON object of name: key entries")
    keys: Dict[str, str] = {}
    for name, value in data.items():
        if not isinstance(value, str):
            raise RuntimeError(f"BUILDER_API_KEYS_JSON value for '{name}' must be a string")
        value = value.strip()
        if not value:
            continue
        if ":" in value and not value.startswith(API_KEY_PREFIX_NS):
            prefix, key_hash = value.split(":", 1)
            prefix = prefix.strip()
            key_hash = key_hash.strip()
            if prefix.startswith(API_KEY_PREFIX_NS):
                prefix = prefix[len(API_KEY_PREFIX_NS):]
            if not prefix or not key_hash:
                raise RuntimeError(f"BUILDER_API_KEYS_JSON value for '{name}' must be full api key or prefix:hash")
            keys[prefix] = key_hash
            continue
        parsed = hash_presented_key(value)
        if not parsed:
            raise RuntimeError(
                f"BUILDER_API_KEYS_JSON value for '{name}' must be full api key or prefix:hash",
            )
        prefix, key_hash = parsed
        keys[prefix] = key_hash
    return keys


if not API_KEY_PEPPER:
    raise RuntimeError("BUILDER_KEY_PEPPER env var must be set")

_API_KEY_HASHES = _parse_api_key_json(API_KEY_JSON)
if not _API_KEY_HASHES:
    raise RuntimeError(
        "Set BUILDER_API_KEYS (prefix:hash list) or BUILDER_API_KEYS_JSON (name: key map)",
    )

api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


def hash_presented_key(presented: str) -> Optional[tuple[str, str]]:
    if not presented.startswith(API_KEY_PREFIX_NS):
        return None
    try:
        rest = presented[len(API_KEY_PREFIX_NS):]
        prefix, secret = rest.split(".", 1)
    except ValueError:
        return None
    key_hash = hashlib.sha256((f"{prefix}.{secret}{API_KEY_PEPPER}").encode()).hexdigest()
    return prefix, key_hash


def get_api_key(
    request: Request,
    api_key: Optional[str] = Security(api_key_header),
) -> str:
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key - Please provide x-api-key header",
        )

    parsed = hash_presented_key(api_key)
    if not parsed:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bad API key format - Please provide a valid x-api-key header",
        )

    prefix, presented_hash = parsed
    stored_hash = _API_KEY_HASHES.get(prefix)
    if not stored_hash or not hmac.compare_digest(stored_hash, presented_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or revoked API key",
        )

    request.state.api_key_prefix = prefix
    return prefix


def _lower_addr(a: Optional[str]) -> Optional[str]:
    if not a:
        return None
    a = a.strip()
    return a.lower()


def _explorer_to_api_url(explorer_url: str) -> str:
    return f"{explorer_url.rstrip('/')}/api"


def _resolve_chain(chain: Optional[str]) -> Dict[str, Any]:
    if not chain:
        chain = DEFAULT_CHAIN_KEY
    chain_key = chain.strip().lower()
    if chain_key in BLOCKSCOUT_CHAINS:
        cfg = BLOCKSCOUT_CHAINS[chain_key].copy()
        cfg["base_api_url"] = _explorer_to_api_url(cfg["explorer_url"])
        if cfg["key"] == DEFAULT_CHAIN_KEY:
            cfg["base_api_url"] = DEFAULT_BASE_API_URL
        return cfg
    if chain_key.isdigit():
        for cfg in BLOCKSCOUT_CHAINS.values():
            if str(cfg["chain_id"]) == chain_key:
                cfg = cfg.copy()
                cfg["base_api_url"] = _explorer_to_api_url(cfg["explorer_url"])
                return cfg
    supported = ", ".join(sorted(BLOCKSCOUT_CHAINS.keys()))
    raise HTTPException(status_code=400, detail=f"Unsupported chain '{chain}'. Supported: {supported}")


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


app = FastAPI(title="Builder API", dependencies=[Depends(get_api_key)])


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/chains")
def builder_chains() -> Dict[str, Any]:
    data = []
    for cfg in BLOCKSCOUT_CHAINS.values():
        cfg = cfg.copy()
        ## remove explorer_url
        del cfg["explorer_url"]
        
        # cfg["base_api_url"] = _explorer_to_api_url(cfg["explorer_url"])
        # if cfg["key"] == DEFAULT_CHAIN_KEY:
        #     cfg["base_api_url"] = DEFAULT_BASE_API_URL
        data.append(cfg)
    return {"count": len(data), "data": data}


@app.get("/wallet/contract-interactions")
def wallet_contract_interactions(
    wallet: str = Query(..., description="Wallet address to analyze"),
    chain: Optional[str] = Query(DEFAULT_CHAIN_KEY, description="Chain key or chain id. Default: ethereum"),
    startblock: Optional[int] = Query(None, description="Start block"),
    endblock: Optional[int] = Query(None, description="End block"),
    max_txs: int = Query(10_000, description="Maximum transactions to fetch"),
) -> Dict[str, Any]:
    try:
        contracts_df = load_contract_labels()
        ## filter contracts df to chain only
        chain_cfg = _resolve_chain(chain)
        contracts_df = contracts_df[contracts_df["caip2"] == f"eip155:{chain_cfg['chain_id']}"].copy()    
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to load labels: {exc}") from exc
    
    try:
        tx_list = fetch_blockscout_txlist(
            wallet,
            chain_cfg["base_api_url"],
            keep_inbound_tx=False,
            startblock=startblock,
            endblock=endblock,
            max_txs=max_txs,
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
    
    tx_agg_labeled = tx_agg_labeled[['to_address', 'tx_count', 'total_gas_used', 'contract_name', 'owner_project', 'usage_category']]

    data_records = _records_for_json(tx_agg_labeled)
    return {"wallet": _lower_addr(wallet), "count": len(data_records), "data": data_records}

@app.get("/wallet/project-interactions")
def wallet_project_interactions(
    wallet: str = Query(..., description="Wallet address to analyze"),
    chain: Optional[str] = Query(DEFAULT_CHAIN_KEY, description="Chain key or chain id. Default: ethereum"),
    startblock: Optional[int] = Query(None, description="Start block"),
    endblock: Optional[int] = Query(None, description="End block"),
    max_txs: int = Query(10_000, description="Maximum transactions to fetch"),
) -> Dict[str, Any]:
    try:
        contracts_df = load_contract_labels()
        ## filter contracts df to chain only
        chain_cfg = _resolve_chain(chain)
        contracts_df = contracts_df[contracts_df["caip2"] == f"eip155:{chain_cfg['chain_id']}"].copy()    
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to load labels: {exc}") from exc
    
    try:
        tx_list = fetch_blockscout_txlist(
            wallet,
            chain_cfg["base_api_url"],
            keep_inbound_tx=False,
            startblock=startblock,
            endblock=endblock,
            max_txs=max_txs,
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
    
    ## aggregate by owner_project
    tx_agg_labeled_grouped = (
        tx_agg_labeled.groupby("owner_project", as_index=False)
        .agg(
            tx_count=("tx_count", "sum"),
            total_gas_used=("total_gas_used", "sum"),
        )
        .sort_values(["tx_count", "total_gas_used"], ascending=[False, False])
        .reset_index(drop=True)
    )

    tx_agg_labeled_grouped = tx_agg_labeled_grouped[['owner_project', 'tx_count', 'total_gas_used']]
    
    data_records = _records_for_json(tx_agg_labeled_grouped)
    return {"wallet": _lower_addr(wallet), "count": len(data_records), "data": data_records}


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8080"))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)
