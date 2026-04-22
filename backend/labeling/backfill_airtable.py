"""
backfill_airtable.py
====================
Finds all contracts attested by the automated labeler that are missing from
Airtable "Label Pool Automated", fetches their latest 30-day metrics from the
DB, and writes them to Airtable.

Run from backend/ directory:
    python labeling/backfill_airtable.py [--days 30] [--dry-run]
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_THIS_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _THIS_DIR.parent
for _p in [str(_THIS_DIR), str(_BACKEND_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ATTESTER_GENERAL  = 'aDbf2b56995b57525Aa9a45df19091a2C5a2A970'
ATTESTER_TRADING  = 'ab81887E560e7C7b5993cE45D1c584d7FC22e898'


def fetch_attested_labels(engine) -> 'pd.DataFrame':
    """Pull all usage_category labels attested by our two wallets from public.labels."""
    import pandas as pd
    from sqlalchemy import text

    query = text(f"""
        WITH latest AS (
            SELECT
                address,
                chain_id,
                MAX(time) AS latest_time
            FROM public.labels
            WHERE attester IN (
                decode('{ATTESTER_GENERAL}', 'hex'),
                decode('{ATTESTER_TRADING}',  'hex')
            )
              AND tag_id = 'usage_category'
            GROUP BY address, chain_id
        )
        SELECT
            lbl.address,
            lbl.chain_id   AS caip2,
            lbl.tag_value  AS usage_category,
            encode(lbl.attester, 'hex') AS attester_hex,
            lbl.time
        FROM public.labels lbl
        JOIN latest ON lbl.address = latest.address
                    AND lbl.chain_id = latest.chain_id
                    AND lbl.time     = latest.latest_time
        WHERE lbl.tag_id = 'usage_category'
          AND lbl.attester IN (
                decode('{ATTESTER_GENERAL}', 'hex'),
                decode('{ATTESTER_TRADING}',  'hex')
          )
        ORDER BY lbl.chain_id, lbl.address
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    # Also pull contract_name and _comment for same rows
    name_query = text(f"""
        WITH attested_addrs AS (
            SELECT DISTINCT address, chain_id
            FROM public.labels
            WHERE attester IN (
                decode('{ATTESTER_GENERAL}', 'hex'),
                decode('{ATTESTER_TRADING}',  'hex')
            )
              AND tag_id = 'usage_category'
        )
        SELECT
            lbl.address,
            lbl.chain_id AS caip2,
            lbl.tag_id,
            lbl.tag_value
        FROM public.labels lbl
        JOIN attested_addrs a ON lbl.address = a.address AND lbl.chain_id = a.chain_id
        WHERE lbl.tag_id IN ('contract_name', '_comment', '_source')
          AND lbl.attester IN (
                decode('{ATTESTER_GENERAL}', 'hex'),
                decode('{ATTESTER_TRADING}',  'hex')
          )
    """)
    with engine.connect() as conn:
        df_meta = pd.read_sql(name_query, conn)

    if not df_meta.empty:
        df_pivot = df_meta.pivot_table(
            index=['address', 'caip2'], columns='tag_id', values='tag_value', aggfunc='first'
        ).reset_index()
        df = df.merge(df_pivot, on=['address', 'caip2'], how='left')

    return df


def fetch_latest_metrics(engine, addresses_caip2: list[tuple[str, str]], days: int = 30) -> 'pd.DataFrame':
    """
    Fetch latest txcount, gas_eth, avg_daa, rel_cost, success_rate for each (address, caip2).
    addresses_caip2: list of (address_hex_with_0x, caip2) tuples.
    """
    import pandas as pd
    from sqlalchemy import text

    if not addresses_caip2:
        return pd.DataFrame()

    # Build filter list
    pairs = [(addr.lower().replace('0x', '\\x'), caip2) for addr, caip2 in addresses_caip2]

    # Use sys_main_conf to map caip2 → origin_key for blockspace table join
    query = text(f"""
        SELECT
            '0x' || encode(cl.address, 'hex') AS address,
            smc.caip2,
            SUM(cl.txcount)                AS txcount,
            SUM(cl.gas_fees_eth)           AS gas_eth,
            ROUND(AVG(cl.daa))             AS avg_daa,
            AVG(cl.success_rate)           AS success_rate
        FROM sys_main_conf smc
        JOIN public.blockspace_fact_contract_level cl
            ON cl.origin_key = smc.origin_key
        WHERE cl.date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
          AND cl.date <  DATE_TRUNC('day', NOW())
        GROUP BY cl.address, smc.caip2
    """)
    with engine.connect() as conn:
        df_all = pd.read_sql(query, conn)

    # Filter to only the addresses we care about
    target_set = {(a.lower(), c) for a, c in addresses_caip2}
    mask = df_all.apply(lambda r: (r['address'].lower(), r['caip2']) in target_set, axis=1)
    return df_all[mask].copy()


def main():
    parser = argparse.ArgumentParser(description='Backfill Airtable with attested labels missing from Label Pool Automated')
    parser.add_argument('--days', type=int, default=30, help='Lookback window for metrics (default 30)')
    parser.add_argument('--dry-run', action='store_true', help='Print rows but do not write to Airtable')
    args = parser.parse_args()

    import pandas as pd
    from pyairtable import Api

    _backend_dir = Path(__file__).resolve().parent.parent
    for _p in [str(_backend_dir)]:
        if _p not in sys.path:
            sys.path.insert(0, _p)
    import src.misc.airtable_functions as at
    from src.db_connector import DbConnector

    db = DbConnector()
    engine = db.engine

    # 1. Pull all attested labels from DB
    logger.info("Fetching attested labels from public.labels...")
    df_attested = fetch_attested_labels(engine)
    logger.info(f"Found {len(df_attested)} attested labels")

    if df_attested.empty:
        logger.info("Nothing attested yet. Exiting.")
        return

    # 2. Pull existing Airtable rows
    api_key = os.getenv('AIRTABLE_API_KEY')
    base_id = os.getenv('AIRTABLE_BASE_ID')
    if not api_key or not base_id:
        logger.error("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set")
        return

    api = Api(api_key)
    table = api.table(base_id, 'Label Pool Automated')
    chains_table = api.table(base_id, 'Chains')
    cat_table = api.table(base_id, 'Sub Categories')

    logger.info("Reading existing Airtable rows...")
    df_existing = at.read_airtable(table)

    # 3. Find missing rows
    if not df_existing.empty and 'address' in df_existing.columns and 'caip2' in df_existing.columns:
        existing_pairs = set(
            zip(df_existing['address'].str.lower(), df_existing['caip2'])
        )
        mask_missing = ~df_attested.apply(
            lambda r: (str(r['address']).lower(), r['caip2']) in existing_pairs, axis=1
        )
        df_missing = df_attested[mask_missing].copy()
    else:
        df_missing = df_attested.copy()

    logger.info(f"{len(df_missing)} rows missing from Airtable")

    if df_missing.empty:
        logger.info("Airtable is already up to date.")
        return

    # 4. Fetch latest metrics for missing rows
    logger.info(f"Fetching {args.days}-day metrics for {len(df_missing)} contracts...")
    pairs = list(zip(df_missing['address'].tolist(), df_missing['caip2'].tolist()))
    df_metrics = fetch_latest_metrics(engine, pairs, days=args.days)

    df_out = df_missing.merge(df_metrics, on=['address', 'caip2'], how='left')

    # 5. Compute rel_cost (avg contract txcost vs chain median)
    # Approximate: rel_cost = (gas_eth / txcount) / chain_median_txcost - 1
    # We skip rel_cost here — it requires fact_kpis join; leave as 0
    if 'rel_cost' not in df_out.columns:
        df_out['rel_cost'] = None

    # 6. Derive attester address for display
    df_out['attester'] = df_out['attester_hex'].apply(
        lambda h: f"0x{h}" if pd.notna(h) else '0xaDbf2b56995b57525Aa9a45df19091a2C5a2A970'
    )

    # 7. Map caip2 → Airtable Chains record ID
    df_chains = at.read_airtable(chains_table)
    df_out = df_out.replace({'caip2': df_chains.set_index('caip2')['id']})
    df_out['caip2'] = df_out['caip2'].apply(
        lambda x: [x] if pd.notna(x) and str(x).startswith('rec') else []
    )
    df_out = df_out.rename(columns={'caip2': 'origin_key'})

    # 8. Map usage_category → Airtable Sub Categories record ID
    df_cat = at.read_airtable(cat_table)
    df_out = df_out.replace({'usage_category': df_cat.set_index('category_id')['id']})
    df_out['usage_category'] = df_out['usage_category'].apply(
        lambda x: [x] if pd.notna(x) and str(x).startswith('rec') else []
    )

    # 9. Add sentinel flag
    df_out['is_protocol_contract_likely'] = False  # backfilled rows — unknown, default False

    # 10. Rename _source if present
    if '_source' not in df_out.columns and 'source' in df_out.columns:
        df_out = df_out.rename(columns={'source': '_source'})
    if '_source' not in df_out.columns:
        df_out['_source'] = 'gtp-automated_labeler_v1'

    # 11. Keep only Airtable columns
    AIRTABLE_COLS = {
        'address', 'origin_key', 'contract_name', 'usage_category',
        '_comment', '_source', 'attester', 'is_protocol_contract_likely',
        'txcount', 'gas_eth', 'avg_daa', 'rel_cost',
    }
    df_out = df_out.drop(columns=[c for c in df_out.columns if c not in AIRTABLE_COLS])

    logger.info(f"\nSample of rows to write:\n{df_out[['address', 'usage_category', 'txcount']].head(10).to_string()}")

    if args.dry_run:
        logger.info(f"[dry-run] Would write {len(df_out)} rows to Airtable.")
        return

    at.push_to_airtable(table, df_out)
    logger.info(f"Wrote {len(df_out)} backfill rows to 'Label Pool Automated'.")


if __name__ == '__main__':
    main()
