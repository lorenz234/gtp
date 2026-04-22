"""
Contract Classifier Tool
========================
Classify a single contract by address + chain. Enriches via Blockscout,
GitHub, and Tenderly, then runs the AI classifier and prints the result.

USAGE
-----
Run from backend/labeling/ or backend/:

    python backend/labeling/classify_contract_tool.py \\
        --address 0x1e04602a8ba9cd99ae30781e0d1933f68fa6398c \\
        --chain base

Optional metric overrides (default to 0/None if not provided):
    --txcount 50000 --daa 120 --gas-eth 0.5 --success-rate 0.23 --rel-cost 3.2

REQUIREMENTS
------------
Same as automated_labeler.py — GEMINI_API_KEY, TENDERLY_ACCESS_KEY, etc. in .env
"""

import asyncio
import aiohttp
import argparse
import json
import logging
import os
import random
import ssl
import sys
from pathlib import Path

# ── Path setup ────────────────────────────────────────────────────────────────
_THIS_DIR = Path(__file__).resolve().parent      # …/backend/labeling
_BACKEND_DIR = _THIS_DIR.parent                  # …/backend

for _p in [str(_THIS_DIR), str(_BACKEND_DIR)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dotenv import load_dotenv
load_dotenv(dotenv_path=_BACKEND_DIR / ".env", override=False)
load_dotenv(dotenv_path=_BACKEND_DIR.parent / ".env", override=False)
load_dotenv(override=False)

# ── Logging: minimal — we print our own formatted output ─────────────────────
logging.basicConfig(
    level=logging.WARNING,
    format='%(levelname)s %(name)s: %(message)s',
)
# Show classifier prompt/response logs when --verbose
_classifier_logger = logging.getLogger('ai_classifier')

# ── Imports ───────────────────────────────────────────────────────────────────
from concurrent_contract_analyzer import (
    BlockscoutAPI_Async,
    GitHubAPI_Async,
    TenderlyAPI_Async,
    Config,
    create_ssl_context,
    rate_limit_manager,
)
from ai_classifier import classify_contract, CHAIN_TO_EIP155, _oli_cache
import ai_classifier as _ai_classifier


def _load_chain_config() -> None:
    """Populate Config and ai_classifier chain mappings from sys_main_conf."""
    try:
        for _p in [str(_BACKEND_DIR)]:
            if _p not in sys.path:
                sys.path.insert(0, _p)
        from src.db_connector import DbConnector
        db = DbConnector()
        Config.load_from_db(db.engine)
        _ai_classifier.CHAIN_TO_EIP155.update({
            ok: f"eip155:{chain_id}"
            for ok, chain_id in Config.SUPPORTED_CHAINS.items()
        })
    except Exception as e:
        logging.getLogger(__name__).warning(f"Could not load chain config from DB: {e}")


_load_chain_config()


def _ssl_ctx():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# ── DB metrics fetch ──────────────────────────────────────────────────────────

def fetch_metrics_from_db(address: str, origin_key: str, days: int) -> dict:
    """Query blockspace_fact_contract_level for a single contract's metrics.

    Returns a metrics dict matching what automated_labeler.py produces.
    Returns None if DB is unavailable or contract not found.
    """
    try:
        for _p in [str(_BACKEND_DIR)]:
            if _p not in sys.path:
                sys.path.insert(0, _p)
        from src.db_connector import DbConnector
        from sqlalchemy import text
    except ImportError:
        return None

    try:
        db = DbConnector()
        # Strip 0x prefix — DB stores address as bytes
        addr_hex = address.lower().removeprefix('0x')

        query = text(f"""
            WITH chain_median AS (
                SELECT
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_tx_fee) AS median_tx_fee
                FROM public.blockspace_fact_contract_level
                WHERE origin_key = :origin_key
                  AND date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
                  AND date < DATE_TRUNC('day', NOW())
                  AND median_tx_fee IS NOT NULL
            )
            SELECT
                SUM(cl.txcount)                                                          AS txcount,
                SUM(cl.gas_fees_eth)                                                     AS gas_eth,
                ROUND(AVG(cl.daa))                                                       AS avg_daa,
                ROUND(
                    SUM(cl.success_rate * cl.txcount)::numeric / NULLIF(SUM(cl.txcount), 0),
                    3
                )                                                                        AS success_rate,
                ROUND(
                    (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cl.median_tx_fee)
                     / NULLIF(cm.median_tx_fee, 0))::numeric,
                    3
                )                                                                        AS rel_cost
            FROM public.blockspace_fact_contract_level cl
            CROSS JOIN chain_median cm
            WHERE cl.address = decode(:addr_hex, 'hex')
              AND cl.origin_key = :origin_key
              AND cl.date >= DATE_TRUNC('day', NOW() - INTERVAL '{days} days')
              AND cl.date < DATE_TRUNC('day', NOW())
            GROUP BY cm.median_tx_fee
        """)

        with db.engine.connect() as conn:
            row = conn.execute(query, {'addr_hex': addr_hex, 'origin_key': origin_key}).fetchone()

        if row is None:
            return None

        txcount = int(row.txcount or 0)
        gas_eth = float(row.gas_eth or 0)
        return {
            'txcount': txcount,
            'gas_eth': gas_eth,
            'avg_daa': float(row.avg_daa or 0),
            'avg_gas_per_tx': round(gas_eth / txcount, 8) if txcount else 0,
            'rel_cost': float(row.rel_cost or 0),
            'success_rate': float(row.success_rate) if row.success_rate is not None else None,
            'day_range': days,
        }
    except Exception as e:
        print(f"      [warn] DB metrics fetch failed: {e}")
        return None


# ── Enrichment ────────────────────────────────────────────────────────────────

async def enrich_single(address: str, origin_key: str, session: aiohttp.ClientSession) -> dict:
    chain_id = Config.SUPPORTED_CHAINS.get(origin_key)
    if not chain_id:
        print(f"[!] Unknown chain '{origin_key}'. Supported: {', '.join(sorted(Config.SUPPORTED_CHAINS))}")
        sys.exit(1)

    blockscout = BlockscoutAPI_Async(
        session=session,
        api_keys=Config.BLOCKSCOUT_API_KEYS,
        chain_config=Config.CHAIN_EXPLORER_CONFIG,
    )
    github = GitHubAPI_Async(
        session=session,
        headers=Config.get_github_headers(),
        excluded_repos=Config.EXCLUDED_REPOS,
    )
    tenderly = TenderlyAPI_Async(session=session)

    print(f"\n[1/5] Blockscout — fetching info + transactions + logs + token-transfers in parallel...")

    async def _get_info():
        try:
            return await blockscout.get_contract_info(address, chain_id)
        except Exception as e:
            print(f"      [warn] contract_info failed: {e}")
            return {}

    async def _get_txs():
        try:
            return await blockscout.get_transactions(address, chain_id, limit=30)
        except Exception as e:
            print(f"      [warn] transactions failed: {e}")
            return []

    async def _get_logs():
        try:
            return await blockscout.get_address_logs(address, chain_id, limit=50)
        except Exception as e:
            print(f"      [warn] address logs failed: {e}")
            return []

    async def _get_token_transfers():
        try:
            return await blockscout.get_token_transfers(address, chain_id, limit=20)
        except Exception as e:
            print(f"      [warn] token_transfers failed: {e}")
            return []

    bs_info, txs, address_logs, token_transfers = await asyncio.gather(
        _get_info(), _get_txs(), _get_logs(), _get_token_transfers()
    )

    all_hashes = [t['hash'] for t in txs if t.get('hash')]
    sample_hashes = random.sample(all_hashes, min(5, len(all_hashes)))
    print(f"      name={bs_info.get('contract_name', 'n/a')!r}  "
          f"verified={bs_info.get('is_verified')}  proxy={bs_info.get('is_proxy')}")
    print(f"      sampled {len(sample_hashes)} tx hashes  |  {len(address_logs)} logs  |  "
          f"{len(token_transfers)} token-transfer types")

    print(f"\n[2/5] GitHub — searching contract address...")
    has_repo, repo_count = False, 0
    is_verified = bs_info.get('is_verified', False)
    bs_name = bs_info.get('contract_name', '')
    if not is_verified or not bs_name:
        try:
            has_repo, repo_count = await github.search_contract_address(address)
            print(f"      has_repo={has_repo}  repo_count={repo_count}")
        except Exception as e:
            print(f"      [warn] GitHub search failed: {e}")
    else:
        print(f"      skipped (verified contract with known name)")

    print(f"\n[3/4] Tenderly — tracing {len(sample_hashes)} transactions...")
    traces = []
    if sample_hashes:
        trace_tasks = [tenderly.trace_transaction(h, chain_id) for h in sample_hashes]
        trace_results = await asyncio.gather(*trace_tasks, return_exceptions=True)
        for r in trace_results:
            if isinstance(r, dict) and r:
                traces.append(r)
        print(f"      got {len(traces)} traces")
    else:
        print(f"      no hashes to trace")

    return {
        'blockscout': bs_info,
        'github': {'has_valid_repo': has_repo, 'repo_count': repo_count},
        'traces': traces,
        'address_logs': address_logs,
        'token_transfers': token_transfers,
    }


# ── Output formatting ─────────────────────────────────────────────────────────

def _bar(confidence: float) -> str:
    filled = round(confidence * 20)
    return f"[{'█' * filled}{'░' * (20 - filled)}] {confidence:.0%}"


def print_result(address: str, origin_key: str, enriched: dict, label: dict, metrics: dict):
    chain_eip = CHAIN_TO_EIP155.get(origin_key, f"eip155:?")
    bs = enriched.get('blockscout', {})
    gh = enriched.get('github', {})
    traces = enriched.get('traces', [])

    sep = "─" * 60
    print(f"\n{sep}")
    print(f"  CONTRACT CLASSIFICATION RESULT")
    print(sep)
    print(f"  Address   : {address}")
    print(f"  Chain     : {origin_key} ({chain_eip})")
    print(sep)

    print(f"\n  ENRICHMENT")
    print(f"  Blockscout name : {bs.get('contract_name') or 'n/a'}")
    print(f"  Verified        : {bs.get('is_verified', False)}")
    print(f"  Proxy           : {bs.get('is_proxy', False)}")
    print(f"  GitHub repo     : {gh.get('has_valid_repo', False)} ({gh.get('repo_count', 0)} repos)")
    print(f"  Traces fetched  : {len(traces)}")
    logs = enriched.get('address_logs', [])
    if logs:
        from ai_classifier import decode_log_signals
        ls = decode_log_signals(logs)
        print(f"  Log events      : {ls['summary']}")
        hints = ', '.join(ls['category_hints']) if ls['category_hints'] else 'none'
        print(f"  Event hints     : {hints}")
    token_xfers = enriched.get('token_transfers', [])
    if token_xfers:
        names = ', '.join(f"{t['token_name']}({t.get('token_symbol','')})" for t in token_xfers[:8])
        print(f"  Token transfers : {names}")

    if metrics.get('txcount'):
        print(f"\n  METRICS (provided)")
        print(f"  txcount={metrics['txcount']:,}  avg_daa={metrics.get('avg_daa', 0):.1f}  "
              f"gas_eth={metrics.get('gas_eth', 0):.6f}")
        sr = metrics.get('success_rate')
        print(f"  rel_cost={metrics.get('rel_cost', 0):.2f}x  "
              f"success_rate={'n/a' if sr is None else f'{sr:.1%}'}")

    print(f"\n{sep}")
    print(f"  CLASSIFICATION")
    print(sep)
    print(f"  Name       : {label.get('contract_name', 'n/a')}")
    print(f"  Category   : {label.get('usage_category', 'n/a')}")
    conf = label.get('confidence', 0.0)
    print(f"  Confidence : {_bar(conf)}")
    reasoning = label.get('reasoning', '')
    if reasoning:
        print(f"\n  Reasoning  : {reasoning}")
    print(f"{sep}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(args):
    address = args.address.lower()
    origin_key = args.chain.lower()

    print(f"\n[0/4] DB — fetching metrics for last {args.days} days...")
    db_metrics = fetch_metrics_from_db(address, origin_key, args.days)
    if db_metrics:
        sr_str = 'n/a' if db_metrics['success_rate'] is None else f"{db_metrics['success_rate']:.1%}"
        print(f"      txcount={db_metrics['txcount']:,}  avg_daa={db_metrics['avg_daa']:.0f}  "
              f"gas_eth={db_metrics['gas_eth']:.6f}  rel_cost={db_metrics['rel_cost']:.2f}x  "
              f"success_rate={sr_str}")
        metrics = db_metrics
    else:
        print(f"      not found in DB — using CLI args (all zeros if not provided)")
        metrics = {
            'txcount': args.txcount,
            'avg_daa': args.daa,
            'gas_eth': args.gas_eth,
            'avg_gas_per_tx': round(args.gas_eth / args.txcount, 8) if args.txcount else 0,
            'rel_cost': args.rel_cost,
            'success_rate': args.success_rate,
            'day_range': args.days,
        }

    if args.verbose:
        _classifier_logger.setLevel(logging.INFO)
        logging.getLogger('AutoLabeler').setLevel(logging.INFO)

    connector = aiohttp.TCPConnector(ssl=_ssl_ctx())
    async with aiohttp.ClientSession(connector=connector) as session:
        # Pre-warm OLI cache
        await _oli_cache.get(session)

        enriched = await enrich_single(address, origin_key, session)

        print(f"\n[4/4] AI Classifier — running Gemini classification...")
        label = await classify_contract(
            address=address,
            origin_key=origin_key,
            metrics=metrics,
            blockscout=enriched['blockscout'],
            github=enriched['github'],
            traces=enriched['traces'],
            session=session,
            address_logs=enriched.get('address_logs', []),
            token_transfers=enriched.get('token_transfers', []),
        )

    print_result(address, origin_key, enriched, label, metrics)

    if args.json:
        out = {
            'address': address,
            'chain': origin_key,
            'label': label,
            'blockscout': enriched['blockscout'],
            'github': enriched['github'],
            'metrics': metrics,
        }
        print(json.dumps(out, indent=2))


def parse_args():
    p = argparse.ArgumentParser(
        description="Classify a single smart contract by address + chain"
    )
    p.add_argument('--address', required=True, help='Contract address (0x...)')
    p.add_argument('--chain', required=True,
                   help=f"Chain origin_key (e.g. base, ethereum, arbitrum). "
                        f"Supported: {', '.join(sorted(Config.SUPPORTED_CHAINS))}")
    p.add_argument('--txcount', type=int, default=0, help='Transaction count (default: 0)')
    p.add_argument('--daa', type=float, default=0.0, help='Avg daily active addresses (default: 0)')
    p.add_argument('--gas-eth', type=float, default=0.0, help='Total gas in ETH (default: 0)')
    p.add_argument('--rel-cost', type=float, default=0.0,
                   help='Gas rel_cost vs chain median (default: 0)')
    p.add_argument('--success-rate', type=float, default=None,
                   help='Tx success rate 0.0–1.0 (default: None = unknown)')
    p.add_argument('--days', type=int, default=7, help='Metric day range (default: 7)')
    p.add_argument('--verbose', action='store_true',
                   help='Show full classifier prompt and Gemini response')
    p.add_argument('--json', action='store_true',
                   help='Also print raw JSON output after the formatted result')
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()
    asyncio.run(run(args))
