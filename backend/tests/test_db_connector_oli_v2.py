"""
Integration tests for OLI DB methods.

Tests:
  A. get_unlabelled_contracts_v2() — column schema, 0.1% threshold, top-10 cap
  B. get_unlabelled_contracts() dedup fix — automated labeler exclusion

Requires a live DB connection (DB_HOST, DB_USERNAME, DB_PASSWORD, DB_DATABASE env vars).

Run from backend/:
    python3 tests/test_db_connector_oli_v2.py
"""
import sys
from pathlib import Path
import pandas as pd

_BACKEND = Path(__file__).resolve().parents[1]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from src.db_connector import DbConnector

PASS = []
FAIL = []

def check(name: str, ok: bool, detail: str = ''):
    status = "PASS" if ok else "FAIL"
    if not ok:
        FAIL.append(name)
    else:
        PASS.append(name)
    suffix = f" — {detail}" if detail else ""
    print(f"  [{status}] {name}{suffix}")


def assert_ok(name: str, condition: bool, detail: str = ''):
    check(name, condition, detail)


# ─── Setup ────────────────────────────────────────────────────────────────────

print("Connecting to DB...")
db = DbConnector()
print("Connected.\n")

DAYS = 7
CHAINS = ['base', 'arbitrum', 'optimism']  # limit scope for speed

# ─── Test A: get_unlabelled_contracts_v2 ──────────────────────────────────────

print("=== A. get_unlabelled_contracts_v2() ===")

df_v2 = db.get_unlabelled_contracts_v2(days=DAYS, origin_keys=CHAINS)
print(f"  Returned {len(df_v2)} rows across {df_v2['origin_key'].nunique() if not df_v2.empty else 0} chains")

assert_ok("Returns a DataFrame", isinstance(df_v2, pd.DataFrame))
assert_ok("Not empty (at least some unlabeled contracts exist)", not df_v2.empty)

required_cols = ['address', 'origin_key', 'txcount', 'chain_share', 'rel_cost', 'gas_eth', 'avg_daa']
for col in required_cols:
    assert_ok(f"Column '{col}' present", col in df_v2.columns)

assert_ok("txcount is numeric", pd.api.types.is_numeric_dtype(df_v2['txcount']))
assert_ok("chain_share is numeric", pd.api.types.is_numeric_dtype(df_v2['chain_share']))

# All returned contracts must pass the 0.1% threshold
below_threshold = df_v2[df_v2['chain_share'] < 0.001]
assert_ok(
    "All contracts >= 0.1% of chain total txcount",
    below_threshold.empty,
    f"{len(below_threshold)} below threshold: {below_threshold[['address','origin_key','chain_share']].to_dict('records')}" if not below_threshold.empty else "",
)

# Top-10 cap per chain
per_chain = df_v2.groupby('origin_key').size()
over_cap = per_chain[per_chain > 10]
assert_ok(
    "No chain returns more than 10 contracts",
    over_cap.empty,
    f"Over-cap chains: {over_cap.to_dict()}" if not over_cap.empty else "",
)

# No duplicates (address + origin_key)
dups = df_v2.duplicated(subset=['address', 'origin_key'])
assert_ok("No duplicate (address, origin_key) pairs", not dups.any(),
          f"{dups.sum()} duplicates found" if dups.any() else "")

print(f"\n  chain_share range: {df_v2['chain_share'].min():.4f} – {df_v2['chain_share'].max():.4f}")
print(f"  txcount range:     {int(df_v2['txcount'].min()):,} – {int(df_v2['txcount'].max()):,}")
print(f"  per-chain counts:  {per_chain.to_dict()}")

# ─── Test B: dedup fix — automated labeler exclusion ─────────────────────────

print("\n=== B. Automated labeler exclusion (dedup fix) ===")

# Check which contracts are already attested by the automated labeler
df_attested = pd.read_sql("""
    SELECT
        '0x' || encode(lbl.address, 'hex') AS address,
        smc.origin_key
    FROM public.labels lbl
    JOIN sys_main_conf smc ON lbl.chain_id = smc.caip2
    WHERE lbl.attester = decode('aDbf2b56995b57525Aa9a45df19091a2C5a2A970', 'hex')
      AND smc.origin_key IN ('base', 'arbitrum', 'optimism')
""", db.engine.connect())

print(f"  Contracts currently in public.labels (automated attester): {len(df_attested)}")

if df_attested.empty:
    print("  INFO: No automated-labeler attestations found — dedup exclusion untestable at runtime.")
    print("        Verify by manually attesting a test contract and re-running.")
    check("Dedup exclusion (skipped — no attested contracts in DB)", True)
else:
    attested_pairs = set(zip(df_attested['address'].str.lower(), df_attested['origin_key']))

    # v2 must not contain any attested contracts
    v2_pairs = set(zip(df_v2['address'].str.lower(), df_v2['origin_key']))
    leaked_v2 = v2_pairs & attested_pairs
    assert_ok(
        "v2: zero already-attested contracts returned",
        len(leaked_v2) == 0,
        f"Leaked: {leaked_v2}" if leaked_v2 else "",
    )

    # v1 check: does the old method still leak them? (informational only)
    df_v1 = db.get_unlabelled_contracts(number_of_contracts=50, days=DAYS)
    df_v1['address'] = df_v1['address'].apply(
        lambda x: '0x' + bytes(x).hex() if isinstance(x, (bytes, bytearray, memoryview)) else x
    )
    v1_pairs = set(zip(df_v1['address'].str.lower(), df_v1['origin_key']))
    leaked_v1 = v1_pairs & attested_pairs
    if leaked_v1:
        print(f"  INFO: v1 (old method) still has {len(leaked_v1)} leaked contracts — dedup fix working in v2 ✓")
    else:
        print(f"  INFO: v1 also clean (attested contracts may have rolled out of view already)")

# ─── Test C: day_range column set correctly ───────────────────────────────────

print("\n=== C. day_range column ===")
assert_ok("day_range column present", 'day_range' in df_v2.columns)
if 'day_range' in df_v2.columns:
    assert_ok(f"day_range = {DAYS}", (df_v2['day_range'] == DAYS).all())

# ─── Summary ─────────────────────────────────────────────────────────────────

total = len(PASS) + len(FAIL)
print(f"\n{'='*50}")
print(f"RESULT: {len(PASS)}/{total} passed")
if FAIL:
    print(f"FAILED: {FAIL}")
    sys.exit(1)
else:
    print("All tests passed.")
