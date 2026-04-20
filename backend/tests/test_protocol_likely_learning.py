"""
Learning test: validates _is_protocol_likely() signal calibration against
184 real labeled Base contracts from labels-owner-project.json.

The JSON has known usage_category (no nulls) and owner_project (null for 34 records).
Since confidence/blockscout/event data is not in the JSON, this test uses:
  - confidence = 0.70 (typical Gemini output)
  - success_rate = 0.85 (healthy protocol default)
  - no blockscout verification, no log signals (tests the moderate-confidence path only)

This isolates the question: "if the AI gives us a specific category at 0.70 confidence
with no other signals, does _is_protocol_likely() fire correctly?"

Expected: all 150 contracts WITH owner_project should return True (recall = 100%).
The 34 WITHOUT owner_project are split into "also flagged" (protocol-likely unknowns —
these are exactly what the sentinel is designed to surface) and "not flagged".

Run from repo root:
    python3 backend/tests/test_protocol_likely_learning.py

Or from backend/:
    python3 tests/test_protocol_likely_learning.py
"""

import json
import sys
from collections import defaultdict
from pathlib import Path

# Resolve repo root and backend paths
_HERE = Path(__file__).resolve()
_BACKEND = _HERE.parents[1]
_REPO_ROOT = _BACKEND.parent
_LABELING = _BACKEND / 'labeling'

for _p in [str(_BACKEND), str(_LABELING)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Stub heavy imports so we can import automated_labeler without API keys.
# Use MagicMock for modules that define classes using their types at class-definition
# time (e.g. aiohttp.ClientSession used as a type annotation inside a class body).
import types, unittest.mock as _mock

_magic_mods = ['aiohttp', 'google', 'google.genai', 'google.genai.types']
_simple_mods = ['oli', 'yaml', 'eth_utils', 'pyairtable']

for _mod in _magic_mods:
    if _mod not in sys.modules:
        sys.modules[_mod] = _mock.MagicMock()

for _mod in _simple_mods:
    if _mod not in sys.modules:
        sys.modules[_mod] = types.ModuleType(_mod)

import concurrent_contract_analyzer as _cca
_cca.BlockscoutAPI_Async = object
_cca.GitHubAPI_Async = object
_cca.TenderlyAPI_Async = object
_cca.Config = type('Config', (), {
    'SUPPORTED_CHAINS': {}, 'BLOCKSCOUT_API_KEYS': {},
    'CHAIN_EXPLORER_CONFIG': {}, 'EXCLUDED_REPOS': [],
    'get_github_headers': staticmethod(lambda: {}),
})()
_cca.create_ssl_context = lambda: None
_cca.rate_limit_manager = None

import ai_classifier as _ac
_ac.classify_contract = None
_ac.CHAIN_TO_EIP155 = {'base': 'eip155:8453'}
_ac._oli_cache = None
_ac.decode_log_signals = None

from automated_labeler import _is_protocol_likely

# ─── Load data ────────────────────────────────────────────────────────────────

json_path = _REPO_ROOT / 'labels-owner-project.json'
if not json_path.exists():
    # Try CWD fallback
    json_path = Path('labels-owner-project.json')
if not json_path.exists():
    print(f"ERROR: labels-owner-project.json not found. Run from repo root.")
    sys.exit(1)

records = json.loads(json_path.read_text())
print(f"\nLoaded {len(records)} records from {json_path.name}")

# ─── Config ───────────────────────────────────────────────────────────────────

CONFIDENCE = 0.70
SUCCESS_RATE = 0.85

# ─── Run signal function on each record ──────────────────────────────────────

def make_label(r, confidence):
    return {
        'usage_category': r['usage_category'],
        'contract_name': r.get('name') or '',
        'confidence': confidence,
        'reasoning': '',
    }

def make_metrics(r, success_rate):
    return {
        'txcount': r.get('txcount', 0),
        'gas_eth': (r.get('gas_fees_usd') or 0) / 3000,
        'avg_daa': r.get('daa', 0),
        'success_rate': success_rate,
        'rel_cost': 0.5,
        'day_range': 7,
    }

EMPTY_BLOCKSCOUT = {}
EMPTY_LOG_SIGNALS = {
    'has_governance': False, 'has_staking': False, 'has_bridge_events': False,
    'has_erc4337': False, 'has_token_transfers': False, 'has_swaps': False,
    'has_access_control': False, 'has_valid_repo': False,
    'event_counts': {}, 'category_hints': [],
}

results = []
for r in records:
    lbl = make_label(r, CONFIDENCE)
    mtr = make_metrics(r, SUCCESS_RATE)
    flagged = _is_protocol_likely(lbl, EMPTY_BLOCKSCOUT, EMPTY_LOG_SIGNALS, mtr)
    results.append({**r, 'flagged': flagged})

# ─── Analysis ─────────────────────────────────────────────────────────────────

labeled   = [r for r in results if r.get('owner_project') is not None]
unlabeled = [r for r in results if r.get('owner_project') is None]

labeled_flagged   = [r for r in labeled   if r['flagged']]
unlabeled_flagged = [r for r in unlabeled if r['flagged']]

recall = len(labeled_flagged) / len(labeled) * 100 if labeled else 0

print(f"\n{'='*60}")
print(f"Learning Test — _is_protocol_likely() on {len(records)} real contracts")
print(f"Settings: confidence={CONFIDENCE}, success_rate={SUCCESS_RATE}, no blockscout/log signals")
print(f"{'='*60}")

# Per-category breakdown for labeled contracts
print(f"\n--- RECALL (owner_project IS NOT NULL — should be flagged) ---")
print(f"{'Category':<25} {'Total':>7} {'Flagged':>8} {'Recall':>8}")
print("-" * 50)

by_cat_labeled = defaultdict(list)
for r in labeled:
    by_cat_labeled[r['usage_category']].append(r)

all_recall_ok = True
for cat in sorted(by_cat_labeled):
    group = by_cat_labeled[cat]
    flagged_count = sum(1 for r in group if r['flagged'])
    cat_recall = flagged_count / len(group) * 100
    ok = "✓" if cat_recall == 100 else "✗"
    print(f"  {cat:<23} {len(group):>7} {flagged_count:>8} {cat_recall:>7.0f}%  {ok}")
    if cat_recall < 100:
        all_recall_ok = False

print("-" * 50)
print(f"  {'TOTAL':<23} {len(labeled):>7} {len(labeled_flagged):>8} {recall:>7.1f}%")

# Contracts with owner_project that were NOT flagged (false negatives)
not_flagged_labeled = [r for r in labeled if not r['flagged']]
if not_flagged_labeled:
    print(f"\n  ⚠ {len(not_flagged_labeled)} labeled contracts NOT flagged (false negatives):")
    for r in not_flagged_labeled:
        print(f"    {r['address'][:12]}... {r['owner_project']} / {r['usage_category']}")

# Unlabeled breakdown
print(f"\n--- UNLABELED CONTRACTS (owner_project IS NULL) ---")
print(f"Total without owner_project: {len(unlabeled)}")
print(f"  Flagged as protocol-likely: {len(unlabeled_flagged)} — these are sentinel candidates")
print(f"  Not flagged:                {len(unlabeled) - len(unlabeled_flagged)} — correct (ambiguous or excluded)")

by_cat_unlabeled = defaultdict(list)
for r in unlabeled:
    by_cat_unlabeled[r['usage_category']].append(r)

print(f"\n{'Category':<25} {'Total':>7} {'Flagged':>8}  Note")
print("-" * 65)
for cat in sorted(by_cat_unlabeled):
    group = by_cat_unlabeled[cat]
    flagged_count = sum(1 for r in group if r['flagged'])
    note = "→ sentinel will surface these" if flagged_count > 0 else "→ excluded or ambiguous"
    print(f"  {cat:<23} {len(group):>7} {flagged_count:>8}  {note}")

# Category exclusion check: no 'trading' or true 'other' in ground truth
trading_records = [r for r in results if r['usage_category'] == 'trading']
other_records   = [r for r in results if r['usage_category'] == 'other']
print(f"\n--- CATEGORY EXCLUSION SANITY ---")
print(f"  Records with usage_category='trading': {len(trading_records)}  (expected 0 in gold-labeled data)")
print(f"  Records with usage_category='other':   {len(other_records)}  (expected 0 in gold-labeled data)")

# Strong signal sensitivity analysis: what happens if we add is_verified=True?
verified_boost = [r for r in unlabeled if not r['flagged']]
could_boost = []
for r in verified_boost:
    lbl = make_label(r, CONFIDENCE)
    mtr = make_metrics(r, SUCCESS_RATE)
    if _is_protocol_likely(lbl, {'is_verified': True}, EMPTY_LOG_SIGNALS, mtr):
        could_boost.append(r)

print(f"\n--- SIGNAL SENSITIVITY ---")
print(f"  Unlabeled, currently NOT flagged: {len(verified_boost)}")
print(f"  Of those, would flip to True if is_verified=True: {len(could_boost)}")
print(f"  → These contracts need Blockscout verification data or GitHub hit to surface")

# ─── Final verdict ────────────────────────────────────────────────────────────

print(f"\n{'='*60}")
print(f"RECALL on labeled contracts: {recall:.1f}%")
if all_recall_ok and recall == 100.0:
    print("PASS — All known protocols correctly identified at confidence=0.70")
else:
    print(f"REVIEW — {len(not_flagged_labeled)} labeled contracts missed at confidence=0.70")
    print("  Consider lowering the moderate-signal threshold or adding log_signals")
    sys.exit(1)
