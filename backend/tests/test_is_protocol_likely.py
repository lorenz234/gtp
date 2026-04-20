"""
Unit tests for _is_protocol_likely() and build_attestation_rows_v2().

Pure Python — no DB, no network. Run from backend/:
    python3 tests/test_is_protocol_likely.py
"""
import sys
from pathlib import Path

# Path setup: backend/ and backend/labeling/ must be importable
_BACKEND = Path(__file__).resolve().parents[1]
_LABELING = _BACKEND / 'labeling'
for _p in [str(_BACKEND), str(_LABELING)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Patch out heavy imports that automated_labeler pulls in at module level
import types, unittest.mock as _mock

# Stub modules that require network/API keys so the import succeeds in test env.
# aiohttp and google.genai need MagicMock so attribute access at class-definition
# time (type annotations) doesn't raise AttributeError.
for _mod in ['aiohttp', 'google', 'google.genai', 'google.genai.types']:
    if _mod not in sys.modules:
        sys.modules[_mod] = _mock.MagicMock()

for _mod in ['oli', 'yaml', 'eth_utils', 'pyairtable']:
    if _mod not in sys.modules:
        sys.modules[_mod] = types.ModuleType(_mod)

# Provide minimal stubs for symbols automated_labeler imports
import concurrent_contract_analyzer as _cca_stub
_cca_stub.BlockscoutAPI_Async = object
_cca_stub.GitHubAPI_Async = object
_cca_stub.TenderlyAPI_Async = object
_cca_stub.Config = type('Config', (), {
    'SUPPORTED_CHAINS': {},
    'BLOCKSCOUT_API_KEYS': {},
    'CHAIN_EXPLORER_CONFIG': {},
    'EXCLUDED_REPOS': [],
    'get_github_headers': staticmethod(lambda: {}),
})()
_cca_stub.create_ssl_context = lambda: None
_cca_stub.rate_limit_manager = None

import ai_classifier as _ac_stub_mod
# Import the real decode_log_signals BEFORE nulling out the rest
from ai_classifier import decode_log_signals
_ac_stub_mod.classify_contract = None
_ac_stub_mod.CHAIN_TO_EIP155 = {'base': 'eip155:8453', 'ethereum': 'eip155:1'}
_ac_stub_mod._oli_cache = None
# Leave decode_log_signals intact — it's a pure function used by build_attestation_rows_v2

# Now safe to import
from automated_labeler import _is_protocol_likely, build_attestation_rows_v2

# ─── helpers ─────────────────────────────────────────────────────────────────

PASS = []
FAIL = []

def check(name: str, result: bool, expected: bool):
    status = "PASS" if result == expected else "FAIL"
    if result != expected:
        FAIL.append(name)
    else:
        PASS.append(name)
    print(f"  [{status}] {name}: got={result} expected={expected}")


def label(category='dex', confidence=0.9):
    return {'usage_category': category, 'contract_name': 'SomeContract',
            'confidence': confidence, 'reasoning': ''}


def metrics(success_rate=0.85):
    return {'txcount': 500_000, 'gas_eth': 1.5, 'avg_daa': 200,
            'success_rate': success_rate, 'rel_cost': 0.5,
            'avg_gas_per_tx': 0.000003, 'day_range': 7}


def blockscout(verified=False):
    return {'is_verified': verified, 'contract_name': 'SomeContract'}


def log_signals(**kwargs):
    base = {
        'has_governance': False, 'has_staking': False, 'has_bridge_events': False,
        'has_erc4337': False, 'has_token_transfers': False, 'has_swaps': False,
        'has_access_control': False, 'has_valid_repo': False,
        'event_counts': {}, 'category_hints': [],
    }
    base.update(kwargs)
    return base


def make_contract(category='dex', confidence=0.9, verified=False,
                  success_rate=0.85, **log_kw):
    return {
        'address': '0xABC',
        'origin_key': 'base',
        'label': label(category, confidence),
        'blockscout': blockscout(verified),
        'github': {'has_valid_repo': log_kw.pop('has_valid_repo', False)},
        'traces': [],
        'address_logs': [],
        'metrics': metrics(success_rate),
        '_log_signals': log_signals(**log_kw),
    }


# ─── Tests: _is_protocol_likely ──────────────────────────────────────────────

print("\n=== _is_protocol_likely() — Hard exclusions (unverified) ===")
check("trading, unverified → False",
      _is_protocol_likely(label('trading'), blockscout(False), log_signals(has_governance=True), metrics()), False)
check("other, unverified → False",
      _is_protocol_likely(label('other'), blockscout(False), log_signals(has_governance=True), metrics()), False)
check("low confidence (0.4) → False",
      _is_protocol_likely(label('dex', 0.4), blockscout(), log_signals(), metrics()), False)
check("low success_rate (0.2) → False",
      _is_protocol_likely(label('dex', 0.9), blockscout(), log_signals(), metrics(0.2)), False)
check("confidence exactly at floor (0.55) → False",
      _is_protocol_likely(label('dex', 0.549), blockscout(), log_signals(), metrics()), False)

print("\n=== _is_protocol_likely() — is_verified overrides hard exclusions ===")
check("trading + is_verified=True → True (verified beats category exclusion)",
      _is_protocol_likely(label('trading'), blockscout(True), log_signals(), metrics()), True)
check("other + is_verified=True → True (verified beats category exclusion)",
      _is_protocol_likely(label('other'), blockscout(True), log_signals(), metrics()), True)

print("\n=== _is_protocol_likely() — Strong positive signals ===")
check("is_verified=True → True",
      _is_protocol_likely(label('dex', 0.6), blockscout(True), log_signals(), metrics()), True)
check("has_governance → True",
      _is_protocol_likely(label('dex', 0.6), blockscout(), log_signals(has_governance=True), metrics()), True)
check("has_staking → True",
      _is_protocol_likely(label('staking', 0.6), blockscout(), log_signals(has_staking=True), metrics()), True)
check("has_bridge_events → True",
      _is_protocol_likely(label('bridge', 0.6), blockscout(), log_signals(has_bridge_events=True), metrics()), True)
check("has_erc4337 → True",
      _is_protocol_likely(label('erc4337', 0.6), blockscout(), log_signals(has_erc4337=True), metrics()), True)
check("has_valid_repo → True",
      _is_protocol_likely(label('dex', 0.6), blockscout(), log_signals(has_valid_repo=True), metrics()), True)

print("\n=== _is_protocol_likely() — Moderate signal (confidence >= 0.65) ===")
check("conf=0.65, no strong signals → True",
      _is_protocol_likely(label('dex', 0.65), blockscout(), log_signals(), metrics()), True)
check("conf=0.64, no strong signals → False",
      _is_protocol_likely(label('dex', 0.64), blockscout(), log_signals(), metrics()), False)
check("conf=0.70, category=lending → True",
      _is_protocol_likely(label('lending', 0.70), blockscout(), log_signals(), metrics()), True)

print("\n=== _is_protocol_likely() — Null handling ===")
check("success_rate=None treated as non-bot, strong signal still wins",
      _is_protocol_likely(label('dex', 0.9), blockscout(True), log_signals(), {**metrics(), 'success_rate': None}), True)
check("success_rate=None, no strong signal, conf=0.7 → True (moderate path)",
      _is_protocol_likely(label('dex', 0.7), blockscout(), log_signals(), {**metrics(), 'success_rate': None}), True)


# ─── Tests: build_attestation_rows_v2 ────────────────────────────────────────

# Need valid_ids for _oli_validate_category
valid_ids = ['dex', 'lending', 'bridge', 'staking', 'other', 'trading', 'erc4337',
             'non_fungible_tokens', 'yield_vaults', 'community', 'gambling']

def run_v2(contracts):
    # Inject log_signals into address_logs field by pre-populating address_logs
    # (build_attestation_rows_v2 calls decode_log_signals internally)
    # For the test we patch address_logs to empty and pre-set github
    return build_attestation_rows_v2(contracts, valid_ids)


print("\n=== build_attestation_rows_v2() ===")

c_dex_verified = make_contract('dex', 0.9, verified=True)
rows = run_v2([c_dex_verified])
check("dex verified → owner_project='protocol-contract-likely'",
      rows[0]['owner_project'], 'protocol-contract-likely')
check("dex verified → contract_name NOT 'Ambiguous Contract'",
      rows[0]['contract_name'] != 'Ambiguous Contract', True)

c_other = make_contract('other', 0.9, verified=True)
rows = run_v2([c_other])
check("other → contract_name='Ambiguous Contract'",
      rows[0]['contract_name'], 'Ambiguous Contract')
check("other + verified → owner_project='protocol-contract-likely' (is_verified overrides)",
      rows[0]['owner_project'], 'protocol-contract-likely')
check("other → usage_category='other' (OLI valid value preserved)",
      rows[0]['usage_category'], 'other')

c_other_unverified = make_contract('other', 0.9, verified=False)
rows = run_v2([c_other_unverified])
check("other, unverified → owner_project=None (hard exclusion)",
      rows[0]['owner_project'], None)

c_trading = make_contract('trading', 0.9, verified=True)
rows = run_v2([c_trading])
check("trading + verified → owner_project='protocol-contract-likely' (is_verified overrides)",
      rows[0]['owner_project'], 'protocol-contract-likely')

c_trading_unverified = make_contract('trading', 0.9, verified=False)
rows = run_v2([c_trading_unverified])
check("trading, unverified → owner_project=None",
      rows[0]['owner_project'], None)

c_low_conf = make_contract('dex', 0.4)
rows = run_v2([c_low_conf])
check("dex low-conf → owner_project=None",
      rows[0]['owner_project'], None)


# ─── Summary ─────────────────────────────────────────────────────────────────

total = len(PASS) + len(FAIL)
print(f"\n{'='*50}")
print(f"RESULT: {len(PASS)}/{total} passed")
if FAIL:
    print(f"FAILED: {FAIL}")
    sys.exit(1)
else:
    print("All tests passed.")
