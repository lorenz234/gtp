# Automated Smart Contract Labeling — Architecture

## Overview

The pipeline fetches unlabeled smart contracts from the production DB, enriches each via Blockscout + GitHub + Tenderly, classifies via Gemini 2.5 Flash using structured decision paths, and attests results on-chain via oli-python. It runs as an Airflow DAG nightly.

---

## File Map

| File | Role |
|---|---|
| `labeling/automated_labeler.py` | Main orchestration. DB fetch → enrich → classify → attest → Airtable write. Runs as CLI or called by Airflow DAG |
| `labeling/ai_classifier.py` | Gemini 2.5 Flash classifier. Structured output via function calling. Decision paths A–G |
| `labeling/concurrent_contract_analyzer.py` | Shared async API clients + config: `BlockscoutAPI_Async`, `GitHubAPI_Async`, `TenderlyAPI_Async`, `CHAIN_EXPLORER_CONFIG`, `BLOCKSCOUT_API_KEYS`, `SUPPORTED_CHAINS` |
| `labeling/classify_contract_tool.py` | One-shot manual analysis CLI for individual contract inspection |
| `labeling/eval_performance.py` | Evaluation harness: 100 Ethereum top contracts with known GT labels from growthepie API |
| `airflow/dags/oli/oli_automated_labeler.py` | Airflow DAG: `oli_automated_labeler`, runs at 1:30am UTC every Monday |
| `airflow/dags/oli/oli_airtable.py` | Airflow DAG: refreshes materialized views, runs at 00:50 UTC (must complete before labeler) |
| `airflow/dags/oli/oli_misc_sync.py` | Syncs OLI tags, categories, and trust table to web3 DB daily |
| `src/db_connector.py` | `get_unlabelled_contracts_v2()` — DB query returning top unlabeled contract candidates |

---

## Supported Chains

Blockscout centralized API: `https://api.blockscout.com/{chain_id}/api/v2/`
Single key `BLOCKSCOUT_API_KEY_PRO` passed as `?apikey=` query param (`auth_type: "param"`, not Bearer header).

| Chain | chain_id | origin_key |
|---|---|---|
| Ethereum | 1 | ethereum |
| Optimism | 10 | optimism |
| Base | 8453 | base |
| Arbitrum One | 42161 | arbitrum |
| Scroll | 534352 | scroll |
| Polygon PoS | 137 | polygon |
| Unichain | 130 | unichain |
| Celo | 42220 | celo |
| MegaETH | 4326 | megaeth |
| Mode | 34443 | mode |

> Base (8453) returns HTTP 500 from Blockscout's upstream — server-side issue, not our key. Excluded from scheduled runs until resolved.

---

## Airflow Scheduling

```
Every Monday

00:50 UTC  oli_airtable DAG
           └─ refreshes materialized views (vw_oli_label_pool_gold_v2, etc.)
              so get_unlabelled_contracts_v2 sees up-to-date exclusions

01:30 UTC  oli_automated_labeler DAG  (schedule: '30 01 * * 1')
           └─ label_and_attest() task
              ├─ fetch unlabeled candidates (get_unlabelled_contracts_v2, days=7, top-200)
              ├─ enrich async (concurrency=5) — GitHub searched for ALL contracts
              ├─ classify (Gemini 2.5 Flash, concurrency=3)
              ├─ attest (OLI off-chain, confidence >= 0.5)
              └─ write protocol-likely rows → Airtable "Label Pool Automated"
```

Output files go to `/tmp/oli_labeler_output` — ephemeral, never committed. Airflow captures all stdout/stderr via its own task logging. No log files are produced separately.

On failure: `alert_via_webhook` + `claude_fix_on_failure` callbacks fire. 1 retry after 10 minutes.

---

## Pipeline Steps

```
DB (blockspace_fact_contract_level)
  ↓ get_unlabelled_contracts_v2()
  │   • 0.1% relative txcount threshold (SUM(txcount)/chain_total >= 0.001)
  │   • top-10 per chain, ranked by txcount DESC
  │   • excludes contracts already attested by 0xaDbf... or 0xab81... in public.labels
  │   • secondary dedup against Airtable queue (same-day guard before gold view refresh)
  │   • --per-chain-limit N caps to N per chain (local testing only; 0 in production)
  ↓
ENRICH (per contract, async, concurrency=5)
  │   • Blockscout: contract_name, is_verified, proxy info, last 30 transactions
  │   • Logs fetch: DISABLED — conserving Blockscout Pro API credits
  │   • GitHub: code search → has_valid_repo flag (all contracts, no gate)
  │   • Tenderly: decode 10 sampled tx traces (public endpoint, no auth)
  │   • SKIP contract entirely if all three return empty (enrichment failure guard)
  ↓
CLASSIFY (Gemini 2.5 Flash, concurrency≤3)
  │   • Pre-computes signals from traces + metrics (see Decision Paths below)
  │   • chain_median_daa injected per chain for PUBLIC diversity threshold
  │   • Paths evaluated in order: A → B → C → D → E → F → G1 → G2 → G3 → G0
  ↓
ATTEST (oli-python v2+, submit_label_bulk, only when --attest / attest=True)
  │   • confidence < 0.5 → skipped (0.3 for local testing)
  │   • General attester  (OLI_automated_labeler key):         dex, bridge, lending, …
  │   • Trading attester  (OLI_automated_labeler_trading key): trading contracts only
  │     → separate trust entry with higher contract_name confidence (rule-based names)
  ↓
AIRTABLE WRITE ("Label Pool Automated")
    • Only protocol-contract-likely rows written (trading/other are fully automated)
    • Fields: address, origin_key (linked), contract_name, usage_category (linked),
      confidence, gas_eth, avg_daa, rel_cost, github_found,
      _comment (confidence | reasoning), _source, attester, is_protocol_contract_likely
    • Deduped on (address + caip2) — safe to re-run
```

---

## CLI Usage (local / testing)

Run from `backend/` directory:

```bash
python labeling/automated_labeler.py \
  --chains ethereum optimism arbitrum \
  --days 30 \
  --per-chain-limit 2 \
  --concurrency 3 \
  --output-dir ./output
  # add --attest to submit on-chain
```

Key flags:

| Flag | Default | Notes |
|---|---|---|
| `--chains` | all | space-separated origin_keys |
| `--days` | 7 | lookback window |
| `--max-contracts` | 50 | total cap |
| `--per-chain-limit` | 0 (off) | use for local testing |
| `--concurrency` | 5 | async enrichment workers |
| `--attest` | off | dry-run without this flag |
| `--confidence-threshold` | 0.3 | attestation floor |
| `--input-json` | — | skip DB fetch, re-attest from existing output file |
| `--output-dir` | `./output` | JSON + CSV output location |

---

## Classifier Decision Paths

Paths are evaluated in order — first match wins. Numerical signals (success_rate, DAA/tx, rel_cost) are tiebreakers; they shift confidence but do not override clear semantic or trace evidence.

Caller diversity threshold = `max(chain_median_daa * 0.05, 3.0)` unique callers in sampled txs.

---

### PATH A — Verified + Named

**Trigger:** `source_verified=True` AND `blockscout_name != "unknown"`

Use the semantic meaning of the verified name. Read it as a human — you know what protocols do. Traces disambiguate when the name alone is genuinely unclear (VRF calls → gaming, NFT interactions → non_fungible_tokens).

Protocol-specific keyword overrides (checked first):
- `Permit2` / `AllowanceHolder` → `developer_tools`
- `GPv2Settlement` / `CoWSwap*` → `dex`
- `Relay*Router` / `RelayBridge` → `bridge`
- `Rango*` / `RangoDiamond` → `bridge`
- `*DVN` / `*Dvn` → `cc_communication` (LayerZero verifier)
- `MayanSwift*` / `Mayan*Bridge` → `bridge`
- `*Router` without clear DEX context → examine trace, do NOT default to `dex`

`contract_name` = exact Blockscout name.

---

### PATH B — Proxy Delegating to Named Implementation

**Trigger:** `"this contract itself delegates to (named)"` is not `"none"`

This proxy IS that implementation — classify by what the delegate target does.
- `CLPool` / `UniswapV3Pool` / `PancakeV3Pool` → `dex`
- `EntryPoint` → `erc4337`
- `FiatToken` / `USDC` / ERC20 → `stablecoin` or `fungible_tokens`
- `RangoDiamond` / `Rango*` → `bridge`
- `Relay*Router` → `bridge`

Apply PATH A protocol corrections for any ambiguous delegate name.

`contract_name` = delegate name + "Proxy" suffix.

---

### PATH C — Calls Official DEX Router (depth ≤1)

**Trigger:** `"Direct calls into official DEX routers"` list is non-empty

This contract sits ABOVE official routers — an aggregator, bot, or strategy.

- Low DAA/tx → `trading`
- Low success_rate → `trading`, high confidence
- High DAA/tx + high success_rate → possibly public aggregator → `dex`
- Low DAA/tx AND low success_rate → near-certain private trading

---

### PATH D — Calls AMM Pool Swap Directly (depth ≤1)

**Trigger:** `"Direct calls into AMM pool swap functions"` = True AND `no_meaningful_identity` = True

Note: AMM pools list non-empty but swap = False means price reads only (slot0, getReserves) — do not use PATH D, fall through to F or G.

- PRIVATE callers → `trading`; DIVERSE callers → `dex`
- success_rate < 50% → aggressive bot → `trading`; ≥70% + diverse → `dex`
- No caller data: low DAA/tx → `trading`; high DAA/tx → `dex`

---

### PATH E — EIP-1167 Minimal Clone (Empty Traces)

**Trigger:** `"this contract has raw-address DELEGATE"` = True AND `"all trace call graphs empty"` = True

Factory clone forwarding to an unresolvable implementation.

- PRIVATE callers → trading executor
- DIVERSE callers → dex infrastructure clone
- High txcount → lean `trading`; low → `dex`
- Confidence is inherently limited — cannot resolve without the implementation

---

### PATH F — Pure Price Scanner

**Trigger:** Trace contains ONLY STATICCALL/SLOAD ops AND calls are exclusively slot0/price-read functions (`slot0`, `observe`, `getReserves`, `latestRoundData`) across multiple pools, AND no swap/transfer calls

Hunts opportunities without executing — Searcher or PriceScanner.

→ `trading`

Distinguish from oracle consumers: oracle consumers call Chainlink EACAggregatorProxy feeds; Searchers call AMM pool `slot0` across many different pools to compare prices.

---

### PATH G1 — Private Callers (PRIVATE ≤20%)

**Trigger:** `caller_diversity_label = "PRIVATE"` AND no prior path matched

**Critical:** applies even when traces are raw opcodes only and identity is unknown. Raw-opcode traces + PRIVATE callers + no identity IS the trading bot signature — intentionally unverified. Do NOT fall to G0 because traces are unreadable.

**Exception — verified contracts:** do NOT apply trading defaults. Verified + private callers = protocol infrastructure (vault, liquidator, position manager).
- Verified + DEX router calls → `dex` (settlement/fulfillment infrastructure)
- Verified + bridge calls → `bridge` or `cc_communication`
- Only assign `trading` if traces explicitly show MEV patterns

**Unverified PRIVATE callers → `trading` is the ONLY valid classification.**
A single operator running automated on-chain transactions is by definition a trading bot. Unverified bots intentionally leave no decoded traces — raw opcodes and no identity ARE the signal.

Naming within `trading`:
- LP/gauge contracts + novel tokens in traces, active (slot0 reads + collect + burns/mints same tx) → `"[Protocol]CLRebalancer"`
- LP/gauge contracts + passive (only deposit/withdraw/mint/burn, no slot0) → `"[Protocol][Token]LPManager"`
- Very low success_rate → `"MEVBot"` or `"ArbitrageBot"`
- High success_rate, efficient → `"StrategyExecutor"`
- Raw opcodes only / undifferentiated → `"StrategyExecutor"`

---

### PATH G2 — Keeper Callers (KEEPER 20–80%)

**Trigger:** `caller_diversity_label = "KEEPER"` AND no prior path matched

Unverified multi-operator permissioned system. Lean `trading` unless traces show clear non-trading purpose.

**Exception — verified contracts:** classify by trace content. Verified keeper = protocol infrastructure.
- Verified + DEX router calls → `dex` (1inch resolvers, CoW solvers, RFQ market makers are all KEEPER)
- Verified + bridge calls → `bridge` or `cc_communication`

**Unverified KEEPER:**
- Raw opcodes + no identity → `trading`, `"StrategyExecutor"`. Do NOT use `other`.
- LP/gauge + novel tokens → same ACTIVE/PASSIVE rebalancer distinction as G1
- AccessControl events + no identifiable protocol → trading executor
- Known non-trading protocol interactions in traces → classify by what it interacts with
- No DeFi signals but specific name → classify by semantic meaning; do NOT default to `other`
- Truly unclear → `trading`, `"StrategyExecutor"` (only use `other` if specific non-trading signal: governance events, NFT transfers, bridge events)

---

### PATH G3 — Public Callers (PUBLIC ≥80%)

**Trigger:** `caller_diversity_label = "PUBLIC"` AND no prior path matched

Many different callers — public-facing. Classify by what the contract does:

- Known DEX interactions (swap events, pool calls) → `dex`
- Named STATIC calls to EACAggregatorProxy/Chainlink feeds → `oracle`
- Pool-internal math (computeSwapStep, getSqrtRatioAtTick as internal nodes) → `dex`
- Calls to LayerZero, Wormhole, Axelar → `cc_communication`
- VRF calls (Chainlink VRF, Pyth randomness) → `gaming`
- Social/check-in pattern (simple state writes per user, no DeFi) → `community`
- Calls to `liquidate`, `auction`, `seize`, `settle` on lending/perp contracts → `lending` or `derivative`
- No DeFi signals → classify by semantic meaning of contract name; do NOT default to `other` for a specific name
- Truly ambiguous name AND no usable signals → `other`

Important: named contracts in the deep trace are downstream callees — do NOT classify this contract as CLPool just because CLPool appears in the trace.

---

### PATH G0 — No Caller Data

**Trigger:** `caller_diversity_label = "UNKNOWN"` (caller sampling failed — a data gap)

Only use this path if the data literally says UNKNOWN. If it says PRIVATE/KEEPER/PUBLIC use G1/G2/G3 regardless of trace quality.

Decision tree:
1. Non-trading events (bridge, NFT, governance, staking) → classify by those signals
2. success_rate clearly < 30% → near-certain MEV → `trading`, `"MEVBot"`
3. Named protocol interactions in traces → classify accordingly
4. txcount > 300k AND empty traces AND no events AND no identity → lean `trading`, `"StrategyExecutor"` (unless DAA/tx signal says BROAD USER BASE >5% → override to `other`)
5. All else → `other`, `"AmbiguousContract"`

Note: DAA/tx = 0.00% often means missing data, not confirmed automation — do not treat as strong trading signal alone.

---

## protocol-contract-likely Sentinel

`_is_protocol_likely()` in `automated_labeler.py` determines which attested contracts get flagged `owner_project = "protocol-contract-likely"` and written to Airtable for human review.

**Immediate True (checked first, before all other logic):**
- `blockscout.is_verified = True` → always flagged regardless of category

**Hard exclusions (only applies when NOT verified):**
- `usage_category` in `("trading", "other")` → False
- `confidence < 0.55` → False
- `success_rate < 0.40` → False (None treated as 1.0 — don't penalise missing data)

**Strong positive signals — any one is sufficient:**
- `has_governance` events → True
- `has_staking` events → True
- `has_bridge_events` → True
- `has_erc4337` → True
- `has_valid_repo` (GitHub) → True

**Fallback:**
- `confidence >= 0.65` with a non-trading, non-other category → True

**Result:** `protocol-contract-likely` rows go to Airtable "Label Pool Automated" for a human to assign a real `owner_project` slug. Trading and other contracts are considered fully automated and never need human review.

---

## OLI Trust List (gtp-dna/oli/trusted_entities_v2.yml)

| Attester | Address | tag_id | confidence | Rationale |
|---|---|---|---|---|
| General (`OLI_automated_labeler`) | `0xaDbf2b56995b57525Aa9a45df19091a2C5a2A970` | `usage_category` | 0.4 | ~75–88% eval accuracy |
| General | | `contract_name` | 0.25 | PATH A = Blockscout passthrough; PATH G = AI-generated |
| General | | `owner_project` | 0.05 | Sentinel `protocol-contract-likely` only, not real attribution |
| General | | `*` | 0.1 | Metadata tags |
| Trading (`OLI_automated_labeler_trading`) | `0xab81887E560e7C7b5993cE45D1c584d7FC22e898` | `usage_category` | 0.5 | Bot detection is highly reliable |
| Trading | | `contract_name` | 0.6 | Rule-based from heuristics: PRIVATE + success_rate + gas → MEVBot/ArbitrageBot |
| Trading | | `*` | 0.05 | Should not attest other categories |

Transitive score for a consumer trusting GrowthEpie at 0.8:
- `usage_category` (general): 0.8 × 0.4 = **0.32**
- `usage_category` (trading): 0.8 × 0.5 = **0.40**
- `contract_name` (trading): 0.8 × 0.6 = **0.48**

---

## DB Query: get_unlabelled_contracts_v2

Key design decisions in `src/db_connector.py`:

- **Relative threshold**: `SUM(txcount) / chain_total >= 0.001` — adapts to chain volume, no hardcoded floor
- **Top-10 per chain**: `ROW_NUMBER() OVER (PARTITION BY origin_key ORDER BY txcount DESC)`
- **Two-phase exclusion**:
  1. `pg.usage_category IS NULL` via `vw_oli_label_pool_gold_v2` — steady-state exclusion after view refresh
  2. `NOT EXISTS` on `public.labels` filtered to attester addresses — same-day guard before view refresh lag
- **Address type mismatch**: `blockspace_fact_contract_level.address` is `bytea`; `public.labels.address` is `text`. Joined as `'0x' || encode(cl.address, 'hex')`
- **CTE alias pitfall**: `chain_totals` CTE has no table alias — filter uses bare `origin_key IN (...)` not `cl.origin_key IN (...)`

---

## Known Issues

**1. Blockscout log fetch disabled**
`get_address_logs` commented out in `automated_labeler.py` and `classify_contract_tool.py`; `get_transaction_logs` in `concurrent_contract_analyzer.py`. Reduces signal quality for event-driven paths (ERC-4337, bridge, staking events). Re-enable once Blockscout Pro credit budget is confirmed.

**2. Base (8453) returning HTTP 500**
Blockscout's centralized API returns 500 for Base regardless of key — server-side upstream issue. Base excluded from scheduled pipeline. Bug reported to Blockscout.

**3. Blockscout empty tx lists**
Some high-volume contracts return 200 OK with empty tx arrays. No traces → PATH G0 → `other/AmbiguousContract 0.30`. Enrichment failure guard skips these from attestation, leaving them unlabeled. Not rate limiting — API variability under load.

**4. GitHub rate limiting**
`github_sem = Semaphore(2)` limits concurrency but 429s cause a wait without proper backoff. GitHub is now searched for all contracts (gate removed) — weekly schedule reduces total calls per run vs daily.

**5. owner_project always a sentinel**
Real `owner_project` values are never produced. `protocol-contract-likely` contracts surface in Airtable for manual assignment only.

**6. Gemini is paid (not free tier)**
`gemini-2.5-flash` now bills per token. Cost per run scales with number of contracts × trace length. Monitor via Google AI Studio usage dashboard.

---

## Classifier Weaknesses

### W1 — PATH C misses contract-to-contract aggregators (medium severity)

**Problem:** PATH C defaults `low DAA/tx → trading`. But intent-settlement and solver contracts called primarily by other contracts (not EOAs) will also show low DAA/tx — they're public infrastructure that looks private by this metric.

**Impact:** Solver/fulfillment contracts (e.g. 1inch resolvers, CoW settlement) that aren't verified and call official routers get misclassified as `trading`.

**Current mitigation:** PATH C only fires when the contract calls an official router, bounding the false positive surface. Verified contracts are an exception in G1/G2 but not yet in PATH C.

**Planned fix:** Before applying the low-DAA/tx→trading default in PATH C, check `is_verified`. If verified and calls official routers, prefer `dex` regardless of DAA/tx — same pattern as the G1/G2 verified exceptions.

```
PATH C fix:
  if is_verified AND calls_official_router:
      → dex (settlement/fulfillment infrastructure)
      → only assign trading if explicit failed-tx MEV patterns visible
  else:
      → existing DAA/tx logic
```

---

### W2 — PATH D sampling bias on mixed-behavior contracts (low severity)

**Problem:** The `"Direct calls into AMM pool swap functions"` flag is derived from a 10-tx sample. A contract that does both slot0 reads (price scouting) and swaps (execution) in different transactions may only show price-read txs in the sample. It then falls to PATH F (pure price scanner) instead of PATH D.

**Impact:** Limited — Tenderly samples the last 30 txs by recency, so swap-dominant contracts will usually show swaps in the sample. The misclassification direction (price-scanner → trading) is also usually correct for such contracts.

**No planned fix:** Acceptable tradeoff given sample size constraints. Document as known noise source in eval.

---

### W3 — Sentinel thresholds not empirically validated (medium severity)

**Problem:** `_is_protocol_likely()` uses `confidence >= 0.55` and `success_rate >= 0.40` as hard cutoffs. These are engineering intuitions, not calibrated against a labeled holdout.

**Impact:** Unknown false positive/negative rate for the sentinel. If thresholds are too low, Airtable fills with noise (trading bots flagged for human review). If too high, real protocol contracts are missed and never get `owner_project` assigned.

**Current feedback loop:** Airtable only — if reviewers consistently reject certain contract types, that's manual signal to raise the threshold.

**Planned fix:** Build a labeled holdout of ~50 contracts: "should have real owner_project" vs "fully automated, no project". Run `_is_protocol_likely()` against it and measure precision/recall. Adjust thresholds based on results. This is the highest-ROI use of the eval harness after `usage_category` accuracy is stable.

---

### W4 — PATH G2 KEEPER band is too wide (high severity, likely highest error rate)

**Problem:** 20–80% caller diversity covers radically different contract archetypes: CoW Protocol solvers, 1inch Fusion resolvers, RFQ market makers, liquidation bots, and governance executors all land in G2. The verified/unverified split handles the common cases but the `lean trading` default for unverified contracts mislabels protocol liquidators, bridge settlement contracts, and keeper-based vault managers.

**Impact:** Unverified lending liquidators (calls `liquidate` on Aave/Compound) and bridge settlement contracts (emits `MessagePassed`) get labeled `trading` because they're unverified + KEEPER. These are real protocol contracts with wrong categories.

**Planned fix:** Add specific trace-signal overrides inside G2 before the `lean trading` default, applied even when unverified:

```
PATH G2 unverified — check these BEFORE defaulting to trading:
  1. Calls to known lending selectors (liquidate, seize, auction, repay)
     AND no slot0/price-read pattern → lending (or derivative for perps)
  2. Bridge event signals (MessagePassed, SentMessage, PacketSent)
     OR calls to known bridge contracts (Stargate, LayerZero endpoint)
     → bridge or cc_communication
  3. ERC-4337 events (UserOperationEvent) → erc4337
  4. Governance events (ProposalCreated, VoteCast) → governance
  → All others → existing trading default
```

This requires logs to be re-enabled (W1 in Known Issues) for full signal coverage. With logs disabled, only trace-based signals (#1, bridge contracts in trace) are available.

---

## Optimization Room

- **Re-enable log fetching** once credit budget confirmed — events (ERC-4337, bridge, staking) significantly improve PATH G accuracy
- **Caching enrichment to disk** — `--cache-dir` flag: save/load Blockscout+Tenderly JSON, iterate on classifier logic without new API calls
- **Retry on empty Blockscout returns** — distinguish empty-array response (transient) from real zero-tx contract; one retry with delay

---

## Roadmap: owner_project Mapping

**Phase 1 — Protocol name from traces**
Traces contain named contracts (`UniswapV3Pool`, `AerodromeRouter`). Map top-level trace name → canonical project slug. Works for PATH A/B contracts without new data sources.

**Phase 2 — Deployer address → project**
Blockscout returns deployer for verified contracts. Maintain deployer → slug lookup, seedable from Sourcify attestations (`deployer_address` already in OLI trust list at 0.9).

**Phase 3 — GitHub repo → project**
`has_valid_repo=True` search result contains the repo. `Uniswap/v3-core` → `uniswap`. Requires storing `repo_url` string (currently only boolean flag stored).

**Phase 4 — OLI Label Pool transitive**
Query OLI trusted labels for deployer address. If deployer has `owner_project = X`, inherit X at lower confidence.

**Trust list impact:** Raise general attester `owner_project` confidence from 0.05 to 0.3–0.4 once real values are produced.
