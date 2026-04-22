# Contract Label Features — Database Schema

This document describes the proposed `contract_label_features` table.
It captures every signal computed during the automated labeling pipeline per contract,
alongside the AI output and any available OLI ground truth.
The intended use is training and evaluating ML models for smart contract classification.

---

## Primary Key

`(address, origin_key)` — one row per contract per chain.
Re-running the labeler on the same contract should upsert (update `labeled_at` and all signal columns).

---

## Column Groups

### 1. Identity

| Column | Type | Description |
|---|---|---|
| `address` | `bytea` | Contract address (PK) |
| `origin_key` | `varchar` | Chain identifier, e.g. `base`, `optimism` (PK) |
| `labeled_at` | `timestamptz` | Timestamp when the AI classifier ran |
| `blockscout_name` | `varchar` | Raw contract name from Blockscout |
| `is_verified` | `bool` | Source code verified on Blockscout |
| `is_proxy` | `bool` | Contract is a proxy pattern |
| `impl_address` | `bytea` | Implementation address (null if not proxy) |
| `impl_name` | `varchar` | Resolved implementation contract name |
| `impl_verified` | `bool` | Implementation source verified |
| `has_github_repo` | `bool` | Contract address found in a GitHub repo |
| `github_repo_count` | `int` | Number of repos referencing this address |

---

### 2. Activity Metrics

Aggregated from `blockspace_fact_contract_level` over `metric_day_range` days.

| Column | Type | Description |
|---|---|---|
| `metric_day_range` | `int` | Window used for aggregation (e.g. 7, 30) |
| `txcount` | `int` | Total transactions in window |
| `gas_eth` | `numeric` | Total gas fees in ETH |
| `avg_daa` | `numeric` | Average daily active addresses |
| `avg_gas_per_tx` | `numeric` | Gas ETH / txcount |
| `rel_cost` | `numeric` | Median tx fee vs chain median (1.0 = at median) |
| `success_rate` | `numeric` | Fraction of successful transactions (0.0–1.0) |
| `daa_ratio` | `numeric` | avg_daa / txcount × 100 — proxy for automation vs human use |

---

### 3. Caller Diversity

Derived from the sampled transaction traces.

| Column | Type | Description |
|---|---|---|
| `caller_diversity_pct` | `numeric` | Unique callers / sampled txns × 100 |
| `unique_callers` | `int` | Distinct sender addresses in sample |
| `total_traces_sampled` | `int` | Total transactions sampled (sample size context) |
| `caller_diversity_label` | `varchar` | `PRIVATE` (≤20%) / `KEEPER` (20–80%) / `PUBLIC` (≥80% + DAA threshold) |

---

### 4. Trace-Derived Protocol Signals

Computed from Tenderly traces (depth-0/1 direct calls).

| Column | Type | Description |
|---|---|---|
| `novel_tokens` | `text[]` | ERC20 token names seen in ≥50% of traces, not common infra (WETH/USDC/etc.) — high signal for protocol identity |
| `common_tokens` | `text[]` | ERC20 token names seen in ≥50% of traces that ARE common infra — low signal |
| `bs_token_transfers` | `jsonb` | Raw Blockscout token-transfer records: `[{token_name, token_symbol, token_type}]` |
| `matched_routers` | `text[]` | Named DEX routers called directly (e.g. `UniswapV3Router`) |
| `matched_dex_pools` | `text[]` | AMM pool contracts called directly |
| `calls_into_dex_pool_swap` | `bool` | Made state-changing swap calls into AMM pools |
| `matched_lending` | `text[]` | Lending protocols called (Aave, Compound, Morpho, etc.) |
| `matched_staking` | `text[]` | Staking/gauge contracts called (Gauge, Voter, MasterChef, etc.) |
| `matched_bridges` | `text[]` | Bridge protocols called (Relay, LayerZero, Stargate, etc.) |
| `matched_oracle_fns` | `text[]` | Oracle function calls detected (latestAnswer, getPrice, etc.) |
| `delegates_to` | `varchar` | Named contract this contract delegates to |
| `raw_delegate` | `bool` | EIP-1167 minimal proxy / clone |
| `all_traces_empty` | `bool` | All sampled traces had no call graph |
| `traces_only_raw_opcodes` | `bool` | Traces had no decoded function or contract names |
| `named_contracts_in_traces` | `text[]` | All named contracts seen anywhere in traces |

---

### 5. Log Signals

Decoded from the contract's own emitted events (Blockscout logs endpoint).

| Column | Type | Description |
|---|---|---|
| `log_has_access_control` | `bool` | RoleGranted / Revoked / OwnershipTransferred events |
| `log_has_token_transfers` | `bool` | ERC20/721 Transfer or Approval events |
| `log_has_swaps` | `bool` | DEX Swap events |
| `log_has_bridge_events` | `bool` | MessagePassed / SentMessage / TransactionDeposited |
| `log_has_erc4337` | `bool` | UserOperationEvent / AccountDeployed (account abstraction) |
| `log_has_staking` | `bool` | Staking-related events |
| `log_has_governance` | `bool` | ProposalCreated / VoteCast |
| `log_category_hints` | `text[]` | Derived category signals from event patterns |
| `log_top_events` | `jsonb` | Event name → occurrence count, e.g. `{"Swap": 41, "Transfer": 12}` |

---

### 6. AI Output

The label and metadata produced by the Gemini classifier.

| Column | Type | Description |
|---|---|---|
| `ai_usage_category` | `varchar` | Assigned OLI usage category ID (e.g. `dex`, `token_contract`) |
| `ai_contract_name` | `varchar` | Assigned human-readable protocol/contract name |
| `ai_confidence` | `numeric` | Model confidence 0.0–1.0 |
| `ai_reasoning` | `text` | Free-text reasoning from the model |
| `ai_model` | `varchar` | Model version used (e.g. `gemini-2.5-flash`) |
| `sentinel_fired` | `bool` | Whether the rule-based sentinel short-circuited the AI call |
| `sentinel_category` | `varchar` | Category assigned by sentinel (null if sentinel did not fire) |

---

### 7. OLI Ground Truth

Available only for contracts that have been attested in the OLI label pool.
Use `is_human_label = true` rows as the high-quality supervised training set.

| Column | Type | Description |
|---|---|---|
| `oli_usage_category` | `varchar` | Confirmed OLI usage category |
| `oli_owner_project` | `varchar` | Protocol / project name from OLI |
| `oli_confirmed_at` | `timestamptz` | When the attestation was confirmed |
| `is_human_label` | `bool` | `true` = manually attested, `false` = automated attester (noisy label) |

---

## Recommended Indexes

```sql
-- Primary key
PRIMARY KEY (address, origin_key)

-- Slice by chain + time for training data queries
CREATE INDEX ON contract_label_features (origin_key, labeled_at DESC);

-- Filter to human-confirmed ground truth
CREATE INDEX ON contract_label_features (is_human_label) WHERE is_human_label = true;

-- Filter to low-confidence AI outputs (most informative for active learning)
CREATE INDEX ON contract_label_features (ai_confidence) WHERE ai_confidence < 0.7;
```

---

## Notes for ML Use

### Feature encoding

- **`text[]` columns** — one-hot encode with `unnest()` at query time or embed as bag-of-words. Do not flatten to individual boolean columns in the DB; the array form is more flexible.
- **`jsonb` columns** (`bs_token_transfers`, `log_top_events`) — extract structured features at training time. Token names are high-cardinality free text; consider embedding them separately (e.g. token name → embedding vector).
- **`ai_confidence`** — use as sample weight during training, not as a filter. Low-confidence rows are often the hardest and most informative cases.

### Label quality tiers

| Tier | Filter | Use for |
|---|---|---|
| High quality | `is_human_label = true` | Validation, fine-tuning |
| Medium quality | `is_human_label = false AND ai_confidence >= 0.85` | Pre-training, weak supervision |
| Exploratory | `ai_confidence < 0.7` | Active learning, error analysis |

### Model ideas

| Task | Input features | Target |
|---|---|---|
| Category classifier | All signal columns | `ai_usage_category` or `oli_usage_category` |
| Confidence predictor | All signal columns | `ai_confidence` — find systematic over/under-confidence |
| Sentinel bypass | Cheap signals only (metrics + log flags, no traces) | Category — skip Gemini for obvious cases |
| Anomaly detection | All feature vectors | Unsupervised — flag contracts that fit no known cluster |
| Label correction | Features where `ai_category != oli_category` | Error pattern analysis |

### What is NOT stored here

- Raw transaction traces (too large; stored transiently during enrichment only)
- Raw Blockscout API responses (derive features from them instead)
- ABI or source code (too large; use Blockscout `is_verified` + `blockscout_name` as proxies)
