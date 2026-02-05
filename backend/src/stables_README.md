# Maintaining `stables_config_v2.py`

This document explains the structure and maintenance workflow for stablecoin metadata and address mappings in `backend/src/stables_config_v2.py`.

## Overview

`stables_config_v2.py` contains two main structures:

1. `stables_metadata`: per‑stablecoin metadata.
2. `address_mapping`: per‑chain token address + decimals mapping keyed by `token_id`.

## `stables_metadata`

Example entry:

```json
{
    "owner_project": "aegis-im",
    "token_id": "aegis-im_yusd",
    "symbol": "YUSD",
    "coingecko_id": ["aegis-yusd"],
    "metric_key": "direct",
    "bridged_origin_chain": null,
    "bridged_origin_token_id": null,
    "fiat": "usd",
    "logo": ""
}
```

### Required fields

- `origin_key`: matches our chain origin key (see `sys_main_conf`).
- `token_id`: unique identifier. Recommended format: `owner_project + "_" + symbol`.
- `metric_key`: either `direct` or `bridged`.
- If `metric_key == "bridged"`, then both `bridged_origin_chain` and `bridged_origin_token_id` are required. These define the canonical stablecoin to deduct bridged amounts from to avoid double counting.
- `fiat`: must match a fiat key defined in `backend/src/currency_config.py` (see the `fiat_*` entries in the DB).

Reference query for fiat keys:

```sql
SELECT metric_key, origin_key, "date", value
FROM public.fact_kpis
WHERE origin_key LIKE 'fiat_%'
ORDER BY date DESC;
```

### Optional fields

- `owner_project`: strongly recommended. Should map to our OLI schema `owner_project` tag. For adding projects, see: https://www.openlabelsinitiative.org/project
- `coingecko_id`: list of Coingecko IDs. Recommended for semi‑automated address mapping.
- `logo`: optional.

## `address_mapping`

- Maps `token_id` to per‑chain addresses.
- We track **local token addresses on each chain** (not bridge contracts).
- Always track `decimals` per address because decimal places can differ across chains.

If `coingecko_id` is provided in `stables_metadata`, you can generate new mappings via the script below. It prints a full `address_mapping` blob for copy‑paste into `stables_config_v2.py`. **Do not comment the `address_mapping` section in the file**, to keep auto‑generated diffs clean.

### Semi‑automated update script

```python
import json
import os
import time
import requests
from src.stables_config_v2 import coin_mapping, address_mapping


def get_coin_data(coin_id):
    response = requests.get(
        f"https://pro-api.coingecko.com/api/v3/coins/{coin_id}",
        headers={"x-cg-pro-api-key": os.getenv("COINGECKO_API")},
    )
    time.sleep(1)  # avoid rate limits
    return response.json()


# Load aliases_coingecko_chain <> origin_key mapping
from src.db_connector import DbConnector

db_connector = DbConnector()
config = db_connector.get_table("sys_main_conf")[["origin_key", "aliases_coingecko_chain"]]
config = config.loc[config["aliases_coingecko_chain"].notnull()]
config = config.set_index("aliases_coingecko_chain")
origin_key_mapping = config.to_dict()["origin_key"]

# Which token_ids to check
# Use ['*'] for all, or a specific list like ['circlefin_usdc', 'tetherto_usdt']
# token_ids_to_check = ['*']
token_ids_to_check = ["circlefin_usdce"]

address_mapping_cg = {}
unrecognised_chains = set()

for coin in coin_mapping:
    if token_ids_to_check != ["*"] and coin["token_id"] not in token_ids_to_check:
        continue
    for cg_id in coin["coingecko_id"]:
        if cg_id is None:
            continue
        coin_data = get_coin_data(cg_id)
        for chain in coin_data["platforms"]:
            origin_key = origin_key_mapping.get(chain)
            if origin_key is None:
                unrecognised_chains.add(chain)
                continue
            address_mapping_cg.setdefault(origin_key, {})[coin["token_id"]] = {
                "address": coin_data["detail_platforms"][chain]["contract_address"],
                "decimals": coin_data["detail_platforms"][chain]["decimal_place"],
            }

# Merge address_mapping_cg into address_mapping
for chain in address_mapping_cg:
    for token_id in address_mapping_cg[chain]:
        if chain not in address_mapping:
            print(f"Adding new chain to address_mapping: {chain}")
            address_mapping[chain] = {}
        if token_id not in address_mapping[chain]:
            print(f"Adding new token_id to address_mapping for chain {chain}: {token_id}")
            address_mapping[chain][token_id] = address_mapping_cg[chain][token_id]

# Report unknown chains
if unrecognised_chains:
    print(
        "The following Coingecko chains are not recognised. "
        "Please map them in sys_main_conf (aliases_coingecko_chain):"
    )
    for chain in unrecognised_chains:
        print(f"- {chain}")

# Pretty print the address_mapping for copy/paste
print(json.dumps(address_mapping, indent=4))
```

## Finding stablecoins on a new chain

Start with a quick scan, then confirm contracts on the explorer:

1. Coingecko ecosystem page (example): https://www.coingecko.com/en/categories/manta-network-ecosystem
2. The chain’s block explorer: search for `USD` and inspect prominent tokens.
3. L2BEAT mint mapping for bridge contracts (Ethereum): https://github.com/l2beat/l2beat/blob/db894b3572879ef8b45b315da948de2fc0f8c879/packages/config/src/tvs/json/mint.json
4. DeFi Llama stablecoin mapping (may require digging in GitHub for addresses): https://defillama.com/stablecoins/arbitrum
4. Defillamas stablecoin mapping (though need to dig through github to find addresese): https://defillama.com/stablecoins/arbitrum
