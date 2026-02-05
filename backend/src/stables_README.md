# How to maintain stables_config_v2.py

## Structure of mapping

We have stables_metadata (backend/src/stables_config_v2.py), which gives the metadata information on each stablecoin.

we have address_mapping, which maps tokenids to each chain oprigin_key and is semi automatatble.

### stables_metadata

basic strucutre is as follows:

```json
{
        "owner_project": "aegis-im",
        "token_id": "aegis-im_yusd",
        "symbol": "YUSD",
        "coingecko_id": ["aegis-yusd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
```
mandatory fields:

origin_key like in our gpt dna.
token_id is just a uinqieuy identification stiring. good practive owner project + symbol.
metric_key: either "direct" or "bridged"
if metric_key = "bridged" then also bridged_origin_chain, bridged_origin_token_id are mandatory. THey give info on which stablecioin to deduct the bruidged amounts from to not double count stabvlecoins that are locked in bridged contracts.
fiat as defined in the db under (backend/src/currency_config.py):
```sql
SELECT metric_key, origin_key, "date", value
FROM public.fact_kpis
where origin_key like 'fiat_%'
order by date desc
```

optional fields:

owner_project, highly recommended to map this to our OLI Schema owner_project tag. For easy creation of new projects in OSS see here: https://www.openlabelsinitiative.org/project
coingecko_id is a list and optional, thouygh highly recommended for semi automation
logo


### address_mapping

connects the token id with the chains. We track addresses locally on each chain rather than bridge contracts. We also track the decimals on each address, as for some stablecoins this changes bnased on which chain you.

can be semiu automated if coingecko_id was added in stables_metadata. in this case ran the following script to look into each coingecko_id for new deployments. THis scriupts then prints out a new  address_mapping for you. to copy paste and PR in. Therefore please do not comment on the address_mapping.

Addresses can also be added manually, it will not be deleted or so, it is just more diteas...!


```python
import requests, os, time
from src.stables_config_v2 import coin_mapping, address_mapping

def get_coin_data(coin_id):
    response_id = requests.get(
        f"https://pro-api.coingecko.com/api/v3/coins/{coin_id}", 
        headers={ "x-cg-pro-api-key": os.getenv("COINGECKO_API") }
    )
    time.sleep(1) # be gentle to the API and avoid hitting rate limits
    return response_id.json()

# load aliases_coingecko_chain <> origin_key mapping, will only load chain if in sys_main_conf & aliases_coingecko_chain column set
from src.db_connector import DbConnector
db_connector = DbConnector()
config = db_connector.get_table('sys_main_conf')[['origin_key', 'aliases_coingecko_chain']]
config = config.loc[config['aliases_coingecko_chain'].notnull()]
config = config.set_index('aliases_coingecko_chain')
origin_key_mapping = config.to_dict()['origin_key']

# which token_ids to check, ['*'] for all, or specify list like e.g. ['circlefin_usdc', 'tetherto_usdt']
token_ids_to_check = ['*']
token_ids_to_check = ['circlefin_usdce']

# create address_mapping_cg based on coingecko_ids
address_mapping_cg = {}
unrecognised_chains = set()
for coin in coin_mapping:
    if token_ids_to_check != ['*'] and coin['token_id'] not in token_ids_to_check:
        continue
    cg_ids = coin['coingecko_id']
    #print(f"Processing coin: {coin['token_id']} with Coingecko IDs: {cg_ids}") 
    for cg_id in cg_ids:
        if cg_id is None:
            continue
        coin_data = get_coin_data(cg_id)
        for chain in coin_data['platforms']:
            origin_key = origin_key_mapping.get(chain, None)
            if origin_key is None:
                unrecognised_chains.add(chain)
                continue
            address_mapping_cg.setdefault(origin_key, {})[coin['token_id']] = {
                'address': coin_data['detail_platforms'][chain]['contract_address'],
                'decimals': coin_data['detail_platforms'][chain]['decimal_place']
            }

# merge address_mapping_cg into address_mapping, if chain, owner_project, coin_symbol not already present
for chain in address_mapping_cg:
    for token_id in address_mapping_cg[chain]:
        if chain not in address_mapping:
            print(f"Adding new chain to address_mapping: {chain}")
            address_mapping[chain] = {}
        if token_id not in address_mapping[chain]:
            print(f"Adding new token_id to address_mapping for chain {chain}: {token_id}")
            address_mapping[chain][token_id] = address_mapping_cg[chain][token_id]

# print all chains that are unrecognised, need to be mapped to coingecko
if unrecognised_chains:
    print("The following chains from Coingecko are not recognised in your origin_key mapping. Please add them to sys_main_conf with the correct origin_key:")
    for chain in unrecognised_chains:
        print(f"- {chain}")

# pretty print the address_mapping for easy copy pasting
import json
print(json.dumps(address_mapping, indent=4))
```


## Finding Stablecoins on a new chain

great way to find stablecoins:
1) coingecko ecosystem page such as here: https://www.coingecko.com/en/categories/manta-network-ecosystem
2) open most prominent blockexplorer and type "USD" see what tokens appear
3) to get bridge contract on Ethereum use L2beats mapping: https://github.com/l2beat/l2beat/blob/db894b3572879ef8b45b315da948de2fc0f8c879/packages/config/src/tvs/json/mint.json


##