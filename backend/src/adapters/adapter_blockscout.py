import csv
import json
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Any
import time

from src.adapters.abstract_adapters import AbstractAdapter
from src.main_config import get_main_config
from src.misc.helper_functions import print_init, print_load, print_extract


class AdapterBlockscout(AbstractAdapter):

    @dataclass
    class Contract:
        address: str
        origin_key: str = None
        name: str = None
        deployment_date: str = None
        is_proxy: bool = None
        source_code_verified: str = None

    @dataclass
    class BlockscoutResponse:
        name: str = None
        additional_fields: Dict[str, Any] = field(default_factory=dict)
        def __post_init__(self):
            self.additional_fields = {}
        def __init__(self, **kwargs):
            self.__post_init__()  # Ensure additional_fields is initialized
            fields = set(field.name for field in self.__dataclass_fields__.values())
            for key, value in kwargs.items():
                if key in fields:
                    setattr(self, key, value)
                else:
                    self.additional_fields[key] = value

    @dataclass
    class ProcessingStats:
        total_contracts: int = 0
        processed_count: int = 0
        contracts_with_name: int = 0
        proxy_contracts: int = 0
        elapsed_time: str = ""

    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Blockscout", adapter_params, db_connector)
        self.BLOCKSCOUT_APIS = {
            "optimism": "https://optimism.blockscout.com/api/v2/smart-contracts/",
            "polygon_zkevm": "https://zkevm.blockscout.com/api/v2/smart-contracts/",
            "mode": "https://explorer.mode.network/api/v2/smart-contracts/",
            "arbitrum": "https://arbitrum.blockscout.com//api/v2/smart-contracts/",
            "zora": "https://explorer.zora.energy/api/v2/smart-contracts/",
            "base": "https://base.blockscout.com/api/v2/smart-contracts/",
            "zksync_era": "https://zksync.blockscout.com/api/v2/smart-contracts/",
            "linea": "https://explorer.linea.build/api/v2/smart-contracts/",
            "redstone": "https://explorer.redstone.xyz/api/v2/smart-contracts/",
        }
        main_conf = get_main_config(db_connector)
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        metric_keys:list - list of metrics that should be loaded. E.g. prices, total_volumes, market_cap
        origin_keys:list - the projects that this metric should be loaded for. If None, all available projects will be loaded
        days:str - days of historical data that should be loaded, starting from today. Can be set to 'max'
        vs_currencies:list - list of currencies that we load financials for. E.g. eth, usd
        load_type:str - can be project or imx_tokens 
    """

    def extract(self, load_params:dict):
        return None

    def load(self, df:pd.DataFrame):
        pass      


    ## ----------------- Helper functions --------------------

    def process_csv(self, file_path: str) -> List[Contract]: #### rewrite to use db_connector!
        contracts = []
        with open(file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                address = row['encode']
                if not address.startswith('0x'):
                    address = '0x' + address
                contract = AdapterBlockscout.Contract(
                    address=address,
                    origin_key=row['origin_key']
                )
                contracts.append(contract)
        return contracts

    def fetch_blockscout_data(self, api_url: str, address: str) -> BlockscoutResponse:
        url = api_url + address
        max_retries = 3
        retry_delay = 1

        for _ in range(max_retries):
            response = requests.get(url)
            if response.status_code == 200:
                return AdapterBlockscout.BlockscoutResponse(**response.json())
            elif response.status_code == 404:
                raise ValueError("Contract not found")
            time.sleep(retry_delay)

        raise Exception(f"Failed to fetch data after {max_retries} retries")

    def map_blockscout_to_open_labels(self, contract: Contract, blockscout_data: BlockscoutResponse) -> Contract:
        contract.name = blockscout_data.name
        contract.source_code_verified = blockscout_data.sourcify_repo_url
        contract.is_proxy = bool(blockscout_data.minimal_proxy_address_hash)

        if blockscout_data.verified_at:
            contract.deployment_date = blockscout_data.verified_at

        return contract

    def process_contract(self, contract: Contract) -> Contract:
        api_url = self.BLOCKSCOUT_APIS.get(contract.origin_key)
        if not api_url:
            print(f"Unknown origin key for contract {contract.address}: {contract.origin_key}")
            return contract
        try:
            blockscout_data = self.fetch_blockscout_data(api_url, contract.address)
            print(blockscout_data)
            return self.map_blockscout_to_open_labels(contract, blockscout_data)
        except ValueError:
            print(f"Contract not found on Blockscout for {contract.address} on {contract.origin_key}")
        except Exception as e:
            print(f"Error processing contract {contract.address}: {str(e)}")

        return contract

    def process_contracts(self, contracts: List[Contract], max_queries: int = 0) -> (List[Contract], ProcessingStats):
        processed_contracts = []
        stats = AdapterBlockscout.ProcessingStats(total_contracts=len(contracts))
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=50) as executor:
            future_to_contract = {executor.submit(self.process_contract, contract): contract for contract in contracts[:max_queries or len(contracts)]}
            for future in as_completed(future_to_contract):
                contract = future.result()
                processed_contracts.append(contract)
                stats.processed_count += 1
                if contract.name:
                    stats.contracts_with_name += 1
                if contract.is_proxy:
                    stats.proxy_contracts += 1
                stats.elapsed_time = f"{time.time() - start_time:.2f} seconds"
                #print(f"Processed {stats.processed_count}/{stats.total_contracts} contracts")

        return processed_contracts, stats

"""def main(self):
    # Example usage
    csv_file_path = '/Users/ahoura/Desktop/unlabelled.csv'
    contracts = process_csv(csv_file_path)
    processed_contracts, stats = process_contracts(contracts, max_queries=2153)  # Process up to 100 contracts

    # Print stats
    print(json.dumps(asdict(stats), indent=2))

    # Save processed contracts to a JSON file
    with open('processed_contracts.json', 'w') as f:
        json.dump([asdict(c) for c in processed_contracts], f, indent=2)

if __name__ == "__main__":
    main()"""