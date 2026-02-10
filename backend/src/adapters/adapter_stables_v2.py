import time
import os 
import pandas as pd
from datetime import datetime, timedelta

from web3 import Web3
from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.adapter_SupplyReader import SupplyReaderAdapter

class AdapterStablecoinSupply(AbstractAdapter):
    """
    Adapter for scraping stablecoin supply across different chains.
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Stablecoin Adapter v2", adapter_params, db_connector)
        
        # Store stablecoin metadata and mapping
        from src.stables_config_v2 import address_mapping, coin_mapping
        self.address_mapping = address_mapping
        self.coin_mapping = coin_mapping

        # try to update sys_stables_v2
        #self.update_sys_stables_v2(self.coin_mapping)

        # keep track of which chain is currently being used
        self.current_chain = None
        self.current_rpc = None
        self.list_of_rpcs = None

        # dune adapter for backfilling historical data
        self.dune_adapter = None

        # load chain main config from db
        self.config = self.db_connector.get_table("sys_main_conf")
        self.config = self.config[['origin_key', 'deployed_supplyreader', 'aliases_dune']]


    def extract(self, extract_params: dict):
        """
        Extract stablecoin supply data for specified chains and token_ids.
        
        Args:
            extract_params (dict): Dictionary containing the parameters for extraction. Expected keys are:
            - origin_keys (list): List of chain names to extract data for.  Use ['*'] for all.
            - token_ids (list): List of token_ids to extract data for. Use ['*'] for all.
        """
        # all data combined
        df_all = None

        chains = extract_params.get('origin_keys', ['*'])
        token_ids = extract_params.get('token_ids', ['*'])

        print("Extracting latest stablecoin supply data for chains:", chains, "and token_ids:", token_ids)

        # exchange '*' for all chains in mapping
        if chains == ['*']:
            chains = list(self.address_mapping.keys())
        # exchange '*' for all token_ids in mapping
        if token_ids == ['*']:
            token_ids = [coin['token_id'] for coin in self.coin_mapping]
        
        # load current progress from db
        db_progress = self.check_db_for_current_data()

        # iterate through each chain and get data
        for chain in chains:

            # check if chain is in mapping, if not skip and log warning
            if chain not in self.address_mapping:
                print(f"Chain '{chain}' not found in address mapping, skipping. Please check the file: src/stables_config_v2.py.")
                continue

            # get df with totalSupplies
            df = self.get_stablecoin_data_for_chain(chain, token_ids, db_progress)

            # merge df into df_all
            if df_all is None:
                df_all = df
            elif df is not None:
                df_all = pd.concat([df_all, df], ignore_index=True)

        # return the combined df with all data
        return df_all
    

    # load data into db
    def load(self, df, table_name: str="fact_stables_v2"):
        df = df.set_index(['origin_key', 'token_id', 'address', 'date'])
        self.db_connector.upsert_table(table_name, df)


    #-#-#-# Helper Functions #-#-#-#

    def get_stablecoin_data_for_chain(self, chain, token_ids, db_progress):
        """
        """
        # if chain is listed on dune, use that to get the data
        if self.config[self.config['origin_key'] == chain]['aliases_dune'].iloc[0] is not None:
            print(f"Chain '{chain}' is listed on Dune, using Dune adapter to get stablecoin data.")
            df = self.pull_from_dune(chain, token_ids, db_progress)
            return df
        else:
            print(f"Chain '{chain}' is not listed on Dune, skipping Dune adapter.")

        # if chain has SupplyReader deployed, use that to get the data
        # ...

        # if chain does not have any of the above options, scrape using manual rpc calls
        # ...

    def pull_from_dune(self, chain, token_ids, db_progress):
        # get dune chain id from config
        dune_chain_id = self.config[self.config['origin_key'] == chain]['aliases_dune'].iloc[0]

        # filter token_addresses, token_decimals and token_ids based on token_ids list
        chain_token_addresses = [
            self.address_mapping[chain][token]['address'] 
            for token in self.address_mapping[chain]
            if token in token_ids
        ]
        chain_token_decimals = [
            self.address_mapping[chain][token]['decimals'] 
            for token in self.address_mapping[chain]
            if token in token_ids
        ]
        chain_token_ids = [
            token for token in self.address_mapping[chain]
            if token in token_ids
        ]

        # see if we have new coins which would require a full backfill
        new_coins = [token for token in chain_token_ids if token not in db_progress[db_progress['origin_key'] == chain]['token_id'].unique()]

        # filter out yesterdays date in db_progress
        date_yesterday = (datetime.now() - timedelta(days=1)).date()
        db_progress = db_progress[db_progress['date'] != date_yesterday]

        # special case for starknet
        if chain == 'starknet':
            return None

        # if we have even just one "new_coins", might aswell backfill all token_ids using dune
        if len(new_coins) > 0:
            print(f"New coins for chain '{chain}' found, which are not in the database and will be backfilled using Dune: {new_coins}")
            df = self.backfill_dune(
                chain, 
                dune_chain_id, 
                chain_token_addresses,
                chain_token_ids, 
                chain_token_decimals
            )
            if df is not None:
                return df
        
        # if we already have data for token_ids in the db, we can use the more efficient SupplyReader delta script to only get the new datapoints
        if len(db_progress) > 0:
            df = self.read_total_supplies_dune(
                chain, 
                dune_chain_id,
                db_progress[db_progress['origin_key'] == chain]['address'].tolist(),
                db_progress[db_progress['origin_key'] == chain]['token_id'].tolist(),
                db_progress[db_progress['origin_key'] == chain]['decimals'].tolist(),
                db_progress[db_progress['origin_key'] == chain]['value'].tolist(),
                db_progress[db_progress['origin_key'] == chain]['date'].tolist()
            )
            if df is not None:
                return df
        
        print(f"Nothing to backfill for chain '{chain}'.")

    #-#-#-# Raw Helper Functions #-#-#-#

    def backfill_dune(self, origin_key: str, dune_chain_id: str, token_addresses: list, token_ids: list, decimals: list):
        """
        Function to get historical supply data for a given chain and token_ids using dune.
        """
        # import dune adapter here to avoid multiple/unecessary imports
        if self.dune_adapter is None:
            from src.adapters.adapter_dune import AdapterDune
            self.dune_adapter  = AdapterDune({'api_key' : os.getenv("DUNE_API")}, self.db_connector)

        # create the strings for dune query parameters
        token_addresses_string = ','.join([f"{address}" for address in token_addresses])
        token_ids_string = ','.join([f"{token_id}" for token_id in token_ids])

        # define & load parameters from dune adapter
        load_params = {
            'queries': [
                {
                    'name': 'stablecoin_totalSupplies_history',
                    'query_id': 6676647,
                    'params': {
                        'chain': dune_chain_id,
                        'token_ids': token_ids_string,
                        'contracts': token_addresses_string
                    }
                }
            ]
        }
        df = self.dune_adapter.extract(load_params)

        # add additional columns
        df['origin_key'] = origin_key
        df['decimals'] = df.apply(lambda row: decimals[token_ids.index(row['token_id'])], axis=1)
        df['address'] = df.apply(lambda row: token_addresses[token_ids.index(row['token_id'])].lower(), axis=1)
        df['totalSupply'] = df.apply(lambda row: int(row['totalSupply']) / (10 ** row['decimals']), axis=1)
        df = df.rename(columns={'totalSupply': 'value'})

        return df
    
    def read_total_supplies_dune(self, origin_key: str, dune_chain_id: str, token_addresses: list, token_ids: list, decimals: list, last_totalSupplies: list, dates_of_last_totalSupplies: list):
        """
        Function to calculate total supplies based on values in our database for a chain and token_ids using dune.
        """
        # import dune adapter here to avoid multiple/unecessary imports
        if self.dune_adapter is None:
            from src.adapters.adapter_dune import AdapterDune
            self.dune_adapter  = AdapterDune({'api_key' : os.getenv("DUNE_API")}, self.db_connector)

        # convert last_totalSupplies to original format with decimals
        last_totalSupplies = [int(supply * (10 ** decimal)) for supply, decimal in zip(last_totalSupplies, decimals)]

        # create the strings for dune query parameters
        token_addresses_string = ','.join([f"{address}" for address in token_addresses])
        token_ids_string = ','.join([f"{token_id}" for token_id in token_ids])
        last_totalSupplies_string = ','.join([f"{totalSupply}" for totalSupply in last_totalSupplies])
        dates_of_totalSupplies_string = ','.join([f"{date}" for date in dates_of_last_totalSupplies])

        # define load parameters for dune adapter
        load_params = {
            'queries': [
                {
                    'name': 'stablecoin_totalSupplies_from_date',
                    'query_id': 6680327,
                    'params': {
                        'chain': dune_chain_id,
                        'token_ids': token_ids_string,
                        'contracts': token_addresses_string,
                        'totalSupplies': last_totalSupplies_string,
                        'dates_of_totalSupplies': dates_of_totalSupplies_string
                    }
                }
            ]
        }
        df = self.dune_adapter.extract(load_params)

        # add additional columns
        df['origin_key'] = origin_key
        df['decimals'] = df.apply(lambda row: decimals[token_ids.index(row['token_id'])], axis=1)
        df['address'] = df.apply(lambda row: token_addresses[token_ids.index(row['token_id'])].lower(), axis=1)
        df['totalSupply'] = df.apply(lambda row: int(row['totalSupply']) / (10 ** row['decimals']), axis=1)
        df = df.rename(columns={'totalSupply': 'value'})
        
        return df

    def read_total_supplies_SupplyReader(self, w3: Web3, token_addresses: list, token_decimals: list, block_number: int = None):
        """
        Using SupplyReader to read total supplies for 
        """
        try:
            SupplyReader = SupplyReaderAdapter(w3)
            supplies = SupplyReader.get_total_supplies(token_addresses, block_number)
            supplies = [supply / (10 ** decimals) for supply, decimals in zip(supplies, token_decimals)]
            return supplies
        except Exception as e:
            print(f"Error reading total supplies with SupplyReader on chain {self.current_chain} with RPC {self.current_rpc} for contracts {token_addresses}: {e}")
            return None
        
    def get_new_w3(self, chain: str):
        """
        Function to create a new Web3 instance for a given chain. Automatically rotates through available RPCs in case it is called multiple times for the same chain.
        """

        # if we are switching to a new chain first rpc we try is always get_special_use_rpc. Then we set current_chain, current_rpc and list_of_rpcs variables.
        if self.current_chain != chain:

            self.current_chain = chain
            self.current_rpc = self.db_connector.get_special_use_rpc(chain)
            self.list_of_rpcs = None
            print(f"Switched w3 instance to new chain: {chain} with RPC: {self.current_rpc}")
            return Web3(Web3.HTTPProvider(self.current_rpc))

        # if we are not switching to a new chain, it means we are trying to create a new w3 instance for the same chain, which means the previous rpc we were using might not be working
        else:

            if self.list_of_rpcs is None:
                self.list_of_rpcs = self.db_connector.get_all_rpcs_for_chain(chain)
                self.list_of_rpcs.remove(self.current_rpc)

            # in case the list of rpcs is empty, it means we have tried all available rpcs for the chain and they are all not working
            if len(self.list_of_rpcs) == 0:
                raise ValueError(f"No RPCs found for chain {chain}. Please add more RPCs to your database for this chain.")
            
            # rotate to the next rpc in the list
            self.current_rpc = self.list_of_rpcs[0]
            self.list_of_rpcs.append(self.current_rpc)
            print(f"Rotating w3 instance to new RPC for chain: {chain} to new RPC: {self.current_rpc}")
            return Web3(Web3.HTTPProvider(self.current_rpc))

    def check_db_for_current_data(self):
        """
        Function to check what stablecoin data we already have in our database and filled until which date.
        """
        query = f"""
            SELECT DISTINCT ON (f.origin_key, f.token_id, f.address)
                f.date,
                f.origin_key, 
                f.token_id, 
                f.address,
                f.decimals,
                f.value,
                s.owner_project,
                s.symbol,
                s.metric_key,
                s.bridged_origin_chain,
                s.bridged_origin_token_id,
                s.coingecko_id,
                s.fiat,
                s.logo
            FROM public.fact_stables_v2 f
            LEFT JOIN public.sys_stables_v2 s ON f.token_id = s.token_id
            ORDER BY f.origin_key, f.token_id, f.address, f.date DESC;
        """
        result = self.db_connector.execute_query(query, load_df=True)
        return result

    def update_sys_stables_v2(self, coin_mapping):
        """
        Function to update the sys_stables_v2 table with the latest stablecoin metadata from the config file.
        """
        try:
            self.db_connector.delete_all_rows("sys_stables_v2")
            df = pd.DataFrame(coin_mapping).set_index('token_id')
            self.db_connector.upsert_table("sys_stables_v2", df)
            print(f"{len(df)} rows inserted into sys_stables_v2.")
        except Exception as e:
            print(f"Error updating sys_stables_v2 table: {e}")