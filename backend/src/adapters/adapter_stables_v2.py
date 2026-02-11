from itertools import chain
from logging import config
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

        # adapter for getting data
        self.dune_adapter = None
        self.SupplyReader = None

        # abi for reading totalSupply directly from ERC20 contracts
        self.erc20_abi = [{"constant": True,"inputs": [],"name": "totalSupply","outputs": [{"name": "", "type": "uint256"}],"type": "function"}]

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
        db_progress = self.get_db_progress()

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
            if df is not None:
                return df

        # else scrape using rpc calls & SupplyReader (if deployed on the chain)
        df = self.pull_from_rpc(chain, token_ids, db_progress)
        if df is not None:
            return df
        
        # if we cannot get any data for the chain, log a warning
        print(f"Could not get stablecoin supply data for chain '{chain}' using any method!")
        return None

    def pull_from_dune(self, chain, token_ids, db_progress):
        # get dune chain id from config
        dune_chain_id = self.config[self.config['origin_key'] == chain]['aliases_dune'].iloc[0]

        # create specific chain db_progress df (includes new coins and removes already upto-date coins)
        db_progress_chain = self.get_db_progress_chain(chain, token_ids, db_progress)

        ##### special case for starknet
        if chain == 'starknet':
            return None

        # one df
        df_all = None

        # pull in new coins with complete history
        if len(db_progress_chain[db_progress_chain['date'].isna()]) > 0:
            new_coins = db_progress_chain[db_progress_chain['date'].isna()]['token_id'].tolist()
            print(f"New coins for chain '{chain}' found, backfilled complete history using Dune: {new_coins}")
            df = self.backfill_dune(
                chain, 
                dune_chain_id, 
                db_progress_chain[db_progress_chain['date'].isna()]['address'].tolist(),
                new_coins,
                db_progress_chain[db_progress_chain['date'].isna()]['decimals'].tolist()
            )
            if df is not None:
                df_all = pd.concat([df_all, df], ignore_index=True) if df_all is not None else df
        
        # pull in existing coins only for the last few missing recent days
        if len(db_progress_chain[db_progress_chain['date'].notna()]) > 0:
            df = self.read_total_supplies_dune(
                chain, 
                dune_chain_id,
                db_progress_chain[db_progress_chain['date'].notna()]['address'].tolist(),
                db_progress_chain[db_progress_chain['date'].notna()]['token_id'].tolist(),
                db_progress_chain[db_progress_chain['date'].notna()]['decimals'].tolist(),
                db_progress_chain[db_progress_chain['date'].notna()]['value'].tolist(),
                db_progress_chain[db_progress_chain['date'].notna()]['date'].tolist()
            )
            if df is not None:
                df_all = pd.concat([df_all, df], ignore_index=True) if df_all is not None else df
        
        # return and print report
        if df_all is not None:
            print(f"Successfully pulled stablecoin supply data for chain '{chain}' from Dune for {len(df_all)} records.")
        else:
            print(f"No new stablecoin supply data for chain '{chain}' found on Dune.")
        return df_all

    def pull_from_rpc(self, chain, token_ids, db_progress):
        # create new w3 instance for the chain
        w3 = self.get_new_w3(chain)

        # check if SupplyReader is deployed on the chain, if yes create adapter instance
        is_supplyreader_deployed_date = pd.to_datetime(config[config['origin_key'] == chain]['deployed_supplyreader'].iloc[0]).date()
        is_supplyreader_deployed = is_supplyreader_deployed_date != None
        if is_supplyreader_deployed:
            SupplyReader = SupplyReaderAdapter(w3)

        # create specific chain db_progress df (includes new coins and removes already upto-date coins)
        db_progress_chain = self.get_db_progress_chain(chain, token_ids, db_progress)

        # keep track of everything
        df_all = pd.DataFrame()

        ## pull in complete history for new coins
        if len(db_progress_chain[db_progress_chain['date'].isna()]) > 0:
            print(f"Found {len(db_progress_chain[db_progress_chain['date'].isna()])} new coins for chain {chain}. Pulling in complete history for these coins.")
            block_date_mapping = self.get_last_block_of_day_from_db(chain, '2000-01-01')
            db_progress_chain_new_coins = db_progress_chain[db_progress_chain['date'].isna()].reset_index(drop=True)
            for index, row in block_date_mapping.iterrows():
                block_number = row['value']
                date = row['date']

                # use SupplyReader
                if is_supplyreader_deployed and is_supplyreader_deployed_date <= date:
                    supplies = SupplyReader.get_total_supplies(
                        db_progress_chain_new_coins['address'].tolist(),
                        block_number
                    )
                    print(f"Used SupplyReader on chain {chain} on block {block_number} and date {date} for {len(supplies)} coins. {supplies}") ###
                    df_supplies = db_progress_chain_new_coins[['origin_key', 'token_id', 'address', 'decimals']].copy()
                    df_supplies['value'] = pd.Series(supplies) / (10 ** df_supplies['decimals'])
                    df_supplies['date'] = date
                    df_all = pd.concat([df_all, df_supplies], ignore_index=True)
                    # remove coins from db_progress_chain_new_coins if supply is 0 (TODO: remove row also when call fails due to contract not yet being deployed?)
                    if 0 in supplies:
                        zero_supply_coins = db_progress_chain_new_coins[db_progress_chain_new_coins['token_id'].isin([db_progress_chain_new_coins['token_id'][i] for i, s in enumerate(supplies) if s == 0])]
                        db_progress_chain_new_coins = db_progress_chain_new_coins[~db_progress_chain_new_coins['token_id'].isin(zero_supply_coins['token_id'])]
                        print(f"Removed {len(zero_supply_coins)} coins with 0 supply from db_progress_chain_new_coins for chain {chain} on date {date}.")

                # single RPC calls
                else:
                    for index, row in db_progress_chain_new_coins.iterrows():
                        origin_key = row['origin_key']
                        token_id = row['token_id']
                        address = row['address']
                        decimals = row['decimals']
                        try:
                            supply = self.get_total_supply_at_block(w3, address, block_number, decimals)
                            df_all = pd.concat([df_all, pd.DataFrame({
                                'origin_key': [origin_key],
                                'token_id': [token_id],
                                'address': [address.lower()],
                                'decimals': [decimals],
                                'value': [supply],
                                'date': [date]
                            })], ignore_index=True)
                            print(f"Used single RPC call to get total supply for {token_id} on chain {origin_key} for date {date} at block {block_number}: {supply}")
                            if supply == 0: # remove row from db_progress_chain_new_coins if supply is 0
                                db_progress_chain_new_coins = db_progress_chain_new_coins[db_progress_chain_new_coins['token_id'] != token_id]
                        except Exception as e:
                            if "Could not decode contract function call" in str(e): # remove row from db_progress_chain_new_coins if contract not yet deployed
                                print(f"Contract for token_id {token_id} not yet deployed {date}.")
                                db_progress_chain_new_coins = db_progress_chain_new_coins[db_progress_chain_new_coins['token_id'] != token_id]
                            else:
                                print(f"Error pulling total supply for {token_id} on chain {origin_key} for date {date}: {e}")
        
        ## pull in existing coins only for the last few missing recent days
        if len(db_progress_chain[db_progress_chain['date'].notna()]) > 0:
            block_date_mapping = self.get_last_block_of_day_from_db(chain, (db_progress_chain['date'].min() + pd.Timedelta(days=1)).isoformat())
            
            for index, row in block_date_mapping.iterrows():
                block_number = row['value']
                date = row['date']

                # use SupplyReader
                if is_supplyreader_deployed and is_supplyreader_deployed_date <= date:
                    db_progress_chain_date = db_progress_chain[db_progress_chain['date'] < date]
                    supplies = SupplyReader.get_total_supplies(
                        db_progress_chain_date['address'].tolist(),
                        block_number
                    )
                    print(f"Used SupplyReader on chain {chain} on block {block_number} and date {date} for {len(supplies)} coins.")
                    df_supplies = db_progress_chain_date[['origin_key', 'token_id', 'address', 'decimals']].copy()
                    df_supplies['value'] = pd.Series(supplies) / (10 ** df_supplies['decimals'])
                    df_supplies['date'] = date
                    df_all = pd.concat([df_all, df_supplies], ignore_index=True)

                # single RPC calls
                else:
                    db_progress_chain_date = db_progress_chain[db_progress_chain['date'] < date]
                    for index, row in db_progress_chain_date.iterrows():
                        origin_key = row['origin_key']
                        token_id = row['token_id']
                        address = row['address']
                        decimals = row['decimals']
                        try:
                            supply = self.get_total_supply_at_block(w3, address, block_number, decimals)
                            df_all = pd.concat([df_all, pd.DataFrame({
                                'origin_key': [origin_key],
                                'token_id': [token_id],
                                'address': [address.lower()],
                                'decimals': [decimals],
                                'value': [supply],
                                'date': [date]
                            })], ignore_index=True)
                            print(f"Used single RPC call to get total supply for {token_id} on chain {origin_key} for date {date} at block {block_number}: {supply}")
                        except Exception as e:
                            print(f"Error pulling total supply for {token_id} on chain {origin_key} for date {date} using single RPC call: {e}")

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

    def read_total_supplies_SupplyReader(self, token_addresses: list, token_decimals: list, block_number: int = None):
        """
        Using SupplyReader to read total supplies for 
        """
        try:
            supplies = self.SupplyReader.get_total_supplies(token_addresses, block_number)
            supplies = [supply / (10 ** decimals) for supply, decimals in zip(supplies, token_decimals)]
            return supplies
        except Exception as e:
            print(f"Error reading total supplies with SupplyReader on chain {self.current_chain} with RPC {self.current_rpc} for contracts {token_addresses}: {e}")
            self.SupplyReader = SupplyReaderAdapter(self.get_new_w3(self.current_chain))
            return self.read_total_supplies_SupplyReader(token_addresses, token_decimals, block_number)
        
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

    def get_last_block_of_day_from_db(self, chain: str, start_date: str):
        query = f"""
            SELECT
                DATE("date" - INTERVAL '1 day') as "date",
                CAST(value - 1 AS INTEGER) as value
            FROM public.fact_kpis
            WHERE
                metric_key = 'first_block_of_day'
                AND origin_key = '{chain}'
                AND "date" > '{start_date}'
            ORDER BY "date" DESC
        """
        result = self.db_connector.execute_query(query, load_df=True)
        return result
    
    def get_db_progress(self):
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

    def get_db_progress_chain(self, chain: str, token_ids: list, db_progress: pd.DataFrame):
        # filter token_ids down to what is actully deployed on this chain
        chain_token_ids = [token for token in self.address_mapping[chain] if token in token_ids]
        # see if we have new coins
        new_coins = [token for token in chain_token_ids if token not in db_progress[db_progress['origin_key'] == chain]['token_id'].unique()]
        # add the new coins to db_progress
        db_progress_chain = pd.concat([db_progress[db_progress['origin_key'] == chain], pd.DataFrame({
            'origin_key': [chain] * len(new_coins),
            'token_id': new_coins,
            'address': [self.address_mapping[chain][token]['address'] for token in new_coins],
            'decimals': [self.address_mapping[chain][token]['decimals'] for token in new_coins],
            'value': [None] * len(new_coins),
            'date': [None] * len(new_coins)
        })], ignore_index=True)
        # filter out yesterdays date
        date_yesterday = (datetime.now() - timedelta(days=1)).date()
        db_progress_chain = db_progress_chain[db_progress_chain['date'] != date_yesterday]
        return db_progress_chain
    
    def get_total_supply_at_block(self, w3: Web3, address: str, block_number: int, decimals: int = 18) -> float:
        """Get ERC20 total supply at a specific block height."""
        contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=self.erc20_abi)
        total_supply = contract.functions.totalSupply().call(block_identifier=block_number)
        return total_supply / (10 ** decimals)