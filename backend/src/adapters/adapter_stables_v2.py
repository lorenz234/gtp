import os
import pandas as pd
from datetime import datetime, timedelta

from web3 import Web3
from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.adapter_SupplyReader import SupplyReaderAdapter

#!# 'dune' query for totalSupply logic only works on 95% of the stablecoins! It tracks transfers events to 0x0 and from 0x0. Not all stables follow that logic.
#!# 'rpc' requires 'first_block_of_day' data to be available in the database.

class AdapterStablecoinSupply(AbstractAdapter):
    """
    Adapter for scraping stablecoin supply across different chains.

    Args:
        adapter_params (dict): Dictionary containing parameters for the adapter. Expected keys are:
        - nothing
        db_connector: Database connector instance for interacting with the database.
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("Stablecoin Adapter v2", adapter_params, db_connector)
        
        # Store stablecoin metadata and mapping
        from src.stables_config_v2 import address_mapping, coin_mapping
        self.address_mapping = address_mapping
        self.coin_mapping = coin_mapping

        # try to update sys_stables_v2
        self.update_sys_stables_v2(self.coin_mapping)

        # keep track of which chain is currently being used
        self.current_chain = None
        self.current_rpc = None
        self.list_of_rpcs = None
        self.current_rpc_index = 0

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
            - method (str): Method to use for extraction. Options are 'dune', 'rpc', or 'auto'. Default is 'auto', which will try 'dune' and fall back to 'rpc'. 'rpc' requires 'first_block_of_day' data to be available in the database for the chain!
        """
        # all data combined
        df_all = None

        chains = extract_params.get('origin_keys', ['*'])
        token_ids = extract_params.get('token_ids', ['*'])
        method = extract_params.get('method', 'auto')

        print("Extracting latest stablecoin supply data for chains:", chains, "and token_ids:", token_ids, "using method:", method)

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
                print(f"No stablecoins in address mapping for chain '{chain}', skipping. Please check the file: src/stables_config_v2.py.")
                continue

            # get df with totalSupplies
            df = self.get_stablecoin_data_for_chain(chain, token_ids, db_progress, method=method)

            # merge df into df_all
            if df_all is None:
                df_all = df
            elif df is not None:
                df_all = pd.concat([df_all, df], ignore_index=True)

        # return the combined df with all data
        return df_all
    

    # load data into db
    def load(self, df, table_name: str="fact_stables_v2"):
        """
        Load the extracted data into the database.

        Args:
            df (DataFrame): DataFrame from extract as is.
            table_name (str): Name of the table to load the data into. Default is "fact_stables_v2".
        """
        df = df.set_index(['origin_key', 'token_id', 'address', 'date'])
        self.db_connector.upsert_table(table_name, df)
        print(f"Loaded {len(df)} records into {table_name}.")

    #-#-#-# Helper Functions #-#-#-#

    def get_stablecoin_data_for_chain(self, chain, token_ids, db_progress, method='auto'):
        """
        Function to get stablecoin supply data for a specific chain and (multiple) token_ids using the specified method.
        
        Args:
            chain (str): The origin_key of the chain to get data for.
            token_ids (list): List of token_ids to get data for.
            db_progress (DataFrame): DataFrame containing the current progress of data collection for all chains and token_ids.
            method (str): Method to use for getting data. Options are 'dune', 'rpc', or 'auto'. Default is 'auto', which will try 'dune' and fall back to 'rpc'. 'rpc' requires 'first_block_of_day' data to be available in the database for the chain!
        """
        # if chain is listed on dune, use that to get the data
        if self.config[self.config['origin_key'] == chain]['aliases_dune'].iloc[0] is not None and method in ['auto', 'dune']:
            print(f"Using Dune adapter to get stablecoin data for {chain}.")
            df = self.pull_from_dune(chain, token_ids, db_progress)
            if df is not None:
                return df

        # else use RPC calls
        if method in ['auto', 'rpc']:
            print(f"Using RPC calls to get stablecoin data for {chain}.")
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
            df_all['method'] = 'dune'
            print(f"Successfully pulled stablecoin supply data for chain '{chain}' from Dune for {len(df_all)} records.")
        else:
            print(f"No new stablecoin supply data for chain '{chain}' found on Dune.")
        return df_all

    def pull_from_rpc(self, chain, token_ids, db_progress):
        # initialize chain rpc state
        self.get_new_w3(chain)

        # check if SupplyReader is deployed on the chain
        chain_config = self.config[self.config['origin_key'] == chain]
        supplyreader_deployed_raw = chain_config['deployed_supplyreader'].iloc[0]
        is_supplyreader_deployed = pd.notna(supplyreader_deployed_raw)
        is_supplyreader_deployed_date = (
            pd.to_datetime(supplyreader_deployed_raw).date()
            if is_supplyreader_deployed
            else None
        )

        # create specific chain db_progress df (includes new coins and removes already upto-date coins)
        db_progress_chain = self.get_db_progress_chain(chain, token_ids, db_progress)

        # keep track of everything
        df_all = pd.DataFrame()

        ## pull in complete history for new coins
        if len(db_progress_chain[db_progress_chain['date'].isna()]) > 0:
            
            print(f"Found {len(db_progress_chain[db_progress_chain['date'].isna()])} new coins for chain {chain}. {db_progress_chain[db_progress_chain['date'].isna()]['token_id'].tolist()}")
            
            block_date_mapping = self.get_last_block_of_day_from_db(chain, '2000-01-01')
            if len(block_date_mapping) == 0: # raise error if we have no 'first_block_of_day' data for the chain
                print(f"ERROR: Missing dates in block date mapping for chain {chain}.")
                raise ValueError(f"No block date mapping found for chain {chain}. Cannot pull complete history for new coins. Please check if 'first_block_of_day' data is available in db.")
            
            db_progress_chain_new_coins = db_progress_chain[db_progress_chain['date'].isna()].reset_index(drop=True)
            
            for index, row in block_date_mapping.iterrows():
                block_number = row['value']
                date = row['date']
                use_supply_reader = (
                    is_supplyreader_deployed
                    and is_supplyreader_deployed_date is not None
                    and is_supplyreader_deployed_date <= pd.to_datetime(date).date()
                )

                # use SupplyReader
                if use_supply_reader:
                    try:
                        supplies = self.read_total_supplies_SupplyReader(
                            chain=chain,
                            token_addresses=db_progress_chain_new_coins['address'].tolist(),
                            block_number=block_number,
                        )
                        print(f"- Used SupplyReader for {len(db_progress_chain_new_coins)} token_ids, chain {chain}, {date} = block {block_number}: {supplies}")
                        df_supplies = db_progress_chain_new_coins[['origin_key', 'token_id', 'address', 'decimals']].copy()
                        df_supplies['value'] = pd.Series(supplies) / (10 ** df_supplies['decimals'])
                        df_supplies['date'] = date
                        df_all = pd.concat([df_all, df_supplies], ignore_index=True)

                        # remove coins from db_progress_chain_new_coins if supply is 0
                        if 0 in supplies:
                            zero_supply_coins = db_progress_chain_new_coins[db_progress_chain_new_coins['token_id'].isin([db_progress_chain_new_coins['token_id'][i] for i, s in enumerate(supplies) if s == 0])]
                            db_progress_chain_new_coins = db_progress_chain_new_coins[~db_progress_chain_new_coins['token_id'].isin(zero_supply_coins['token_id'])]
                            print(f"Removed {len(zero_supply_coins)} coin(s) with 0 supply from db_progress_chain_new_coins for chain {chain} on date {date}.")
                            if len(db_progress_chain_new_coins) == 0:
                                break
                        continue
                    except Exception as e:
                        print(f"SupplyReader failed for chain {chain} on block {block_number} and date {date}. Falling back to single RPC calls: {e}")

                # single RPC calls
                for index, row in db_progress_chain_new_coins.iterrows():
                    origin_key = row['origin_key']
                    token_id = row['token_id']
                    address = row['address']
                    decimals = row['decimals']
                    try:
                        supply = self.read_total_supply_rpc(
                            chain=chain,
                            address=address,
                            block_number=block_number,
                            decimals=decimals,
                        )
                        df_all = pd.concat([df_all, pd.DataFrame({
                            'origin_key': [origin_key],
                            'token_id': [token_id],
                            'address': [address.lower()],
                            'decimals': [decimals],
                            'value': [supply],
                            'date': [date]
                        })], ignore_index=True)
                        print(f"- Used single RPC call for {token_id}, chain {origin_key}, {date} = block {block_number}: {supply}")
                        if supply == 0: # remove row from db_progress_chain_new_coins if supply is 0
                            db_progress_chain_new_coins = db_progress_chain_new_coins[db_progress_chain_new_coins['token_id'] != token_id]
                            print(f"Removed {token_id} from db_progress_chain_new_coins for chain {chain} on date {date} due to 0 supply.")
                            if len(db_progress_chain_new_coins) == 0:
                                break
                    except Exception as e:
                        if "Could not decode contract function call" in str(e): # remove row from db_progress_chain_new_coins if contract not yet deployed
                            print(f"Contract for token_id {token_id} not yet deployed {date}.")
                            db_progress_chain_new_coins = db_progress_chain_new_coins[db_progress_chain_new_coins['token_id'] != token_id]
                            if len(db_progress_chain_new_coins) == 0:
                                break
                        else:
                            print(f"Error pulling total supply for {token_id} on chain {origin_key} for date {date}: {e}")
        
        ## pull in existing coins only for the last few missing recent days
        db_progress_existing = db_progress_chain[db_progress_chain['date'].notna()]
        if len(db_progress_existing) > 0:

            block_date_mapping = self.get_last_block_of_day_from_db(chain, (db_progress_existing['date'].min() + pd.Timedelta(days=1)).isoformat())
            if len(block_date_mapping) == 0: # raise error if we have no 'first_block_of_day' data for the chain
                print(f"ERROR: No block date mapping found for chain {chain}.")
                raise ValueError(f"No block date mapping found for chain {chain}. Cannot pull recent history for existing coins. Please check if 'first_block_of_day' data is available in db.")

            for index, row in block_date_mapping.iterrows():
                block_number = row['value']
                date = row['date']
                use_supply_reader = (
                    is_supplyreader_deployed
                    and is_supplyreader_deployed_date is not None
                    and is_supplyreader_deployed_date <= pd.to_datetime(date).date()
                )

                # use SupplyReader
                if use_supply_reader:
                    db_progress_chain_date = db_progress_chain[db_progress_chain['date'] < date]
                    try:
                        supplies = self.read_total_supplies_SupplyReader(
                            chain=chain,
                            token_addresses=db_progress_chain_date['address'].tolist(),
                            block_number=block_number,
                        )
                        print(f"- Used SupplyReader for {len(db_progress_chain_date)} token_ids, chain {chain}, {date} = block {block_number}: {supplies}")
                        df_supplies = db_progress_chain_date[['origin_key', 'token_id', 'address', 'decimals']].copy()
                        df_supplies['value'] = pd.Series(supplies) / (10 ** df_supplies['decimals'])
                        df_supplies['date'] = date
                        df_all = pd.concat([df_all, df_supplies], ignore_index=True)
                        continue
                    except Exception as e:
                        print(f"SupplyReader failed for chain {chain} on block {block_number} and date {date}. Falling back to single RPC calls: {e}")

                # single RPC calls
                db_progress_chain_date = db_progress_chain[db_progress_chain['date'] < date]
                for index, row in db_progress_chain_date.iterrows():
                    origin_key = row['origin_key']
                    token_id = row['token_id']
                    address = row['address']
                    decimals = row['decimals']
                    try:
                        supply = self.read_total_supply_rpc(
                            chain=chain,
                            address=address,
                            block_number=block_number,
                            decimals=decimals,
                        )
                        df_all = pd.concat([df_all, pd.DataFrame({
                            'origin_key': [origin_key],
                            'token_id': [token_id],
                            'address': [address.lower()],
                            'decimals': [decimals],
                            'value': [supply],
                            'date': [date]
                        })], ignore_index=True)
                        print(f"- Used single RPC call for {token_id}, chain {origin_key}, {date} = block {block_number}: {supply}")
                    except Exception as e:
                        print(f"Error pulling total supply for {token_id} on chain {origin_key} for date {date} using single RPC call: {e}")

        # return and print report
        if df_all is not None:
            df_all['method'] = 'rpc'
            print(f"Successfully pulled stablecoin supply data for chain '{chain}' using RPC calls for {len(df_all)} records.")
        else:
            print(f"No new stablecoin supply data for chain '{chain}' found using RPC calls.")
        return df_all

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

    def _is_non_retryable_rpc_error(self, error: Exception) -> bool:
        error_str = str(error).lower()
        return (
            "could not decode contract function call" in error_str
            or "execution reverted" in error_str
            or "invalid opcode" in error_str
        )

    def _call_with_rpc_failover(self, chain: str, call_fn):
        """
        Execute a read-only chain call and rotate through chain RPCs on retryable failures.
        """
        self.get_new_w3(chain)
        max_attempts = len(self.list_of_rpcs)
        last_error = None

        for attempt in range(max_attempts):
            rotate = attempt > 0
            w3 = self.get_new_w3(chain, rotate=rotate)
            try:
                return call_fn(w3)
            except Exception as e:
                if self._is_non_retryable_rpc_error(e):
                    raise e
                last_error = e
                print(f"RPC call failed on chain {chain} with RPC {self.current_rpc} (rpcs {attempt + 1}/{max_attempts}): {e}")

        raise RuntimeError(
            f"RPC call failed on chain {chain} after trying {max_attempts} RPC endpoints."
        ) from last_error

    def read_total_supplies_SupplyReader(self, chain: str, token_addresses: list, block_number: int = None):
        """
        Read raw total supplies using the SupplyReader contract with RPC failover.
        """
        def _call_fn(w3):
            self.SupplyReader = SupplyReaderAdapter(w3)
            return self.SupplyReader.get_total_supplies(token_addresses, block_number)

        return self._call_with_rpc_failover(chain, _call_fn)

    def read_total_supply_rpc(self, chain: str, address: str, block_number: int, decimals: int = 18) -> float:
        """
        Read token totalSupply via direct ERC20 contract call with RPC failover.
        """
        def _call_fn(w3):
            return self.get_total_supply_at_block(w3, address, block_number, decimals)

        return self._call_with_rpc_failover(chain, _call_fn)
        
    def get_new_w3(self, chain: str, rotate: bool = False):
        """
        Create a new Web3 instance for a chain and optionally rotate to the next RPC.
        """
        if self.current_chain != chain or self.list_of_rpcs is None:
            primary_rpc = self.db_connector.get_special_use_rpc(chain)
            fallback_rpcs = self.db_connector.get_all_rpcs_for_chain(chain)
            all_rpcs = [primary_rpc] + fallback_rpcs

            # remove nulls and duplicates while preserving order
            self.list_of_rpcs = [rpc for rpc in dict.fromkeys(all_rpcs) if rpc]
            if len(self.list_of_rpcs) == 0:
                raise ValueError(f"No RPCs found for chain {chain}. Please add RPCs to your database for this chain.")

            self.current_chain = chain
            self.current_rpc_index = 0
            self.current_rpc = self.list_of_rpcs[self.current_rpc_index]
            print(f"Switched w3 instance to new chain: {chain} with RPC: {self.current_rpc}")
        elif rotate:
            self.current_rpc_index = (self.current_rpc_index + 1) % len(self.list_of_rpcs)
            self.current_rpc = self.list_of_rpcs[self.current_rpc_index]
            print(f"Rotating w3 instance to new RPC for chain: {chain} to RPC: {self.current_rpc}")

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
