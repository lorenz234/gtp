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
            - metric_keys (list): List of metric_keys to extract data for. Use ['*'] for all. Options are ...
        """
        # all data combined
        df_all = None

        chains = extract_params.get('origin_keys', ['*'])
        token_ids = extract_params.get('token_ids', ['*'])
        metric_keys = extract_params.get('metric_keys', ['*'])

        print("Extracting latest stablecoin supply data for chains:", chains, "and token_ids:", token_ids, "using metric_keys:", metric_keys)

        # exchange '*' for all chains in mapping
        if chains == ['*']:
            chains = list(self.address_mapping.keys())
        # exchange '*' for all token_ids in mapping
        if token_ids == ['*']:
            token_ids = [coin['token_id'] for coin in self.coin_mapping]
        # exchange '*' for all metric_keys
        if metric_keys == ['*']:
            metric_keys = ['total_supply'] # 'volume' and 'transactions' ... not implemented yet

        # get db_progress, DataFrame that keeps track of which coins, chains and metric_keys are up to date. Used to determine from which day onwards to pull data.
        db_progress = self.get_db_progress()

        # iterate through each chain and get data
        for chain in chains:

            # check if chain is in mapping, if not skip and log warning
            if chain not in self.address_mapping:
                print(f"No stablecoins in address mapping for chain '{chain}', skipping. Please check the file: src/stables_config_v2.py.")
                continue

            # get df with all data for the chain and merge into df_all
            df = self.extract_data_from_chain(chain, token_ids, metric_keys, db_progress)

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


    def extract_data_from_chain(self, chain, token_ids, metric_keys, db_progress, pretend_today_is=None):
        """
        Function to get stablecoin data for a specific chain and (multiple) token_ids and (multiple) metric_keys.
        
        Args:
            chain (str): The origin_key of the chain to get data for.
            token_ids (list): List of token_ids to get data for.
            metric_keys (list): List of metric_keys to extract data for.
            db_progress (DataFrame): DataFrame that keeps track of which coins, chains and metric_keys are up to date. Used to determine from which day onwards to pull data.
            pretend_today_is (pd.Timestamp, optional): A date string to pretend as today's date for testing purposes. Defaults to today. e.g. pd.Timestamp('2024-01-01').date()
        """
        # combine all data into one df
        df_all = pd.DataFrame()

        for metric_key in metric_keys:

            ## extract data for total_supply
            if metric_key == 'total_supply':

                # total_supply is pulled in only using RPC, because calculating total_supply based on transfer events in dune is not reliable!
                df = self.total_supply_from_rpc(chain, token_ids, db_progress, pretend_today_is=pretend_today_is)
                df['metric_key'] = 'total_supply'
                if df.empty:
                    print(f"ERROR got no new total_supply data using RPC.")
                    continue
                df_all = pd.concat([df_all, df], ignore_index=True)

            ## extract data for volume ...
            # if metric_key == 'volume': ...
                
    def total_supply_from_rpc(self, chain, token_ids, db_progress, pretend_today_is=None):
        # initialize chain rpc state
        self.get_new_w3(chain)

        # check if SupplyReader is deployed on the chain
        chain_config = self.config[self.config['origin_key'] == chain]
        supplyreader_deployed_raw = chain_config['deployed_supplyreader'].iloc[0]
        is_supplyreader_deployed = pd.notna(supplyreader_deployed_raw)
        is_supplyreader_deployed_date = (pd.to_datetime(supplyreader_deployed_raw).date() if is_supplyreader_deployed else None)

        # create specific chain db_progress df (includes new coins and removes already upto-date coins)
        db_progress_filtered = self.get_db_progress_filtered(chain, 'total_supply', token_ids, db_progress)

        # keep track of everything
        df_supplies_all = pd.DataFrame()

        # replace NaN with a dummy old date
        db_progress_filtered = db_progress_filtered.fillna({'date': pd.Timestamp('2000-01-01').date()})
        block_date_mapping = self.get_last_block_of_day_from_db(chain, db_progress_filtered['date'].min().isoformat())
        if pretend_today_is is not None:
            block_date_mapping = block_date_mapping[block_date_mapping['date'] <= pretend_today_is]
        # send warning & return if no block_date_mapping is found
        if len(block_date_mapping) == 0: # raise error if we have no 'first_block_of_day' data for the chain
            print(f"ERROR: Missing dates in block date mapping for chain {chain}. Can't pull in stablecoin supply via RPC. Please backfill 'first_block_of_day' first.")
            return None
        
        # going from newest to oldest date
        for index, row in block_date_mapping.iterrows():
            block_number = row['value']
            date = row['date']

            # can we use SupplyReader for this date to pull data
            use_supply_reader = (is_supplyreader_deployed and is_supplyreader_deployed_date <= pd.to_datetime(date).date())

            # Yes, use SupplyReader :)
            if use_supply_reader:
                try:
                    supplies = self.read_total_supplies_SupplyReader(
                        chain=chain,
                        token_addresses=db_progress_filtered['address'].tolist(),
                        block_number=block_number,
                    )
                    print(f"- Used SupplyReader for {len(db_progress_filtered)} token_ids, chain {chain}, {date} = block {block_number}: {supplies}")
                    df_supplies = db_progress_filtered[['origin_key', 'token_id', 'address', 'decimals']].copy()
                    df_supplies['value'] = pd.Series(supplies) / (10 ** df_supplies['decimals'])
                    df_supplies['date'] = date
                    df_supplies_all = pd.concat([df_supplies_all, df_supplies], ignore_index=True)

                    # remove coins from db_progress_filtered if supply is 0
                    if 0 in supplies:
                        to_be_removed_coins = db_progress_filtered[db_progress_filtered['token_id'].isin([db_progress_filtered['token_id'][i] for i, s in enumerate(supplies) if s == 0])]
                        db_progress_filtered = db_progress_filtered[~db_progress_filtered['token_id'].isin(to_be_removed_coins['token_id'])]
                        print(f"- Removed {len(to_be_removed_coins)} coin(s) with 0 supply from db_progress_filtered: {to_be_removed_coins['token_id'].tolist()}")
                    # remove coins from db_progress_filtered if backfill date is reached
                    if db_progress_filtered['date'].max() >= date:
                        to_be_removed_coins = db_progress_filtered[db_progress_filtered['date'] >= date]
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['date'] < date]
                        print(f"- Removed {len(to_be_removed_coins)} coin(s) from db_progress_filtered that have date >= {date}: {to_be_removed_coins['token_id'].tolist()}")
                    # break if db_progress_filtered is empty after removals
                    if len(db_progress_filtered) == 0:
                        print("- All coins fully processed, exiting loop.")
                        break
                    continue
                except Exception as e:
                    print(f"SupplyReader failed for chain {chain} on block {block_number} and date {date}. Falling back to single RPC calls: {e}")

            # No, single RPC calls :(
            for index, row in db_progress_filtered.iterrows():
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
                    df_supplies_all = pd.concat([df_supplies_all, pd.DataFrame({
                        'origin_key': [origin_key],
                        'token_id': [token_id],
                        'address': [address.lower()],
                        'decimals': [decimals],
                        'value': [supply],
                        'date': [date]
                    })], ignore_index=True)
                    print(f"- Used single RPC call for {token_id}, chain {origin_key}, {date} = block {block_number}: {supply}")

                    # remove coin from db_progress_filtered if supply is 0
                    if supply == 0:
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['token_id'] != token_id]
                        print(f"- Removed {token_id} from db_progress_filtered for chain {chain} on date {date} due to 0 supply.")
                    # remove coin from db_progress_filtered if backfill date is reached
                    elif db_progress_filtered[db_progress_filtered['token_id'] == token_id]['date'].max() >= date:
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['token_id'] != token_id]
                        print(f"- Removed {token_id} from db_progress_filtered for chain {chain} on date {date} due to backfill date reached.")
                    # break if db_progress_filtered is empty after removals
                    if len(db_progress_filtered) == 0:
                        print("- All coins fully processed, exiting loop.")
                        break
                except Exception as e:
                    # remove coins from db_progress_filtered if contract not yet deployed
                    if "Could not decode contract function call" in str(e):
                        print(f"- Removed {token_id} from db_progress_filtered for chain {chain} on date {date} due to contract not deployed yet.")
                        db_progress_filtered = db_progress_filtered[db_progress_filtered['token_id'] != token_id]
                        if len(db_progress_filtered) == 0:
                            break
                    else:
                        print(f"RPC call failed for chain {chain}, token {token_id} on block {block_number} and date {date}: {e}")

        # return and print report
        if df_supplies_all.empty:
            df_supplies_all['method'] = 'rpc'
            print(f"Successfully pulled stablecoin supply data for chain '{chain}' using RPC calls for {len(df_supplies_all)} records.")
        else:
            print(f"No new stablecoin supply data for chain '{chain}' found using RPC calls.")
        return df_supplies_all
    

    #-#-#-# Raw Helper Functions #-#-#-#


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
            contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=self.erc20_abi)
            total_supply = contract.functions.totalSupply().call(block_identifier=block_number)
            return total_supply / (10 ** decimals)

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
            SELECT DISTINCT ON (f.origin_key, f.metric_key, f.token_id, f.address, f.decimals)
                f.date,
                f.origin_key,
                f.metric_key,
                f.token_id,
                f.address,
                f.decimals,
                f.method,
                f.value,
                s.owner_project,
                s.symbol,
                s.bridged_origin_chain,
                s.bridged_origin_token_id,
                s.coingecko_id,
                s.fiat,
                s.logo
            FROM public.fact_stables_v2 f
            LEFT JOIN public.sys_stables_v2 s ON f.token_id = s.token_id
            ORDER BY f.origin_key, f.metric_key, f.token_id, f.address, f.decimals, f.date DESC;
        """
        result = self.db_connector.execute_query(query, load_df=True)
        return result

    def get_db_progress_filtered(self, chain: str, metric_key: str, token_ids: list, db_progress: pd.DataFrame):
        # filter db_progress for specific chain and metric_key
        db_progress_filtered = db_progress[(db_progress['origin_key'] == chain) & (db_progress['metric_key'] == metric_key)]
        # filter token_ids down to what is actully deployed on this chain
        chain_token_ids = [token for token in self.address_mapping[chain] if token in token_ids]
        # see if we have new coins
        new_coins = [token for token in chain_token_ids if token not in db_progress_filtered['token_id'].unique()]
        # add the new coins to db_progress_filtered
        db_progress_filtered = pd.concat([db_progress_filtered, pd.DataFrame({
            'date': [None] * len(new_coins),
            'origin_key': [chain] * len(new_coins),
            'metric_key': [metric_key] * len(new_coins),
            'token_id': new_coins,
            'address': [self.address_mapping[chain][token]['address'] for token in new_coins],
            'decimals': [self.address_mapping[chain][token]['decimals'] for token in new_coins],
            'value': [None] * len(new_coins)
        })], ignore_index=True)
        # filter out coins which are already at yesterdays date (= up to date)
        date_yesterday = (datetime.now() - timedelta(days=1)).date()
        db_progress_filtered = db_progress_filtered[db_progress_filtered['date'] != date_yesterday]
        return db_progress_filtered
