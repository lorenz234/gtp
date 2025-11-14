from src.adapters.adapter_logs import AdapterLogs
from eth_abi.abi import decode
from datetime import timezone
from web3 import Web3
import pandas as pd
import platform
import json

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract

class AdapterOLIOffchain(AbstractAdapter):
    """
    adapter_params require the following fields
        rpc_url:str - the RPC URL to connect to the blockchain
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("OLI_offchain", adapter_params, db_connector)

        # setup web3
        self.w3 = Web3(Web3.HTTPProvider(self.adapter_params['rpc_url']))
        self.schema_chain = self.w3.eth.chain_id # which chain we are extracting from

        # setup logs adapter
        self.adapter_logs = AdapterLogs(self.w3)
        # if this script is running on Linux, we are on the backend, then use 'backend/', else ''
        self.additional_folder_structure = 'backend/' if platform.system() == 'Linux' else ''

        print_init(self.name, self.adapter_params)

    """
    extract_params require the following fields:
        contract_address: str - (optional) the contract address to extract logs from
        from_block: int - the starting block number (negative for latest block - x or 'last_run_block')
        to_block: int - the ending block number (or 'latest')
        topics: list - list of log topics to filter by
        chunk_size: int - number of blocks to process in each chunk (default on most free rpcs: 1000)
    """
    def extract(self, extract_params:dict = None) -> pd.DataFrame:

        # get block range if 'latest' or negative number
        if extract_params.get('to_block', None) == 'latest':
            extract_params['to_block'] = self.w3.eth.block_number
        if extract_params.get('from_block', 0) == 'last_run_block':
            extract_params['from_block'] = self.get_last_run_block(self.schema_chain) - 1 # to avoid error messages in case RPC syncs a bit slowly
        elif extract_params.get('from_block', 0) < 0:
            extract_params['from_block'] = extract_params.get('to_block', 0) + extract_params['from_block']
        if extract_params.get('from_block', 0) > extract_params.get('to_block', 0):
            raise ValueError("'from_block' must be less than 'to_block' in extract_params")

        # extract logs
        logs = self.adapter_logs.extract(extract_params)

        # store extracted logs with input information in d
        d = []
        for log in logs:
            uid = '0x' + log['topics'][2].hex()
            d.append({
                'tx_hash': '0x' + log['transactionHash'].hex(),
                'block_number': log['blockNumber'],
                'uid': uid,
                'revocation_time': int(log['topics'][3].hex(),16),
                'revoker': '0x' + log['topics'][1].hex()[24:]
            })
        df = pd.DataFrame(d)

        # print extract info only if there was data extracted
        if not df.empty:
            print_extract(self.name, extract_params, df.shape)

        self.extract_params = extract_params  # store for later use in saving last run block

        return df

    """
    table_names: list - the names of the tables to load data into
    """
    def load(self, df: pd.DataFrame, table_names: list = ['attestations', 'trust_lists']):
        
        if df.empty:
            #print(f"No data to load.")
            pass

        else:
            # turn revocation_time into ISO string
            if 'revocation_time' in df.columns:
                df['revocation_time'] = df['revocation_time'].apply(lambda x: pd.to_datetime(x, unit='s').strftime('%Y-%m-%d %H:%M:%S'))
            
            # Process in batches of 100
            batch_size = 100
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                
                # Build VALUES list for this batch
                values_list = []
                for _, row in batch_df.iterrows():
                    uid_hex = row['uid'][2:] if row['uid'].startswith('0x') else row['uid']
                    tx_hash_hex = row['tx_hash'][2:] if row['tx_hash'].startswith('0x') else row['tx_hash']
                    values_list.append(
                        f"(decode('{uid_hex}', 'hex'), '{row['revocation_time']}', decode('{tx_hash_hex}', 'hex'))"
                    )
                
                # Update each table 
                for table in table_names:
                    values_str = ',\n            '.join(values_list)
                    query = f"""
                            UPDATE public.{table} AS t
                            SET
                                revoked = true,
                            revocation_time = v.revocation_time::timestamp,
                            tx_hash = v.tx_hash,
                            last_updated_time = NOW()
                        FROM (VALUES
                            {values_str}
                        ) AS v(uid, revocation_time, tx_hash)
                        WHERE t.uid = v.uid;
                    """
                    r = self.db_connector.execute_query(query)
                    print(f"Updated {table} batch {i//batch_size + 1}: {len(batch_df)} rows")
            
            print_load(self.name, {'table_names': table_names}, df.shape)
        
        self.save_last_run_block(self.schema_chain, self.extract_params['from_block'], self.extract_params['to_block'])

    ## ----------------- Helper functions --------------------

    def get_last_run_block(self, schema_chain) -> int:
        """
        Retrieve the last run block number based on the connected chain from a stored file.
        
        Returns:
            int: The last run block number.
        """
        try:
            with open(f'{self.additional_folder_structure}src/adapters/adapter_oli_offchain_last_run_{schema_chain}.txt', 'r') as f:
                content = f.read()
                content = json.loads(content.replace("'", '"'))
                return content.get('to_block', 0)
        except FileNotFoundError:
            return 0

    def save_last_run_block(self, schema_chain, from_block: int, to_block: int):
        """
        Save the last run block number based on the connected chain to a stored file.
        
        Args:
            from_block (int): The last run from block number.
            to_block (int): The last run to block number.
        """
        with open(f'{self.additional_folder_structure}src/adapters/adapter_oli_offchain_last_run_{schema_chain}.txt', 'w') as f:
            f.write(str(
                {'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
                'from_block': from_block, 
                'to_block': to_block}
            ))