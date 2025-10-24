from src.adapters.adapter_logs import AdapterLogs
from eth_abi.abi import decode
from web3 import Web3
import pandas as pd
import json

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract, upsert_table

class AdapterOLI(AbstractAdapter):
    """
    adapter_params require the following fields
        rpc_url:str - the RPC URL to connect to the blockchain
        eas_address:str - the address of the EAS contract (optional)
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("OLIv2", adapter_params, db_connector)

        # setup web3
        self.w3 = Web3(Web3.HTTPProvider(self.adapter_params['rpc_url']))
        self.schema_chain = self.w3.eth.chain_id # which chain we are extracting from

        # setup logs adapter
        self.adapter_logs = AdapterLogs(self.w3)

        # setup EAS contract instance (address might be different on other chains!)
        abi = [ { "inputs": [ { "internalType": "bytes32", "name": "uid", "type": "bytes32" } ], "name": "getAttestation", "outputs": [ { "components": [ { "internalType": "bytes32", "name": "uid", "type": "bytes32" }, { "internalType": "bytes32", "name": "schema", "type": "bytes32" }, { "internalType": "uint64", "name": "time", "type": "uint64" }, { "internalType": "uint64", "name": "expirationTime", "type": "uint64" }, { "internalType": "uint64", "name": "revocationTime", "type": "uint64" }, { "internalType": "bytes32", "name": "refUID", "type": "bytes32" }, { "internalType": "address", "name": "recipient", "type": "address" }, { "internalType": "address", "name": "attester", "type": "address" }, { "internalType": "bool", "name": "revocable", "type": "bool" }, { "internalType": "bytes", "name": "data", "type": "bytes" } ], "internalType": "struct Attestation", "name": "", "type": "tuple" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "revoker", "type": "address" }, { "internalType": "bytes32", "name": "data", "type": "bytes32" } ], "name": "getRevokeOffchain", "outputs": [ { "internalType": "uint64", "name": "", "type": "uint64" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "uid", "type": "bytes32" } ], "name": "isAttestationValid", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" } ]
        address = adapter_params.get("eas_address", "0x4200000000000000000000000000000000000021")
        self.contract = self.w3.eth.contract(address=address, abi=abi)

        print_init(self.name, self.adapter_params)

    """
    extract_params require the following fields:
    """
    def extract(self, extract_params:dict = None) -> pd.DataFrame:
        # store extracted logs with input information in d
        d = []
        logs = self.adapter_logs.extract(extract_params)
        for log in logs:
            uid = '0x' + log['data'].hex()
            # check if attestation is valid
            is_valid = self.contract.functions.isAttestationValid(uid).call()
            if is_valid:
                # read onchain attestation
                r = self.get_attestation_data(uid)
                # decode label data
                label_data = self.decode_label_data(r['data'])
                # append to d
                d.append({
                    'uid': uid,
                    'time': r['time'],
                    'attester': r['attester'],
                    'recipient': r['recipient'],
                    'revoked': False,
                    'is_offchain': False,
                    'tx_id': '0x' + log['transactionHash'].hex(),
                    'ipfs_hash': None,
                    'revocation_time': r['revocationTime'],
                    'chain_id': label_data['chain_id'],
                    'tags_json': label_data['tags_json'],
                    'raw': None,
                    'last_updated_time': None,
                    'schema_info': str(self.schema_chain) + '_' + r['schema']
                })
        df = pd.DataFrame(d)
        print_extract(self.name, extract_params, df.shape)
        return df

    """
    load_params require the following fields:
        table_name: str - the name of the table to load data into
    """
    def load(self, df: pd.DataFrame, load_params:dict = None):
        self.db_connector.upsert_table('attestations', df)
        print_load(self.name, load_params, df.shape)

    ## ----------------- Helper functions --------------------

    def get_attestation_data(self, UID) -> dict:
        """
        Retrieve attestation data from the OLI contract.
        
        Args:
            UID (str): The unique identifier of the attestation.

        Returns:
            dict: Dictionary containing 'chain_id' and 'tags_json'
        """

        r = self.contract.functions.getAttestation(UID).call()

        r_dict = {
            'uid': '0x' + r[0].hex(),
            'schema': '0x' + r[1].hex(),
            'time': r[2],
            #'expirationTime': r[3],
            'revocationTime': r[4],
            #'refUID': '0x' + r[5].hex(),
            'recipient': r[6],
            'attester': r[7],
            'revocable': r[8],
            'data': r[9].hex()
        }

        return r_dict

    def decode_label_data(self, encoded_hex: str) -> dict:
        """
        Decode label data from the OLI format.
        
        Args:
            encoded_hex (str): Encoded label data (with or without '0x' prefix)

        Returns:
            dict: Dictionary containing 'chain_id' and 'tags_json'
        """
        # Remove '0x' prefix if present
        if encoded_hex.startswith('0x'):
            encoded_hex = encoded_hex[2:]
        
        # Convert hex string to bytes
        encoded_bytes = bytes.fromhex(encoded_hex)
        
        # ABI decode the data
        decoded_data = decode(['string', 'string'], encoded_bytes)
        
        chain_id = decoded_data[0]
        tags_json_str = decoded_data[1]
        
        # Parse JSON string back to dict
        tags_json = json.loads(tags_json_str)
        
        return {
            'chain_id': chain_id,
            'tags_json': tags_json
        }
    