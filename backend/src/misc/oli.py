import json
import yaml
import secrets
import time
import requests
from web3 import Web3
from eth_abi.abi import encode
import eth_account
from eth_keys import keys

class OLI:
    def __init__(self, private_key, is_production=True):
        """
        Initialize the OLI API client.
        
        Args:
            private_key (str): The private key to sign attestations
            is_production (bool): Whether to use production or testnet
        """
        # Set network based on environment
        if is_production:
            self.rpc = "https://mainnet.base.org"
            self.graphql = "https://base.easscan.org/graphql"
            self.rpc_chain_number = 8453
            self.eas_api_url = "https://base.easscan.org/offchain/store"
            self.eas_address = "0x4200000000000000000000000000000000000021"  # EAS contract address on mainnet
        else:
            self.rpc = "https://sepolia.base.org"
            self.graphql = "https://base-sepolia.easscan.org/graphql"
            self.rpc_chain_number = 84532
            self.eas_api_url = "https://base-sepolia.easscan.org/offchain/store"
            self.eas_address = "0x4200000000000000000000000000000000000021"  # EAS contract address on testnet
            
        # Initialize Web3 and account
        self.w3 = Web3(Web3.HTTPProvider(self.rpc))
        if not self.w3.is_connected():
            raise Exception("Failed to connect to the Ethereum node")
            
        # Convert the hex private key to the proper key object
        self.private_key = private_key
        if private_key.startswith('0x'):
            private_key_bytes = private_key[2:]
        else:
            private_key_bytes = private_key
        private_key_obj = keys.PrivateKey(bytes.fromhex(private_key_bytes))
        
        # Create account from private key
        self.account = eth_account.Account.from_key(private_key_obj)
        self.address = self.account.address
        
        # Label Pool Schema for OLI
        self.oli_label_pool_schema = '0xb763e62d940bed6f527dd82418e146a904e62a297b8fa765c9b3e1f0bc6fdd68'
        
        # Load EAS ABI
        self.eas_abi = '[{"inputs": [],"stateMutability": "nonpayable","type": "constructor"},{"inputs": [],"name": "AccessDenied","type": "error"},{"inputs": [],"name": "AlreadyRevoked","type": "error"},{"inputs": [],"name": "AlreadyRevokedOffchain","type": "error"},{"inputs": [],"name": "AlreadyTimestamped","type": "error"},{"inputs": [],"name": "DeadlineExpired","type": "error"},{"inputs": [],"name": "InsufficientValue","type": "error"},{"inputs": [],"name": "InvalidAttestation","type": "error"},{"inputs": [],"name": "InvalidAttestations","type": "error"},{"inputs": [],"name": "InvalidExpirationTime","type": "error"},{"inputs": [],"name": "InvalidLength","type": "error"},{"inputs": [],"name": "InvalidNonce","type": "error"},{"inputs": [],"name": "InvalidOffset","type": "error"},{"inputs": [],"name": "InvalidRegistry","type": "error"},{"inputs": [],"name": "InvalidRevocation","type": "error"},{"inputs": [],"name": "InvalidRevocations","type": "error"},{"inputs": [],"name": "InvalidSchema","type": "error"},{"inputs": [],"name": "InvalidSignature","type": "error"},{"inputs": [],"name": "InvalidVerifier","type": "error"},{"inputs": [],"name": "Irrevocable","type": "error"},{"inputs": [],"name": "NotFound","type": "error"},{"inputs": [],"name": "NotPayable","type": "error"},{"inputs": [],"name": "WrongSchema","type": "error"},{"anonymous": false,"inputs": [{"indexed": true,"internalType": "address","name": "recipient","type": "address"},{"indexed": true,"internalType": "address","name": "attester","type": "address"},{"indexed": false,"internalType": "bytes32","name": "uid","type": "bytes32"},{"indexed": true,"internalType": "bytes32","name": "schemaUID","type": "bytes32"}],"name": "Attested","type": "event"},{"anonymous": false,"inputs": [{"indexed": false,"internalType": "uint256","name": "oldNonce","type": "uint256"},{"indexed": false,"internalType": "uint256","name": "newNonce","type": "uint256"}],"name": "NonceIncreased","type": "event"},{"anonymous": false,"inputs": [{"indexed": true,"internalType": "address","name": "recipient","type": "address"},{"indexed": true,"internalType": "address","name": "attester","type": "address"},{"indexed": false,"internalType": "bytes32","name": "uid","type": "bytes32"},{"indexed": true,"internalType": "bytes32","name": "schemaUID","type": "bytes32"}],"name": "Revoked","type": "event"},{"anonymous": false,"inputs": [{"indexed": true,"internalType": "address","name": "revoker","type": "address"},{"indexed": true,"internalType": "bytes32","name": "data","type": "bytes32"},{"indexed": true,"internalType": "uint64","name": "timestamp","type": "uint64"}],"name": "RevokedOffchain","type": "event"},{"anonymous": false,"inputs": [{"indexed": true,"internalType": "bytes32","name": "data","type": "bytes32"},{"indexed": true,"internalType": "uint64","name": "timestamp","type": "uint64"}],"name": "Timestamped","type": "event"},{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "address","name": "recipient","type": "address"},{"internalType": "uint64","name": "expirationTime","type": "uint64"},{"internalType": "bool","name": "revocable","type": "bool"},{"internalType": "bytes32","name": "refUID","type": "bytes32"},{"internalType": "bytes","name": "data","type": "bytes"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct AttestationRequestData","name": "data","type": "tuple"}],"internalType": "struct AttestationRequest","name": "request","type": "tuple"}],"name": "attest","outputs": [{"internalType": "bytes32","name": "","type": "bytes32"}],"stateMutability": "payable","type": "function"},{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "address","name": "recipient","type": "address"},{"internalType": "uint64","name": "expirationTime","type": "uint64"},{"internalType": "bool","name": "revocable","type": "bool"},{"internalType": "bytes32","name": "refUID","type": "bytes32"},{"internalType": "bytes","name": "data","type": "bytes"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct AttestationRequestData","name": "data","type": "tuple"},{"components": [{"internalType": "uint8","name": "v","type": "uint8"},{"internalType": "bytes32","name": "r","type": "bytes32"},{"internalType": "bytes32","name": "s","type": "bytes32"}],"internalType": "struct Signature","name": "signature","type": "tuple"},{"internalType": "address","name": "attester","type": "address"},{"internalType": "uint64","name": "deadline","type": "uint64"}],"internalType": "struct DelegatedAttestationRequest","name": "delegatedRequest","type": "tuple"}],"name": "attestByDelegation","outputs": [{"internalType": "bytes32","name": "","type": "bytes32"}],"stateMutability": "payable","type": "function"},{"inputs": [],"name": "getAttestTypeHash","outputs": [{"internalType": "bytes32","name": "","type": "bytes32"}],"stateMutability": "pure","type": "function"},{"inputs": [{"internalType": "bytes32","name": "uid","type": "bytes32"}],"name": "getAttestation","outputs": [{"components": [{"internalType": "bytes32","name": "uid","type": "bytes32"},{"internalType": "bytes32","name": "schema","type": "bytes32"},{"internalType": "uint64","name": "time","type": "uint64"},{"internalType": "uint64","name": "expirationTime","type": "uint64"},{"internalType": "uint64","name": "revocationTime","type": "uint64"},{"internalType": "bytes32","name": "refUID","type": "bytes32"},{"internalType": "address","name": "recipient","type": "address"},{"internalType": "address","name": "attester","type": "address"},{"internalType": "bool","name": "revocable","type": "bool"},{"internalType": "bytes","name": "data","type": "bytes"}],"internalType": "struct Attestation","name": "","type": "tuple"}],"stateMutability": "view","type": "function"},{"inputs": [],"name": "getDomainSeparator","outputs": [{"internalType": "bytes32","name": "","type": "bytes32"}],"stateMutability": "view","type": "function"},{"inputs": [],"name": "getName","outputs": [{"internalType": "string","name": "","type": "string"}],"stateMutability": "view","type": "function"},{"inputs": [{"internalType": "address","name": "account","type": "address"}],"name": "getNonce","outputs": [{"internalType": "uint256","name": "","type": "uint256"}],"stateMutability": "view","type": "function"},{"inputs": [{"internalType": "address","name": "revoker","type": "address"},{"internalType": "bytes32","name": "data","type": "bytes32"}],"name": "getRevokeOffchain","outputs": [{"internalType": "uint64","name": "","type": "uint64"}],"stateMutability": "view","type": "function"},{"inputs": [],"name": "getRevokeTypeHash","outputs": [{"internalType": "bytes32","name": "","type": "bytes32"}],"stateMutability": "pure","type": "function"},{"inputs": [],"name": "getSchemaRegistry","outputs": [{"internalType": "contract ISchemaRegistry","name": "","type": "address"}],"stateMutability": "pure","type": "function"},{"inputs": [{"internalType": "bytes32","name": "data","type": "bytes32"}],"name": "getTimestamp","outputs": [{"internalType": "uint64","name": "","type": "uint64"}],"stateMutability": "view","type": "function"},{"inputs": [{"internalType": "uint256","name": "newNonce","type": "uint256"}],"name": "increaseNonce","outputs": [],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "bytes32","name": "uid","type": "bytes32"}],"name": "isAttestationValid","outputs": [{"internalType": "bool","name": "","type": "bool"}],"stateMutability": "view","type": "function"},{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "address","name": "recipient","type": "address"},{"internalType": "uint64","name": "expirationTime","type": "uint64"},{"internalType": "bool","name": "revocable","type": "bool"},{"internalType": "bytes32","name": "refUID","type": "bytes32"},{"internalType": "bytes","name": "data","type": "bytes"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct AttestationRequestData[]","name": "data","type": "tuple[]"}],"internalType": "struct MultiAttestationRequest[]","name": "multiRequests","type": "tuple[]"}],"name": "multiAttest","outputs": [{"internalType": "bytes32[]","name": "","type": "bytes32[]"}],"stateMutability": "payable","type": "function"},{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "address","name": "recipient","type": "address"},{"internalType": "uint64","name": "expirationTime","type": "uint64"},{"internalType": "bool","name": "revocable","type": "bool"},{"internalType": "bytes32","name": "refUID","type": "bytes32"},{"internalType": "bytes","name": "data","type": "bytes"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct AttestationRequestData[]","name": "data","type": "tuple[]"},{"components": [{"internalType": "uint8","name": "v","type": "uint8"},{"internalType": "bytes32","name": "r","type": "bytes32"},{"internalType": "bytes32","name": "s","type": "bytes32"}],"internalType": "struct Signature[]","name": "signatures","type": "tuple[]"},{"internalType": "address","name": "attester","type": "address"},{"internalType": "uint64","name": "deadline","type": "uint64"}],"internalType": "struct MultiDelegatedAttestationRequest[]","name": "multiDelegatedRequests","type": "tuple[]"}],"name": "multiAttestByDelegation","outputs": [{"internalType": "bytes32[]","name": "","type": "bytes32[]"}],"stateMutability": "payable","type": "function"},{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "bytes32","name": "uid","type": "bytes32"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct RevocationRequestData[]","name": "data","type": "tuple[]"}],"internalType": "struct MultiRevocationRequest[]","name": "multiRequests","type": "tuple[]"}],"name": "multiRevoke","outputs": [],"stateMutability": "payable","type": "function"},{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "bytes32","name": "uid","type": "bytes32"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct RevocationRequestData[]","name": "data","type": "tuple[]"},{"components": [{"internalType": "uint8","name": "v","type": "uint8"},{"internalType": "bytes32","name": "r","type": "bytes32"},{"internalType": "bytes32","name": "s","type": "bytes32"}],"internalType": "struct Signature[]","name": "signatures","type": "tuple[]"},{"internalType": "address","name": "revoker","type": "address"},{"internalType": "uint64","name": "deadline","type": "uint64"}],"internalType": "struct MultiDelegatedRevocationRequest[]","name": "multiDelegatedRequests","type": "tuple[]"}],"name": "multiRevokeByDelegation","outputs": [],"stateMutability": "payable","type": "function"},{"inputs": [{"internalType": "bytes32[]","name": "data","type": "bytes32[]"}],"name": "multiRevokeOffchain","outputs": [{"internalType": "uint64","name": "","type": "uint64"}],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "bytes32[]","name": "data","type": "bytes32[]"}],"name": "multiTimestamp","outputs": [{"internalType": "uint64","name": "","type": "uint64"}],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "bytes32","name": "uid","type": "bytes32"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct RevocationRequestData","name": "data","type": "tuple"}],"internalType": "struct RevocationRequest","name": "request","type": "tuple"}],"name": "revoke","outputs": [],"stateMutability": "payable","type": "function"},{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "bytes32","name": "uid","type": "bytes32"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct RevocationRequestData","name": "data","type": "tuple"},{"components": [{"internalType": "uint8","name": "v","type": "uint8"},{"internalType": "bytes32","name": "r","type": "bytes32"},{"internalType": "bytes32","name": "s","type": "bytes32"}],"internalType": "struct Signature","name": "signature","type": "tuple"},{"internalType": "address","name": "revoker","type": "address"},{"internalType": "uint64","name": "deadline","type": "uint64"}],"internalType": "struct DelegatedRevocationRequest","name": "delegatedRequest","type": "tuple"}],"name": "revokeByDelegation","outputs": [],"stateMutability": "payable","type": "function"},{"inputs": [{"internalType": "bytes32","name": "data","type": "bytes32"}],"name": "revokeOffchain","outputs": [{"internalType": "uint64","name": "","type": "uint64"}],"stateMutability": "nonpayable","type": "function"},{"inputs": [{"internalType": "bytes32","name": "data","type": "bytes32"}],"name": "timestamp","outputs": [{"internalType": "uint64","name": "","type": "uint64"}],"stateMutability": "nonpayable","type": "function"},{"inputs": [],"name": "version","outputs": [{"internalType": "string","name": "","type": "string"}],"stateMutability": "view","type": "function"}]'

        # Initialize EAS contract
        self.eas = self.w3.eth.contract(address=self.eas_address, abi=self.eas_abi)
    
        # get latest official OLI tag ids
        self.tag_definitions = self.get_OLI_tags()
        self.tag_ids = list(self.tag_definitions.keys())

        # get latest value_sets for the OLI tags
        self.tag_value_sets = self.get_OLI_value_sets()
    
    ### internal functions the user should not call directly

    def encode_label_data(self, chain_id, tags_json):
        """
        Encode label data in the OLI format.
        
        Args:
            chain_id (str): Chain ID in CAIP-2 format of the label (e.g. 'eip155:8453')
            tags_json (dict): Dictionary of tag data following the OLI format
            
        Returns:
            str: Hex-encoded ABI data
        """
        # Convert dict to JSON string if needed
        if isinstance(tags_json, dict):
            tags_json = json.dumps(tags_json)
            
        # ABI encode the data
        encoded_data = encode(['string', 'string'], [chain_id, tags_json])
        return f"0x{encoded_data.hex()}"

    # offchain attestation functions
    
    def create_offchain_attestation(self, recipient, schema, data, ref_uid, revocable=True, expiration_time=0):
        """
        Create an attestation with the given parameters.
        
        Args:
            recipient (str): Ethereum address of the contract to be labeled
            schema (str): Schema hash
            data (str): Hex-encoded data
            ref_uid (str): Reference UID
            revocable (bool): Whether the attestation is revocable
            expiration_time (int): Expiration time in seconds since epoch
            
        Returns:
            dict: The signed attestation and UID
        """
        # Create a random salt
        salt = f"0x{secrets.token_hex(32)}"
        
        # Current time in seconds
        current_time = int(time.time())
        
        # Typed data for the attestation
        typed_data = {
            "version": 2,
            "recipient": recipient,
            "time": current_time,
            "revocable": revocable,
            "schema": schema,
            "refUID": ref_uid,
            "data": data,
            "expirationTime": expiration_time,
            "salt": salt,
        }
        
        # EIP-712 typed data format
        types = {
            "domain": {
                "name": "EAS Attestation",
                "version": "1.2.0",
                "chainId": self.rpc_chain_number,
                "verifyingContract": self.eas_address
            },
            "primaryType": "Attest",
            "message": typed_data,
            "types": {
                "Attest": [
                    {"name": "version", "type": "uint16"},
                    {"name": "schema", "type": "bytes32"},
                    {"name": "recipient", "type": "address"},
                    {"name": "time", "type": "uint64"},
                    {"name": "expirationTime", "type": "uint64"},
                    {"name": "revocable", "type": "bool"},
                    {"name": "refUID", "type": "bytes32"},
                    {"name": "data", "type": "bytes"},
                    {"name": "salt", "type": "bytes32"}
                ]
            }
        }

        # Make sure to install correct version of eth-account (pip install eth-account==0.13.5)
        signed_message = self.account.sign_typed_data(
            domain_data=types["domain"],
            message_types=types["types"],
            message_data=typed_data
        )
        
        # Calculate the UID
        attester = '0x0000000000000000000000000000000000000000'  # for offchain UID calculation
        uid = self.calculate_attestation_uid_v2(schema, recipient, attester, current_time, data, expiration_time, revocable, ref_uid, salt=salt)
        uid_hex = '0x' + uid.hex()
        
        # Package the result
        result = {
            "sig": {
                "domain": types["domain"],
                "primaryType": types["primaryType"],
                "types": types["types"],
                "message": typed_data,
                "uid": uid_hex,
                "version": 2,
                "signature": {
                    "r": hex(signed_message.r),
                    "s": hex(signed_message.s),
                    "v": signed_message.v
                }
            },
            "signer": self.address
        }
        
        return result
    
    def submit_offchain_attestation(self, attestation, filename="OLI.txt"):
        """
        Submit an attestation to the EAS API.
        
        Args:
            attestation (dict): The attestation package
            filename (str): Custom filename
            
        Returns:
            dict: API response
        """
        # Convert numerical values to strings for JSON serialization
        attestation["sig"]["message"]["time"] = str(attestation["sig"]["message"]["time"])
        attestation["sig"]["message"]["expirationTime"] = str(attestation["sig"]["message"]["expirationTime"])
        attestation["sig"]["domain"]["chainId"] = str(attestation["sig"]["domain"]["chainId"])
        
        # Prepare payload for the API endpoint
        payload = {
            "filename": filename,
            "textJson": json.dumps(attestation, separators=(',', ':'))
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        # Post the data to the API
        response = requests.post(self.eas_api_url, json=payload, headers=headers)
        return response
    
    def calculate_attestation_uid_v2(self, schema, recipient, attester, timestamp, data, expiration_time=0, revocable=True, ref_uid="0x0000000000000000000000000000000000000000000000000000000000000000", bump=0, salt=None):
        """
        Calculate the UID for an offchain attestation (v2).
        
        Args:
            schema (str): Schema hash
            recipient (str): Recipient address
            attester (str): Attester address
            timestamp (int): Timestamp
            data (str): Attestation data
            expiration_time (int): Expiration time
            revocable (bool): Whether attestation is revocable
            ref_uid (str): Reference UID
            bump (int): Bump value
            salt (str): Salt value
            
        Returns:
            bytes: The calculated UID
        """
        # Generate salt if not provided
        if salt is None:
            salt = f"0x{secrets.token_hex(32)}"
            
        # Version
        version = 2
        version_bytes = version.to_bytes(2, byteorder='big')
        
        # Handle schema formatting
        if not schema.startswith('0x'):
            schema = '0x' + schema
        schema_utf8_bytes = schema.encode('utf-8')
        schema_bytes = schema_utf8_bytes
        
        # Convert values to bytes
        recipient_bytes = Web3.to_bytes(hexstr=recipient)
        attester_bytes = Web3.to_bytes(hexstr=attester)
        timestamp_bytes = timestamp.to_bytes(8, byteorder='big')
        expiration_bytes = expiration_time.to_bytes(8, byteorder='big')
        revocable_bytes = bytes([1]) if revocable else bytes([0])
        ref_uid_bytes = Web3.to_bytes(hexstr=ref_uid)
        data_bytes = Web3.to_bytes(hexstr=data)
        salt_bytes = Web3.to_bytes(hexstr=salt)
        bump_bytes = bump.to_bytes(4, byteorder='big')
        
        # Pack all values
        packed_data = (
            version_bytes + schema_bytes + recipient_bytes + attester_bytes + 
            timestamp_bytes + expiration_bytes + revocable_bytes + ref_uid_bytes + 
            data_bytes + salt_bytes + bump_bytes
        )
        
        # Calculate keccak256 hash
        uid = Web3.keccak(packed_data)
        return uid
    
    # functions for onchain attestations

    def estimate_gas_limit(self, function, tx_params:dict, gas_limit:int):
        """
        Estimate gas for a transaction.
        
        Args:
            function: The function to estimate gas for
            tx_params (dict): Transaction parameters
            
        Returns:
            tx_params (dict): Transaction parameters with estimated 'gas' field
        """
        try:
            if gas_limit == 0:
                # Estimate gas with a buffer (e.g., 10% more than the estimate)
                estimated_gas = function.estimate_gas(tx_params)
                tx_params["gas"] = int(estimated_gas * 1.1)  # Add 10% buffer
            else:
                tx_params["gas"] = gas_limit
        except Exception as e:
            tx_params["gas"] = 10000000  # Default fallback
        return tx_params
    
    # init functions

    def get_OLI_tags(self) -> dict:
        """
        Get latest OLI tags from OLI Github repo.
        
        Returns:
            dict: Dictionary of official OLI tags
        """
        url = "https://raw.githubusercontent.com/lorenz234/OLI/refs/heads/main/1_data_model/tags/tag_definitions.yml" #### change from my fork to official repo
        response = requests.get(url)
        if response.status_code == 200:
            y = yaml.safe_load(response.text)
            y = {i['tag_id']: i for i in y['tags']}
            return y
        else:
            raise Exception(f"Failed to fetch OLI tags from Github: {response.status_code} - {response.text}")

    def get_OLI_value_sets(self) -> dict:
        """
        Get latest value sets for OLI tags.
        
        Returns:
            dict: Dictionary of value sets with tag_id as key
        """
        value_sets = {}

        # value set for owner_project
        url = "https://api.growthepie.xyz/v1/labels/projects.json" 
        response = requests.get(url)
        if response.status_code == 200:
            y = yaml.safe_load(response.text)
            value_sets["owner_project"] = [i[0] for i in y['data']['data']]
        else:
            raise Exception(f"Failed to fetch owner_project value set from grwothepie projects api: {response.status_code} - {response.text}")

        # value set for usage_category
        url = "https://raw.githubusercontent.com/lorenz234/OLI/refs/heads/main/1_data_model/tags/valuesets/usage_category.yml" ### change from my fork to official repo
        response = requests.get(url)
        if response.status_code == 200:
            y = yaml.safe_load(response.text)
            value_sets['usage_category'] = [i['category_id'] for i in y['categories']]
        else:
            raise Exception(f"Failed to fetch usage_category value set from OLI Github: {response.status_code} - {response.text}")

        return value_sets

    # data check functions

    def check_label_correctness(self, address:str, chain_id:str, tags:dict, ref_uid:str="0x0000000000000000000000000000000000000000000000000000000000000000") -> bool:
        """
        Check if the label is correct.
        
        Args:
            address (str): Address to check
            chain_id (str): Chain ID to check
            tags (dict): Tags to check
            
        Returns:
            bool: True if the label is correct, False otherwise
        """
        self.checks_address(address)
        self.checks_chain_id(chain_id)
        self.checks_tags(tags)
        self.checks_ref_uid(ref_uid)
        return True
        
    def checks_chain_id(self, chain_id:str) -> bool:
        """
        Check if chain_id for a label is in CAIP-2 format.
        
        Args:
            chain_id (str): Chain ID to check
            
        Returns:
            bool: True if correct, False otherwise
        """
        # Define whitelist of chain ID prefixes according to CAIP-2 format
        self.allowed_prefixes = [
            'eip155:',  # Ethereum and EVM-compatible chains
            'solana:',  # Solana
            'tron:',    # TRON
            'stellar:', # Stellar
            'bip122:'   # Bitcoin
        ]
        
        # Check if the chain_id starts with any of the allowed prefixes
        for prefix in self.allowed_prefixes:
            if chain_id.startswith(prefix):
                # For eip155, further validate that the rest is a number or 'any'
                if prefix == 'eip155:':
                    rest = chain_id[len(prefix):]
                    if rest == 'any' or rest.isdigit():
                        return True
                    else:
                        print(f"Invalid EIP155 chain ID format: {chain_id}")
                        raise ValueError("For EIP155 chains, format must be 'eip155:' followed by a number or 'any'")
                return True
        
        # If we get here, the chain_id didn't match any allowed format
        print(f"Unsupported chain ID format: {chain_id}")
        raise ValueError("Chain ID must be in CAIP-2 format (e.g., Base -> 'eip155:8453'), see this guide on CAIP-2: https://docs.portalhq.io/resources/chain-id-formatting")

    def checks_address(self, address):
        """
        Check if address is a valid Ethereum address.
        
        Args:
            address (str): Address to check
            
        Returns:
            bool: True if correct, False otherwise
        """
        if self.w3.is_address(address):
            return True
        else:
            print(address)
            raise ValueError("Address must be a valid Ethereum address in hex format")
        
    def checks_tags(self, tags:dict) -> bool:
        """
        Check if tags are in the correct format.
        
        Args:
            tags (dict): Tags to check
            
        Returns:
            bool: True if correct, False otherwise
        """

        # Check if tags is a dictionary
        if isinstance(tags, dict):
            pass
        else:
            print(tags)
            raise ValueError("Tags must be a dictionary with OLI compliant tags (e.g., {'contract_name': 'example', 'is_eoa': True})")
        
        # Check each tag_id in the dictionary
        for tag_id in tags.keys():
            
            # Check if the tag_id is in the official OLI tag list
            if tag_id not in self.tag_ids:
                print(f"WARNING: Tag tag_id '{tag_id}' is not an official OLI tag. Please check the 'oli.tag_definitions' or https://github.com/openlabelsinitiative/OLI/blob/main/1_data_model/tags/tag_definitions.yml.")
            
            # Check if the tag_id is in the correct format. So far implemented [boolean, string, integer, float, string(42), string(66), date (YYYY-MM-DD HH:MM:SS)]
            else:
                if self.tag_definitions[tag_id]['type'] == 'boolean' and not isinstance(tags[tag_id], bool):
                    print(f"WARNING: Tag value for {tag_id} must be a boolean (True/False).")
                elif self.tag_definitions[tag_id]['type'] == 'string' and not isinstance(tags[tag_id], str):
                    print(f"WARNING: Tag value for {tag_id} must be a string.")
                elif self.tag_definitions[tag_id]['type'] == 'integer' and not isinstance(tags[tag_id], int):
                    print(f"WARNING: Tag value for {tag_id} must be an integer.")
                elif self.tag_definitions[tag_id]['type'] == 'float' and not isinstance(tags[tag_id], float):
                    print(f"WARNING: Tag value for {tag_id} must be a float.")
                elif self.tag_definitions[tag_id]['type'] == 'string(42)' and not self.w3.is_address(tags[tag_id]):
                    print(f"WARNING: Tag value for {tag_id} must be a valid Ethereum address string with '0x'.")
                elif self.tag_definitions[tag_id]['type'] == 'string(66)' and not (len(tags[tag_id]) == 66 and tags[tag_id].startswith('0x')):
                    print(f"WARNING: Tag value for {tag_id} must be a valid hex string with '0x' prefix and 64 hex characters (66 characters total).")
                elif self.tag_definitions[tag_id]['type'] == 'date (YYYY-MM-DD HH:MM:SS)' and not isinstance(tags[tag_id], str):
                    print(f"WARNING: Tag value for {tag_id} must be a string in the format 'YYYY-MM-DD HH:MM:SS'.")

            # Check if the value is in the value set
            if tag_id in self.tag_value_sets and tags[tag_id] not in self.tag_value_sets[tag_id]:
                print(f"WARNING: Invalid tag value for {tag_id}: {tags[tag_id]}")
                if tag_id == 'owner_project':
                    print(f"Please use a valid owner_project name from the OSS directory: https://github.com/opensource-observer/oss-directory/tree/main/data/projects")
                else:
                    print(f"Please use one of the following values: {self.tag_value_sets[tag_id]}")


    def checks_ref_uid(self, ref_uid):
        """
        Check if ref_uid is a valid UID.
        
        Args:
            ref_uid (str): Reference UID to check
            
        Returns:
            bool: True if correct, False otherwise
        """
        if ref_uid.startswith('0x') and len(ref_uid) == 66:
            return True
        else:
            print(ref_uid)
            raise ValueError("Ref_uid must be a valid UID in hex format, leave empty if not used")

    
    ### functions the user should call to create attestations
    
    def create_offchain_label(self, address, chain_id, tags, ref_uid="0x0000000000000000000000000000000000000000000000000000000000000000", retry=4):
        """
        Create an offchain OLI label attestation for a contract.
        
        Args:
            address (str): The contract address to label
            chain_id (str): Chain ID in CAIP-2 format where the address/contract resides
            tags (dict): OLI compliant tags as a dict  information (name, version, etc.)
            ref_uid (str): Reference UID
            retry (int): Number of retries for the API post request to EAS ipfs
            
        Returns:
            dict: API request response
        """
        # Check all necessary input parameters
        address = self.checks_address(address)
        chain_id = self.checks_chain_id(chain_id)
        tags = self.checks_tags(tags)
        ref_uid = self.checks_ref_uid(ref_uid)
        
        # Encode the label data
        data = self.encode_label_data(chain_id, tags)
        
        # Create the attestation
        attestation = self.create_offchain_attestation(recipient=address, schema=self.oli_label_pool_schema, data=data, ref_uid=ref_uid)
        
        # Submit to the API & retry if status code is not 200
        response = self.submit_offchain_attestation(attestation)
        n0 = retry
        while response.status_code != 200 and retry > 0:
            retry -= 1
            time.sleep(2 ** (n0 - retry)) # exponential backoff
            response = self.submit_offchain_attestation(attestation)
        
        # if it fails after all retries, raise an error
        if response.status_code != 200:
            raise Exception(f"Failed to submit offchain attestation to EAS API ipfs post endpoint after {n0} retries: {response.status_code} - {response.text}")

        return response
    
    def create_onchain_label(self, address:str, chain_id:str, tags:dict, ref_uid:str="0x0000000000000000000000000000000000000000000000000000000000000000", gas_limit:int=0) -> tuple[str, str]:
        """
        Create an onchain OLI label attestation for a contract.
        
        Args:
            address (str): The contract address to label
            chain_id (str): Chain ID in CAIP-2 format where the address/contract resides
            tags (dict): OLI compliant tags as a dict  information (name, version, etc.)
            ref_uid (str): Reference UID
            gas_limit (int): Gas limit for the transaction. If set to 0, the function will estimate the gas limit.
            
        Returns:
            str: Transaction hash
            str: UID of the attestation
        """
        # Check all necessary input parameters
        address = self.checks_address(address)
        chain_id = self.checks_chain_id(chain_id)
        tags = self.checks_tags(tags)
        ref_uid = self.checks_ref_uid(ref_uid)

        # Encode the label data
        data = self.encode_label_data(chain_id, tags)
        
        # Create the attestation
        function = self.eas.functions.attest({
            'schema': self.w3.to_bytes(hexstr=self.oli_label_pool_schema),
            'data': {
                'recipient': self.w3.to_checksum_address(address),
                'expirationTime': 0,
                'revocable': True,
                'refUID': self.w3.to_bytes(hexstr=ref_uid),
                'data': self.w3.to_bytes(hexstr=data),
                'value': 0
            }
        })

        # Define the transaction parameters
        tx_params = {
            'chainId': self.rpc_chain_number,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.w3.eth.get_transaction_count(self.address),
        }

        # Estimate gas if no limit provided
        tx_params = self.estimate_gas_limit(function, tx_params, gas_limit)
        
        # Build the transaction to attest one label
        transaction = function.build_transaction(tx_params)

        # Sign the transaction with the private key
        signed_txn = self.w3.eth.account.sign_transaction(transaction, private_key=self.private_key)
        
        # Send the transaction
        try:
            txn_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
        except Exception as e:
            raise Exception(f"Failed to send transaction to mempool: {e}")

        # Wait for the transaction receipt
        txn_receipt = self.w3.eth.wait_for_transaction_receipt(txn_hash)
        
        # Check if the transaction was successful
        if txn_receipt.status == 1:
            return f"0x{txn_hash.hex()}", f"0x{txn_receipt.logs[0].data.hex()}"
        else:
            raise Exception(f"Transaction failed onchain: {txn_receipt}")
    
    def create_multi_onchain_labels(self, labels:list, gas_limit:int=0) -> tuple[str, list]:
        """
        Batch submit OLI labels in one transaction.
        
        Args:
            labels (list): List of labels, containing dictionaries with 'address', 'tags', and 'chain_id' (, optional 'ref_uid')
                address (str): The contract address to label
                chain_id (str): Chain ID in CAIP-2 format where the address/contract resides
                tags (dict): OLI compliant tags as a dict  information (name, version, etc.)
                ref_uid (str): Reference UID
            gas_limit (int): Gas limit for one transaction to submit all labels passed, make sure to set it high enough for multiple attestations! If set to 0, the function will estimate the gas limit.
            
        Returns:
            str: Transaction hash
            list: List of UID of the attestation
        """
        # Prepare the list of "data" requests
        full_data = []
        for label in labels:
            # check if address, chain_id & tags are provided
            if 'chain_id' not in label:
                raise ValueError("chain_id must be provided for each label in CAIP-2 format (e.g., Base -> 'eip155:8453')")
            elif 'address' not in label:
                raise ValueError("An address must be provided for each label")
            elif 'tags' not in label:
                raise ValueError("tags dictionary must be provided for each label")
            
            # run checks on each label
            chain_id = self.checks_chain_id(label.get('chain_id'))
            address = self.checks_address(label.get('address'))
            tags = self.checks_tags(label.get('tags'))

            # check if ref_uid is provided
            if 'ref_uid' not in label:
                label['ref_uid'] = "0x0000000000000000000000000000000000000000000000000000000000000000"
            else:
                label['ref_uid'] = self.checks_ref_uid(label.get('ref_uid'))

            # ABI encode data for each attestation
            #encoded_data = encode(['string', 'string'], [chain_id, tags])
            #data = f"0x{encoded_data.hex()}"
            data = self.encode_label_data(chain_id, tags)
            full_data.append({
                'recipient': self.w3.to_checksum_address(address),
                'expirationTime': 0,
                'revocable': True,
                'refUID': self.w3.to_bytes(hexstr=label['ref_uid']),
                'data': self.w3.to_bytes(hexstr=data),
                'value': 0
            })

        # Create the multi-attestation request
        multi_requests = [{
            'schema': self.w3.to_bytes(hexstr=self.oli_label_pool_schema),
            'data': full_data
        }]

        # Create the function call
        function = self.eas.functions.multiAttest(multi_requests)

        # Define the transaction parameters
        tx_params = {
            'chainId': self.rpc_chain_number,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.w3.eth.get_transaction_count(self.address),
        }

        # Estimate gas if no limit provided
        tx_params = self.estimate_gas_limit(function, tx_params, gas_limit)

        # Build the transaction to revoke an attestation
        transaction = function.build_transaction(tx_params)

        # Sign the transaction with the private key
        signed_txn = self.w3.eth.account.sign_transaction(transaction, private_key=self.private_key)
        
        # Send the transaction
        try:
            txn_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
        except Exception as e:
            raise Exception(f"Failed to send transaction to mempool: {e}")
        
        # Wait for the transaction receipt
        txn_receipt = self.w3.eth.wait_for_transaction_receipt(txn_hash)

        # Check if the transaction was successful
        if txn_receipt.status != 1:
            raise Exception(f"Transaction failed onchain: {txn_receipt}")

        # log the UIDs of the attestations in a list
        uids = ['0x' + log.data.hex() for log in txn_receipt.logs]

        return f"0x{txn_hash.hex()}", uids

    def revoke_attestation(self, uid_hex:str, onchain:bool, gas_limit:int=200000) -> str:
        """
        Revoke an onchain attestation (onchain or offchain) using its UID. Revoking an attestation, weather it is onchain or offchain, requires an onchain transaction.
        
        Args:
            uid_hex (str): UID of the attestation to revoke (in hex format)
            onchain (bool): Whether the attestation is onchain or offchain
            gas_limit (int): Gas limit for the transaction. If not set, defaults to 200000. Gas estimation is not possible for revoke transactions.
            
        Returns:
            str: Transaction hash
        """
        # different function required based on wether the attestation is onchain or offchain
        if onchain:
            function = self.eas.functions.revoke({
                'schema': self.w3.to_bytes(hexstr=self.oli_label_pool_schema),
                'data': {
                    'uid': self.w3.to_bytes(hexstr=uid_hex),
                    'value': 0
                }
            })
        else:
            function = self.eas.functions.revokeOffchain(self.w3.to_bytes(hexstr=uid_hex))

        # Define the transaction parameters
        tx_params = {
            'chainId': self.rpc_chain_number,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.w3.eth.get_transaction_count(self.address),
        }

        # Estimate gas if no limit provided
        tx_params = self.estimate_gas_limit(function, tx_params, gas_limit)

        # Build the transaction to revoke an attestation
        transaction = function.build_transaction(tx_params)

        # Sign the transaction
        signed_txn = self.w3.eth.account.sign_transaction(transaction, private_key=self.private_key)

        # Send the transaction
        try:
            txn_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
        except Exception as e:
            raise Exception(f"Failed to send revoke transaction to mempool: {e}")

        # Get the transaction receipt
        txn_receipt = self.w3.eth.wait_for_transaction_receipt(txn_hash)
        
        # Check if the transaction was successful
        if txn_receipt.status == 1:
            return f"0x{txn_hash.hex()}"
        else:
            raise Exception(f"Transaction failed: {txn_receipt}")
    
    def multi_revoke_attestations(self, uids:list, onchain:bool, gas_limit:int=10000000) -> tuple[str, int]:
        """
        Revoke multiple attestations (onchain or offchain, no mixing!) in a single transaction. Revoking attestations, weather it is onchain or offchain, requires an onchain transaction.
        
        Args:
            uids (list): List of UIDs to revoke (in hex format)
            onchain (bool): Whether the attestations are onchain or offchain (no mix possible)
            gas_limit (int): Gas limit for the transaction. If not set, defaults to 10000000. Gas estimation is not possible for revoke transactions.
            
        Returns:
            str: Transaction hash
            int: Number of attestations revoked
        """
        # different function required based on wether the attestation is onchain or offchain
        if onchain:
            revocation_data = []
            for uid in uids:
                revocation_data.append({
                    'uid': self.w3.to_bytes(hexstr=uid),
                    'value': 0
                })
            multi_requests = [{
                'schema': self.w3.to_bytes(hexstr=self.oli_label_pool_schema),
                'data': revocation_data
            }]
            function = self.eas.functions.multiRevoke(multi_requests)
        else:
            revocation_data = []
            for uid in uids:
                revocation_data.append(self.w3.to_bytes(hexstr=uid))
            function = self.eas.functions.multiRevokeOffchain(revocation_data)

        # Define the transaction parameters
        tx_params = {
            'chainId': self.rpc_chain_number,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.w3.eth.get_transaction_count(self.address),
        }

        # Estimate gas if no limit provided
        tx_params = self.estimate_gas_limit(function, tx_params, gas_limit)

        # Build the transaction
        transaction = function.build_transaction(tx_params)

        # Sign the transaction
        signed_txn = self.w3.eth.account.sign_transaction(transaction, private_key=self.private_key)

        # Send the transaction
        txn_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)

        # Get the transaction receipt
        txn_receipt = self.w3.eth.wait_for_transaction_receipt(txn_hash)
        
        # Check if the transaction was successful
        if txn_receipt.status == 1:
            return f"0x{txn_hash.hex()}", len(uids)
        else:
            raise Exception(f"Transaction failed: {txn_receipt}")

    # functions the user should call to query attestations

    def graphql_query_attestations(self, address:str=None, attester:str=None, timeCreated:int=None, revocationTime:int=None) -> dict:
        # add keywords to search in decodedDataJson
        """
        Queries attestations from the EAS GraphQL API based on the specified filters.
        
        Args:
            address (str, optional): Ethereum address of the labeled contract
            attester (str, optional): Ethereum address of the attester
            timeCreated (int, optional): Filter for attestations created after this timestamp
            revocationTime (int, optional): Filter for attestations with revocation time >= this timestamp
            
        Returns:
            dict: JSON response containing matching attestation data
        """
        query = """
            query Attestations($take: Int, $where: AttestationWhereInput, $orderBy: [AttestationOrderByWithRelationInput!]) {
                attestations(take: $take, where: $where, orderBy: $orderBy) {
                    attester
                    decodedDataJson
                    expirationTime
                    id
                    ipfsHash
                    isOffchain
                    recipient
                    refUID
                    revocable
                    revocationTime
                    revoked
                    time
                    timeCreated
                    txid
                }
            }
        """
            
        variables = {
            "where": {
                "schemaId": {
                    "equals": self.oli_label_pool_schema
                }
            },
            "orderBy": [
                {
                "timeCreated": "desc"
                }
            ]
        }
        
        # Add address to where clause if not None
        if address is not None:
            variables["where"]["recipient"] = {"equals": address}

        # Add attester to where clause if not None
        if attester is not None:
            variables["where"]["attester"] = {"equals": attester}
        
        # Add timeCreated to where clause if not None, ensuring it's an int
        if timeCreated is not None:
            timeCreated = int(timeCreated)
            variables["where"]["timeCreated"] = {"gt": timeCreated}
        
        # Add revocationTime to where clause if not None, ensuring it's an int
        if revocationTime is not None:
            revocationTime = int(revocationTime)
            variables["where"]["revocationTime"] = {"gte": revocationTime}
        
        headers = {
            "Content-Type": "application/json"
        }
        
        response = requests.post(self.graphql, json={"query": query, "variables": variables}, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"GraphQL query failed with status code {response.status_code}: {response.text}")
    
    # function to get raw data labels (from gtp query)
    # function to get decoded data labels (from gtp query)


"""
# Examples

# Initialize the API
private_key = "..."  # Replace with your private key
oli = OLI(private_key, is_production=False)

# Example of a label in OLI format
address = "0x498581ff718922c3f8e6a244956af099b2652b2b"
chain = "eip155:8453" # Base 
tags = {
    'contract_name': 'Pool Manager v4',
    'is_eoa': False, 
    'deployment_tx': '0x25f482fbd94cdea11b018732e455b8e9a940b933cabde3c0c5dd63ea65e85349',
    'deployer_address': '0x2179a60856E37dfeAacA0ab043B931fE224b27B6',
    'owner_project': 'uniswap',
    'version': 4,
    'deployment_date': '2025-01-21 20:28:43',
    'source_code_verified': 'https://repo.sourcify.dev/contracts/partial_match/8453/0x498581fF718922c3f8e6A244956aF099B2652b2b/',
    'is_proxy': False
}
"""

"""
# Example of submitting one onchain attestation
tx_hash, uid = oli.create_onchain_label(address, chain, tags)
print(f"Transaction successful with hash: {tx_hash}")
print(f"UID of the attestation: {uid}")
""" 

"""
# Example of submitting multiple onchain attestations
tx_hash, uids = oli.create_multi_onchain_labels(
    [
        {"address": address, "chain_id": chain, "tags": tags},
        {"address": address, "chain_id": chain, "tags": tags} # of course you can add different/more labels here
    ],
    gas_limit=5000000 # make sure to set it high enough for multiple attestations!
)
print(f"Transaction successful with hash: {tx_hash}")
print(f"UIDs of the attestations: {uids}")
"""

"""
# Example of submitting one offchain attestation
response = oli.create_offchain_label(address, chain, tags)
print(json.dumps(response, indent=2))
"""

"""
# Example of revoking one attestation
tx_hash = oli.revoke_attestation('0xbc5ff96cfb82f7b4a440fd6e0a1dfb9c03f0cc04144f45ec8a8685c9d725c5c8', onchain=True)
print(f"Revocation transaction successful with hash: {tx_hash}")
"""

"""
# Example of revoking multiple attestations
uids = [
    '0x347711384c78ba2aca936115562f9a40c191e3a8dd60cb6a74a6da5e635e7780',
    '0xb9022663266177751b492b3bc22fafba90464ef5cc2d5b70b6a9e8066d68b739'
]
tx_hash, num_revoked = oli.multi_revoke_attestations(uids, onchain=False)
print(f"Revocation transaction successful with hash: {tx_hash}")
print(f"Number of attestations revoked: {num_revoked}")
"""
