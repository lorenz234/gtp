import pandas as pd
import json 

from src.adapters.abstract_adapters import AbstractAdapter
from src.adapters.adapter_logs import AdapterLogs

from web3 import Web3


class EIP8004Adapter(AbstractAdapter):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("EIP-8004", adapter_params, db_connector)
        self.adapter_params = adapter_params

        # contract addresses are the same across all evm chains
        self.address_reputation = "0x8004BAa17C55a88189AE136b182e5fdA19dE9b63"
        self.abi_reputation = json.loads('[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[{"internalType":"address","name":"target","type":"address"}],"name":"AddressEmptyCode","type":"error"},{"inputs":[{"internalType":"address","name":"implementation","type":"address"}],"name":"ERC1967InvalidImplementation","type":"error"},{"inputs":[],"name":"ERC1967NonPayable","type":"error"},{"inputs":[],"name":"FailedCall","type":"error"},{"inputs":[],"name":"InvalidInitialization","type":"error"},{"inputs":[],"name":"NotInitializing","type":"error"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"OwnableInvalidOwner","type":"error"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"OwnableUnauthorizedAccount","type":"error"},{"inputs":[],"name":"UUPSUnauthorizedCallContext","type":"error"},{"inputs":[{"internalType":"bytes32","name":"slot","type":"bytes32"}],"name":"UUPSUnsupportedProxiableUUID","type":"error"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":true,"internalType":"address","name":"clientAddress","type":"address"},{"indexed":true,"internalType":"uint64","name":"feedbackIndex","type":"uint64"}],"name":"FeedbackRevoked","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint64","name":"version","type":"uint64"}],"name":"Initialized","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":true,"internalType":"address","name":"clientAddress","type":"address"},{"indexed":false,"internalType":"uint64","name":"feedbackIndex","type":"uint64"},{"indexed":false,"internalType":"int128","name":"value","type":"int128"},{"indexed":false,"internalType":"uint8","name":"valueDecimals","type":"uint8"},{"indexed":true,"internalType":"string","name":"indexedTag1","type":"string"},{"indexed":false,"internalType":"string","name":"tag1","type":"string"},{"indexed":false,"internalType":"string","name":"tag2","type":"string"},{"indexed":false,"internalType":"string","name":"endpoint","type":"string"},{"indexed":false,"internalType":"string","name":"feedbackURI","type":"string"},{"indexed":false,"internalType":"bytes32","name":"feedbackHash","type":"bytes32"}],"name":"NewFeedback","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":true,"internalType":"address","name":"clientAddress","type":"address"},{"indexed":false,"internalType":"uint64","name":"feedbackIndex","type":"uint64"},{"indexed":true,"internalType":"address","name":"responder","type":"address"},{"indexed":false,"internalType":"string","name":"responseURI","type":"string"},{"indexed":false,"internalType":"bytes32","name":"responseHash","type":"bytes32"}],"name":"ResponseAppended","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"implementation","type":"address"}],"name":"Upgraded","type":"event"},{"inputs":[],"name":"UPGRADE_INTERFACE_VERSION","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address","name":"clientAddress","type":"address"},{"internalType":"uint64","name":"feedbackIndex","type":"uint64"},{"internalType":"string","name":"responseURI","type":"string"},{"internalType":"bytes32","name":"responseHash","type":"bytes32"}],"name":"appendResponse","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"name":"getClients","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getIdentityRegistry","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address","name":"clientAddress","type":"address"}],"name":"getLastIndex","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address","name":"clientAddress","type":"address"},{"internalType":"uint64","name":"feedbackIndex","type":"uint64"},{"internalType":"address[]","name":"responders","type":"address[]"}],"name":"getResponseCount","outputs":[{"internalType":"uint64","name":"count","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address[]","name":"clientAddresses","type":"address[]"},{"internalType":"string","name":"tag1","type":"string"},{"internalType":"string","name":"tag2","type":"string"}],"name":"getSummary","outputs":[{"internalType":"uint64","name":"count","type":"uint64"},{"internalType":"int128","name":"summaryValue","type":"int128"},{"internalType":"uint8","name":"summaryValueDecimals","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getVersion","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"int128","name":"value","type":"int128"},{"internalType":"uint8","name":"valueDecimals","type":"uint8"},{"internalType":"string","name":"tag1","type":"string"},{"internalType":"string","name":"tag2","type":"string"},{"internalType":"string","name":"endpoint","type":"string"},{"internalType":"string","name":"feedbackURI","type":"string"},{"internalType":"bytes32","name":"feedbackHash","type":"bytes32"}],"name":"giveFeedback","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"identityRegistry_","type":"address"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"proxiableUUID","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address[]","name":"clientAddresses","type":"address[]"},{"internalType":"string","name":"tag1","type":"string"},{"internalType":"string","name":"tag2","type":"string"},{"internalType":"bool","name":"includeRevoked","type":"bool"}],"name":"readAllFeedback","outputs":[{"internalType":"address[]","name":"clients","type":"address[]"},{"internalType":"uint64[]","name":"feedbackIndexes","type":"uint64[]"},{"internalType":"int128[]","name":"values","type":"int128[]"},{"internalType":"uint8[]","name":"valueDecimals","type":"uint8[]"},{"internalType":"string[]","name":"tag1s","type":"string[]"},{"internalType":"string[]","name":"tag2s","type":"string[]"},{"internalType":"bool[]","name":"revokedStatuses","type":"bool[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address","name":"clientAddress","type":"address"},{"internalType":"uint64","name":"feedbackIndex","type":"uint64"}],"name":"readFeedback","outputs":[{"internalType":"int128","name":"value","type":"int128"},{"internalType":"uint8","name":"valueDecimals","type":"uint8"},{"internalType":"string","name":"tag1","type":"string"},{"internalType":"string","name":"tag2","type":"string"},{"internalType":"bool","name":"isRevoked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"uint64","name":"feedbackIndex","type":"uint64"}],"name":"revokeFeedback","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"upgradeToAndCall","outputs":[],"stateMutability":"payable","type":"function"}]')
        self.address_identity = "0x8004A169FB4a3325136EB29fA0ceB6D2e539a432"
        self.abi_identity = json.loads('[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[{"internalType":"address","name":"target","type":"address"}],"name":"AddressEmptyCode","type":"error"},{"inputs":[{"internalType":"address","name":"implementation","type":"address"}],"name":"ERC1967InvalidImplementation","type":"error"},{"inputs":[],"name":"ERC1967NonPayable","type":"error"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"address","name":"owner","type":"address"}],"name":"ERC721IncorrectOwner","type":"error"},{"inputs":[{"internalType":"address","name":"operator","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ERC721InsufficientApproval","type":"error"},{"inputs":[{"internalType":"address","name":"approver","type":"address"}],"name":"ERC721InvalidApprover","type":"error"},{"inputs":[{"internalType":"address","name":"operator","type":"address"}],"name":"ERC721InvalidOperator","type":"error"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"ERC721InvalidOwner","type":"error"},{"inputs":[{"internalType":"address","name":"receiver","type":"address"}],"name":"ERC721InvalidReceiver","type":"error"},{"inputs":[{"internalType":"address","name":"sender","type":"address"}],"name":"ERC721InvalidSender","type":"error"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ERC721NonexistentToken","type":"error"},{"inputs":[],"name":"FailedCall","type":"error"},{"inputs":[],"name":"InvalidInitialization","type":"error"},{"inputs":[],"name":"NotInitializing","type":"error"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"OwnableInvalidOwner","type":"error"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"OwnableUnauthorizedAccount","type":"error"},{"inputs":[],"name":"UUPSUnauthorizedCallContext","type":"error"},{"inputs":[{"internalType":"bytes32","name":"slot","type":"bytes32"}],"name":"UUPSUnsupportedProxiableUUID","type":"error"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"_fromTokenId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_toTokenId","type":"uint256"}],"name":"BatchMetadataUpdate","type":"event"},{"anonymous":false,"inputs":[],"name":"EIP712DomainChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint64","name":"version","type":"uint64"}],"name":"Initialized","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":true,"internalType":"string","name":"indexedMetadataKey","type":"string"},{"indexed":false,"internalType":"string","name":"metadataKey","type":"string"},{"indexed":false,"internalType":"bytes","name":"metadataValue","type":"bytes"}],"name":"MetadataSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"_tokenId","type":"uint256"}],"name":"MetadataUpdate","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":false,"internalType":"string","name":"agentURI","type":"string"},{"indexed":true,"internalType":"address","name":"owner","type":"address"}],"name":"Registered","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":false,"internalType":"string","name":"newURI","type":"string"},{"indexed":true,"internalType":"address","name":"updatedBy","type":"address"}],"name":"URIUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"implementation","type":"address"}],"name":"Upgraded","type":"event"},{"inputs":[],"name":"UPGRADE_INTERFACE_VERSION","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"eip712Domain","outputs":[{"internalType":"bytes1","name":"fields","type":"bytes1"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"version","type":"string"},{"internalType":"uint256","name":"chainId","type":"uint256"},{"internalType":"address","name":"verifyingContract","type":"address"},{"internalType":"bytes32","name":"salt","type":"bytes32"},{"internalType":"uint256[]","name":"extensions","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"name":"getAgentWallet","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"string","name":"metadataKey","type":"string"}],"name":"getMetadata","outputs":[{"internalType":"bytes","name":"","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getVersion","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"agentId","type":"uint256"}],"name":"isAuthorizedOrOwner","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"proxiableUUID","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"register","outputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"agentURI","type":"string"},{"components":[{"internalType":"string","name":"metadataKey","type":"string"},{"internalType":"bytes","name":"metadataValue","type":"bytes"}],"internalType":"struct IdentityRegistryUpgradeable.MetadataEntry[]","name":"metadata","type":"tuple[]"}],"name":"register","outputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"agentURI","type":"string"}],"name":"register","outputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"string","name":"newURI","type":"string"}],"name":"setAgentURI","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address","name":"newWallet","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bytes","name":"signature","type":"bytes"}],"name":"setAgentWallet","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"operator","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"string","name":"metadataKey","type":"string"},{"internalType":"bytes","name":"metadataValue","type":"bytes"}],"name":"setMetadata","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"name":"unsetAgentWallet","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"upgradeToAndCall","outputs":[],"stateMutability":"payable","type":"function"}]')


    def extract(self, extract_params: dict):
        self.all_events_implemented = ['URIUpdated', 'Registered', 'ResponseAppended', 'NewFeedback']
        chains = extract_params.get('chains', ['*'])
        events = extract_params.get('events', self.all_events_implemented)
        rpc_map = self.get_origin_keys_with_rpcs()
        
        if chains == ['*']:
            chains = rpc_map['origin_key'].unique().tolist()
        print(f"Following chains selected: {chains}")
        print(f"Following events selected: {events}")

        # all data
        df_all = pd.DataFrame()

        # get current progress from db
        df_progress = self.get_df_progress()

        # loop through all chains and rpcs and extract events
        for chain in chains:

            # get df_progress_chain
            df_progress_chain = self.get_df_progress_chain(df_progress, chain)
            print(f"Progress for {chain}: {df_progress_chain}") ###

            # get block_date_map for this chain
            block_date_map = self.db_connector.get_first_block_of_the_day_range(chain, df_progress_chain['date'].min().strftime('%Y-%m-%d'))
            print(f"Block date map for {chain}: {block_date_map}")
            
            # get a w3 instance for this chain using the rpc map
            w3 = Web3(Web3.HTTPProvider(rpc_map[rpc_map['origin_key'] == chain]['url'].values[2]))
            
            for event in events:
                if event in ['URIUpdated', 'Registered']:
                    contract_address = self.address_identity
                    contract_abi = self.abi_identity
                elif event in ['ResponseAppended', 'NewFeedback']:
                    contract_address = self.address_reputation
                    contract_abi = self.abi_reputation
                
                ad = AdapterLogs(w3, contract_abi)

                # loop through all days, extract logs, aggregate and merge into df_all
                for date in list(block_date_map)[1:]:
                    
                    if pd.Timestamp(date) >= df_progress_chain[df_progress_chain['event'] == event]['date'].values[0]:

                        # extract
                        from_block = block_date_map[date]
                        to_block = block_date_map[(pd.Timestamp(date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')] - 1
                        daily_logs = self.extract_logs(ad, contract_address, event, from_block, to_block)
                        print(f"Extracting {event} for {chain} on {date} starting from block {from_block} to {to_block}")

                        # aggregate
                        df = self.aggregate_daily_logs(daily_logs, event)
                        df['date'] = date
                        df['origin_key'] = chain

                        # merge df into df_all
                        df_all = pd.concat([df_all, df], ignore_index=True)

        return df_all     
        
    def load(self, df:pd.DataFrame, table=None):
        if table is None:
            table = 'eip8004'
        # set index
        df = df.set_index(['origin_key', 'event', 'date'])
        self.db_connector.upsert_table(table, df)
        print(f"Loaded {len(df)} rows into {table} table.")

    #-#-# helper functions #-#-#

    def extract_logs(self, ad, contract_address, event, from_block, to_block):
        data = ad.extract({
            'contract_address': contract_address,
            'from_block': from_block,
            'to_block': to_block,
            'topics': ad.get_topic0_by_event_name(event),
            'chunk_size': 1000,
            'decode': True
        })
        return data
    
    def get_df_progress(self): ####
        # get current progress for all chains and events from db
        df = self.db_connector.execute_query(f"""
            SELECT
                null AS origin_key,
                null AS event,
                null AS date
            FROM public.eip8004
            GROUP BY 1,2,3
        """, load_df=True)
        return df
    
    def get_df_progress_chain(self, df_progress, chain):
        # get current progress for a specific chain, plus add new events if they don't exist yet
        df_chain = df_progress[df_progress['origin_key'] == chain]
        for event in self.all_events_implemented:
            if event not in df_chain['event'].values:
                df_chain = pd.concat([df_chain, pd.DataFrame({
                    'origin_key': [chain],
                    'event': [event],
                    'date': [pd.Timestamp(2026,2,13)]#[pd.Timestamp(2026,1,29)] # first time EIP8004 was deployed
                })], ignore_index=True)
        return df_chain

    def get_origin_keys_with_rpcs(self):
        # get all origin keys to pull in with all rpcs we have available
        df = self.db_connector.execute_query(f"""
            SELECT
                rpc.url,
                rpc.origin_key
            FROM public.sys_rpc_config rpc
            INNER JOIN public.sys_main_conf mc ON rpc.origin_key = mc.origin_key
            WHERE 
                mc.api_deployment_flag = 'PROD' 
                AND mc.chain_type IN ('L2', 'L1') 
                AND mc.evm_chain_id IS NOT NULL
        """,
        load_df=True)
        return df

    import pandas as pd

    def aggregate_daily_logs(self, daily_logs, event):
        """
        Aggregate daily logs by event type and return a DataFrame.
        
        Args:
            daily_logs: List of event data
            event: Event type ('NewFeedback', 'ResponseAppended', 'Registered', 'URIUpdated')
        
        Returns:
            pd.DataFrame with columns: event, count, detail
        """
        if event == 'NewFeedback':
            agents = {}
            for log in daily_logs:
                agent_id = log['args']['agentId']
                
                if agent_id not in agents:
                    agents[agent_id] = []
                
                agents[agent_id].append({
                    'feedbackIndex': log['args']['feedbackIndex'],
                    'rating': log['args']['value'],
                    'client': log['args']['clientAddress'],
                    'uri': log['args']['feedbackURI'],
                    'hash': log['transactionHash'].hex(),
                    'tag1': log['args']['tag1'],
                    'tag2': log['args']['tag2']
                })
            
            count = len(daily_logs)
            detail = agents
            
        elif event == 'ResponseAppended':
            agents = {}
            for log in daily_logs:
                agent_id = log['args']['agentId']
                
                if agent_id not in agents:
                    agents[agent_id] = []
                
                agents[agent_id].append({
                    'feedbackIndex': log['args']['feedbackIndex'],
                    'client': log['args']['clientAddress'],
                    'responder': log['args']['responder'],
                    'uri': log['args']['responseURI'],
                    'hash': log['transactionHash'].hex()
                })
            
            count = len(daily_logs)
            detail = agents
            
        elif event == 'Registered':
            agents = {}
            for log in daily_logs:
                agent_id = log['args']['agentId']
                
                if agent_id not in agents:
                    agents[agent_id] = []
                
                agents[agent_id].append({
                    'owner': log['args']['owner'],
                    'uri': log['args']['agentURI'],
                    'hash': log['transactionHash'].hex()
                })
            
            count = len(daily_logs)
            detail = agents
            
        elif event == 'URIUpdated':
            agents = {}
            for log in daily_logs:
                agent_id = log['args']['agentId']
                
                if agent_id not in agents:
                    agents[agent_id] = []
                
                agents[agent_id].append({
                    'updatedBy': log['args']['updatedBy'],
                    'uri': log['args']['newURI'],
                    'hash': log['transactionHash'].hex()
                })
            
            count = len(daily_logs)
            detail = agents
        
        else:
            raise ValueError(f"Unknown event type: {event}")
        
        # Create DataFrame
        df = pd.DataFrame([{
            'event': event,
            'count': count,
            'detail': detail
        }])
        
        return df
                