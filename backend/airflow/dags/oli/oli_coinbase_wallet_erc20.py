from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'lorenz',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_coinbase_wallet_erc20',
    description='Attest ERC20 tokens from Coinbase Wallet EAS schema to OLI label pool',
    tags=['oli', 'daily'],
    start_date=datetime(2025,6,6),
    schedule='13 01 * * *'
)

def etl():
    @task()
    def reattest():
        from oli import OLI
        import pandas as pd
        import os

        # attest from OLI_coinbase_wallet_pk wallet
        oli = OLI(private_key=os.getenv("OLI_coinbase_wallet_pk"))
        # check where we left off with, last attestations to OLI Label Pool from OLI_coinbase_wallet_pk
        j_latest_OLI_Label_Pool = oli.graphql_query_attestations(attester=oli.address, take=1)

        # set schema to Coinbase wallet schema, to get new attestations from that schema (revocable: False)
        oli.oli_label_pool_schema = "0x1134b93c315c222968305b0467339b4fe8fc42c4646c4d4fce5d89e506c5aa6c"
        # extract timeCreated of last attestation we have reattested to OLI Label Pool in Coinbase wallet schema
        ref_id = j_latest_OLI_Label_Pool["data"]["attestations"][0]["refUID"]
        timeCreated = oli.graphql_query_attestations(id=ref_id, take=1)["data"]["attestations"][0]["timeCreated"]
        # get all new attestations to attest
        j = oli.graphql_query_attestations(take=1000, timeCreated=timeCreated)

        # switch back to OLI Label Pool schema and start attesting
        oli.oli_label_pool_schema = "0xb763e62d940bed6f527dd82418e146a904e62a297b8fa765c9b3e1f0bc6fdd68"
        print(f"attesting from EOA: {oli.address} to OLI Label Pool schema: {oli.oli_label_pool_schema}")
        # iterate over the attestations in reverse and attest them to the OLI Label Pool
        for attestation in reversed(j['data']['attestations']):
            refUID = attestation['id']
            chain_id = None
            address = None
            tag_ids = {}
            
            # assign chain_id and address based on the attestation data
            if attestation['chainId'] != '':
                chain_id = 'eip155:' + str(attestation['chainId'])
            if attestation['contractAddress'] != '0x0000000000000000000000000000000000000000':    
                address = attestation['contractAddress']
            
            # fill tag_ids with the attestation data
            if attestation['name'] != '':
                tag_ids['erc20.name'] = attestation['name']
            if attestation['symbol'] != '':
                tag_ids['erc20.symbol'] = attestation['symbol']
            tag_ids['erc_type'] = ['erc20']

            # attest label is sufficient fields are filled
            if chain_id is not None and address is not None and len(tag_ids) > 1:
                oli.submit_offchain_label(
                    address=address,
                    chain_id=chain_id,
                    tags=tag_ids,
                    ref_uid=refUID,
                    retry=5
                )
                print(f"Successfully attested label for {address} on {chain_id}: {tag_ids}")
    
    reattest()

etl()