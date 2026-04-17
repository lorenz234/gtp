from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook, claude_fix_on_failure


@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': [alert_via_webhook, claude_fix_on_failure]
    },
    dag_id='other_qb_sourcify',
    description='Quick-bite around Sourcify contract verification data across chains.',
    tags=['other', 'weekly'],
    start_date=datetime(2026, 4, 1),
    schedule='0 9 * * 2'  # Every Tuesday at 09:00 UTC
)
def run_dag():

    @task()
    def main():
        import pandas as pd
        from src.db_connector import DbConnector

        db_connector = DbConnector()

        config = db_connector.get_table("sys_main_conf")
        config = config[config['chain_type'].isin(['L1', 'L2'])]
        config = config[config['api_deployment_flag'] == 'PROD']
        df_chains = config[['origin_key', 'name']]

        QUERY = """
            WITH sourcify_attestations AS (
                SELECT DISTINCT ON (chain_id, recipient)
                    chain_id,
                    recipient,
                    tags_json->>'code_compiler' AS code_compiler,
                    tags_json->>'code_language' AS code_language
                FROM attestations
                WHERE
                    attester = decode('8DBAE854E53AEDFC3B8D78F7C1ADD53935939192', 'hex')
                    AND revoked = false
                ORDER BY chain_id, recipient, time DESC
            ),
            sourcify_data AS (
                SELECT
                    mc.origin_key,
                    decode(substring(a.recipient, 3), 'hex') AS address,
                    a.code_language,
                    -- Extract major.minor version from e.g. "solc-0.7.6+commit.7338295f" or "vyper-0.3.1+..."
                    substring(a.code_compiler FROM '[a-z]+-(\d+\.\d+)\.\d+') AS code_version
                FROM sourcify_attestations a
                INNER JOIN sys_main_conf mc ON a.chain_id = mc.caip2
            )
            SELECT
                cl.origin_key,
                date_trunc('week', cl."date") AS week,
                --cl."date",
                s.code_language,
                s.code_version,
                SUM(cl.gas_fees_eth)  AS total_gas_fees_eth,
                SUM(cl.gas_fees_usd)  AS total_gas_fees_usd,
                SUM(cl.txcount)       AS total_txcount
            FROM blockspace_fact_contract_level cl
            LEFT JOIN sourcify_data s ON cl.address = s.address AND s.origin_key = cl.origin_key
            GROUP BY 1, 2, 3, 4
        """

        import os
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan

        df = pd.read_sql(QUERY, db_connector.engine)
        print(f"Fetched {len(df)} rows from sourcify query.")

        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        METRICS = ['total_gas_fees_eth', 'total_gas_fees_usd', 'total_txcount']

        def pivot_to_dict_per_metric(df_pivot):
            """Return dict of {metric: list_of_records} with simplified key names and unix ms date."""
            result = {}
            for metric in METRICS:
                df_m = df_pivot[metric].copy()
                df_m.columns = [str(c) for c in df_m.columns]
                df_m = df_m.reset_index()
                df_m['week'] = (pd.to_datetime(df_m['week']).astype('int64') // 10**6)
                df_m = df_m.rename(columns={'week': 'date'})
                result[metric] = df_m.to_dict(orient='records')
            return result

        def upload_weekly(df_pivot, s3_prefix, label):
            for metric, records in pivot_to_dict_per_metric(df_pivot).items():
                payload = fix_dict_nan({"data": records}, f'{label}_{metric}', send_notification=False)
                upload_json_to_cf_s3(s3_bucket, f'{s3_prefix}_{metric}', payload, cf_distribution_id, invalidate=False)

        chains = df['origin_key'].unique().tolist()

        for chain in chains:
            df_chain = df[df['origin_key'] == chain]

            # --- compiler (code_language) breakdown ---
            df_grouped = df_chain.groupby(['code_language', 'week']).agg({
                'total_gas_fees_eth': 'sum',
                'total_gas_fees_usd': 'sum',
                'total_txcount': 'sum'
            }).reset_index()
            df_total = df_chain.groupby('code_language').agg({
                'total_gas_fees_eth': 'sum',
                'total_gas_fees_usd': 'sum',
                'total_txcount': 'sum'
            }).reset_index()

            df_grouped_pivot = df_grouped.pivot(index='week', columns='code_language', values=['total_gas_fees_eth', 'total_gas_fees_usd', 'total_txcount'])

            compiler_total = {"data": df_total.to_dict(orient='records')}

            upload_weekly(df_grouped_pivot, f'v1/quick-bites/sourcify/{chain}_compiler_weekly', f'{chain}_compiler_weekly')
            upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/sourcify/{chain}_compiler_total', fix_dict_nan(compiler_total, f'{chain}_compiler_total', send_notification=False), cf_distribution_id, invalidate=False)
            print(f"[{chain}] Uploaded compiler weekly & total.")

            # --- Solidity version breakdown ---
            df_chain_sol = df_chain[(df_chain['code_language'] == 'solidity') & (df_chain['code_version'].notna())]
            if not df_chain_sol.empty:
                df_sol_grouped = df_chain_sol.groupby(['code_version', 'week']).agg({
                    'total_gas_fees_eth': 'sum',
                    'total_gas_fees_usd': 'sum',
                    'total_txcount': 'sum'
                }).reset_index()
                df_sol_total = df_chain_sol.groupby('code_version').agg({
                    'total_gas_fees_eth': 'sum',
                    'total_gas_fees_usd': 'sum',
                    'total_txcount': 'sum'
                }).reset_index()

                df_sol_pivot = df_sol_grouped.pivot(index='week', columns='code_version', values=['total_gas_fees_eth', 'total_gas_fees_usd', 'total_txcount'])

                sol_total = {"data": df_sol_total.to_dict(orient='records')}

                upload_weekly(df_sol_pivot, f'v1/quick-bites/sourcify/{chain}_solidity_version_weekly', f'{chain}_solidity_version_weekly')
                upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/sourcify/{chain}_solidity_version_total', fix_dict_nan(sol_total, f'{chain}_solidity_version_total', send_notification=False), cf_distribution_id, invalidate=False)
                print(f"[{chain}] Uploaded solidity version weekly & total.")

            # --- Vyper version breakdown ---
            df_chain_vyper = df_chain[(df_chain['code_language'] == 'vyper') & (df_chain['code_version'].notna())]
            if not df_chain_vyper.empty:
                df_vyper_grouped = df_chain_vyper.groupby(['code_version', 'week']).agg({
                    'total_gas_fees_eth': 'sum',
                    'total_gas_fees_usd': 'sum',
                    'total_txcount': 'sum'
                }).reset_index()
                df_vyper_total = df_chain_vyper.groupby('code_version').agg({
                    'total_gas_fees_eth': 'sum',
                    'total_gas_fees_usd': 'sum',
                    'total_txcount': 'sum'
                }).reset_index()

                df_vyper_pivot = df_vyper_grouped.pivot(index='week', columns='code_version', values=['total_gas_fees_eth', 'total_gas_fees_usd', 'total_txcount'])

                vyper_total = {"data": df_vyper_total.to_dict(orient='records')}

                upload_weekly(df_vyper_pivot, f'v1/quick-bites/sourcify/{chain}_vyper_version_weekly', f'{chain}_vyper_version_weekly')
                upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/sourcify/{chain}_vyper_version_total', fix_dict_nan(vyper_total, f'{chain}_vyper_version_total', send_notification=False), cf_distribution_id, invalidate=False)
                print(f"[{chain}] Uploaded vyper version weekly & total.")

        ## chain dropdown (only PROD L1/L2 chains)
        ticker_name_list = df_chains[df_chains['origin_key'].isin(df['origin_key'].unique())][['origin_key', 'name']].to_dict(orient='records')
        dict_dropdown = {"dropdown_values": ticker_name_list}
        dict_dropdown = fix_dict_nan(dict_dropdown, 'sourcify_chain_dropdown', send_notification=False)
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/sourcify/dropdown-chains', dict_dropdown, cf_distribution_id, invalidate=False)

        from src.misc.helper_functions import empty_cloudfront_cache
        empty_cloudfront_cache(cf_distribution_id, '/v1/quick-bites/sourcify/*')
        print("CloudFront cache invalidated for /v1/quick-bites/sourcify/*")


    main()


run_dag()
