from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

NUM_WORKERS = 5  # number of parallel Airflow tasks

@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 1,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_indexer_ipfs',
    description='Uploads offchain OLI attestations to IPFS and updates the database with the IPFS hashes',
    tags=['oli'],
    start_date=datetime(2025, 10, 27),
    schedule='*/30 * * * *',  # Runs every 30 minutes
    catchup=False  # Prevents backfilling
)
def main():

    @task()
    def get_pending_chunks():
        import math
        from src.db_connector import DbConnector

        db_connector = DbConnector(db_name='oli')
        query = """
            SELECT uid, table_name
            FROM (
                SELECT uid, time, 'attestations' AS table_name
                FROM public.attestations
                WHERE is_offchain = true AND (ipfs_hash IS NULL OR ipfs_hash = '')
                UNION ALL
                SELECT uid, time, 'trust_lists' AS table_name
                FROM trust_lists
                WHERE is_offchain = true AND (ipfs_hash IS NULL OR ipfs_hash = '')
            ) AS combined_tables
            ORDER BY time
        """
        df = db_connector.execute_query(query, load_df=True)
        if df.empty:
            return [[] for _ in range(NUM_WORKERS)]

        df['uid'] = df['uid'].apply(lambda x: '0x' + x.hex())
        records = df[['uid', 'table_name']].to_dict('records')

        chunk_size = math.ceil(len(records) / NUM_WORKERS)
        chunks = [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
        # Pad to NUM_WORKERS so expand always gets the same number of tasks
        while len(chunks) < NUM_WORKERS:
            chunks.append([])

        print(f'Total pending: {len(records)} records split into {len(chunks)} chunks')
        return chunks

    @task()
    def process_chunk(chunk):
        if not chunk:
            print('No records to process in this chunk.')
            return

        import concurrent.futures
        from src.db_connector import DbConnector
        from src.misc.helper_functions import upload_json_to_filebase_ipfs

        db_connector = DbConnector(db_name='oli')

        # Group UIDs by table so we can fetch full data in two queries
        uids_by_table: dict[str, list[str]] = {}
        for record in chunk:
            uids_by_table.setdefault(record['table_name'], []).append(record['uid'])

        # Re-fetch full row data (raw, schema_info) for this chunk only
        rows_to_process = []
        for table_name, uids in uids_by_table.items():
            uid_array = ', '.join(f"decode('{uid[2:]}', 'hex')" for uid in uids)
            query = f"""
                SELECT uid, raw, schema_info, '{table_name}' AS table_name
                FROM public.{table_name}
                WHERE uid = ANY(ARRAY[{uid_array}]::bytea[])
            """
            df = db_connector.execute_query(query, load_df=True)
            df['uid'] = df['uid'].apply(lambda x: '0x' + x.hex())
            rows_to_process.extend(df.to_dict('records'))

        # Upload all files in this chunk concurrently
        bucket = 'oli-offchain-attestations'

        def upload_row(row):
            path_name = f'{row["table_name"]}/{row["schema_info"]}/{row["uid"]}.json'
            cid = upload_json_to_filebase_ipfs(bucket, path_name, row['raw'])
            print(f'Uploaded UID {row["uid"]} to IPFS with CID: {cid}')
            return {'uid': row['uid'], 'cid': cid, 'table_name': row['table_name']}

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(upload_row, row): row for row in rows_to_process}
            for future in concurrent.futures.as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    print(f'Error uploading: {e}')

        # Batch update DB grouped by table
        if results:
            results_by_table: dict[str, list] = {}
            for result in results:
                results_by_table.setdefault(result['table_name'], []).append(result)

            for table_name, table_results in results_by_table.items():
                values_list = []
                for result in table_results:
                    uid_hex = result['uid'][2:] if result['uid'].startswith('0x') else result['uid']
                    values_list.append(f"(decode('{uid_hex}', 'hex'), '{result['cid']}')")

                values_str = ',\n        '.join(values_list)

                query_update = f"""
                    UPDATE public.{table_name} AS t
                    SET
                        ipfs_hash = v.cid,
                        last_updated_time = NOW()
                    FROM (VALUES
                        {values_str}
                    ) AS v(uid, cid)
                    WHERE t.uid = v.uid;
                """
                db_connector.execute_query(query_update)
                print(f'Updated {len(table_results)} rows in table {table_name}')

    chunks = get_pending_chunks()
    process_chunk.expand(chunk=chunks)

main()
