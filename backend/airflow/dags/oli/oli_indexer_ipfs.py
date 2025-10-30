import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

# Define the DAG and task using decorators
@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='lorenz')
    },
    dag_id='oli_indexer_ipfs',
    description='Uploads offchain OLI attestations to IPFS and updates the database with the IPFS hashes',
    tags=['indexing', 'oli', 'raw'],
    start_date=datetime(2025, 10, 27),
    schedule='*/30 * * * *',  # Runs every 30 minutes
    catchup=False  # Prevents backfilling
)

def main():

    @task()
    def sync():

        import time
        from src.db_connector import DbConnector
        from src.misc.helper_functions import upload_json_to_filebase_ipfs
        import concurrent.futures

        db_connector = DbConnector(db_name='oli')

        while True:

            ## track time ##
            start_time = time.time()

            ## fetch data to upload ##
            query_select = """
                SELECT uid, time, is_offchain, raw, schema_info
                FROM public.attestations
                WHERE
                    is_offchain = true
                    AND (ipfs_hash IS NULL OR ipfs_hash = '')
                ORDER BY 2 
                LIMIT 250
            """
            df = db_connector.execute_query(query_select, load_df=True)
            df['uid'] = df['uid'].apply(lambda x: '0x' + x.hex())

            ## pause if we have no more data to process ##
            if df.empty:
                time.sleep(30)
                continue

            ## upload to IPFS ##
            bucket = 'oli-offchain-attestations'
            table_name = 'attestations'

            # Function to upload a single row
            def upload_row(row):
                path_name = f'attestations/{row["schema_info"]}/{row["uid"]}.json'
                cid = upload_json_to_filebase_ipfs(bucket, path_name, row['raw'])
                print(f'Uploaded UID {row["uid"]}... to IPFS with CID: {cid}')
                return {
                    'uid': row['uid'],
                    'cid': cid
                }

            # Upload all files concurrently with 10 workers
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(upload_row, row) for _, row in df.iterrows()]
                
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        print(f'Error uploading: {e}')

            # Batch update all rows in a single query
            if results:
                values_list = []
                for result in results:
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
                print(f'Updated {len(results)} rows in database')

            ## break the loop if time reached ##
            if start_time + (29 * 60) < time.time():  # 29 minutes
                print("⏰ Approaching task timeout, exiting loop to allow for graceful restart.")
                break

            time.sleep(1)

    sync()

main()
