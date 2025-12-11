from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.db_connector import DbConnector
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        "owner": "lorenz",
        "retries": 1,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="other_qb_fusaka",
    description="Quick Bite on Fusaka + blob_base_fee backfilling",
    tags=["other"],
    start_date=datetime(2025, 12, 11),
    schedule="17 0 * * *",  # Every day at 00:17
)
def run_dag():

    @task
    def backfill_blob_base_fee():
        """
        Backfills target_blob_gas, blob_base_fee_update_fraction & blob_base_fee in public.ethereum_blocks table
        """
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        from_block = db_connector.execute_query("SELECT MIN(number) FROM public.ethereum_blocks WHERE blob_base_fee IS NULL;", load_df=True).iloc[0,0]
        to_block = db_connector.execute_query("SELECT MAX(number) FROM public.ethereum_blocks;", load_df=True).iloc[0,0]
        query = f"""
            WITH BlockCalculations AS (
                SELECT
                    t.number,
                    t.excess_blob_gas,
                    
                    -- 1. Calculate Target Blob Gas (Value to be updated)
                    CASE
                        WHEN t.number >= 23975778 THEN 10 * 128 * 1024 -- BPO1 Fusaka (10 blobs)
                        WHEN t.number >= 22431084 THEN 6 * 128 * 1024  -- Pectra & Fusaka (6 blobs)
                        ELSE 3 * 128 * 1024                           -- Dencun (3 blobs of 128kib)
                    END AS calculated_target_blob_gas,
                    
                    -- 2. Calculate Blob Base Fee Update Fraction (Value to be updated, also reused in next step)
                    CASE
                        WHEN t.number >= 23975778 THEN 8346193 -- BPO1 Fusaka
                        WHEN t.number >= 22431084 THEN 5007716 -- Pectra & Fusaka
                        ELSE 3338477                           -- Dencun
                    END AS calculated_blob_base_fee_update_fraction
                    
                FROM 
                    public.ethereum_blocks AS t
                WHERE 
                    t.number BETWEEN {from_block} AND {to_block} -- Restrict to the required range
            )
            UPDATE public.ethereum_blocks AS t_update
            SET
                target_blob_gas = c.calculated_target_blob_gas,
                blob_base_fee_update_fraction = c.calculated_blob_base_fee_update_fraction,
                blob_base_fee = 
                    -- Calculate the base fee (in Wei) using the exponential formula and TRUNCATION (cut-off)
                    (
                        CAST(
                            TRUNC( -- Applying TRUNC() as requested by the final SELECT query
                                EXP(
                                    CAST(c.excess_blob_gas AS NUMERIC) / 
                                    CAST(c.calculated_blob_base_fee_update_fraction AS NUMERIC)
                                )
                            )
                        AS BIGINT)
                    )
            FROM 
                BlockCalculations AS c
            WHERE 
                t_update.number = c.number;
        """
        db_connector.execute_query(query)
        print(f"Updated blob_base_fee, target_blob_gas, and blob_base_fee_update_fraction for blocks {from_block} to {to_block}.")


    backfill_data = backfill_blob_base_fee()


run_dag()