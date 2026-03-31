from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        "owner": "lorenz",
        "retries": 1,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="other_qb_pol_burn",
    description="Quick Bite on Polygon POL burn (Dune query 6926431)",
    tags=["other"],
    start_date=datetime(2026, 3, 30),
    schedule="15 4 * * *",  # Every day at 4:15
)
def run_dag():

    @task
    def create_jsons():
        """
        Pull POL burn data from Dune (query 6926431), join with gas_per_second from
        fact_kpis, and upload as JSON to S3/CloudFront.

        Columns from Dune:
            day                     - date of the record
            total_base_polygon      - daily POL burned
            cumulative_base_polygon - cumulative POL burned

        Joined from DB (left join on day):
            gas_per_second          - polygon_pos gas per second
        """
        import os
        import pandas as pd
        from src.adapters.adapter_dune import AdapterDune
        from src.db_connector import DbConnector
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan, empty_cloudfront_cache

        db_connector = DbConnector()
        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        adapter_params = {
            "api_key": os.getenv("DUNE_API")
        }
        ad = AdapterDune(adapter_params, db_connector)

        load_params = {
            "queries": [
                {
                    "name": "POL_burn",
                    "query_id": 6926431,
                }
            ]
            # no prepare_df / load_type — we handle the DataFrame ourselves
        }
        df = ad.extract(load_params)

        # Normalise column names to lowercase
        df.columns = [c.lower() for c in df.columns]

        # Parse date and convert to unix timestamp in milliseconds
        df["day"] = pd.to_datetime(df["day"], utc=True)
        df["unix_timestamp"] = (
            (df["day"] - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta("1ms")
        ).astype("int64")

        # Pull gas_per_second from fact_kpis
        df_gas = db_connector.execute_query(
            """
            SELECT "date", value AS gas_per_second
            FROM public.fact_kpis
            WHERE origin_key = 'polygon_pos'
              AND metric_key = 'gas_per_second'
            """,
            load_df=True,
        )
        df_gas["date"] = pd.to_datetime(df_gas["date"], utc=True)

        # Left join Dune data with gas_per_second on day
        df = df.merge(df_gas, left_on="day", right_on="date", how="left")
        df = df.drop(columns=["date"])

        # Drop rows where Dune returned '<nil>' for the burn columns
        df = df[df["total_base_polygon"] != "<nil>"].reset_index(drop=True)

        # Sort chronologically
        df = df.sort_values("day").reset_index(drop=True)

        values = [
            [
                int(row["unix_timestamp"]),
                float(row["total_base_polygon"]),
                float(row["cumulative_base_polygon"]),
                None if pd.isna(row["gas_per_second"]) else float(row["gas_per_second"]),
            ]
            for _, row in df.iterrows()
        ]

        data_dict = {
            "data": {
                "daily": {
                    "types": [
                        "unix",
                        "total_base_polygon",
                        "cumulative_base_polygon",
                        "gas_per_second",
                    ],
                    "values": values,
                }
            }
        }

        data_dict = fix_dict_nan(data_dict, "pol_burn")
        upload_json_to_cf_s3(
            s3_bucket,
            "v1/quick-bites/pol-burn/timeseries",
            data_dict,
            cf_distribution_id,
            invalidate=False,
        )

        empty_cloudfront_cache(cf_distribution_id, "/v1/quick-bites/pol-burn/*")

    create_jsons()

run_dag()
