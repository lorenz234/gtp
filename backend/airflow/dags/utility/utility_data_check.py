from datetime import datetime, timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook, claude_fix_on_failure

@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 1,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': [alert_via_webhook, claude_fix_on_failure]
    },
    dag_id='utility_data_check',
    description='Checks data correctness across key metrics and alerts when anomalies are detected (e.g. negative deltas, missing mappings, inconsistent values).',
    tags=['utility', 'data-quality'],
    start_date=datetime(2026, 3, 27),
    schedule='0 14 * * *'  # Run daily at 2:00 PM
)

def etl():
    @task()
    def check_tvl_stables_mcap():
        from src.db_connector import DbConnector
        from src.misc.helper_functions import send_discord_message
        import os

        query = """
            SELECT
                origin_key,
                MAX(CASE WHEN metric_key = 'tvl' THEN value END) - MAX(CASE WHEN metric_key = 'stables_mcap' THEN value END) AS delta,
                MAX(CASE WHEN metric_key = 'tvl' THEN value END) / NULLIF(MAX(CASE WHEN metric_key = 'stables_mcap' THEN value END), 0) AS difference
            FROM public.fact_kpis
            WHERE
                metric_key IN ('stables_mcap', 'tvl')
                AND date = CURRENT_DATE - INTERVAL '1 day'
                AND origin_key != 'ethereum_ecosystem'
            GROUP BY origin_key
            ORDER BY 2 ASC
        """

        db_connector = DbConnector()
        df = db_connector.execute_query(query, load_df=True)

        negative = df[df['delta'] < 0]
        if negative.empty:
            print("All TVL >= stables_mcap. No issues detected.")
            return

        lines = "\n".join(
            f"- **{row.origin_key}**: stables exceed TVL by ${abs(row.delta):,.0f} (TVL covers only {row.difference * 100:.0f}% of stables mcap)"
            for _, row in negative.iterrows()
        )
        message = (
            f"⚠️ **Stables MCap > TVL** on the following chains — stablecoins are likely missing from the L2Beat TVL mapping. "
            f"Please add the missing stablecoins to L2Beat:\n{lines}"
        )
        send_discord_message(message, os.getenv('DISCORD_ALERTS'))

    check_tvl_stables_mcap()

etl()
