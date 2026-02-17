from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        "owner": "lorenz",
        "retries": 1,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": alert_via_webhook,
    },
    dag_id="other_qb_eip8004",
    description="Quick Bite on EIP-8004 + backfilling of raw tables for EIP-8004",
    tags=["other"],
    start_date=datetime(2026, 2, 13),
    schedule="41 1 * * *",  # Every day at 01:41
)
def run_dag():

    @task
    def scrape_eip8004_events():
        from src.adapters.adapter_eip8004 import EIP8004Adapter
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        ad = EIP8004Adapter({}, db_connector)
        df = ad.extract({
            'chains': ['*'],
            'events': ['*']
        })
        df_uri = ad.extract_uri({
            'chains': ['*'],
            'df': df # alternatively: 'days_back': 3, scrapes metadata behind all URIs from the last 3 days
        })
        ad.load(df)
        ad.load_uri(df_uri)

    @task
    def create_jsons_for_quick_bite():
        from src.db_connector import DbConnector
        db_connector = DbConnector()
        
        # json 1: feedback count, average rating, and unique clients per agent
        query = f"""
            SELECT
                origin_key,
                agent_id,
                COUNT(*) AS feedback_count_all,
                COUNT(*) FILTER (
                    WHERE agent_details->>'rating' ~ '^[0-9]+\.?[0-9]*$'
                    AND (agent_details->>'rating')::numeric BETWEEN 0 AND 100
                ) AS feedback_count_valid,
                ROUND(AVG((agent_details->>'rating')::numeric) FILTER (
                    WHERE agent_details->>'rating' ~ '^[0-9]+\.?[0-9]*$'
                    AND (agent_details->>'rating')::numeric BETWEEN 0 AND 100
                ), 2) AS avg_rating,
                COUNT(DISTINCT agent_details->>'client') AS unique_clients
            FROM public.vw_eip8004_agents
            WHERE
                event = 'NewFeedback'
                AND agent_details ? 'rating'
            GROUP BY origin_key, agent_id
            ORDER BY unique_clients DESC;
        """
        df = db_connector.execute_query(query, fetch=True)

        # json 2 - event count per origin key
        query = f"""
            SELECT 
                origin_key, 
                event, 
                COUNT(*) AS event_count
            FROM public.vw_eip8004_agents
            GROUP BY origin_key, event
            ORDER BY event_count DESC;
        """
        df_events = db_connector.execute_query(query, fetch=True)

        # json 3 - cumulative count of 'Registered' events per origin key over time
        query = f"""
            SELECT
                origin_key,
                date,
                COUNT(*) AS event_count,
                SUM(COUNT(*)) OVER (PARTITION BY origin_key ORDER BY date) AS cumulative_sum
            FROM public.vw_eip8004_agents
            WHERE event = 'Registered'
            GROUP BY origin_key, date
            ORDER BY date DESC, origin_key;
        """
        df_registered = db_connector.execute_query(query, fetch=True)

        # json 4 - kpis for events count
        query = f"""
            SELECT 
                event, 
                COUNT(*) AS event_count
            FROM public.vw_eip8004_agents
            GROUP BY event
            ORDER BY event_count DESC;
        """
        df_kpis = db_connector.execute_query(query, fetch=True)

        # json 5 - kpi for unique owners
        query = f"""
            SELECT 
                COUNT(DISTINCT agent_details->>'owner') AS unique_owners
            FROM public.vw_eip8004_agents
            WHERE event = 'Registered';
        """
        df_owners = db_connector.execute_query(query, fetch=True)

        # json 6 

run_dag()