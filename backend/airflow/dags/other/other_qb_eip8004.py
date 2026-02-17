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
    def get_eip8004_events():
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
        
        # json 1: table, top200 AI agents with metadata based on feedback count (gamable metric... no way around this)
        query = f"""
            WITH feedback AS (
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
            ),
            uri_safe AS (
                SELECT
                    origin_key,
                    agent_id,
                    uri_json,
                    CASE WHEN jsonb_typeof(uri_json->'services') = 'array'
                        THEN uri_json->'services'
                        ELSE '[]'::jsonb
                    END AS services
                FROM public.eip8004_uri
            )
            SELECT
                v.origin_key,
                v.agent_id,
                u.uri_json->>'name' AS name,
                u.uri_json->>'image' AS image,
                u.uri_json->>'description' AS description,
                (u.uri_json->>'x402Support')::boolean AS x402_support,
                (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem WHERE elem->>'name' = 'web' LIMIT 1) AS service_web_endpoint,
                (SELECT elem->>'endpoint' FROM jsonb_array_elements(u.services) elem WHERE elem->>'name' = 'MCP' LIMIT 1) AS service_mcp_endpoint,
                v.feedback_count_all,
                v.feedback_count_valid,
                v.avg_rating,
                v.unique_clients
            FROM feedback v
            LEFT JOIN uri_safe u
                ON v.origin_key = u.origin_key
                AND v.agent_id = u.agent_id
            ORDER BY v.unique_clients desc
            limit 200;
        """
        df = db_connector.execute_query(query, fetch=True)

        # json 2 - chart, cumulative sum of total count 'Registered' and 'NewFeedback' events per day
        query = f"""
            SELECT
                date,
                event,
                COUNT(*) AS daily_count,
                (SUM(COUNT(*)) OVER (PARTITION BY event ORDER BY date))::integer AS cumulative_sum
            FROM public.vw_eip8004_agents
            WHERE event IN ('Registered', 'NewFeedback')
            GROUP BY date, event
            ORDER BY date, event;
        """
        df_cumulative = db_connector.execute_query(query, fetch=True)

        # json 3 - table, breakdown by origin_key
        query = f"""
        SELECT 
            v.origin_key,
            MIN(v.date) FILTER (WHERE v.event = 'Registered') AS first_registered_date,
            COUNT(*) FILTER (WHERE v.event = 'Registered') AS total_registered,
            u.valid_registrations,
            COUNT(*) FILTER (WHERE v.event = 'NewFeedback') AS total_feedback,
            COUNT(DISTINCT v.agent_details->>'owner') FILTER (WHERE v.event = 'Registered') AS unique_owners,
            CASE 
                WHEN COUNT(DISTINCT v.agent_details->>'owner') FILTER (WHERE v.event = 'Registered') > 0 
                THEN (ROUND(
                    COUNT(*) FILTER (WHERE v.event = 'Registered')::numeric / 
                    COUNT(DISTINCT v.agent_details->>'owner') FILTER (WHERE v.event = 'Registered')::numeric, 
                    2
                ))::float
                ELSE NULL
            END AS agents_per_owner
        FROM public.vw_eip8004_agents v
        LEFT JOIN (
            SELECT origin_key, COUNT(*) AS valid_registrations
            FROM public.eip8004_uri
            WHERE uri_json != '{}'
            GROUP BY origin_key
        ) u ON v.origin_key = u.origin_key
        GROUP BY v.origin_key, u.valid_registrations
        ORDER BY total_registered DESC;
        """

        # json 3 chart, cumulative count of 'Registered' events per origin key over time
        query = f"""
            SELECT
                origin_key,
                date,
                COUNT(*) AS registered_count,
                SUM(COUNT(*)) OVER (PARTITION BY origin_key ORDER BY date) AS registered_cum_sum
            FROM public.vw_eip8004_agents
            WHERE event = 'Registered'
            GROUP BY origin_key, date
            ORDER BY date DESC, origin_key;
        """
        df_registered = db_connector.execute_query(query, fetch=True)

        # json 4 kpis, total agents, unique owners, agents with feedback
        query = f"""
            SELECT 'total_agents' AS kpi, COUNT(*) AS value
            FROM public.vw_eip8004_agents
            WHERE event = 'Registered'

            UNION ALL

            SELECT 'unique_owners' AS kpi, COUNT(DISTINCT agent_details->>'owner') AS value
            FROM public.vw_eip8004_agents
            WHERE event = 'Registered'

            UNION ALL

            SELECT 'agents_with_feedback' AS kpi, COUNT(*) AS value
            FROM (
                SELECT origin_key, agent_id
                FROM public.vw_eip8004_agents
                WHERE event = 'NewFeedback'
                GROUP BY 1, 2
            ) sub;
        """
        df_kpis = db_connector.execute_query(query, fetch=True)

        # json 5 chart, percentage of agents with invalid URI metadata
        query = f"""
            SELECT 
                v.date,
                CASE WHEN u.uri_json = '{}' OR u.uri_json IS NULL THEN 'invalid_uri' ELSE 'valid_uri' END AS status,
                COUNT(*) AS value
            FROM public.vw_eip8004_agents v
            LEFT JOIN public.eip8004_uri u 
                ON v.origin_key = u.origin_key 
                AND v.agent_id = u.agent_id
            WHERE v.event = 'Registered'
            GROUP BY 1, 2
            ORDER BY 1;
        """
        df_invalid_uri = db_connector.execute_query(query, fetch=True)

        # json 6 kpis, totals of agents valid vs invalid URIs
        query = f"""
            SELECT 
                CASE WHEN uri_json = '{}' THEN 'empty_uri' ELSE 'valid_uri' END AS status,
                COUNT(*) AS value,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)::float AS percentage
            FROM public.eip8004_uri
            GROUP BY 1;
        """
        df_uri_quality = db_connector.execute_query(query, fetch=True)

        # json 6 

    get_eip8004_events()
        
run_dag()