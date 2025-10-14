import sys, getpass
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook

sys.path.append(f"/home/{getpass.getuser()}/gtp/backend/")

@dag(
    dag_id="metrics_sql",
    description="Run some sql aggregations on database.",
    tags=["metrics", "daily"],
    start_date=datetime(2023, 4, 24),
    schedule="30 4 * * *",          # runs 04:30 
    catchup=False,
    default_args={
        "owner": "mseidl",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "on_failure_callback": alert_via_webhook,
    },
)
def etl():
    @task()
    def run_metrics_dependent():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        
        ad = AdapterSQL({}, DbConnector())
        ad.extract({
            "load_type": "metrics",
            "days": "auto",
            "origin_keys": None,
            "metric_keys": None,
            "currency_dependent": True,
            "upsert": True,
        })

    @task()
    def run_metrics_independent():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        
        ad = AdapterSQL({}, DbConnector())
        ad.extract({
            "load_type": "metrics",
            "days": "auto",
            "origin_keys": None,
            "metric_keys": None,
            "currency_dependent": False,
            "upsert": True,
        })

    @task()
    def run_economics():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        
        ad = AdapterSQL({}, DbConnector())
        df = ad.extract({
            "load_type": "economics",
            "days": 5000,
            "origin_keys": None,
            "metric_keys": None,
        })
        ad.load(df)

    @task()
    def run_da_metrics():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        
        ad = AdapterSQL({}, DbConnector())
        df = ad.extract({
            "load_type": "da_metrics",
            "days": 30,
            "origin_keys": None,
            "metric_keys": None,
        })
        ad.load(df)

    @task()
    def run_usd_to_eth():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        
        ad = AdapterSQL({}, DbConnector())
        df = ad.extract({
            "load_type": "usd_to_eth",
            "days": 5000,
            "origin_keys": None,
            "metric_keys": None,
        })
        ad.load(df)

    @task()
    def run_eth_to_usd():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        
        ad = AdapterSQL({}, DbConnector())
        df = ad.extract({
            "load_type": "eth_to_usd",
            "days": 5000,
            "origin_keys": None,
            "metric_keys": None,
        })
        ad.load(df)

    @task()
    def run_aggregate_ecosystem():
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query
        
        execute_jinja_query(DbConnector(), "chain_metrics/upsert_fact_kpis_agg_ecosystem.sql.j2", {"days": 9999})


    run_metrics_dependent() >> run_economics() >> run_usd_to_eth()
    run_metrics_independent() >> run_da_metrics()

    [run_usd_to_eth(), run_da_metrics()] >> run_eth_to_usd() >> run_aggregate_ecosystem()

etl()