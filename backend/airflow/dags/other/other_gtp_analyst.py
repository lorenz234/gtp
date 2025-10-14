import os
import sys
import getpass
import asyncio
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook
from pendulum import timezone

CET = timezone("Europe/Paris")

@dag(
    default_args={
        'owner': 'nader',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': lambda context: alert_via_webhook(context, user='nader')
    },
    dag_id='other_gtp_analyst',
    description='Generate AI Insights with Social Media Automation',
    tags=['ai', 'milestones', 'metrics', 'social', 'twitter', 'charts'],
    start_date=CET.convert(datetime(2023, 9, 1, 8, 0)),
    schedule_interval='30 8 * * *', ## CET TIMEZONE here instead of UTC
    catchup=False  # Ensures only future runs are scheduled, not backfilled
)

def gtp_analyst():
    @task(execution_timeout=timedelta(minutes=60))
    def run_analyst():
        """
        Run social media automation pipeline with AI-generated tweets and chart images
        
        Required environment variables:
        - GTP_AI_WEBHOOK_URL: Discord webhook URL for posting results
        - OPENAI_API_KEY: OpenAI API key for tweet generation
        """
        from dotenv import load_dotenv
        load_dotenv(override=True)  # Force reload environment variables
        
        from src.misc.social_media_integration import SocialMediaAutomation
        
        # Check for required API key
        if not os.getenv("OPENAI_API_KEY"):
            raise ValueError("❌ OpenAI API key not found! Please add OPENAI_API_KEY to your environment variables.")
        
        # Initialize and run the social media automation pipeline
        automation = SocialMediaAutomation()
        print("🚀 Starting automated social media content generation from Airflow DAG...")
        
        # Run the async pipeline using asyncio
        result = asyncio.run(automation.run_automated_social_pipeline())
        
        if result:
            print(f"✅ Social media pipeline completed successfully!")
            print(f"Generated {len(result.get('tweets', []))} tweets")
            print(f"Generated {len(result.get('generated_images', []))} chart images")
            print(f"Processed {len(result.get('responses', []))} milestone responses")
        else:
            print("⚠️ Social media pipeline completed with no results or encountered errors")
            
    @task()
    def run_highlights():
        from src.db_connector import DbConnector
        from src.misc.jinja_helper import execute_jinja_query
        from src.config import gtp_metrics_new
        from src.misc.helper_functions import highlights_prep, send_discord_message, send_telegram_message

        db_connector = DbConnector()
        TG_BOT_TOKEN = os.getenv("GROWTHEPIE_BOT_TOKEN")
        TG_CHAT_ID = "@growthepie_alerts"

        origin_key = 'ethereum_ecosystem'
        name = 'Ethereum Ecosystem'
        
        query_params = {
            "origin_key": origin_key,
            "days" : 2,
            "limit": 5
        }
        df = execute_jinja_query(db_connector, 'api/select_highlights.sql.j2', query_params, return_df=True)

        if not df.empty:
            highlights = highlights_prep(df, gtp_metrics_new)

            for highlight in highlights:
                message = (
                    f"**{highlight['type'].replace('_', ' ').title()} for {name}**\n"
                    f"_{highlight['text']}_\n"
                    f"**Metric:** {highlight['metric_name']}\n"
                    f"**Date:** {highlight['date']}\n"
                    f"**Value:** {highlight['value']}"
                )
                send_discord_message(message, os.getenv("GTP_AI_WEBHOOK_URL"))
                send_telegram_message(TG_BOT_TOKEN, TG_CHAT_ID, message)

    run_analyst()
    run_highlights()
gtp_analyst()
    