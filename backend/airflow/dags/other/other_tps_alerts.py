import sys, getpass
sys.path.append(f"/home/{getpass.getuser()}/gtp/backend/")

from datetime import datetime,timedelta
from airflow.decorators import dag, task 
from src.misc.airflow_utils import alert_via_webhook

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 1,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=2),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='other_tps_alerts',
    description='Check against our Redis DB and alert via Discord/Telegram if new TPS all-time-highs are detected.',
    tags=['other', 'near-real-time'],
    start_date=datetime(2025,10,14),
    schedule='*/30 * * * *' # run every 30 minutes
)

def run_dag():
    @task()
    def run_tps_global():      
        import os
        import time
        import json
        import signal
        import sys
        import redis
        from src.misc.helper_functions import  generate_screenshot, send_telegram_message, send_discord_message
        
        import time
        start_time = time.time()

        # Redis constants
        REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
        #REDIS_HOST = 'localhost' # for testing
        REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
        REDIS_DB = int(os.getenv("REDIS_DB", "0"))
        REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

        REDIS_ZSET_KEY_TPS_HISTORY = "global:tps:history_24h" # for testing
        REDIS_ZSET_KEY_ATH_HISTORY = "global:tps:ath_history"
        CHECK_INTERVAL_SEC = 2
        COOLDOWN_AFTER_NEW_HIGH_SEC = 30

        TG_BOT_TOKEN = os.getenv("GROWTHEPIE_BOT_TOKEN")
        TG_CHAT_ID = "@growthepie_alerts"

        # Graceful exit flag
        running = True
        def handle_exit(signum, frame):
            global running
            print("\n🛑 Received shutdown signal. Exiting gracefully...")
            running = False

        signal.signal(signal.SIGINT, handle_exit)
        signal.signal(signal.SIGTERM, handle_exit)

        def get_latest_entry(r, REDIS_ZSET_KEY):
            """Return latest zset entry as (payload_dict, score) or (None, None)."""
            latest = r.zrevrange(REDIS_ZSET_KEY, 0, 0, withscores=True)
            if not latest:
                return None, None
            raw_val, score = latest[0]
            try:
                payload = json.loads(raw_val.decode("utf-8"))
            except Exception as e:
                print(f"⚠️ Failed to decode JSON: {e}")
                return None, None
            return payload, score

        def on_new_tps_high(new_tps, payload):
            """Placeholder for your real reaction logic (Telegram, Discord, etc.)."""
            ts = payload.get("timestamp") or payload.get("timestamp_ms")
            ts = ts.split(".")[0]
            sorted_chain_breakdown = dict(
                sorted(payload["chain_breakdown"].items(), key=lambda item: item[1], reverse=True)[:5]
            )
            formatted_chain_breakdown = "\n".join(f"• {name.title()}: {value:.1f}" for name, value in sorted_chain_breakdown.items())

            url = "https://www.growthepie.com/ethereum-ecosystem/metrics?tps=true"
            filename = f"/tps_global/{ts}_ecosystem.png"
            selector = "#content-panel > div > main > div > div.flex.flex-col.pt-\[15px\] > div.px-\[20px\].md\:pl-\[45px\].md\:pr-\[60px\].text-color-text-primary.z-\[1\] > div.grid.grid-cols-\[1fr\,1fr\,1fr\].gap-\[15px\].w-full.\@container > div.flex.flex-col.lg\:flex-row.gap-\[15px\].col-span-3.\@\[1040px\]\:col-span-2 > div:nth-child(1) > div > div"
            generate_screenshot(url, filename, wait_for_timeout=2000, selector=selector)

            message = [
                f"🥧 **New all-time high in Ecosystem TPS:** `{new_tps:.2f}`",
                f"*(at {ts} UTC)*\n",
                "**Top chains by TPS at time of ATH:**",
                formatted_chain_breakdown,
                f"[View on growthepie.com]({url})"
            ]
            message = "\n".join(message)

            send_telegram_message(TG_BOT_TOKEN, TG_CHAT_ID, message, image_path=f"generated_images/{filename}")
            send_discord_message(message, os.getenv("GTP_AI_WEBHOOK_URL"), image_paths=f"generated_images/{filename}")
            
        print("🔌 Connecting to Redis...")
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=False
        )
        try:
            r.ping()
        except redis.exceptions.ConnectionError as e:
            print(f"❌ Failed to connect to Redis: {e}")
            sys.exit(1)
        print(f"✅ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

        # Get initial TPS
        payload, _ = get_latest_entry(r, REDIS_ZSET_KEY_ATH_HISTORY)
        if not payload:
            print(f"ℹ️ No entries found in '{REDIS_ZSET_KEY_ATH_HISTORY}', waiting for data...")
            latest_ath = float("-inf")
        else:
            latest_ath = float(payload.get("tps", float("-inf")))
            print(f"🔧 Initial internal ATH set to {latest_ath:.2f}")

        while running:
            payload, _ = get_latest_entry(r, REDIS_ZSET_KEY_ATH_HISTORY) 
            #payload, _ = get_latest_entry(r, REDIS_ZSET_KEY_TPS_HISTORY) # for testing
            if payload:
                try:
                    current_tps = float(payload.get("tps", float("-inf")))
                except (TypeError, ValueError):
                    current_tps = float("-inf")

                if current_tps > latest_ath:
                #if current_tps > 300: # for testing
                    on_new_tps_high(current_tps, payload)
                    latest_ath = current_tps
                    time.sleep(COOLDOWN_AFTER_NEW_HIGH_SEC)
                    
                    continue
                else:
                    print(f"ℹ️ No new TPS high. Current TPS: {current_tps:.2f}, ATH: {latest_ath:.2f}")
            
            if start_time + (31 * 60) < time.time():  # 31 minutes
                print("⏰ Approaching task timeout, exiting loop to allow for graceful restart.")
                break

            time.sleep(CHECK_INTERVAL_SEC)

        print("👋 Exiting monitor loop.")
        r.close()

    run_tps_global()
run_dag()