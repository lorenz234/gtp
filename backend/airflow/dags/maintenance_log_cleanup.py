"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.
airflow trigger_dag --conf '[curly-braces]"maxLogAgeInDays":30[curly-braces]' airflow-log-cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional
"""

import logging
import os
from datetime import timedelta

import airflow
import jinja2
from airflow.configuration import conf
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# DAG configs
DAG_ID = "airflow_log_cleanup"
START_DATE = airflow.utils.dates.days_ago(1)
SCHEDULE_INTERVAL = "0 4 * * *"  # daily at 04:00
DAG_OWNER_NAME = "mseidl"
ALERT_EMAIL_ADDRESSES = ['matthias@mseidl-analytics.de']

# Config values (with defaults)
try:
    BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER").rstrip("/")
except Exception:
    BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER").rstrip("/")

DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get("airflow_log_cleanup__max_log_age_in_days", 5)
ENABLE_DELETE = True  # set to False for dry-run
NUMBER_OF_WORKERS = 2
LOG_CLEANUP_PROCESS_LOCK_FILE = "/tmp/airflow_log_cleanup_worker.lock"

if not BASE_LOG_FOLDER or BASE_LOG_FOLDER.strip() == "":
    raise ValueError("BASE_LOG_FOLDER is empty. Please set it in airflow.cfg.")

dag = DAG(
    DAG_ID,
    default_args={
        'owner': DAG_OWNER_NAME,
        'depends_on_past': False,
        'email': ALERT_EMAIL_ADDRESSES,
        'email_on_failure': True,
        'start_date': START_DATE,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    tags=['maintenance', 'daily'],
    template_undefined=jinja2.Undefined
)

start = DummyOperator(task_id='start', dag=dag)

log_cleanup = f"""
echo "Starting Airflow log cleanup..."

MAX_LOG_AGE_IN_DAYS="{{{{ dag_run.conf.maxLogAgeInDays }}}}"
if [ -z "$MAX_LOG_AGE_IN_DAYS" ]; then
    MAX_LOG_AGE_IN_DAYS="{DEFAULT_MAX_LOG_AGE_IN_DAYS}"
fi

ENABLE_DELETE={"true" if ENABLE_DELETE else "false"}

echo "BASE_LOG_FOLDER: {BASE_LOG_FOLDER}"
echo "MAX_LOG_AGE_IN_DAYS: $MAX_LOG_AGE_IN_DAYS"
echo "ENABLE_DELETE: $ENABLE_DELETE"

if [ ! -f {LOG_CLEANUP_PROCESS_LOCK_FILE} ]; then
    echo "Creating lock file to prevent collisions..."
    touch {LOG_CLEANUP_PROCESS_LOCK_FILE} || exit 1

    echo "Running cleanup..."

    # Find and delete old log files
    FIND_CMD="find {BASE_LOG_FOLDER} -type f -name '*.log' -mtime +$MAX_LOG_AGE_IN_DAYS"
    DELETE_CMD="$FIND_CMD -delete"

    if [ "$ENABLE_DELETE" == "true" ]; then
        echo "Executing: $DELETE_CMD"
        eval $DELETE_CMD
    else
        echo "Dry-run: $FIND_CMD"
        eval $FIND_CMD
    fi

    # Clean up empty directories (optional)
    FIND_EMPTY_DIRS="find {BASE_LOG_FOLDER} -type d -empty"
    DELETE_EMPTY_DIRS="$FIND_EMPTY_DIRS -delete"

    if [ "$ENABLE_DELETE" == "true" ]; then
        echo "Cleaning empty directories..."
        eval $DELETE_EMPTY_DIRS
    else
        echo "Dry-run: $FIND_EMPTY_DIRS"
        eval $FIND_EMPTY_DIRS
    fi

    echo "Cleanup complete."

    rm -f {LOG_CLEANUP_PROCESS_LOCK_FILE} || exit 1
else
    echo "Another cleanup process is already running. Exiting."
fi
"""

for worker_id in range(1, NUMBER_OF_WORKERS + 1):
    task = BashOperator(
        task_id=f'log_cleanup_worker_{worker_id}',
        bash_command=log_cleanup,
        params={"sleep_time": worker_id * 3},
        dag=dag
    )
    task.set_upstream(start)