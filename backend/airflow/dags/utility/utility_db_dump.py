from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from src.misc.airflow_utils import alert_via_webhook


@dag(
    default_args={
        "owner": "mseidl",
        "retries": 2,
        "email_on_failure": False,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": lambda context: alert_via_webhook(context, user="mseidl"),
    },
    dag_id="utility_db_dump",
    description="Daily full PostgreSQL backup to GCS via pg_dump.",
    tags=["utility", "backup", "daily"],
    start_date=datetime(2026, 3, 12),
    schedule="00 20 * * *",
)
def backup_db_to_gcs():
    @task()
    def run_backup():
        import os
        import re
        import subprocess
        import tempfile
        from src.adapters.rpc_funcs.gcs_utils import connect_to_gcs

        db_user = os.getenv("DB_USERNAME")
        db_passwd = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT") or "5432"

        if not all([db_user, db_passwd, db_host]):
            raise ValueError(
                "Missing required DB env vars: DB_USERNAME, DB_PASSWORD, DB_HOST"
            )

        # Allow DB_HOST to include a port (e.g., "host:5432")
        if db_host and ":" in db_host and not db_host.startswith("["):
            host_part, port_part = db_host.rsplit(":", 1)
            if port_part.isdigit():
                db_host = host_part
                if not os.getenv("DB_PORT"):
                    db_port = port_part

        gcs, default_bucket = connect_to_gcs()
        bucket_name =  "gtp-archive"
        prefix = "db_backups"
        retention_days = 10

        env = os.environ.copy()
        env["PGPASSWORD"] = db_passwd


        db_names = ["web3", "oli", "fun"]

        bucket = gcs.bucket(bucket_name)

        for name in db_names:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
            filename = f"{name}_{ts}.dump"
            blob_path = f"{prefix}/{name}/date={ts[:10]}/{filename}"

            with tempfile.TemporaryDirectory() as tmpdir:
                dump_path = os.path.join(tmpdir, filename)
                cmd = [
                    "pg_dump",
                    "--format=custom",
                    "--compress=9",
                    "--no-owner",
                    "--no-acl",
                    "--host",
                    db_host,
                    "--port",
                    str(db_port),
                    "--username",
                    db_user,
                    "--file",
                    dump_path,
                    name,
                ]

                result = subprocess.run(
                    cmd,
                    check=True,
                    env=env,
                    capture_output=True,
                    text=True,
                )
                if result.stdout:
                    print(result.stdout)
                if result.stderr:
                    print(result.stderr)

                blob = bucket.blob(blob_path)
                blob.upload_from_filename(dump_path)

                print(f"Uploaded backup to gs://{bucket_name}/{blob_path}")

        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        date_re = re.compile(r"/date=(\d{4}-\d{2}-\d{2})/")
        for name in db_names:
            delete_prefix = f"{prefix}/{name}/"
            for blob in bucket.list_blobs(prefix=delete_prefix):
                match = date_re.search(blob.name)
                if not match:
                    continue
                try:
                    blob_date = datetime.strptime(match.group(1), "%Y-%m-%d").replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    continue
                if blob_date < cutoff:
                    print(f"Deleting old backup: gs://{bucket_name}/{blob.name}")
                    blob.delete()

    run_backup()


backup_db_to_gcs()
