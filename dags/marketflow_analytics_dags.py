from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


POWERSHELL = (
    "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/"
    "powershell.exe -NoProfile -NonInteractive "
    "-ExecutionPolicy Bypass "
)

PROJECT_WINDOWS = r"C:\Users\Donni\OneDrive\Desktop\Marketflow-Analysis"
PROJECT_WSL = "/mnt/c/Users/Donni/OneDrive/Desktop/Marketflow-Analysis"


with DAG(
    dag_id="marketflow_analytics",
    description="Calculate crypto metrics, write to PostgreSQL, and archive Parquet to S3",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=timedelta(minutes=6),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["marketflow", "spark", "postgresql", "s3"],
):
    run_analytics = BashOperator(
        task_id="run_spark_analytics",
        bash_command=(
            POWERSHELL
            + f"-File '{PROJECT_WINDOWS}\\Scripts\\run_analytics.ps1'"
        ),
        cwd=PROJECT_WSL,
        retries=1,
        retry_delay=timedelta(minutes=2),
        skip_on_exit_code=None,
        do_xcom_push=False,
    )

    upload_to_s3 = BashOperator(
        task_id="upload_parquet_to_s3",
        bash_command=(
            POWERSHELL
            + f"-File '{PROJECT_WINDOWS}\\Scripts\\run_s3_upload.ps1'"
        ),
        cwd=PROJECT_WSL,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=10),
        skip_on_exit_code=None,
        do_xcom_push=False,
    )

    run_analytics >> upload_to_s3