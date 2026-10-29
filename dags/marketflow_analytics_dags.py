from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="marketflow_analytics",
    description="Calculate crypto metrics and write them to PostgreSQL",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=timedelta(minutes=6),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["marketflow", "spark", "postgresql"],
):
    run_analytics = BashOperator(
        task_id="run_spark_analytics",
        bash_command=(
            "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/"
            "powershell.exe -NoProfile -NonInteractive "
            "-ExecutionPolicy Bypass "
            r"-File 'C:\Users\Donni\OneDrive\Desktop"
            r"\Marketflow-Analysis\scripts\run_analytics.ps1'"
        ),
        cwd="/mnt/c/Users/Donni/OneDrive/Desktop/Marketflow-Analysis",
        retries=1,
        retry_delay=timedelta(minutes=2),
        skip_on_exit_code=None,
        do_xcom_push=False,
    )