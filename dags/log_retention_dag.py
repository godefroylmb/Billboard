from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="log_retention_cleanup",
    description="Delete Airflow task logs older than 30 days",
    start_date=datetime(2026, 1, 1),
    schedule="30 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "cleanup"],
) as dag:
    delete_old_logs = BashOperator(
        task_id="delete_old_logs",
        bash_command="find /opt/airflow/logs -type f -mtime +30 -delete",
    )

    delete_empty_dirs = BashOperator(
        task_id="delete_empty_log_dirs",
        bash_command="find /opt/airflow/logs -type d -empty -delete",
    )

    delete_old_logs >> delete_empty_dirs
