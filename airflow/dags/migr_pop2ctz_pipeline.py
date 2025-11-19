from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="migr_pop2ctz_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline MIGR_POP2CTZ: ingest -> transform -> load",
    schedule_interval="@daily",  # mund ta ndryshosh p.sh. në '@weekly'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["eurostat", "migration"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_migr_pop2ctz",
        bash_command=(
            "cd /opt/airflow/project && "
            "python src/collect/ingest_migr_pop2ctz.py"
        ),
    )

    transform = BashOperator(
        task_id="transform_migr_pop2ctz",
        bash_command=(
            "cd /opt/airflow/project && "
            "python src/transform/migr_pop2ctz_raw_to_silver.py"
        ),
    )

    load_dw = BashOperator(
        task_id="load_migr_pop2ctz_to_sqlserver",
        bash_command=(
            "cd /opt/airflow/project && "
            "python src/load/migr_pop2ctz_to_sqlserver.py"
        ),
    )

    ingest >> transform >> load_dw