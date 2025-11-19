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
    dag_id="demo_pjan_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline DEMO_PJAN: ingest -> transform -> load",
    schedule_interval="@daily",   # mund ta ndryshosh
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["eurostat", "population"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_demo_pjan",
        bash_command="cd /opt/airflow/project && python src/collect/ingest_demo_pjan.py",
    )

    transform = BashOperator(
        task_id="transform_demo_pjan",
        bash_command="cd /opt/airflow/project && python src/transform/demo_pjan_raw_to_silver.py",
    )

    load_dw = BashOperator(
        task_id="load_demo_pjan_to_sqlserver",
        bash_command="cd /opt/airflow/project && python src/load/demo_pjan_to_sqlserver.py",
    )

    ingest >> transform >> load_dw
