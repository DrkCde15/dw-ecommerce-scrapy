"""
DAG para execucao diaria dos spiders de e-commerce.

Executa todos os spiders via script e salva no PostgreSQL.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_etl",
    default_args=default_args,
    description="ETL diario de e-commerce (scraping)",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "scraping"],
) as dag:

    run_all_spiders = BashOperator(
        task_id="run_all_spiders",
        bash_command="bash /opt/airflow/dags/run_spiders.sh;",
    )

    run_all_spiders
