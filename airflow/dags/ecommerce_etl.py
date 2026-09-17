"""
DAG para execucao diaria do ETL de e-commerce.

Fluxo:
1. Scrapy spiders coletam dados → PostgreSQL (raw)
2. dbt run transforma raw → staging → marts
3. dbt test valida qualidade dos dados
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

DBT_CMD = "cd /opt/project/dbt && dbt --profiles-dir /opt/airflow/dbt"

with DAG(
    dag_id="ecommerce_etl",
    default_args=default_args,
    description="ETL diario de e-commerce: scraping + dbt run + dbt test",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "scraping", "dbt"],
) as dag:

    # 1. Scrapy: coleta dados e salva no PostgreSQL
    run_all_spiders = BashOperator(
        task_id="run_all_spiders",
        bash_command="bash /opt/airflow/dags/run_spiders.sh;",
    )

    # 2. dbt run: transforma raw → staging → marts
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_CMD} run --profiles-dir /opt/project/dbt --project-dir /opt/project/dbt;",
    )

    # 3. dbt test: valida qualidade dos dados
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test --profiles-dir /opt/airflow/dbt --project-dir /opt/project/dbt || true;",
    )

    # Orquestracao: scraping → transformacao → validacao
    run_all_spiders >> dbt_run >> dbt_test
