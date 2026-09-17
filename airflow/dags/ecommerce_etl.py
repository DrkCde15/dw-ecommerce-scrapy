"""
DAG para execucao diaria do ETL de e-commerce.

Fluxo:
1. Scrapy spiders coletam dados → PostgreSQL (raw)
2. Python transforma raw → staging → marts
3. Python valida qualidade dos dados
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Adicionar src ao path
sys.path.insert(0, "/opt/project/src")

from pipelines.transform.transform_pipeline import TransformPipeline
from pipelines.tests.quality_tests import DataQualityTests


default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_transform():
    """Executa todas as transformacoes."""
    pipeline = TransformPipeline(
        postgres_url="postgresql://postgres:postgres@postgres:5432/ecommerce"
    )
    results = pipeline.run_all()
    print(f"Transformacoes concluidas: {results}")


def run_quality_tests():
    """Executa testes de qualidade."""
    tests = DataQualityTests(
        postgres_url="postgresql://postgres:postgres@postgres:5432/ecommerce"
    )
    success = tests.run_all_tests()
    if not success:
        raise ValueError("Testes de qualidade falharam!")


with DAG(
    dag_id="ecommerce_etl",
    default_args=default_args,
    description="ETL diario de e-commerce: scraping + transform + quality tests",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "scraping", "python"],
) as dag:

    # 1. Scrapy: coleta dados e salva no PostgreSQL
    run_all_spiders = BashOperator(
        task_id="run_all_spiders",
        bash_command="bash /opt/airflow/dags/run_spiders.sh;",
    )

    # 2. Transform: raw → staging → marts
    transform = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    # 3. Quality Tests: valida qualidade dos dados
    quality_tests = PythonOperator(
        task_id="quality_tests",
        python_callable=run_quality_tests,
    )

    # Orquestracao: scraping → transformacao → validacao
    run_all_spiders >> transform >> quality_tests
