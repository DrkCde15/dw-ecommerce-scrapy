"""
DAG para execucao diaria do ETL de e-commerce.

Fluxo ETL corrigido:
1. Scrapy spiders coletam dados → JSON/Parquet (arquivos temporários)
2. StoragePipeline carrega arquivos → raw.* (append com dedup/UPSERT)
3. Validate Extract (schema, row count)
4. TransformPipeline transforma raw → staging → marts (incremental com UPSERT)
5. Validate Transform (null checks, duplicates)
6. Load marts → report schema
7. Validate Load (referential integrity, uniqueness)
8. Monitoring

Orquestração com:
- depends_on_downstream (trigger_rule="all_success")
- Retries com backoff exponencial
- Data lineage tracking
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/project/src")

from pipelines.transform.transform_pipeline import TransformPipeline
from pipelines.load.load_pipeline import LoadPipeline
from pipelines.storage.storage_pipeline import StoragePipeline
from pipelines.tests.quality_tests import DataQualityTests
from pipelines.monitoring.monitor import PipelineMonitor


default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}


def run_spiders():
    """STAGE 1: Executa os spiders Scrapy → gera JSON/Parquet em data/raw/."""
    import subprocess
    result = subprocess.run(
        ["bash", "/opt/airflow/dags/run_spiders.sh"],
        capture_output=True, text=True, cwd="/opt/project"
    )
    if result.returncode != 0:
        raise RuntimeError(f"Spiders falharam: {result.stderr}")
    print("Spiders executados com sucesso")
    return result.stdout


def run_extract():
    """STAGE 2: StoragePipeline carrega arquivos JSON/Parquet → raw.* (UPSERT + dedup)."""
    pipeline = StoragePipeline(
        postgres_url="postgresql://postgres:postgres@postgres:5432/ecommerce"
    )
    results = pipeline.load_all_from_directory(data_dir="/opt/project/data/raw")
    print(f"Extract concluido: {results}")
    pipeline.log_lineage()
    pipeline.close()
    return results


def run_transform():
    """STAGE 3: Transform raw → staging → marts (incremental com UPSERT)."""
    pipeline = TransformPipeline(
        postgres_url="postgresql://postgres:postgres@postgres:5432/ecommerce"
    )
    results = pipeline.run_all()
    print(f"Transformacoes concluidas: {results}")
    return results


def run_load_report():
    """STAGE 4: Load marts → report schema."""
    pipeline = LoadPipeline(
        postgres_url="postgresql://postgres:postgres@postgres:5432/ecommerce"
    )
    results = pipeline.load_all_to_report()
    print(f"Load concluido: {results}")
    return results


def run_quality_tests():
    """STAGE 5: Quality tests - validacao completa em pipeline."""
    tests = DataQualityTests(
        postgres_url="postgresql://postgres:postgres@postgres:5432/ecommerce"
    )
    success = tests.run_all_tests()
    if not success:
        raise ValueError("Testes de qualidade falharam!")
    return True


def run_quality_extract():
    """STAGE 5a: Validate extract stage (schema, row count, not empty)."""
    tests = DataQualityTests(
        postgres_url="postgresql://postgres:postgres@postgres:5432/ecommerce"
    )
    tests.set_stage("extract")
    tests.validate_extract_not_empty("books", "raw")
    tests.validate_extract_not_empty("amazon", "raw")
    tests.validate_extract_not_empty("americanas", "raw")
    tests.validate_extract_not_empty("kabum", "raw")
    tests.validate_cdc_columns("books", "raw")
    tests.validate_cdc_columns("amazon", "raw")
    tests.validate_cdc_columns("americanas", "raw")
    tests.validate_cdc_columns("kabum", "raw")
    return True


def run_quality_transform():
    """STAGE 5b: Validate transform stage (nulls, duplicates, row counts)."""
    tests = DataQualityTests(
        postgres_url="postgresql://postgres:postgres@postgres:5432/ecommerce"
    )
    tests.set_stage("transform")
    tests.validate_transform_row_count("stg_books", min_rows=0, schema="staging")
    tests.validate_transform_nulls("stg_books", ["product_id", "name"], "staging")
    tests.validate_transform_duplicates("stg_books", "product_id", "staging")
    return True


def run_quality_load():
    """STAGE 5c: Validate load stage (referential integrity, uniqueness, not-null)."""
    tests = DataQualityTests(
        postgres_url="postgresql://postgres:postgres@postgres:5432/ecommerce"
    )
    tests.set_stage("load")
    tests.validate_load_unique("dim_books", "book_id", "marts")
    tests.validate_load_unique("dim_products", "id", "marts")
    tests.validate_load_not_null("dim_books", "book_name", "marts")
    tests.validate_load_not_null("dim_products", "product_name", "marts")
    tests.validate_load_row_count("dim_books", min_rows=0, schema="marts")
    tests.validate_lineage_exists()
    return True


def run_monitoring():
    """STAGE 6: Monitoring - verifica saude do pipeline."""
    monitor = PipelineMonitor(
        postgres_url="postgresql://postgres:postgres@postgres:5432/ecommerce"
    )
    results = monitor.run_full_check()
    if results["status"] == "unhealthy":
        raise ValueError(f"Pipeline unhealthy: {results['alerts']}")
    return results


with DAG(
    dag_id="ecommerce_etl",
    default_args=default_args,
    description="ETL diario de e-commerce: spiders → load → validate → transform → validate → load_report → validate → monitoring",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "scraping", "python"],
    max_active_runs=1,
) as dag:

    # ==========================================
    # STAGE 1: RUN SPIDERS (BashOperator)
    # Executa os spiders → gera JSON/Parquet em data/raw/
    # ==========================================
    run_spiders_task = BashOperator(
        task_id="run_spiders",
        bash_command="bash /opt/airflow/dags/run_spiders.sh",
        retry_exponential_backoff=True,
        max_retry_delay=300,
    )

    # ==========================================
    # STAGE 2: EXTRACT (StoragePipeline)
    # Carrega arquivos JSON/Parquet → raw.* com UPSERT e dedup
    # depends_on_downstream: só executa se run_spiders for sucesso
    # ==========================================
    extract = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
        trigger_rule="all_success",
    )

    # ==========================================
    # STAGE 3: VALIDATE EXTRACT
    # Schema validation, CDC columns, not empty checks
    # depends_on_downstream: só executa se extract for sucesso
    # ==========================================
    validate_extract = PythonOperator(
        task_id="validate_extract",
        python_callable=run_quality_extract,
        trigger_rule="all_success",
    )

    # ==========================================
    # STAGE 4: TRANSFORM
    # raw → staging → marts (incremental com UPSERT)
    # depends_on_downstream: só executa se validate_extract for sucesso
    # ==========================================
    transform = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
        trigger_rule="all_success",
    )

    # ==========================================
    # STAGE 5: VALIDATE TRANSFORM
    # Null checks, duplicate checks, row counts
    # depends_on_downstream: só executa se transform for sucesso
    # ==========================================
    validate_transform = PythonOperator(
        task_id="validate_transform",
        python_callable=run_quality_transform,
        trigger_rule="all_success",
    )

    # ==========================================
    # STAGE 6: LOAD REPORT
    # marts → report schema
    # depends_on_downstream: só executa se validate_transform for sucesso
    # ==========================================
    load_report = PythonOperator(
        task_id="load_report",
        python_callable=run_load_report,
        trigger_rule="all_success",
    )

    # ==========================================
    # STAGE 7: VALIDATE LOAD
    # Referential integrity, uniqueness, not-null, lineage
    # depends_on_downstream: só executa se load_report for sucesso
    # ==========================================
    validate_load = PythonOperator(
        task_id="validate_load",
        python_callable=run_quality_load,
        trigger_rule="all_success",
    )

    # ==========================================
    # STAGE 8: OVERALL QUALITY TESTS
    # Executa todos os testes de qualidade completos
    # depends_on_downstream: só executa se validate_load for sucesso
    # ==========================================
    quality_tests = PythonOperator(
        task_id="quality_tests",
        python_callable=run_quality_tests,
        trigger_rule="all_success",
    )

    # ==========================================
    # STAGE 9: MONITORING
    # Verifica saude do pipeline
    # depends_on_downstream: só executa se quality_tests for sucesso
    # ==========================================
    monitoring = PythonOperator(
        task_id="monitoring",
        python_callable=run_monitoring,
        trigger_rule="all_success",
    )

    # ==========================================
    # ORQUESTRAÇÃO COMPLETA COM depends_on_downstream
    # Cada estágio só executa se o anterior for sucesso
    # ==========================================
    run_spiders_task >> extract >> validate_extract >> transform >> validate_transform >> load_report >> validate_load >> quality_tests >> monitoring
