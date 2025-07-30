from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from airflow.utils.dates import days_ago
import sys

PROJECT_ROOT_IN_AIRFLOW = '/opt/airflow'
if PROJECT_ROOT_IN_AIRFLOW not in sys.path:
    sys.path.append(PROJECT_ROOT_IN_AIRFLOW)    

try:
    # Import your clean, modern functions
    from src.main import (
        process_csv_data,
        run_full_us_covid_ingestion, 
        run_covid_ingestion_only,
        run_covid_staging_only
    )
except ImportError as e:
    print(f"ERROR: Could not import external scripts. Please check volume mounts and PYTHONPATH in Airflow containers.")
    print(f"ImportError: {e}")
    raise

def _run_csv_processing(**kwargs):
    """
    Run CSV data processing workflow
    """
    kwargs['ti'].log.info(f"Running CSV data processing for DAG run {kwargs['dag_run'].run_id}")
    result = process_csv_data()
    kwargs['ti'].log.info(f"Completed CSV data processing for DAG run {kwargs['dag_run'].run_id}")
    if not result:
        raise Exception("CSV data processing failed")
    return result

def _run_covid_ingestion(**kwargs):
    """
    Run COVID data ingestion (API -> MinIO)
    """
    kwargs['ti'].log.info(f"Running COVID ingestion for DAG run {kwargs['dag_run'].run_id}")
    result = run_covid_ingestion_only()
    kwargs['ti'].log.info(f"Completed COVID ingestion for DAG run {kwargs['dag_run'].run_id}")
    if not result:
        raise Exception("COVID ingestion failed")
    return result

def _run_covid_staging(**kwargs):
    """
    Run COVID data staging (MinIO -> Snowflake)
    """
    kwargs['ti'].log.info(f"Running COVID staging for DAG run {kwargs['dag_run'].run_id}")
    result = run_covid_staging_only()
    kwargs['ti'].log.info(f"Completed COVID staging for DAG run {kwargs['dag_run'].run_id}")
    if not result:
        raise Exception("COVID staging failed")
    return result

def _run_full_pipeline(**kwargs):
    """
    Run the complete pipeline (both CSV and COVID workflows)
    """
    kwargs['ti'].log.info(f"Running full pipeline for DAG run {kwargs['dag_run'].run_id}")
    
    # Run CSV processing
    csv_result = process_csv_data()
    if not csv_result:
        kwargs['ti'].log.error("CSV processing failed")
        raise Exception("CSV processing failed")
    
    # Run COVID pipeline
    covid_result = run_full_us_covid_ingestion()
    if not covid_result:
        kwargs['ti'].log.error("COVID pipeline failed")
        raise Exception("COVID pipeline failed")
    
    kwargs['ti'].log.info(f"Completed full pipeline for DAG run {kwargs['dag_run'].run_id}")
    return True

# Parallel workflows - CSV and COVID ingestion run in parallel, COVID staging follows
with DAG(
    dag_id='project3_parallel_pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    description='Modern project3 pipeline with parallel CSV and COVID processing',
    tags=['project3', 'modern', 'parallel', 'csv', 'covid'],
) as dag:

    csv_processing_task = PythonOperator(
        task_id='process_csv_data',
        python_callable=_run_csv_processing,
        provide_context=True,
    )

    covid_ingestion_task = PythonOperator(
        task_id='covid_ingestion',
        python_callable=_run_covid_ingestion,
        provide_context=True,
    )

    covid_staging_task = PythonOperator(
        task_id='covid_staging',
        python_callable=_run_covid_staging,
        provide_context=True,
    )

    # CSV processing runs in parallel with COVID ingestion
    # COVID staging depends on COVID ingestion completing
    [csv_processing_task, covid_ingestion_task] 
    covid_ingestion_task >> covid_staging_task
