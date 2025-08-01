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
    )
except ImportError as e:
    print(f"ERROR: Could not import external scripts. Please check volume mounts and PYTHONPATH in Airflow containers.")
    print(f"ImportError: {e}")
    raise

def _run_full_pipeline(**kwargs):
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

# CSV and COVID ingestion
with DAG(
    dag_id='project3_full_raw_pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    description='Project 3 raw data ingestion pipeline',
    tags=['project3', 'modern', 'parallel', 'csv', 'covid'],
) as dag:

    covid_ingestion_task = PythonOperator(
        task_id='covid_ingestion',
        python_callable=_run_full_pipeline,
        provide_context=True,
    )


    covid_ingestion_task
