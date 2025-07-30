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
    from src.api_ingestion.us_covid_ingestion_v2 import main as us_covid_ingestion_main
    from src.main import main as main
    from src.data_staging_v2 import main as data_staging_main
except ImportError as e:
    print(f"ERROR: Could not import external scripts. Please check volume mounts and PYTHONPATH in Airflow containers.")
    print(f"ImportError: {e}")
    raise

def _run_us_covid_script(**kwargs):
    """
    Run the US COVID ingestion script and return the data
    """
    kwargs['ti'].log.info(f"Running us_covid_ingestion_main for DAG run {kwargs['dag_run'].run_id}")
    us_covid_ingestion_main()
    kwargs['ti'].log.info(f"Completed us_covid_ingestion_main for DAG run {kwargs['dag_run'].run_id}")

def _run_main_script(**kwargs):
    """
    Run the main ingestion script and return the data
    """
    kwargs['ti'].log.info(f"Running main for DAG run {kwargs['dag_run'].run_id}")
    main()
    kwargs['ti'].log.info(f"Completed main for DAG run {kwargs['dag_run'].run_id}")
    

def _run_data_staging(**kwargs):
    """
    Run the data staging script
    """
    kwargs['ti'].log.info(f"Running data_staging_main for DAG run {kwargs['dag_run'].run_id}")
    result = data_staging_main()
    kwargs['ti'].log.info(f"Completed data_staging_main for DAG run {kwargs['dag_run'].run_id}")
    return result


with DAG(
    dag_id='project3_covid_pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['project3', 'ingestion', 'bronze', 'api', 'minio'],
) as dag:

    pull_api_to_minio_task = PythonOperator(
        task_id='pull_api_to_minio',
        python_callable=_run_us_covid_script,
        provide_context=True,
    )

    run_main_task = PythonOperator(
        task_id='run_main',
        python_callable=_run_main_script,
        provide_context=True,
    )

    stage_minio_to_snowflake_task = PythonOperator(
        task_id='stage_minio_to_snowflake',
        python_callable=_run_data_staging,
        provide_context=True,
    )

    pull_api_to_minio_task >> run_main_task >> stage_minio_to_snowflake_task