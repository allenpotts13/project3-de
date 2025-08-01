from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import subprocess
from dotenv import load_dotenv

DBT_PROJECT_DIR_IN_AIRFLOW = '/opt/airflow/my_covid_dbt_project'
DBT_PROFILES_DIR_IN_AIRFLOW = '/opt/airflow/my_covid_dbt_project'

load_dotenv()

def run_dbt_command(dbt_command: str, **kwargs):
    dbt_env = os.environ.copy()
    dbt_env['DBT_PROFILES_DIR'] = DBT_PROFILES_DIR_IN_AIRFLOW

    dbt_env['SNOWFLAKE_USER'] = os.getenv('SNOWFLAKE_USER')
    dbt_env['SNOWFLAKE_PASSWORD'] = os.getenv('SNOWFLAKE_PASSWORD')
    dbt_env['SNOWFLAKE_ACCOUNT'] = os.getenv('SNOWFLAKE_ACCOUNT')
    dbt_env['SNOWFLAKE_WAREHOUSE'] = os.getenv('SNOWFLAKE_WAREHOUSE')
    dbt_env['SNOWFLAKE_DATABASE'] = os.getenv('SNOWFLAKE_DATABASE')
    dbt_env['SNOWFLAKE_SCHEMA_BRONZE'] = os.getenv('SNOWFLAKE_SCHEMA_BRONZE')
    dbt_env['SNOWFLAKE_ROLE'] = os.getenv('SNOWFLAKE_ROLE')
    dbt_env['DBT_SCHEMA'] = 'silver'

    full_command = f"dbt {dbt_command}"

    process = subprocess.run(
        full_command,
        cwd=DBT_PROJECT_DIR_IN_AIRFLOW,
        env=dbt_env,
        shell=True,
        capture_output=True,
        text=True
    )

    if process.returncode != 0:
        print(f"Error running dbt command: {full_command}")
        print(f"stdout:\n{process.stdout}")
        print(f"stderr:\n{process.stderr}")
        raise Exception(f"DBT command failed with return code {process.returncode}")
    else:
        print(f"DBT command succeeded: {full_command}")
        print(f"stdout:\n{process.stdout}")
        print(f"stderr:\n{process.stderr}")

with DAG(
    dag_id='covid_transformation_pipeline',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['covid', 'transformation', 'dbt', 'silver', 'gold'],
) as dag:
    run_dbt_silver_models = PythonOperator(
        task_id='run_dbt_silver_models',
        python_callable=run_dbt_command,
        op_kwargs={'dbt_command': 'run --profile snowflake_dev --select silver'}
    )

    run_dbt_gold_models = PythonOperator(
        task_id='run_dbt_gold_models',
        python_callable=run_dbt_command,
        op_kwargs={'dbt_command': 'run --profile snowflake_dev --select gold'}
    )

    run_dbt_silver_models >> run_dbt_gold_models