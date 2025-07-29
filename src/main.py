import os
import sys
import subprocess
import logging
from dotenv import load_dotenv

# Add src directory to path for imports
sys.path.append('/workspaces/project3-de/src')

from minio_dao.minio import create_minio_client, extract_data_from_minio
from snowflake_dao.snowflake import create_snowflake_connection, create_csv_table_dynamic

# Load environment variables from .env file
load_dotenv()

# Set up logging
def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Create logs directory if it doesn't exist
    log_directory = 'src/logs'
    os.makedirs(log_directory, exist_ok=True)
    log_file = os.path.join(log_directory, 'main_pipeline.log')

    # Clear existing handlers to avoid duplicates
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger()

def process_csv_data():
    """
    Process legacy CSV data workflow
    """
    logger.info("Starting CSV data processing workflow...")
    
    try:
        ## Calls methods from minio_dao/minio.py to extract .csv from MinIO
        # This stores the .csvs extracted from minio into a /tmp/ directory within the container for Snowflake to pick up and COPY INTO
        filenames = ['ukhsa-coverage-report-2024-05-30.csv', 'canada_antibody_seroprevalence_13100818.csv', 'canada_demand_and_usage_13100838.csv']

        minio_client = create_minio_client()
        file_paths = extract_data_from_minio(minio_client, filenames)

        snowflake_conn = create_snowflake_connection()
        cursor = snowflake_conn.cursor()

        ## Calls methods from snowflake_dao/snowflake.py to retrieve files from /tmp/ directories and COPY INTO tables
        # Infers the schema based on the .csv, no need to map columns
        create_csv_table_dynamic(cursor, file_paths)
        
        cursor.close()
        snowflake_conn.close()
        logger.info("CSV data processing completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in CSV data processing: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

def ingest_covid_data():
    """
    Fetch CDC COVID data and save to MinIO (both raw JSON and flattened Parquet)
    """
    logger.info("Starting CDC COVID data ingestion...")
    
    try:
        # Import and run the COVID ingestion
        
        result = subprocess.run([
            sys.executable, 'src/api_ingestion/us_covid_ingestion_v2.py'
        ], capture_output=True, text=True, cwd='/workspaces/project3-de')
        
        if result.returncode == 0:
            logger.info("COVID data ingestion completed successfully")
            logger.info(f"Ingestion output: {result.stdout}")
            return True
        else:
            logger.error(f"COVID data ingestion failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Error in COVID data ingestion: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

def stage_covid_data():
    """
    Process COVID data from MinIO and stage to Snowflake bronze layer
    """
    logger.info("Starting COVID data staging...")
    
    try:
        # Import and run the COVID staging
        import subprocess
        import sys
        
        result = subprocess.run([
            sys.executable, 'src/data_staging_v2.py'
        ], capture_output=True, text=True, cwd='/workspaces/project3-de')
        
        if result.returncode == 0:
            logger.info("COVID data staging completed successfully")
            logger.info(f"Staging output: {result.stdout}")
            return True
        else:
            logger.error(f"COVID data staging failed: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Error in COVID data staging: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

def run_full_us_covid_ingestion():
    """
    Run the complete US COVID data ingestion: Ingestion -> Staging
    """
    logger.info("Starting full US COVID data ingestion pipeline...")

    # Step 1: Ingest COVID data from API to MinIO
    if not ingest_covid_data():
        logger.error("COVID ingestion failed, stopping pipeline")
        return False
    
    # Step 2: Stage COVID data from MinIO to Snowflake
    if not stage_covid_data():
        logger.error("COVID staging failed, pipeline incomplete")
        return False

    logger.info("Full US COVID data ingestion pipeline completed successfully")
    return True

def main():
    """
    Main entry point - orchestrates all data processing workflows
    """
    logger.info("=" * 60)
    logger.info("STARTING MAIN DATA PIPELINE")
    logger.info("=" * 60)
    
    # Track overall success
    overall_success = True
    
    # Workflow 1: Process CSV data (legacy workflow)
    logger.info("\n--- WORKFLOW 1: CSV Data Processing ---")
    csv_success = process_csv_data()
    if not csv_success:
        logger.warning("CSV data processing failed")
        overall_success = False
    
    # Workflow 2: COVID data pipeline (new workflow)
    logger.info("\n--- WORKFLOW 2: COVID Data Pipeline ---")
    covid_success = run_full_us_covid_ingestion()
    if not covid_success:
        logger.warning("COVID data pipeline failed")
        overall_success = False
    
    # Final summary
    logger.info("\n" + "=" * 60)
    if overall_success:
        logger.info("ALL WORKFLOWS COMPLETED SUCCESSFULLY")
    else:
        logger.warning("SOME WORKFLOWS FAILED - CHECK LOGS ABOVE")
    logger.info("=" * 60)
    
    return overall_success

def run_csv_only():
    """
    Convenience function to run only CSV workflow
    """
    logger.info("Running CSV workflow only...")
    return process_csv_data()

def run_covid_only():
    """
    Convenience function to run only COVID workflow
    """
    logger.info("Running COVID workflow only...")
    return run_full_us_covid_ingestion()

def run_covid_ingestion_only():
    """
    Convenience function to run only COVID ingestion (API -> MinIO)
    """
    logger.info("Running COVID ingestion only...")
    return ingest_covid_data()

def run_covid_staging_only():
    """
    Convenience function to run only COVID staging (MinIO -> Snowflake)
    """
    logger.info("Running COVID staging only...")
    return stage_covid_data()

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("Main pipeline execution completed successfully.")
    else:
        logger.error("Main pipeline execution completed with errors.")