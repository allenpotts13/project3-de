from dotenv import load_dotenv
from src.utils.logger import setup_logger
from src.api_ingestion.us_covid_ingestion_v2 import main as us_covid_ingestion_main
from src.data_staging_v2 import main as data_staging_main
from src.minio_dao.minio import create_minio_client, extract_and_process_csv_from_minio
from src.snowflake_dao.snowflake import create_snowflake_connection, upload_dataframes_to_snowflake

# Load environment variables from .env file
load_dotenv()

logger = setup_logger(__name__, "src/logs/main_pipeline.log")


def process_csv_data():
    logger.info("Starting CSV data processing workflow...")

    try:
        # List of CSV files to process
        filenames = [
            'ukhsa-coverage-report-2024-05-30.csv',
            'canada_antibody_seroprevalence_13100818.csv',
            'canada_demand_and_usage_13100838.csv'
        ]

        # Extract and process CSV files from MinIO using modern approach
        minio_client = create_minio_client()
        processed_data = extract_and_process_csv_from_minio(
            minio_client, filenames)

        # Upload processed DataFrames to Snowflake
        snowflake_conn = create_snowflake_connection()
        upload_dataframes_to_snowflake(snowflake_conn, processed_data)

        snowflake_conn.close()
        logger.info("CSV data processing completed successfully")
        return True

    except Exception as e:
        logger.error(f"Error in CSV data processing: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def ingest_us_covid_data():
    logger.info("Starting CDC US COVID data ingestion...")
    try:
        success = us_covid_ingestion_main()
        if success:
            logger.info("US COVID data ingestion completed successfully")
            return True
        else:
            logger.error("US COVID data ingestion failed")
            return False
    except Exception as e:
        logger.error(f"Error in US COVID data ingestion: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def stage_us_covid_data():
    logger.info("Starting US COVID data staging...")
    try:
        success = data_staging_main()
        if success:
            logger.info("US COVID data staging completed successfully")
            return True
        else:
            logger.error("US COVID data staging failed")
            return False
    except Exception as e:
        logger.error(f"Error in US COVID data staging: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def run_full_us_covid_ingestion():
    logger.info("Starting full US COVID data ingestion pipeline...")

    # Step 1: Ingest US COVID data from API to MinIO
    if not ingest_us_covid_data():
        logger.error("US COVID ingestion failed, stopping pipeline")
        return False

    # Step 2: Stage US COVID data from MinIO to Snowflake
    if not stage_us_covid_data():
        logger.error("US COVID staging failed, pipeline incomplete")
        return False

    logger.info("Full US COVID data ingestion pipeline completed successfully")
    return True


def main():
    logger.info("=" * 60)
    logger.info("STARTING MAIN DATA PIPELINE")
    logger.info("=" * 60)

    overall_success = True

    logger.info("\n--- WORKFLOW 1: CSV Data Processing ---")
    csv_success = process_csv_data()
    if not csv_success:
        logger.warning("CSV data processing failed")
        overall_success = False

    logger.info("\n--- WORKFLOW 2: COVID Data Pipeline ---")
    covid_success = run_full_us_covid_ingestion()
    if not covid_success:
        logger.warning("COVID data pipeline failed")
        overall_success = False

    logger.info("\n" + "=" * 60)
    if overall_success:
        logger.info("ALL WORKFLOWS COMPLETED SUCCESSFULLY")
    else:
        logger.warning("SOME WORKFLOWS FAILED - CHECK LOGS ABOVE")
    logger.info("=" * 60)

    return overall_success


if __name__ == "__main__":
    success = main()
    if success:
        logger.info("Main pipeline execution completed successfully.")
    else:
        logger.error("Main pipeline execution completed with errors.")
