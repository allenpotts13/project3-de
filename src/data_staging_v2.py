import os
import json
from io import BytesIO
from minio import Minio
from minio.error import S3Error
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime, timezone
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from src.utils.logger import setup_logger

load_dotenv()

logger = setup_logger(__name__, "src/logs/data_staging_v2.log")


def get_env_var(name, required=True):
    value = os.getenv(name)
    if required and not value:
        logger.error(f"Environment variable {name} is not set.")
        exit(1)
    return value


def process_raw_json_files(minio_client, bucket_name):
    raw_data = []
    objects_to_process = minio_client.list_objects(
        bucket_name, prefix='covid_data_raw', recursive=True)

    for obj in objects_to_process:
        if obj.object_name.endswith(".json"):
            logger.info(f"Processing raw JSON file: {obj.object_name}...")

            try:
                minio_response = minio_client.get_object(
                    bucket_name, obj.object_name)
                json_content = minio_response.read().decode('utf-8')
                minio_response.close()
                minio_response.release_conn()
                logger.info(f"Downloaded {obj.object_name} from MinIO.")

                # Parse JSON to get the raw data structure
                json_data = json.loads(json_content)

                # Extract metadata from the COVID dataset
                dataset_id = ''
                dataset_name = ''
                record_count = 0

                if isinstance(json_data, dict) and 'meta' in json_data:
                    meta_info = json_data.get('meta', {}).get('view', {})
                    dataset_id = meta_info.get('id', '')
                    dataset_name = meta_info.get('name', '')
                    if 'data' in json_data:
                        record_count = len(json_data['data'])
                elif isinstance(json_data, list):
                    dataset_id = 'yrur-wghw'
                    dataset_name = 'Provisional COVID-19 death counts and rates'
                    record_count = len(json_data)

                # Create a row with the raw JSON structure (truncated for Snowflake)
                json_preview = json_content[:10000] + \
                    "..." if len(json_content) > 10000 else json_content

                raw_row = {
                    'DATASET_ID': dataset_id,
                    'DATASET_NAME': dataset_name,
                    'RECORD_COUNT': record_count,
                    # Sample of first 5 records
                    'DATA_STRUCTURE': json.dumps(json_data.get('data', json_data)[:5] if record_count > 0 else []),
                    'SOURCE_FILE': obj.object_name,
                    'LOAD_TIMESTAMP_UTC': datetime.now(timezone.utc),
                    'JSON_SIZE_BYTES': len(json_content),
                    'RAW_JSON_PREVIEW': json_preview  # Store truncated JSON preview
                }

                raw_data.append(raw_row)
                logger.info(
                    f"Added raw data from {obj.object_name} ({record_count} records)")

            except json.JSONDecodeError as json_err:
                logger.error(
                    f"JSON decode error for {obj.object_name}: {json_err}")
            except S3Error as s3_err:
                logger.error(f"MinIO error for {obj.object_name}: {s3_err}")
            except Exception as e:
                logger.error(
                    f"An unexpected error occurred while processing {obj.object_name}: {e}")
        else:
            logger.info(f"Skipping non-JSON object: {obj.object_name}")

    if raw_data:
        df = pd.DataFrame(raw_data)
        logger.info(f"Created raw data DataFrame with {len(df)} records")
        return df
    else:
        logger.warning("No raw JSON files found or processed")
        return pd.DataFrame()


def process_flattened_parquet_files(minio_client, bucket_name):
    flattened_data = []
    objects_to_process = list(minio_client.list_objects(
        bucket_name, prefix='covid_data_flattened', recursive=True))
    parquet_objects = [
        obj for obj in objects_to_process if obj.object_name.endswith(".parquet")]

    if not parquet_objects:
        logger.info("No parquet files found to process")
        return pd.DataFrame()

    # Find the latest parquet file by last_modified timestamp
    latest_obj = max(parquet_objects, key=lambda x: x.last_modified)
    file_name = latest_obj.object_name

    logger.info(f"Processing latest parquet file: {file_name}")

    try:
        minio_response = minio_client.get_object(bucket_name, file_name)
        parquet_bytes_io = BytesIO(minio_response.read())
        minio_response.close()
        minio_response.release_conn()
        logger.info(f"Downloaded {file_name} from MinIO.")

        df = pd.read_parquet(parquet_bytes_io)

        # Add metadata columns
        df['SOURCE_FILE'] = file_name
        df['LOAD_TIMESTAMP_UTC'] = datetime.now(timezone.utc)

        # Uppercase column names for Snowflake
        df.columns = df.columns.str.upper()

        flattened_data.append(df)
        logger.info(
            f"Added {len(df)} records from {file_name} to flattened data")

    except Exception as e:
        logger.error(
            f"An unexpected error occurred while processing {file_name}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

    # Combine flattened data (only one file in this case)
    if flattened_data:
        combined_df = pd.concat(flattened_data, ignore_index=True)
        logger.info(
            f"Created flattened data DataFrame with {len(combined_df)} total records from latest file")
    else:
        combined_df = pd.DataFrame()

    return combined_df


def upload_to_snowflake(conn, df, table_name, database, schema):
    if df.empty:
        logger.warning(f"No data to upload to {table_name}")
        return False

    try:
        # Reset index to avoid pandas warning
        df_reset = df.reset_index(drop=True)

        success, n_chunks, n_rows, _ = write_pandas(
            conn=conn,
            df=df_reset,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=True,  # Use overwrite mode for raw data
            quote_identifiers=True,
            use_logical_type=True
        )

        if success:
            logger.info(
                f"Successfully uploaded {n_rows} rows in {n_chunks} chunks to Snowflake table {table_name}.")
            return True
        else:
            logger.error(
                f"Failed to upload data to Snowflake table {table_name}.")
            return False

    except Exception as e:
        logger.error(f"Error uploading to {table_name}: {e}")
        return False


def main():
    logger.info("Starting COVID data staging process...")

    # Load environment variables
    MINIO_URL_HOST_PORT = get_env_var("MINIO_EXTERNAL_URL")
    MINIO_ACCESS_KEY = get_env_var("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = get_env_var("MINIO_SECRET_KEY")
    MINIO_ZONING_BUCKET_NAME = get_env_var("MINIO_BUCKET_NAME")

    SNOWFLAKE_USER = get_env_var("SNOWFLAKE_USER")
    SNOWFLAKE_PASSWORD = get_env_var("SNOWFLAKE_PASSWORD")
    SNOWFLAKE_ACCOUNT = get_env_var("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_WAREHOUSE = get_env_var("SNOWFLAKE_WAREHOUSE")
    SNOWFLAKE_DATABASE = get_env_var("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA = get_env_var("SNOWFLAKE_SCHEMA_BRONZE")
    SNOWFLAKE_ROLE = get_env_var("SNOWFLAKE_ROLE")

    # Table names
    RAW_TABLE_NAME = "US_COVID_RAW_DATA"
    FLATTENED_TABLE_NAME = "US_COVID_FLATTENED_DATA"

    minio_client = None
    conn = None

    try:
        # Initialize MinIO client
        minio_client = Minio(
            MINIO_URL_HOST_PORT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        logger.info("Connected to MinIO.")

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        snowflake_cursor = conn.cursor()
        logger.info("Connected to Snowflake.")

        # Process raw JSON files
        logger.info("Processing raw JSON files...")
        raw_df = process_raw_json_files(minio_client, MINIO_ZONING_BUCKET_NAME)

        # Process flattened Parquet files (no SQL - just process all files)
        logger.info("Processing flattened Parquet files...")
        flattened_df = process_flattened_parquet_files(
            minio_client,
            MINIO_ZONING_BUCKET_NAME
        )

        # Upload raw data to Snowflake (using overwrite for raw data)
        if not raw_df.empty:
            logger.info(f"Uploading raw data to {RAW_TABLE_NAME}...")
            upload_to_snowflake(conn, raw_df, RAW_TABLE_NAME,
                                SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)

        # Upload flattened data to Snowflake (using overwrite to replace all data)
        if not flattened_df.empty:
            business_columns = [col for col in flattened_df.columns if col not in [
                'SOURCE_FILE', 'LOAD_TIMESTAMP_UTC']]
            initial_count = len(flattened_df)
            flattened_df = flattened_df.drop_duplicates(
                subset=business_columns, keep='first')
            logger.info(
                f"Deduplicated flattened data: {initial_count} → {len(flattened_df)} records")
            logger.info(
                f"Uploading {len(flattened_df)} records to {FLATTENED_TABLE_NAME}...")
            upload_to_snowflake(
                conn, flattened_df, FLATTENED_TABLE_NAME, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)
        else:
            logger.info("No flattened files found to process")

        if raw_df.empty and flattened_df.empty:
            logger.warning("No data files found to process.")
        return True

    except snowflake.connector.errors.ProgrammingError as e:
        logger.error(f"Snowflake SQL Error: {e.msg} (Code: {e.sfqid})")
        if hasattr(e, 'sql') and e.sql:
            logger.error(f"Failed SQL: {e.sql}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False
    finally:
        if 'snowflake_cursor' in locals() and snowflake_cursor:
            snowflake_cursor.close()
        if conn:
            conn.close()
            logger.info("Snowflake connection closed.")


if __name__ == "__main__":
    main()
