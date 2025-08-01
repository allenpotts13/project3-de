import os
import requests
import json
from io import BytesIO
from minio import Minio
from minio.error import S3Error
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from src.utils.logger import setup_logger

# Load environment variables from .env file
load_dotenv()

logger = setup_logger(__name__, "src/logs/us_covid_ingestion.log")

def get_cdc_covid_data():
    ENDPOINT = os.getenv('US_COVID_ENDPOINT')

    # Validate environment variables
    if not ENDPOINT:
        logger.error("US_COVID_ENDPOINT environment variable is not set.")
        raise ValueError("US_COVID_ENDPOINT environment variable is not set.")

    logger.info(f"Making request to: {ENDPOINT}")

    try:
        response = requests.get(ENDPOINT)
        response.raise_for_status()
        data = response.json()
        logger.info("Successfully fetched data from CDC COVID API")

        # Log some basic info about the data
        if 'data' in data:
            logger.info(f"Retrieved {len(data['data'])} records from CDC API")
        elif isinstance(data, list):
            logger.info(f"Retrieved {len(data)} records from CDC API")
        else:
            logger.info("Retrieved data from CDC API (structure unknown)")

        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from CDC COVID API: {e}")
        raise


def save_to_parquet_and_upload(data, bucket_name):
    # Get MinIO configuration
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
    MINIO_URL_HOST_PORT = os.getenv('MINIO_EXTERNAL_URL')

    # Validate MinIO environment variables
    if not MINIO_ACCESS_KEY:
        logger.error("MINIO_ACCESS_KEY environment variable is not set.")
        raise ValueError("MINIO_ACCESS_KEY environment variable is not set.")
    if not MINIO_SECRET_KEY:
        logger.error("MINIO_SECRET_KEY environment variable is not set.")
        raise ValueError("MINIO_SECRET_KEY environment variable is not set.")
    if not MINIO_URL_HOST_PORT:
        logger.error("MINIO_EXTERNAL_URL environment variable is not set.")
        raise ValueError("MINIO_EXTERNAL_URL environment variable is not set.")

    try:
        # Initialize MinIO client
        minio_client = Minio(
            MINIO_URL_HOST_PORT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        logger.info(f"Connected to MinIO at {MINIO_URL_HOST_PORT}")

        # Check and create bucket if it doesn't exist
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        else:
            logger.info(f"Bucket {bucket_name} already exists")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # SAVE RAW JSON DATA (completely unchanged)
        raw_object_name = f'covid_data_raw/us_covid_raw_{timestamp}.json'

        # Keep data completely raw
        json_string = json.dumps(data, indent=2)
        json_bytes = json_string.encode('utf-8')

        raw_buffer = BytesIO(json_bytes)
        raw_file_size = len(json_bytes)

        # Upload raw JSON to MinIO
        minio_client.put_object(
            bucket_name,
            raw_object_name,
            raw_buffer,
            raw_file_size,
            content_type='application/json'
        )
        logger.info(
            f"Raw JSON data saved to {bucket_name}/"
            f"{raw_object_name} successfully."
        )

        # 2. SAVE FLATTENED PARQUET DATA
        flattened_object_name = (
            f'covid_data_flattened/us_covid_flattened_{timestamp}.parquet'
        )

        # Handle CDC COVID API specific structure for flattening
        if isinstance(data, dict) and 'meta' in data and 'data' in data:
            # Full metadata format (from rows.json endpoint)
            meta_info = data.get('meta', {}).get('view', {})
            dataset_id = meta_info.get('id', '')
            dataset_name = meta_info.get('name', '')

            # Extract the data array
            data_rows = data['data']
            if isinstance(data_rows, list) and len(data_rows) > 0:
                # Create structured records from the data rows
                processed_records = []

                for row_data in data_rows:
                    # Map data based on column positions
                    # (starting from index 8 for actual data)
                    data_start_index = 8

                    if len(row_data) > data_start_index:
                        processed_record = {
                            'dataset_id': dataset_id,
                            'dataset_name': dataset_name,
                            'data_as_of': (
                                row_data[data_start_index]
                                if len(row_data) > data_start_index else None
                            ),
                            'jurisdiction_residence': (
                                row_data[data_start_index + 1]
                                if len(row_data) > data_start_index + 1 else None
                            ),
                            'year': (
                                row_data[data_start_index + 2]
                                if len(row_data) > data_start_index + 2 else None
                            ),
                            'month': (
                                row_data[data_start_index + 3]
                                if len(row_data) > data_start_index + 3 else None
                            ),
                            'demographic_group': (
                                row_data[data_start_index + 4]
                                if len(row_data) > data_start_index + 4 else None
                            ),
                            'subgroup1': (
                                row_data[data_start_index + 5]
                                if len(row_data) > data_start_index + 5 else None
                            ),
                            'subgroup2': (
                                row_data[data_start_index + 6]
                                if len(row_data) > data_start_index + 6 else None
                            ),
                            'covid_deaths': (
                                row_data[data_start_index + 7]
                                if len(row_data) > data_start_index + 7 else None
                            ),
                            'crude_covid_rate': (
                                row_data[data_start_index + 8]
                                if len(row_data) > data_start_index + 8 else None
                            ),
                            'aa_covid_rate': (
                                row_data[data_start_index + 9]
                                if len(row_data) > data_start_index + 9 else None
                            ),
                            'crude_covid_rate_ann': (
                                row_data[data_start_index + 10]
                                if len(row_data) > data_start_index + 10 else None
                            ),
                            'aa_covid_rate_ann': (
                                row_data[data_start_index + 11]
                                if len(row_data) > data_start_index + 11 else None
                            ),
                            'footnote': (
                                row_data[data_start_index + 12]
                                if len(row_data) > data_start_index + 12 else None
                            ),
                        }
                        processed_records.append(processed_record)


                df = pd.DataFrame(processed_records)
                logger.info(f"Extracted and flattened {len(df)} COVID records from API response")

            else:
                logger.warning("No data rows found in API response")
                df = pd.DataFrame()
                
        elif isinstance(data, list):
            # If data is already a list, convert directly to DataFrame
            df = pd.json_normalize(data, sep='_')
            logger.info(f"Converted list data to DataFrame with {len(df)} records")
        else:
            df = pd.json_normalize(data, sep='_')
            logger.info(f"Normalized entire data structure with {len(df)} records")

        if not df.empty:
            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_api'] = 'data.cdc.gov/api/views/yrur-wghw/rows.json'
            
            logger.info(
                f"Final flattened DataFrame has {len(df)} records"
                f" and {len(df.columns)} columns"
            )
            logger.info(f"DataFrame columns: {list(df.columns)}")

            # Convert DataFrame to Parquet bytes
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_bytes = parquet_buffer.getvalue()
            parquet_buffer.close()

            # Create BytesIO buffer for upload
            upload_buffer = BytesIO(parquet_bytes)
            file_size = len(parquet_bytes)
            
            # Upload flattened Parquet to MinIO
            minio_client.put_object(
                bucket_name,
                flattened_object_name,
                upload_buffer,
                file_size,
                content_type='application/octet-stream'
            )
            logger.info(
                f"Flattened Parquet data saved to {bucket_name}/"
                f"{flattened_object_name} successfully."
            )
        else:
            logger.warning("No flattened data to save - DataFrame is empty")

        return True, raw_object_name, flattened_object_name

    except S3Error as e:
        logger.error(f"Error saving data to MinIO: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in save_to_parquet_and_upload: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise


def main():
    logger.info("Starting CDC COVID data extraction.")

    try:
        data = get_cdc_covid_data()
        if not data:
            logger.warning("No COVID data found.")
            return False

        bucket_name = os.getenv('MINIO_BUCKET_NAME', 'project3-bronze')
        success, raw_file, flattened_file = save_to_parquet_and_upload(data, bucket_name)

        if success:
            logger.info("CDC COVID data extraction completed successfully.")
            return True
        else:
            logger.error("Failed to save data to MinIO")
            return False
    
    except Exception as e:
        logger.error(f"An error occurred in main: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = main()
