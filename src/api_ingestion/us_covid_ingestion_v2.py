import os
import requests
import json
from io import BytesIO
from minio import Minio
from minio.error import S3Error
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Create logs directory if it doesn't exist
    log_directory = 'src/logs'
    os.makedirs(log_directory, exist_ok=True)
    log_file = os.path.join(log_directory, 'us_covid_ingest.log')

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

def get_cdc_covid_data():
    """
    Fetch CDC COVID-19 data from the API
    """
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
        logger.info(f"Successfully fetched data from CDC COVID API")
        
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
    """
    Convert data to parquet format and upload to MinIO - following Xbox pattern
    """
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

        # 1. SAVE RAW JSON DATA (completely unchanged)
        raw_object_name = f'covid_data_raw/us_covid_raw_{timestamp}.json'
        
        # Keep data completely raw - just serialize to JSON string
        json_string = json.dumps(data, indent=2)
        json_bytes = json_string.encode('utf-8')
        
        # Create BytesIO buffer for raw data
        raw_buffer = BytesIO(json_bytes)
        raw_file_size = len(json_bytes)
        
        logger.info(f"Created raw JSON file in memory, size: {raw_file_size} bytes")

        # Upload raw JSON to MinIO
        minio_client.put_object(
            bucket_name, 
            raw_object_name, 
            raw_buffer, 
            raw_file_size,
            content_type='application/json'
        )
        logger.info(f"Raw JSON data saved to {bucket_name}/{raw_object_name} successfully.")

        # 2. SAVE FLATTENED PARQUET DATA
        flattened_object_name = f'covid_data_flattened/us_covid_flattened_{timestamp}.parquet'
        
        # Handle CDC COVID API specific structure for flattening
        if isinstance(data, dict) and 'meta' in data and 'data' in data:
            # Full metadata format (from rows.json endpoint)
            meta_info = data.get('meta', {}).get('view', {})
            dataset_id = meta_info.get('id', '')
            dataset_name = meta_info.get('name', '')
            
            # Extract the data array - this is what we want as individual rows
            data_rows = data['data']
            
            if isinstance(data_rows, list) and len(data_rows) > 0:
                # Create structured records from the data rows
                processed_records = []
                
                for row_data in data_rows:
                    # Map data based on column positions (starting from index 8 for actual data)
                    data_start_index = 8
                    
                    if len(row_data) > data_start_index:
                        processed_record = {
                            'dataset_id': dataset_id,
                            'dataset_name': dataset_name,
                            'data_as_of': row_data[data_start_index] if len(row_data) > data_start_index else None,
                            'jurisdiction_residence': row_data[data_start_index + 1] if len(row_data) > data_start_index + 1 else None,
                            'year': row_data[data_start_index + 2] if len(row_data) > data_start_index + 2 else None,
                            'month': row_data[data_start_index + 3] if len(row_data) > data_start_index + 3 else None,
                            'demographic_group': row_data[data_start_index + 4] if len(row_data) > data_start_index + 4 else None,
                            'subgroup1': row_data[data_start_index + 5] if len(row_data) > data_start_index + 5 else None,
                            'subgroup2': row_data[data_start_index + 6] if len(row_data) > data_start_index + 6 else None,
                            'covid_deaths': row_data[data_start_index + 7] if len(row_data) > data_start_index + 7 else None,
                            'crude_covid_rate': row_data[data_start_index + 8] if len(row_data) > data_start_index + 8 else None,
                            'aa_covid_rate': row_data[data_start_index + 9] if len(row_data) > data_start_index + 9 else None,
                            'crude_covid_rate_ann': row_data[data_start_index + 10] if len(row_data) > data_start_index + 10 else None,
                            'aa_covid_rate_ann': row_data[data_start_index + 11] if len(row_data) > data_start_index + 11 else None,
                            'footnote': row_data[data_start_index + 12] if len(row_data) > data_start_index + 12 else None
                        }
                        processed_records.append(processed_record)
                
                # Convert to DataFrame
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
            # Fallback: normalize the entire structure
            df = pd.json_normalize(data, sep='_')
            logger.info(f"Normalized entire data structure with {len(df)} records")
        
        if not df.empty:
            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_api'] = 'data.cdc.gov/api/views/yrur-wghw/rows.json'
            
            logger.info(f"Final flattened DataFrame has {len(df)} records and {len(df.columns)} columns")
            logger.info(f"DataFrame columns: {list(df.columns)}")
            
            # Convert DataFrame to Parquet bytes
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_bytes = parquet_buffer.getvalue()
            parquet_buffer.close()

            # Create BytesIO buffer for upload
            upload_buffer = BytesIO(parquet_bytes)
            file_size = len(parquet_bytes)
            
            logger.info(f"Created flattened parquet file in memory, size: {file_size} bytes")

            # Upload flattened Parquet to MinIO
            minio_client.put_object(
                bucket_name, 
                flattened_object_name, 
                upload_buffer, 
                file_size,
                content_type='application/octet-stream'
            )
            logger.info(f"Flattened Parquet data saved to {bucket_name}/{flattened_object_name} successfully.")
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
        # Fetch data from CDC COVID API
        data = get_cdc_covid_data()
        if not data:
            logger.warning("No COVID data found.")
            return False

        # Set up MinIO parameters
        bucket_name = os.getenv('MINIO_BUCKET_NAME', 'project3-bronze')
        
        # Save both raw and flattened data to MinIO
        success, raw_file, flattened_file = save_to_parquet_and_upload(data, bucket_name)
        
        if success:
            logger.info("CDC COVID data extraction completed successfully.")
            logger.info(f"Raw JSON data saved to MinIO bucket '{bucket_name}' as '{raw_file}'")
            logger.info(f"Flattened Parquet data saved to MinIO bucket '{bucket_name}' as '{flattened_file}'")
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
    if success:
        logger.info("Process finished successfully.")
    else:
        logger.error("Process finished with errors.")
