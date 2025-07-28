import os
import logging
import json
import uuid
from io import BytesIO
from minio import Minio
from minio.error import S3Error
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime, timezone
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

LOG_DIR = 'src/logs'
LOG_FILE = os.path.join(LOG_DIR, 'data_staging.log')

logger = logging.getLogger("data_staging")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

def get_env_var(name, required=True):
    """
    Get environment variable with optional requirement check.
    """
    value = os.getenv(name)
    if required and not value:
        logger.error(f"Environment variable {name} is not set.")
        exit(1)
    return value

def process_cdc_covid_dataset(json_data):
    """
    Process CDC COVID dataset JSON structure and extract rows with proper column mapping
    """
    processed_rows = []
    
    # Extract metadata
    meta_info = json_data.get('meta', {}).get('view', {})
    dataset_id = meta_info.get('id', '')
    dataset_name = meta_info.get('name', '')
    
    # Extract column information for proper mapping
    columns = meta_info.get('columns', [])
    column_mapping = {}
    
    # Create mapping of position to field name (skip meta columns with position 0)
    for col in columns:
        if col.get('position', 0) > 0:
            field_name = col.get('fieldName', '').upper()
            position = col.get('position', 0)
            column_mapping[position] = field_name
    
    # Process data rows
    data_rows = json_data.get('data', [])
    
    for row_data in data_rows:
        # Start with basic metadata
        processed_row = {
            'DATASET_ID': dataset_id,
            'DATASET_NAME': dataset_name,
            'LOAD_TIMESTAMP_UTC': datetime.now(timezone.utc)
        }
        
        # Map data based on column positions (starting from index 8 for actual data)
        # The first 8 elements are meta fields that we skip
        data_start_index = 8
        
        if len(row_data) > data_start_index:
            processed_row.update({
                'DATA_AS_OF': row_data[data_start_index] if len(row_data) > data_start_index else None,
                'JURISDICTION_RESIDENCE': row_data[data_start_index + 1] if len(row_data) > data_start_index + 1 else None,
                'YEAR': row_data[data_start_index + 2] if len(row_data) > data_start_index + 2 else None,
                'MONTH': row_data[data_start_index + 3] if len(row_data) > data_start_index + 3 else None,
                'DEMOGRAPHIC_GROUP': row_data[data_start_index + 4] if len(row_data) > data_start_index + 4 else None,
                'SUBGROUP1': row_data[data_start_index + 5] if len(row_data) > data_start_index + 5 else None,
                'SUBGROUP2': row_data[data_start_index + 6] if len(row_data) > data_start_index + 6 else None,
                'COVID_DEATHS': row_data[data_start_index + 7] if len(row_data) > data_start_index + 7 else None,
                'CRUDE_COVID_RATE': row_data[data_start_index + 8] if len(row_data) > data_start_index + 8 else None,
                'AA_COVID_RATE': row_data[data_start_index + 9] if len(row_data) > data_start_index + 9 else None,
                'CRUDE_COVID_RATE_ANN': row_data[data_start_index + 10] if len(row_data) > data_start_index + 10 else None,
                'AA_COVID_RATE_ANN': row_data[data_start_index + 11] if len(row_data) > data_start_index + 11 else None,
                'FOOTNOTE': row_data[data_start_index + 12] if len(row_data) > data_start_index + 12 else None
            })
        
        processed_rows.append(processed_row)
    
    return processed_rows

def process_covid_data_files(minio_client, bucket_name):
    """
    Process COVID-19 death count data files from MinIO and return DataFrame for raw data table
    """
    raw_data = []
    objects_to_process = minio_client.list_objects(bucket_name, prefix='covid_data_raw', recursive=True)
    
    for obj in objects_to_process:
        if obj.object_name.endswith(".json"):
            logger.info(f"Processing COVID data file: {obj.object_name}...")
            
            try:
                minio_response = minio_client.get_object(bucket_name, obj.object_name)
                json_content = minio_response.read().decode('utf-8')
                minio_response.close()
                minio_response.release_conn()
                logger.info(f"Downloaded {obj.object_name} from MinIO.")
                
                # Parse JSON to get the COVID data structure
                json_data = json.loads(json_content)
                
                # Process the CDC dataset structure
                processed_rows = process_cdc_covid_dataset(json_data)
                
                # Add source file and raw JSON data to each row
                for row in processed_rows:
                    row['SOURCE_FILE'] = obj.object_name
                    row['RAW_JSON_DATA'] = json_content
                    raw_data.append(row)
                
                logger.info(f"Added {len(processed_rows)} COVID data records from {obj.object_name}")
                
            except json.JSONDecodeError as json_err:
                logger.error(f"JSON decode error for {obj.object_name}: {json_err}")
            except S3Error as s3_err:
                logger.error(f"MinIO error for {obj.object_name}: {s3_err}")
            except Exception as e:
                logger.error(f"An unexpected error occurred while processing {obj.object_name}: {e}")
        else:
            logger.info(f"Skipping non-JSON object: {obj.object_name}")
    
    if raw_data:
        df = pd.DataFrame(raw_data)
        logger.info(f"Created COVID data DataFrame with {len(df)} records")
        return df
    else:
        logger.warning("No COVID data files found or processed")
        return pd.DataFrame()


def upload_to_snowflake_bronze(df, table_name="COVID_RAW_DATA"):
    """
    Upload raw COVID data DataFrame to Snowflake bronze layer
    """
    if df.empty:
        logger.warning("DataFrame is empty, nothing to upload to Snowflake")
        return False
    
    try:
        # Get Snowflake connection parameters
        connection_params = {
            'account': get_env_var('SNOWFLAKE_ACCOUNT'),
            'user': get_env_var('SNOWFLAKE_USER'),
            'password': get_env_var('SNOWFLAKE_PASSWORD'),
            'database': get_env_var('SNOWFLAKE_DATABASE'),
            'schema': get_env_var('SNOWFLAKE_SCHEMA_BRONZE'),
            'warehouse': get_env_var('SNOWFLAKE_WAREHOUSE'),
            'role': get_env_var('SNOWFLAKE_ROLE')
        }
        
        logger.info(f"Connecting to Snowflake bronze schema: {connection_params['schema']}")
        
        # Establish connection
        conn = snowflake.connector.connect(**connection_params)
        
        # Upload DataFrame to Snowflake (this preserves all raw data)
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            database=connection_params['database'],
            schema=connection_params['schema'],
            auto_create_table=True,
            overwrite=False  # Set to True if you want to replace existing data
        )
        
        conn.close()
        
        if success:
            logger.info(f"Successfully uploaded {nrows} rows to Snowflake table {table_name}")
            return True
        else:
            logger.error(f"Failed to upload data to Snowflake table {table_name}")
            return False
            
    except Exception as e:
        logger.error(f"Error uploading to Snowflake: {e}")
        return False

def main():
    """
    Main function to orchestrate the raw data processing and upload to bronze layer
    """
    try:
        # Initialize MinIO client
        minio_client = Minio(
            get_env_var('MINIO_URL'),
            access_key=get_env_var('MINIO_ACCESS_KEY'),
            secret_key=get_env_var('MINIO_SECRET_KEY'),
            secure=False
        )
        bucket_name = get_env_var('MINIO_BUCKET_NAME')
        
        logger.info("Starting COVID data processing for bronze layer...")
        
        # Option 1: Process files from MinIO
        df_minio = process_covid_data_files(minio_client, bucket_name)
        
        # Use MinIO data or combine both sources
        final_df = df_minio
        
        if not final_df.empty:
            # Upload raw data to Snowflake bronze layer
            success = upload_to_snowflake_bronze(final_df, "COVID_RAW_DATA_BRONZE")
            
            if success:
                logger.info("COVID raw data successfully staged to bronze layer")
            else:
                logger.error("Failed to upload COVID raw data to bronze layer")
        else:
            logger.warning("No COVID data to process")
            
    except Exception as e:
        logger.error(f"Error in main processing: {e}")

if __name__ == "__main__":
    main()

