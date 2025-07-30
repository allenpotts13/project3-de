import os
import pandas as pd
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)

def create_minio_client():
    """Create and return MinIO client"""
    return Minio(
        os.getenv('MINIO_EXTERNAL_URL'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False
    )

def normalize_column_names(df):
    """Normalize DataFrame column names to uppercase with underscores"""
    df.columns = df.columns.str.upper().str.replace(' ', '_').str.replace('-', '_').str.replace('.', '_')
    return df

def extract_and_process_csv_from_minio(minio_client, filenames):
    """
    Extract CSV files from MinIO and return processed DataFrames
    
    Returns:
        dict: {table_name: processed_dataframe}
    """
    bucket_name = os.getenv('MINIO_BUCKET_NAME')
    processed_data = {}
    
    for filename in filenames:
        try:
            logger.info(f"Processing CSV file: {filename}")
            
            # Get file from MinIO
            response = minio_client.get_object(bucket_name, filename)
            
            # Read CSV into pandas DataFrame
            df = pd.read_csv(BytesIO(response.read()))
            
            # Normalize column names
            df = normalize_column_names(df)
            
            # Add metadata columns
            df['INGESTION_DATE'] = pd.Timestamp.now()
            df['FILENAME'] = filename
            
            # Create table name from filename (remove extension and sanitize)
            table_name = filename.replace('.csv', '').upper().replace('-', '_')
            
            processed_data[table_name] = df
            
            logger.info(f"Processed {filename}: {len(df)} rows, {len(df.columns)} columns")
            logger.info(f"Columns: {list(df.columns)}")
            
            response.close()
            response.release_conn()
            
        except S3Error as e:
            logger.error(f"MinIO error processing {filename}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            raise
    
    return processed_data