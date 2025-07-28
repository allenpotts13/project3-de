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
    log_file = os.path.join(log_directory, 'xbox.log')

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

## Andrew 7/28

## Calls methods from minio_dao/minio.py to extract .csv from MinIO
# This stores the .csvs extracted from minio into a /tmp/ directory within the container for Snowflake to pick up and COPY INTO
filenames = ['canada_antibody_seroprevalence_13100818.csv', 'canada_demand_and_usage_13100838.csv']
minio_client = create_minio_client()
file_paths = extract_data_from_minio(minio_client, filenames)

snowflake_conn = create_snowflake_connection()
cursor = snowflake_conn.cursor()

## Calls methods from snowflake_dao/snowflake.py to retrieve files from /tmp/ directories and COPY INTO tables
# Infers the schema based on the .csv, no need to map columns
create_csv_table_dynamic(cursor, file_paths)






