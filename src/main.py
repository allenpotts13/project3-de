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