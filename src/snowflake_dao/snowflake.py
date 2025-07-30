import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)

def create_snowflake_connection():
    """Create and return Snowflake connection"""
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        user=os.getenv('SNOWFLAKE_USER'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA_BRONZE')
    )

def upload_dataframes_to_snowflake(conn, processed_data):
    """
    Upload processed DataFrames to Snowflake tables
    
    Args:
        conn: Snowflake connection
        processed_data: dict of {table_name: dataframe}
    """
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA_BRONZE')
    
    for table_name, df in processed_data.items():
        try:
            logger.info(f"Uploading {table_name} to Snowflake: {len(df)} rows")
            
            # Use pandas write_pandas for efficient upload
            success, n_chunks, n_rows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name,
                database=database,
                schema=schema,
                auto_create_table=True,
                overwrite=True,
                quote_identifiers=True
            )
            
            if success:
                logger.info(f"Successfully uploaded {n_rows} rows to {table_name} in {n_chunks} chunks")
            else:
                logger.error(f"Failed to upload {table_name}")
                
        except Exception as e:
            logger.error(f"Error uploading {table_name}: {e}")
            raise