import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def create_snowflake_connection():
    snowflake_connection = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        user=os.getenv('SNOWFLAKE_USER'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA_BRONZE')     
    )

    return snowflake_connection

def create_csv_table_dynamic(cursor, csv_file_paths):
    cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA_BRONZE')};")

    for table_name, file_path in csv_file_paths.items():
        cursor.execute(f"CREATE OR REPLACE TEMPORARY STAGE temp_csv_stage")
        
        # Single file format for both inference and loading
        cursor.execute(f"""
            CREATE OR REPLACE FILE FORMAT temp_csv_format
            TYPE = 'CSV'
            PARSE_HEADER = TRUE
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            TRIM_SPACE = TRUE
        """)
        
        cursor.execute(f"PUT 'file://{file_path}' @temp_csv_stage AUTO_COMPRESS=FALSE")
        
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {table_name}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(INFER_SCHEMA(
                    LOCATION => '@temp_csv_stage',
                    FILE_FORMAT => 'temp_csv_format'
                ))
            )
        """)
        
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM @temp_csv_stage
            FILE_FORMAT = 'temp_csv_format'
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)
        
        cursor.execute(f"""
            ALTER TABLE {table_name} 
            ADD COLUMN ingestion_date TIMESTAMP_NTZ
        """)
        
        cursor.execute(f"""
            ALTER TABLE {table_name} 
            ADD COLUMN filename STRING
        """)
        
        cursor.execute(f"""
            UPDATE {table_name} 
            SET 
                ingestion_date = CURRENT_TIMESTAMP(),
                filename = '{os.path.basename(file_path)}'
        """)
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"Loaded {row_count} rows into table {table_name}")