import os
from dotenv import load_dotenv
import logging
from minio import Minio
from minio.error import S3Error
import pandas as pd
from datetime import datetime, timezone
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from io import BytesIO
load_dotenv

logging.basicConfig(
    level=logging.INFO,
    filename="./src/logs/data_staging.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA_BRONZE = os.getenv("SNOWFLAKE_SCHEMA_BRONZE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
RAW_TABLE_NAME = os.getenv("RAW_TABLE_NAME")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_EXTERNAL_URL = os.getenv("MINIO_EXTERNAL_URL")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

minio_client = Minio(
    MINIO_EXTERNAL_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA_BRONZE,
    role=SNOWFLAKE_ROLE,
)


def main():
    us_covid_data = []
    for obj in minio_client.list_objects(MINIO_BUCKET_NAME, recursive=False):
        if obj.object_name.endswith(".json"):
            try:
                minio_response = minio_client.get_object(
                    MINIO_BUCKET_NAME, obj.object_name)
                json_bytes_io = BytesIO(minio_response.read())
                minio_response.close()
                minio_response.release_conn()
                logging.info(f"Downloaded {obj.object_name} from Minio")
                file_dataframe = pd.read_json(json_bytes_io)

                file_dataframe['SOURCE_FILE'] = obj.object_name
                file_dataframe["LOAD_TIMESTAMP_UTC"] = datetime.now(
                    timezone.utc)
                file_dataframe.columns = file_dataframe.columns.str.upper()
                us_covid_data.append(file_dataframe)
                logging.info(
                    f"Added {len(file_dataframe)} rows from {obj.object_name} to queue")
                us_covid_data = pd.concat(us_covid_data, ignore_index=True)
            except S3Error as S3e:
                logging.error(f"Minio error for {obj.object_name}: {S3e}")
            except Exception as e:
                logging.error(
                    f"An unexpected error occurred processing {obj.object_name}: {e}")
    if not us_covid_data:
        logging.warning(f"No files found!")
    us_covid_data_raw = pd.concat(us_covid_data, ignore_index=True)
    logging.info(f"Concatenated {len(us_covid_data_raw)} rows for upload")

    success, n_chunks, n_rows, _ = write_pandas(
        conn=conn,
        df=us_covid_data_raw,
        table_name=RAW_TABLE_NAME,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA_BRONZE,
        auto_create_table=True,
        overwrite=True,
        use_logical_type=True,
        quote_identifiers=True
    )
    if success:
        logging.info(
            f"Sucessfully uploaded {n_rows} rows in {n_chunks} chunks to {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA_BRONZE}.{RAW_TABLE_NAME}")
    else:
        logging.error(f"Failed to upload data to Snowflake via write_pandas")
    conn.close()


if __name__ == "__main__":
    main()
