import os
import requests
import json
from dotenv import load_dotenv
from io import BytesIO
from minio import Minio
from minio.error import S3Error
import logging
import pandas as pd


load_dotenv()

US_COVID_ENDPOINT = os.getenv("US_COVID_ENDPOINT")

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

logging.basicConfig(
    level=logging.INFO,
    filename="./src/logs/us_covid_ingest.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def fetch_page():
    logging.info(f"Fetching page")
    params = {}
    try:
        response = requests.get(US_COVID_ENDPOINT, params=params)
        response.raise_for_status()
        response_json = response.json()
        covid_data = response_json.get("data", [])
        columns_meta = response_json.get("meta", {}).get(
            "view", {}).get("columns", [])
        column_names = [col.get("name") for col in columns_meta]

        if not covid_data or not column_names:
            logging.warning("No data or column names found in response.")
            return pd.DataFrame()

        covid_dataframe = pd.DataFrame(covid_data, columns=column_names)
        return covid_dataframe
    except requests.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return []


def upload_chunk_to_minio(dataframe):
    try:
        # Convert Python data to JSON string and encode it as bytes
        json_data = dataframe.to_json(orient="records")
        byte_data = BytesIO(json_data.encode('utf-8'))

        object_name = f"us_covid_data.json"

        # Create bucket if it doesn't exist
        if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
            logging.info(
                f"Bucket '{MINIO_BUCKET_NAME}' does not exist. Creating it.")
            minio_client.make_bucket(MINIO_BUCKET_NAME)

        # Upload to MinIO directly from memory
        minio_client.put_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            data=byte_data,
            length=len(json_data.encode('utf-8')),
            content_type="application/json"
        )

        logging.info(f"Uploaded to MinIO as '{object_name}'")

    except S3Error as e:
        logging.error(f"MinIO S3Error while uploading: {e}")
        raise
    except Exception as e:
        logging.error(f"General error while uploading: {e}")
        raise


def main():
    try:
        covid_dataframe = fetch_page()

        upload_chunk_to_minio(covid_dataframe)
        logging.info(f"Successfully upload chunk to MinIO")

    except Exception as e:
        logging.error(f"Failed to upload: {e}")


if __name__ == "__main__":
    main()
