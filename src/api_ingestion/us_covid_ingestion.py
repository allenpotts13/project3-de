import os
import requests
import json
from datetime import datetime
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
        # Return the full response to preserve metadata structure
        full_response = response.json()
        return full_response
    except requests.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None


def upload_chunk_to_minio(data):
    try:
        # Convert Python data to JSON string and encode it as bytes
        json_data = json.dumps(data)
        byte_data = BytesIO(json_data.encode('utf-8'))

        # Use timestamp to make filename unique and match expected prefix
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"covid_data_raw/us_covid_data_{timestamp}.json"

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
        data = fetch_page()
        if not data:
            logging.warning(f"No data returned.")
            return

        upload_chunk_to_minio(data)
        logging.info(f"Successfully uploaded COVID data to MinIO")

    except Exception as e:
        logging.error(f"Failed to upload: {e}")


if __name__ == "__main__":
    main()
