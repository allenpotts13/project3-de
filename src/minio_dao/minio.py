import json
import os
import tempfile
import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from io import BytesIO

  
load_dotenv()

def create_minio_client():
    minio_client = Minio(
    os.getenv('MINIO_EXTERNAL_URL'),
    access_key = os.getenv('MINIO_ACCESS_KEY'),
    secret_key = os.getenv('MINIO_SECRET_KEY'),
    secure = False
    )

    return minio_client

def extract_data_from_minio(minio_client, file_names):
    bucket = os.getenv('MINIO_BUCKET_NAME')
    tmp_dir = tempfile.gettempdir()

    file_paths = {}
    for file_name in file_names:
        file_path = os.path.join(tmp_dir, file_name)
        minio_client.fget_object(bucket, file_name, file_path)

# This converts json to jsonl to be read by snowflake
        if file_name.endswith('.json'):
            with open(file_path, 'r') as f:
                data = json.load(f)

            jsonl_path = file_path.replace('.json', '.jsonl')

            with open(jsonl_path, 'w') as f:
                f.write(json.dumps(data) + '\n')

            file_path = jsonl_path

        if "canada_antibody" in file_name:
            file_paths["RAW_CANADA_ANTIBODY_SEROPREVALENCE"] = file_path
        elif "canada_demand" in file_name:
            file_paths["RAW_CANADA_DEMAND_AND_USAGE"] = file_path
        elif "ukhsa-coverage" in file_name:
            file_paths["RAW_UK_VAX_COVERAGE"] = file_path

    return file_paths