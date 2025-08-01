import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from io import BytesIO

from src.minio_dao.minio import extract_and_process_csv_from_minio

@pytest.fixture
def mock_minio_client():
    return MagicMock()

@pytest.fixture
def sample_csv_bytes():
    # Simple CSV with two columns and two rows
    csv_content = "name,age\nAlice,30\nBob,25\n"
    return BytesIO(csv_content.encode('utf-8'))

# Testing a successful request
# MonkeyPatch is a fixture by pytest that allows for temporarily modifying/replacing env variables
def test_extract_and_process_csv_from_minio_success(mock_minio_client, sample_csv_bytes, monkeypatch):
    # Given: A .csv file, MinIO bucket, and expected table name
    filenames = ['test-file.csv']
    bucket_name = 'test-bucket'
    expected_table_name = 'TEST_FILE'

    # Patching the MinIO bucket .env variable, setting a fixed timestamp to compare to ingestion date
    monkeypatch.setenv('MINIO_BUCKET_NAME', bucket_name)

    # Mocking the response returned from MinIO as the sample_csv_bytes
    mock_response = MagicMock()
    mock_response.read.return_value = sample_csv_bytes.getvalue()
    mock_minio_client.get_object.return_value = mock_response

    # When: Extracting the .csv from the MinIO bucket
    result = extract_and_process_csv_from_minio(mock_minio_client, filenames)

    # Then: A dataframe is created with expected column names, data matches, and MinIO was called once
    assert expected_table_name in result
    df = result[expected_table_name]
    assert list(df.columns) == ['NAME', 'AGE', 'INGESTION_DATE', 'FILENAME']
    assert df.iloc[0]['NAME'] == 'Alice'
    assert df.iloc[1]['AGE'] == 25
    assert all(df['FILENAME'] == 'test-file.csv')
    mock_minio_client.get_object.assert_called_once_with(bucket_name, 'test-file.csv')
    mock_response.close.assert_called_once()
    mock_response.release_conn.assert_called_once()

def test_extract_and_process_csv_from_minio_minio_error(mock_minio_client, monkeypatch):
    from minio.error import S3Error

    # Given: A bad .csv file and MinIO bucket 
    filenames = ['bad-file.csv']
    bucket_name = 'test-bucket'
    
    # Patch the MinIO bucket .env variable and simulate an s3 error
    monkeypatch.setenv('MINIO_BUCKET_NAME', bucket_name)
    mock_minio_client.get_object.side_effect = S3Error("err", "msg", "req", "host", 400, None)

    # When + Then: Extracting the bad .csv from MinIO, an S3 error will be thrown
    with pytest.raises(S3Error):
        extract_and_process_csv_from_minio(mock_minio_client, filenames)

