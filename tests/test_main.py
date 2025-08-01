import pytest
from unittest.mock import patch, MagicMock

# This replaces the real dependencies with mocks for testing
@patch("src.main.create_minio_client")
@patch("src.main.extract_and_process_csv_from_minio")
@patch("src.main.create_snowflake_connection")
@patch("src.main.upload_dataframes_to_snowflake")
@patch("src.main.logger")
def test_process_csv_data_success(
    mock_logger,
    mock_upload_dataframes_to_snowflake,
    mock_create_snowflake_connection,
    mock_extract_and_process_csv_from_minio,
    mock_create_minio_client,
):
    # This defines what should be returned by the mocked functions
    mock_minio_client = MagicMock()
    mock_create_minio_client.return_value = mock_minio_client

    mock_processed_data = {"dummy": "data"}
    mock_extract_and_process_csv_from_minio.return_value = mock_processed_data

    mock_snowflake_conn = MagicMock()
    mock_create_snowflake_connection.return_value = mock_snowflake_conn

    # This is actually testing the function from the main module
    from src.main import process_csv_data
    result = process_csv_data()

    # This is making sure the function behaves as expected
    assert result is True
    mock_create_minio_client.assert_called_once()
    mock_extract_and_process_csv_from_minio.assert_called_once_with(mock_minio_client, [
        'ukhsa-coverage-report-2024-05-30.csv', 
        'canada_antibody_seroprevalence_13100818.csv', 
        'canada_demand_and_usage_13100838.csv'
    ])
    mock_create_snowflake_connection.assert_called_once()
    mock_upload_dataframes_to_snowflake.assert_called_once_with(mock_snowflake_conn, mock_processed_data)
    mock_snowflake_conn.close.assert_called_once()
    mock_logger.info.assert_any_call("CSV data processing completed successfully")