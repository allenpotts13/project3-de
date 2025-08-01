import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from src.snowflake_dao.snowflake import upload_dataframes_to_snowflake

def test_upload_dataframes_to_snowflake_success(monkeypatch):
    # Given: A mocked snowflake connection, a test dataframe, and test processed data
    conn = MagicMock()
    df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    processed_data = {'TEST_TABLE': df}

    # Patch environment variables
    monkeypatch.setenv('SNOWFLAKE_DATABASE', 'TEST_DB')
    monkeypatch.setenv('SNOWFLAKE_SCHEMA_BRONZE', 'TEST_SCHEMA')

    # Patch write_pandas to simulate successful upload
    with patch('src.snowflake_dao.snowflake.write_pandas') as mock_write_pandas:
        mock_write_pandas.return_value = (True, 1, len(df), None)

        # Act
        upload_dataframes_to_snowflake(conn, processed_data)

        # Assert
        mock_write_pandas.assert_called_once_with(
            conn=conn,
            df=df,
            table_name='TEST_TABLE',
            database='TEST_DB',
            schema='TEST_SCHEMA',
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=True
        )

def test_upload_dataframes_to_snowflake_failure(monkeypatch):
    # Arrange
    conn = MagicMock()
    df = pd.DataFrame({'A': [1]})
    processed_data = {'FAIL_TABLE': df}

    monkeypatch.setenv('SNOWFLAKE_DATABASE', 'TEST_DB')
    monkeypatch.setenv('SNOWFLAKE_SCHEMA_BRONZE', 'TEST_SCHEMA')

    # Patch write_pandas to simulate failure
    with patch('src.snowflake_dao.snowflake.write_pandas') as mock_write_pandas:
        mock_write_pandas.return_value = (False, 1, 1, None)

        # Act & Assert: Should not raise, just log