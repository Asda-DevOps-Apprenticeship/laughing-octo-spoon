import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from app.utils import auto_execute_gdpr_deletions_cdp

class TestAutoExecuteGdprDeletionsCdp(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.env_patcher = patch.dict(os.environ, {
            'DATABRICKS_SERVER_HOSTNAME': 'test_hostname',
            'DATABRICKS_TOKEN': 'test_token',
            'DATABRICKS_HTTP_PATH': 'test_http_path'
        })
        cls.env_patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.env_patcher.stop()

    def setUp(self):
        self.mock_sql_connect_patcher = patch('app.utils.sql.connect')
        self.mock_sql_connect = self.mock_sql_connect_patcher.start()

        self.mock_logging_patcher = patch('app.utils.logging')
        self.mock_logging = self.mock_logging_patcher.start()

        self.mock_profile_store_table_get_gdpr_deletions_patcher = patch('app.utils.profile_store_table_get_gdpr_deletions')
        self.mock_profile_store_table_get_gdpr_deletions = self.mock_profile_store_table_get_gdpr_deletions_patcher.start()

        self.mock_gdpr_deletions_api_call_patcher = patch('app.utils.gdpr_deletions_api_call')
        self.mock_gdpr_deletions_api_call = self.mock_gdpr_deletions_api_call_patcher.start()

    def tearDown(self):
        self.mock_sql_connect_patcher.stop()
        self.mock_logging_patcher.stop()
        self.mock_profile_store_table_get_gdpr_deletions_patcher.stop()
        self.mock_gdpr_deletions_api_call_patcher.stop()

    def test_no_delete_date_provided(self):
        auto_execute_gdpr_deletions_cdp()
        self.mock_logging.warning.assert_called_once_with("No delete_date provided. Exiting function.")


    def test_no_records_to_delete(self):
        mock_connection = MagicMock()
        self.mock_sql_connect.return_value = mock_connection
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = []

        auto_execute_gdpr_deletions_cdp(delete_date="2023-01-01")
        self.mock_logging.info.assert_any_call("No user deletions to process for 2023-01-01")

    def test_more_than_1000_records(self):
        mock_connection = MagicMock()
        self.mock_sql_connect.return_value = mock_connection
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = [("user1",)] * 1001

        auto_execute_gdpr_deletions_cdp(delete_date="2023-01-01")
        self.mock_logging.info.assert_any_call("More than 1000 records (1001) found for 2023-01-01. Manual intervention required.")

    def test_successful_execution(self):
        mock_connection = MagicMock()
        self.mock_sql_connect.return_value = mock_connection
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = [("user1",), ("user2",)]

        self.mock_gdpr_deletions_api_call.return_value = True

        auto_execute_gdpr_deletions_cdp(delete_date="2023-01-01")
        self.mock_logging.info.assert_any_call("Successfully processed GDPR deletions for 2023-01-01")
