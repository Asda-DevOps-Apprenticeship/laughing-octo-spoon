from datetime import datetime, date
import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from app.utils import profile_store_table_get_gdpr_deletions

class TestProfileStoreTableGetGdprDeletions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.env_patcher = patch.dict(os.environ, {
            'DATABRICKS_SERVER_HOSTNAME': 'test_hostname',
            'DATABRICKS_HTTP_PATH': 'test_http_path',
            'DATABRICKS_TOKEN': 'test_token',
            'PROFILE_SNAPSHOT_DATASET': 'test_dataset',
            'IMS_ORG': 'test_org',
            'HOST': 'test_host',
            'PORT': 'test_port',
            'PRODDB': 'test_db'
        })
        cls.env_patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.env_patcher.stop()

    def setUp(self):
        self.mock_sql_connect_patcher = patch('app.utils.sql.connect')
        self.mock_sql_connect = self.mock_sql_connect_patcher.start()

        self.mock_psycopg2_connect_patcher = patch('app.utils.psycopg2.connect')
        self.mock_psycopg2_connect = self.mock_psycopg2_connect_patcher.start()

        self.mock_generate_access_token_patcher = patch('app.utils.generate_access_token')
        self.mock_generate_access_token = self.mock_generate_access_token_patcher.start()

        self.mock_logging_patcher = patch('app.utils.logging')
        self.mock_logging = self.mock_logging_patcher.start()

        self.mock_write_data_to_databricks_table_patcher = patch('app.utils.write_data_to_databricks_table')
        self.mock_write_data_to_databricks_table = self.mock_write_data_to_databricks_table_patcher.start()

        self.mock_open_patcher = patch('builtins.open', unittest.mock.mock_open(read_data='SELECT * FROM test_table'))
        self.mock_open = self.mock_open_patcher.start()

    def tearDown(self):
        self.mock_sql_connect_patcher.stop()
        self.mock_psycopg2_connect_patcher.stop()
        self.mock_generate_access_token_patcher.stop()
        self.mock_logging_patcher.stop()
        self.mock_write_data_to_databricks_table_patcher.stop()
        self.mock_open_patcher.stop()


    @patch('app.utils.customer_table_daily_run_cdd_tables')
    def test_profile_store_table_get_gdpr_deletions_no_data(self, mock_customer_table_daily_run_cdd_tables):
        mock_customer_table_daily_run_cdd_tables.return_value = None

        profile_store_table_get_gdpr_deletions()

        self.mock_logging.error.assert_called_once_with("Failed to retrieve data from customer_table_daily_run_cdd_tables.")

    @patch('app.utils.customer_table_daily_run_cdd_tables')
    def test_profile_store_table_get_gdpr_deletions_empty_list(self, mock_customer_table_daily_run_cdd_tables):
        mock_customer_table_daily_run_cdd_tables.return_value = ("", pd.DataFrame())

        profile_store_table_get_gdpr_deletions()

        self.mock_logging.error.assert_called_once_with("No SINGLEPROFILEID_LIST returned from customer_table_daily_run_cdd_tables.")

