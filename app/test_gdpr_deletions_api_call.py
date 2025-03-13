import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from app.utils import gdpr_deletions_api_call

class TestGdprDeletionsApiCall(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.env_patcher = patch.dict(os.environ, {
            'PRIVACY_END_POINT': 'https://test-endpoint.com',
            'GDRP_API_KEY': 'test_api_key',
            'IMS_ORG': 'test_ims_org'
        })
        cls.env_patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.env_patcher.stop()

    def setUp(self):
        self.mock_generate_access_token_patcher = patch('app.utils.generate_access_token_cdp_gdpr_execution')
        self.mock_generate_access_token = self.mock_generate_access_token_patcher.start()
        self.mock_generate_access_token.return_value = 'test_access_token'

        self.mock_requests_patcher = patch('app.utils.requests.request')
        self.mock_requests = self.mock_requests_patcher.start()

        self.mock_logging_patcher = patch('app.utils.logging')
        self.mock_logging = self.mock_logging_patcher.start()

        self.mock_write_data_to_databricks_table_patcher = patch('app.utils.write_data_to_databricks_table')
        self.mock_write_data_to_databricks_table = self.mock_write_data_to_databricks_table_patcher.start()

        self.mock_merge_data_to_databricks_table_patcher = patch('app.utils.merge_data_to_databricks_table')
        self.mock_merge_data_to_databricks_table = self.mock_merge_data_to_databricks_table_patcher.start()

    def tearDown(self):
        self.mock_generate_access_token_patcher.stop()
        self.mock_requests_patcher.stop()
        self.mock_logging_patcher.stop()
        self.mock_write_data_to_databricks_table_patcher.stop()
        self.mock_merge_data_to_databricks_table_patcher.stop()

    def test_successful_api_call(self):
        chunk = pd.DataFrame({
            'key': ['user1'],
            'action': ['delete'],
            'namespace': ['SPID'],
            'value': ['value1'],
            'type': ['custom']
        })

        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_response.json.return_value = {
            'requestId': 'test_request_id',
            'totalRecords': 1,
            'jobs': [{'jobId': 'test_job_id', 'customer': {'user': {'key': 'user1'}}}]
        }
        self.mock_requests.return_value = mock_response

        result = gdpr_deletions_api_call(chunk)
        self.assertTrue(result)
        self.mock_logging.info.assert_any_call("Deletion request successful. Request ID: test_request_id")
        self.mock_logging.info.assert_any_call("Total records processed: 1")

    def test_api_call_failure(self):
        chunk = pd.DataFrame({
            'key': ['user1'],
            'action': ['delete'],
            'namespace': ['SPID'],
            'value': ['value1'],
            'type': ['custom']
        })

        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = 'Bad Request'
        self.mock_requests.return_value = mock_response

        result = gdpr_deletions_api_call(chunk)
        self.assertFalse(result)
        self.mock_logging.error.assert_called_once_with("Deletion request failed. Status code: 400, Response: Bad Request")

