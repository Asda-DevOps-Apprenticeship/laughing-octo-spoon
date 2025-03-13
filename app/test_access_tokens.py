import os
import unittest
from unittest.mock import patch, MagicMock
import requests
from app.utils import validate_env_vars, generate_access_token, generate_access_token_cdp_gdpr_execution

class AccessTokensTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up class-wide patches for environment variables."""
        cls.env_patcher = patch.dict(os.environ, {
            'CLIENT_SECRET': 'test_secret',
            'API_KEY': 'test_key',
            'SCOPES': 'test_scope',
            'DATASET_ID': 'test_dataset',
            'IMS_ORG': 'test_org',
            'SANDBOX_NAME': 'test_sandbox',
            'GDPR_CLIENT_SECRET': 'test_gdpr_secret',
            'GDRP_API_KEY': 'test_gdpr_key'
        })
        cls.env_patcher.start()

    def setUp(self):
        """Set up mock requests.Session.post for each test."""
        self.mock_post_patcher = patch('requests.Session.post')
        self.mock_post = self.mock_post_patcher.start()
        self.mock_response = MagicMock()
        self.mock_response.json.return_value = {'access_token': 'test_token'}
        self.mock_response.raise_for_status.return_value = None
        self.mock_post.return_value = self.mock_response

    def tearDown(self):
        """Stop the patched requests.Session.post after each test."""
        self.mock_post_patcher.stop()

    @classmethod
    def tearDownClass(cls):
        """Stop the patched environment variables after all tests."""
        cls.env_patcher.stop()

    def test_validate_env_vars(self):
        """Test that validate_env_vars does not raise an error when all env variables are set."""
        try:
            validate_env_vars()
        except EnvironmentError:
            self.fail("validate_env_vars() raised EnvironmentError unexpectedly!")

    def test_generate_access_token(self):
        """Test generate_access_token function."""
        token = generate_access_token()
        self.assertEqual(token, 'test_token')
        self.mock_post.assert_called_once()

    def test_generate_access_token_cdp_gdpr_execution(self):
        """Test generate_access_token function."""
        token = generate_access_token_cdp_gdpr_execution()
        self.assertEqual(token, 'test_token')
        self.mock_post.assert_called_once()

    def test_generate_access_token_http_error(self):
        """Test generate_access_token function raises HTTPError."""
        self.mock_post.side_effect = requests.exceptions.HTTPError("HTTP Error")

        with self.assertRaises(requests.exceptions.HTTPError):
            generate_access_token()

    def test_generate_access_token_cdp_gdpr_execution_http_error(self):
        """Test generate_access_token function raises HTTPError."""
        self.mock_post.side_effect = requests.exceptions.HTTPError("HTTP Error")

        with self.assertRaises(requests.exceptions.HTTPError):
            generate_access_token_cdp_gdpr_execution()            
