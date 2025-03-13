import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from app.utils import merge_data_to_databricks_table

class TestMergeDataToDatabricksTable(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.env_patcher = patch.dict(os.environ, {
            'DATABRICKS_SERVER_HOSTNAME': 'test_hostname',
            'DATABRICKS_TOKEN': 'test_token',
            'DATABRICKS_CLUSTER_ID': 'test_cluster_id'
        })
        cls.env_patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.env_patcher.stop()

    def setUp(self):
        self.mock_spark_session_patcher = patch('app.utils.DatabricksSession.builder')
        self.mock_spark_session_builder = self.mock_spark_session_patcher.start()
        self.mock_spark_session = MagicMock()
        self.mock_spark_session_builder.sdkConfig.return_value.getOrCreate.return_value = self.mock_spark_session

        self.mock_delta_table_patcher = patch('app.utils.DeltaTable.forName')
        self.mock_delta_table = self.mock_delta_table_patcher.start()

        self.mock_logging_patcher = patch('app.utils.logging')
        self.mock_logging = self.mock_logging_patcher.start()

    def tearDown(self):
        self.mock_spark_session_patcher.stop()
        self.mock_delta_table_patcher.stop()
        self.mock_logging_patcher.stop()

    def test_merge_data_success(self):
        dataframe = pd.DataFrame({'id': [1, 2], 'deletion_flag': [False, True], 'deletion_date': [None, None]})
        table_name = 'test_table'
        match_column = 'id'

        mock_delta_table_instance = MagicMock()
        self.mock_delta_table.return_value = mock_delta_table_instance

        merge_data_to_databricks_table(dataframe, table_name, match_column)

        self.mock_spark_session.createDataFrame.assert_called_once_with(dataframe)
        self.mock_delta_table.assert_called_once_with(self.mock_spark_session, 'custanwo.customer_transformation.test_table')
        mock_delta_table_instance.alias.return_value.merge.assert_called_once()
        self.mock_logging.info.assert_any_call("Merging data into table: custanwo.customer_transformation.test_table")
        self.mock_logging.info.assert_any_call("Data successfully merged into table 'custanwo.customer_transformation.test_table'.")
        self.mock_spark_session.stop.assert_called_once()

    def test_merge_data_exception(self):
        dataframe = pd.DataFrame({'id': [1, 2], 'deletion_flag': [False, True], 'deletion_date': [None, None]})
        table_name = 'test_table'
        match_column = 'id'

        self.mock_delta_table.side_effect = Exception("Test exception")

        merge_data_to_databricks_table(dataframe, table_name, match_column)

        self.mock_spark_session.createDataFrame.assert_called_once_with(dataframe)
        self.mock_delta_table.assert_called_once_with(self.mock_spark_session, 'custanwo.customer_transformation.test_table')
        self.mock_logging.error.assert_called_once_with("Error merging data into table 'custanwo.customer_transformation.test_table': Test exception")
        self.mock_spark_session.stop.assert_called_once()
