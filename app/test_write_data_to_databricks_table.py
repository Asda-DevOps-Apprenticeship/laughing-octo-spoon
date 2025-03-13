import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from app.utils import write_data_to_databricks_table

class TestWriteDataToDatabricksTable(unittest.TestCase):

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

        self.mock_logging_patcher = patch('app.utils.logging')
        self.mock_logging = self.mock_logging_patcher.start()

    def tearDown(self):
        self.mock_spark_session_patcher.stop()
        self.mock_logging_patcher.stop()

    def test_write_data_success(self):
        dataframe = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
        table_name = 'test_table'

        mock_spark_df = MagicMock()
        self.mock_spark_session.createDataFrame.return_value = mock_spark_df

        write_data_to_databricks_table(dataframe, table_name)

        self.mock_spark_session.createDataFrame.assert_called_once_with(dataframe)
        mock_spark_df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with('custanwo.customer_transformation.test_table')
        self.mock_logging.info.assert_any_call("Data successfully written to table 'custanwo.customer_transformation.test_table'.")
        self.mock_spark_session.stop.assert_called_once()

    def test_write_data_exception(self):
        dataframe = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
        table_name = 'test_table'

        mock_spark_df = MagicMock()
        self.mock_spark_session.createDataFrame.return_value = mock_spark_df
        mock_spark_df.write.format.return_value.mode.return_value.saveAsTable.side_effect = Exception("Test exception")

        write_data_to_databricks_table(dataframe, table_name)

        self.mock_spark_session.createDataFrame.assert_called_once_with(dataframe)
        mock_spark_df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with('custanwo.customer_transformation.test_table')
        self.mock_logging.error.assert_called_once_with("Error writing data to table 'custanwo.customer_transformation.test_table': Test exception")
        self.mock_spark_session.stop.assert_called_once()
