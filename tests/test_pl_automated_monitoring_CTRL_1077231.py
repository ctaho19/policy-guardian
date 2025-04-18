import datetime
import json
import unittest.mock as mock
from typing import Optional, List, Dict, Any

import pandas as pd
import pytest
from freezegun import freeze_time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                             LongType, TimestampType, ArrayType)
from requests import Response

# Modules to test
import pipelines.pl_automated_monitoring_CTRL_1077231.pipeline as pipeline
import pipelines.pl_automated_monitoring_CTRL_1077231.transform as transform

# Framework and Helpers
from etip_env import Env, ExchangeConfig, SnowflakeConfig # Assuming these exist for mocking
from tests.config_pipeline.helpers import ConfigPipelineTestCase # Base test class


# --- Mock Data Generation ---

# 1. Mock Threshold Data
def _mock_threshold_df_pandas() -> pd.DataFrame:
    """Creates a Pandas DataFrame simulating threshold data from Snowflake."""
    # Schema based on user confirmation
    return pd.DataFrame({
        "MONITORING_METRIC_ID": ["MNTR-1077231-T1", "MNTR-1077231-T2"],
        "CONTROL_ID": ["CTRL-1077231", "CTRL-1077231"],
        "MONITORING_METRIC_TIER": ["Tier 1", "Tier 2"],
        "METRIC_NAME": ["IMDSv2 Enforcement T1", "IMDSv2 Enforcement T2"],
        "METRIC_DESCRIPTION": ["Checks if httpTokens is set", "Checks if httpTokens is required"],
        "WARNING_THRESHOLD": [None, None], # User specified NULL
        "ALERT_THRESHOLD": [100.0, 100.0], # User specified 100
        "CONTROL_EXECUTOR": ["Executor", "Executor"],
        "METRIC_THRESHOLD_START_DATE": [datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 1)],
        "METRIC_THRESHOLD_END_DATE": [None, None]
    })

def _mock_threshold_df_spark(spark: SparkSession) -> DataFrame:
    """Converts the Pandas threshold DataFrame to a Spark DataFrame."""
    return spark.createDataFrame(_mock_threshold_df_pandas())

# 2. Mock API Response Data (Cloud Tooling API)
API_RESPONSE_MIXED = {
    "resourceConfigurations": [
        # Tier 1 Pass, Tier 2 Fail ("optional")
        {
            "resourceId": "i-optional",
            "hybridAmazonResourceNameId": "002745120748_us-east-1_i-00167f125dbdaca3b_AWS_EC2_Instance",
            "accountName": "app-cyber-opsrstrd-da-baueba",
            "amazonResourceName": "arn:aws:ec2:us-east-1:002745120748:instance/i-00167f125dbdaca3b",
            "awsAccountId": "002745120748",
            "awsRegion": "us-east-1",
            "internalConfigurationItemCaptureTimestamp": "2025-04-16T06:02:36Z",
            "resourceType": "AWS::EC2::Instance",
            "configurationList": [
                {"configurationName": "configuration.otherConfig", "configurationValue": "abc"},
                {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "optional"}
            ]
        },
        # Tier 1 Fail ("")
        {
            "resourceId": "i-empty",
            "hybridAmazonResourceNameId": "002745120748_us-east-1_i-001b58d139a6192c9_AWS_EC2_Instance",
            "accountName": "app-cyber-opsrstrd-da-baueba",
            "amazonResourceName": "arn:aws:ec2:us-east-1:002745120748:instance/i-001b58d139a6192c9",
            "awsAccountId": "002745120748",
            "awsRegion": "us-east-1",
            "internalConfigurationItemCaptureTimestamp": "2025-04-01T18:26:09Z",
            "resourceType": "AWS::EC2::Instance",
            "configurationList": [
                {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": ""}
            ]
        },
        # Tier 1 Pass, Tier 2 Pass ("required")
        {
            "resourceId": "i-required-1",
            "hybridAmazonResourceNameId": "002745120748_us-east-1_i-00220314eef92e100_AWS_EC2_Instance",
            "accountName": "app-cyber-opsrstrd-da-baueba",
            "amazonResourceName": "arn:aws:ec2:us-east-1:002745120748:instance/i-00220314eef92e100",
            "awsAccountId": "002745120748",
            "awsRegion": "us-east-1",
            "internalConfigurationItemCaptureTimestamp": "2025-04-18T04:01:14Z",
            "resourceType": "AWS::EC2::Instance",
            "configurationList": [
                {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "required"}
            ]
        },
        # Tier 1 Pass, Tier 2 Pass ("required")
        {
            "resourceId": "i-required-2",
            "hybridAmazonResourceNameId": "002745120748_us-east-1_i-007a81dca0ee53dd1_AWS_EC2_Instance",
            "accountName": "app-cyber-opsrstrd-da-baueba",
            "amazonResourceName": "arn:aws:ec2:us-east-1:002745120748:instance/i-007a81dca0ee53dd1",
            "awsAccountId": "002745120748",
            "awsRegion": "us-east-1",
            "internalConfigurationItemCaptureTimestamp": "2025-04-18T16:14:13Z",
            "resourceType": "AWS::EC2::Instance",
            "configurationList": [
                {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "required"}
            ]
        },
        # Tier 1 Pass, Tier 2 Pass ("required")
        {
            "resourceId": "i-required-3",
            "hybridAmazonResourceNameId": "002745120748_us-east-1_i-00b042de7b49c48a7_AWS_EC2_Instance",
            "accountName": "app-cyber-opsrstrd-da-baueba",
            "amazonResourceName": "arn:aws:ec2:us-east-1:002745120748:instance/i-00b042de7b49c48a7",
            "awsAccountId": "002745120748",
            "awsRegion": "us-east-1",
            "internalConfigurationItemCaptureTimestamp": "2025-04-18T05:01:49Z",
            "resourceType": "AWS::EC2::Instance",
            "configurationList": [
                {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "required"}
            ]
        }
    ],
    "nextRecordKey": "" # Simulate end of pagination for this test case
}

API_RESPONSE_EMPTY = {
    "resourceConfigurations": [],
    "nextRecordKey": "",
    "limit": 0
}

def generate_mock_api_response(content: Optional[dict] = None, status_code: int = 200) -> Response:
    """Creates a mock requests.Response object."""
    mock_response = Response()
    mock_response.status_code = status_code
    if content:
        mock_response._content = json.dumps(content).encode("utf-8")
    return mock_response

# 3. Expected Output DataFrames
@freeze_time("2024-11-05 12:09:00.123456")
def _expected_output_mixed_df(spark: SparkSession) -> DataFrame:
    """Calculates the expected output DataFrame for the mixed API response."""
    current_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

    # Manually determine non-compliant resources based on API_RESPONSE_MIXED
    # Fields based on NON_COMPLIANT_FIELDS + config_key_full in transform.py
    fields_to_keep = [
        "resourceId", "amazonResourceName", "resourceType", "awsRegion",
        "accountName", "awsAccountId", "configuration.metadataOptions.httpTokens"
    ]

    # Resource 2 failed Tier 1 (httpTokens: "")
    t1_non_compliant_details = {
        f: API_RESPONSE_MIXED["resourceConfigurations"][1].get(f, "N/A") for f in fields_to_keep
        if f != "configuration.metadataOptions.httpTokens" # Handle this key separately
    }
    t1_non_compliant_details["configuration.metadataOptions.httpTokens"] = "" # Actual value
    t1_non_compliant_json = [json.dumps(t1_non_compliant_details)]

    # Resource 1 failed Tier 2 (httpTokens: "optional")
    t2_non_compliant_details = {
        f: API_RESPONSE_MIXED["resourceConfigurations"][0].get(f, "N/A") for f in fields_to_keep
        if f != "configuration.metadataOptions.httpTokens"
    }
    t2_non_compliant_details["configuration.metadataOptions.httpTokens"] = "optional" # Actual value
    t2_non_compliant_json = [json.dumps(t2_non_compliant_details)]

    # Expected data based on calculations (T1: 4/5=80%, T2: 3/4=75%)
    output_data = [
        (current_timestamp, "CTRL-1077231", "MNTR-1077231-T1", 80.0, "Red", 4, 5, t1_non_compliant_json),
        (current_timestamp, "CTRL-1077231", "MNTR-1077231-T2", 75.0, "Red", 3, 4, t2_non_compliant_json),
    ]
    return spark.createDataFrame(output_data, schema=transform.OUTPUT_SCHEMA)

def _expected_output_empty_df(spark: SparkSession) -> DataFrame:
    """Expected output when the API returns no resources."""
    return spark.createDataFrame([], schema=transform.OUTPUT_SCHEMA)


# --- Mock Environment ---
# Create simple mock classes if real Env/Config classes are complex
class MockEnv:
    def __init__(self):
        self.exchange = ExchangeConfig("mock_id", "mock_secret", "https://mock.exchange.url")
        self.snowflake = SnowflakeConfig("mock_acc", "mock_user", "mock_pass", "mock_role", "mock_wh", "mock_db", "mock_schema")
        # Add other attributes if ConfigPipeline base class needs them

# --- Test Class ---

# Use pytest fixture for SparkSession if available, otherwise create manually
@pytest.fixture(scope="session")
def spark_session():
    """Provides a SparkSession for the tests."""
    return SparkSession.builder.appName("CTRL-1077231-Tests").master("local[*]").getOrCreate()

class TestAutomatedMonitoringCtrl1077231(ConfigPipelineTestCase):
    """Unit tests for the pl_automated_monitoring_CTRL_1077231 pipeline."""

    # 1. Test Transform Logic Directly
    @freeze_time("2024-11-05 12:09:00.123456")
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.transform._make_api_request")
    def test_transform_logic_mixed_compliance(self, mock_api_request, spark_session):
        """Tests the core transform function with mixed compliance data."""
        # Configure mock API response
        mock_api_request.return_value = generate_mock_api_response(API_RESPONSE_MIXED)

        # Prepare inputs
        mock_threshold_spark_df = _mock_threshold_df_spark(spark_session)
        mock_context = {
            "api_auth_token": "Bearer mock_token",
            "cloud_tooling_api_url": "https://mock.api.url/search-resource-configurations",
            "api_verify_ssl": False
        }
        # Options mimicking config.yml
        options = {
            "resource_type": "AWS::EC2::Instance",
            "config_key": "metadataOptions.httpTokens",
            "config_value": "required",
            "ctrl_id": "CTRL-1077231",
            "tier1_metric_id": "MNTR-1077231-T1",
            "tier2_metric_id": "MNTR-1077231-T2",
        }

        # Execute the transform function
        actual_df = transform.calculate_ctrl1077231_metrics(
            spark=spark_session,
            thresholds_raw=mock_threshold_spark_df,
            context=mock_context,
            **options
        )

        # Assertions
        expected_df = _expected_output_mixed_df(spark_session)
        # Use helper for Spark DataFrame comparison if available, otherwise collect and compare
        self.assertSparkDataFrameEqual(actual_df, expected_df)

    @freeze_time("2024-11-05 12:09:00.123456")
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.transform._make_api_request")
    def test_transform_logic_empty_api_response(self, mock_api_request, spark_session):
        """Tests the core transform function when the API returns no resources."""
        # Configure mock API response
        mock_api_request.return_value = generate_mock_api_response(API_RESPONSE_EMPTY)

        # Prepare inputs (same as previous test, only API response differs)
        mock_threshold_spark_df = _mock_threshold_df_spark(spark_session)
        mock_context = {
            "api_auth_token": "Bearer mock_token",
            "cloud_tooling_api_url": "https://mock.api.url/search-resource-configurations",
            "api_verify_ssl": False
        }
        options = {
            "resource_type": "AWS::EC2::Instance",
            "config_key": "metadataOptions.httpTokens",
            "config_value": "required",
            "ctrl_id": "CTRL-1077231",
            "tier1_metric_id": "MNTR-1077231-T1",
            "tier2_metric_id": "MNTR-1077231-T2",
        }

        # Execute the transform function
        actual_df = transform.calculate_ctrl1077231_metrics(
            spark=spark_session,
            thresholds_raw=mock_threshold_spark_df,
            context=mock_context,
            **options
        )

        # Assertions
        expected_df = _expected_output_empty_df(spark_session)
        self.assertSparkDataFrameEqual(actual_df, expected_df)


    # 2. Test Full Pipeline Run (using mocks for external interactions)
    @freeze_time("2024-11-05 12:09:00.123456")
    @mock.patch("connectors.exchange.oauth_token.refresh", return_value="mock_token_value") # Mock OAuth
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.ConfigPipeline.read") # Mock Snowflake read
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.ConfigPipeline.write") # Mock Snowflake write
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.transform._make_api_request") # Mock API call inside transform
    def test_full_run_mixed_compliance(self, mock_api_req, mock_write, mock_read, mock_refresh, spark_session):
        """Tests the pipeline.run() entrypoint with mocks for I/O."""
        # Configure mocks
        mock_api_req.return_value = generate_mock_api_response(API_RESPONSE_MIXED)
        mock_read.return_value = _mock_threshold_df_spark(spark_session) # Simulate reading thresholds
        mock_env = MockEnv() # Use the simple mock Env

        # Expected data to be written
        expected_output_df = _expected_output_mixed_df(spark_session)

        # Execute the main run function (note: uses is_load=True by default)
        pipeline.run(env=mock_env, is_load=True, dq_actions=False) # Disable DQ for simplicity

        # Assertions
        mock_refresh.assert_called_once() # Check auth was called
        mock_read.assert_called_once() # Check threshold read was called
        mock_api_req.assert_called_once() # Check API was called

        # Check that the write method was called once
        self.assertEqual(mock_write.call_count, 1)
        # Get the DataFrame that was passed to the write method
        # Arguments are passed as positional args, the DataFrame is usually the first one
        call_args, call_kwargs = mock_write.call_args
        actual_written_df = call_args[0]

        # Assert the DataFrame written matches expectations
        self.assertSparkDataFrameEqual(actual_written_df, expected_output_df)


    # 3. Test High-Level Run Entrypoint
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.PLAutomatedMonitoringCtrl1077231")
    def test_run_entrypoint_invocation(self, mock_pipeline_class, spark_session):
        """Tests that pipeline.run() instantiates and runs the pipeline class correctly."""
        mock_env = MockEnv()
        mock_pipeline_instance = mock_pipeline_class.return_value

        # Call the entrypoint
        pipeline.run(mock_env, is_load=False, dq_actions=False)

        # Assertions
        mock_pipeline_class.assert_called_once_with(mock_env) # Check instantiation
        mock_pipeline_instance.configure_from_filename.assert_called_once() # Check config load
        mock_pipeline_instance.run.assert_called_once_with(load=False, dq_actions=False) # Check run call 