import datetime
import json
import unittest.mock as mock
from typing import Optional, List, Dict, Any

import pandas as pd
import pytest
from freezegun import freeze_time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                             LongType, TimestampType, ArrayType, IntegerType)
from requests import Response, RequestException

import pipelines.pl_automated_monitoring_CTRL_1077231.pipeline as pipeline
import pipelines.pl_automated_monitoring_CTRL_1077231.transform as transform

from etip_env import set_env_vars
from tests.config_pipeline.helpers import ConfigPipelineTestCase

def _mock_threshold_df_pandas() -> pd.DataFrame:
    return pd.DataFrame({
        "monitoring_metric_id": [1, 2],
        "control_id": ["CTRL-1077231", "CTRL-1077231"],
        "monitoring_metric_tier": ["Tier 1", "Tier 2"],
        "warning_threshold": [97.0, 75.0],
        "alerting_threshold": [95.0, 50.0],
        "control_executor": ["Individual_1", "Individual_1"],
        "metric_threshold_start_date": [
            datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 00, 21180)
        ],
        "metric_threshold_end_date": [None, None]
    })

def _mock_threshold_df_spark(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("monitoring_metric_id", IntegerType(), True),
        StructField("control_id", StringType(), True),
        StructField("monitoring_metric_tier", StringType(), True),
        StructField("warning_threshold", DoubleType(), True),
        StructField("alerting_threshold", DoubleType(), True),
        StructField("control_executor", StringType(), True),
        StructField("metric_threshold_start_date", TimestampType(), True),
        StructField("metric_threshold_end_date", TimestampType(), True)
    ])
    return spark.createDataFrame(_mock_threshold_df_pandas(), schema=schema)

def _mock_invalid_threshold_df_pandas() -> pd.DataFrame:
    return pd.DataFrame({
        "monitoring_metric_id": [1, 2],
        "control_id": ["CTRL-1077231", "CTRL-1077231"],
        "monitoring_metric_tier": ["Tier 1", "Tier 2"],
        "warning_threshold": ["invalid", None],
        "alerting_threshold": ["not_a_number", 50.0],
        "control_executor": ["Individual_1", "Individual_1"],
        "metric_threshold_start_date": [
            datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 00, 21180)
        ],
        "metric_threshold_end_date": [None, None]
    })

def _mock_invalid_threshold_df_spark(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("monitoring_metric_id", IntegerType(), True),
        StructField("control_id", StringType(), True),
        StructField("monitoring_metric_tier", StringType(), True),
        StructField("warning_threshold", StringType(), True),
        StructField("alerting_threshold", StringType(), True),
        StructField("control_executor", StringType(), True),
        StructField("metric_threshold_start_date", TimestampType(), True),
        StructField("metric_threshold_end_date", TimestampType(), True)
    ])
    pdf = _mock_invalid_threshold_df_pandas()
    pdf['warning_threshold'] = pdf['warning_threshold'].astype(str)
    pdf['alerting_threshold'] = pdf['alerting_threshold'].astype(str)
    return spark.createDataFrame(pdf, schema=schema)

API_RESPONSE_MIXED = {
    "resourceConfigurations": [
        {
            "resourceId": "i-optional",
            "hybridAmazonResourceNameId": "123456789012_us-east-1_i-00167f125dbdaca3b_AWS_EC2_Instance",
            "accountName": "prod-cyber-opsrstrd-da-baueba",
            "amazonResourceName": "arn:aws:ec2:us-east-1:123456789012:instance/i-00167f125dbdaca3b",
            "awsAccountId": "123456789012",
            "awsRegion": "us-east-1",
            "internalConfigurationItemCaptureTimestamp": "2025-04-16T06:02:36Z",
            "resourceType": "AWS::EC2::Instance",
            "configurationList": [
                {"configurationName": "configuration.otherConfig", "configurationValue": "abc"},
                {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "optional"}
            ]
        },
        {
            "resourceId": "i-empty",
            "hybridAmazonResourceNameId": "123456789012_us-east-1_i-001b58d139a6192c9_AWS_EC2_Instance",
            "accountName": "prod-cyber-opsrstrd-da-baueba",
            "amazonResourceName": "arn:aws:ec2:us-east-1:123456789012:instance/i-001b58d139a6192c9",
            "awsAccountId": "123456789012",
            "awsRegion": "us-east-1",
            "internalConfigurationItemCaptureTimestamp": "2025-04-01T18:26:09Z",
            "resourceType": "AWS::EC2::Instance",
            "configurationList": [
                {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": ""}
            ]
        },
        {
            "resourceId": "i-required-1",
            "hybridAmazonResourceNameId": "123456789012_us-east-1_i-00220314eef92e100_AWS_EC2_Instance",
            "accountName": "prod-cyber-opsrstrd-da-baueba",
            "amazonResourceName": "arn:aws:ec2:us-east-1:123456789012:instance/i-00220314eef92e100",
            "awsAccountId": "123456789012",
            "awsRegion": "us-east-1",
            "internalConfigurationItemCaptureTimestamp": "2025-04-18T04:01:14Z",
            "resourceType": "AWS::EC2::Instance",
            "configurationList": [
                {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "required"}
            ]
        },
        {
            "resourceId": "i-required-2",
            "hybridAmazonResourceNameId": "123456789012_us-east-1_i-007a81dca0ee53dd1_AWS_EC2_Instance",
            "accountName": "prod-cyber-opsrstrd-da-baueba",
            "amazonResourceName": "arn:aws:ec2:us-east-1:123456789012:instance/i-007a81dca0ee53dd1",
            "awsAccountId": "123456789012",
            "awsRegion": "us-east-1",
            "internalConfigurationItemCaptureTimestamp": "2025-04-18T16:14:13Z",
            "resourceType": "AWS::EC2::Instance",
            "configurationList": [
                {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "required"}
            ]
        },
        {
            "resourceId": "i-required-3",
            "hybridAmazonResourceNameId": "123456789012_us-east-1_i-00b042de7b49c48a7_AWS_EC2_Instance",
            "accountName": "prod-cyber-opsrstrd-da-baueba",
            "amazonResourceName": "arn:aws:ec2:us-east-1:123456789012:instance/i-00b042de7b49c48a7",
            "awsAccountId": "123456789012",
            "awsRegion": "us-east-1",
            "internalConfigurationItemCaptureTimestamp": "2025-04-18T05:01:49Z",
            "resourceType": "AWS::EC2::Instance",
            "configurationList": [
                {"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "required"}
            ]
        }
    ],
    "nextRecordKey": ""
}

API_RESPONSE_PAGE_1 = {
    "resourceConfigurations": API_RESPONSE_MIXED["resourceConfigurations"][0:1],
    "nextRecordKey": "page2_key"
}

API_RESPONSE_PAGE_2 = {
    "resourceConfigurations": API_RESPONSE_MIXED["resourceConfigurations"][1:],
    "nextRecordKey": ""
}

API_RESPONSE_EMPTY = {
    "resourceConfigurations": [],
    "nextRecordKey": "",
    "limit": 0
}

def generate_mock_api_response(content: Optional[dict] = None, status_code: int = 200) -> Response:
    mock_response = Response()
    mock_response.status_code = status_code
    if content:
        mock_response._content = json.dumps(content).encode("utf-8")
    mock_response.request = mock.Mock()
    mock_response.request.url = "https://mock.api.url/search-resource-configurations"
    mock_response.request.method = "POST"
    return mock_response

@freeze_time("2024-11-05 12:09:00.123456")
def _expected_output_mixed_df(spark: SparkSession) -> DataFrame:
    current_timestamp = datetime.datetime.now(timezone.utc)

    fields_to_keep = [
        "resourceId", "amazonResourceName", "resourceType", "awsRegion",
        "accountName", "awsAccountId", "configuration.metadataOptions.httpTokens"
    ]

    t1_non_compliant_details = {
        f: API_RESPONSE_MIXED["resourceConfigurations"][1].get(f, "N/A") for f in fields_to_keep
        if f != "configuration.metadataOptions.httpTokens"
    }
    t1_non_compliant_details["configuration.metadataOptions.httpTokens"] = ""
    t1_non_compliant_json = [json.dumps(t1_non_compliant_details)]

    t2_non_compliant_details = {
        f: API_RESPONSE_MIXED["resourceConfigurations"][0].get(f, "N/A") for f in fields_to_keep
        if f != "configuration.metadataOptions.httpTokens"
    }
    t2_non_compliant_details["configuration.metadataOptions.httpTokens"] = "optional"
    t2_non_compliant_json = [json.dumps(t2_non_compliant_details)]

    output_data = [
        (current_timestamp, "CTRL-1077231", 1, 80.0, "Red", 4, 5, t1_non_compliant_json),
        (current_timestamp, "CTRL-1077231", 2, 75.0, "Green", 3, 4, t2_non_compliant_json),
    ]
    return spark.createDataFrame(output_data, schema=transform.OUTPUT_SCHEMA)

def _expected_output_empty_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], schema=transform.OUTPUT_SCHEMA)

class MockExchangeConfig:
    def __init__(self, client_id="etip-client-id", client_secret="etip-client-secret", exchange_url="https://api.cloud.capitalone.com/exchange"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.exchange_url = exchange_url

class MockSnowflakeConfig:
     def __init__(self):
        self.account = "capitalone"
        self.user = "etip_user"
        self.password = "etip_password"
        self.role = "etip_role"
        self.warehouse = "etip_wh"
        self.database = "etip_db"
        self.schema = "etip_schema"

class MockEnv:
    def __init__(self, exchange_config=None, snowflake_config=None):
        self.exchange = exchange_config if exchange_config else MockExchangeConfig()
        self.snowflake = snowflake_config if snowflake_config else MockSnowflakeConfig()

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.appName("CTRL-1077231-Tests").master("local[*]").getOrCreate()
    yield spark
    spark.stop()

class TestAutomatedMonitoringCtrl1077231(ConfigPipelineTestCase):
    def test_pipeline_init_success(self):
        mock_env = MockEnv()
        try:
            pipe = pipeline.PLAutomatedMonitoringCtrl1077231(mock_env)
            self.assertEqual(pipe.client_id, "etip-client-id")
            self.assertEqual(pipe.client_secret, "etip-client-secret")
            self.assertEqual(pipe.exchange_url, "https://api.cloud.capitalone.com/exchange")
        except Exception as e:
            pytest.fail(f"Pipeline initialization failed unexpectedly: {e}")

    def test_pipeline_init_missing_oauth_config(self):
        mock_env_bad = MockEnv(exchange_config=None)

        with pytest.raises(ValueError, match="Environment object missing expected OAuth attributes"):
            pipeline.PLAutomatedMonitoringCtrl1077231(mock_env_bad)

    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.refresh_oauth_token")
    def test_get_api_token_success(self, mock_refresh):
        mock_refresh.return_value = "mock_token_value"
        mock_env = MockEnv()
        pipe = pipeline.PLAutomatedMonitoringCtrl1077231(mock_env)
        token = pipe._get_api_token()
        self.assertEqual(token, "mock_token_value")
        mock_refresh.assert_called_once_with(
            client_id="etip-client-id",
            client_secret="etip-client-secret",
            exchange_url="https://api.cloud.capitalone.com/exchange"
        )

    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.refresh_oauth_token")
    def test_get_api_token_failure(self, mock_refresh):
        mock_refresh.side_effect = Exception("Token refresh failed")
        mock_env = MockEnv()
        pipe = pipeline.PLAutomatedMonitoringCtrl1077231(mock_env)
        with pytest.raises(Exception, match="Token refresh failed"):
            pipe._get_api_token()

    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.transform.requests.post")
    def test_make_api_request_success_no_pagination(self, mock_post):
        mock_response = generate_mock_api_response(API_RESPONSE_MIXED)
        mock_post.return_value = mock_response
        response = transform._make_api_request(
            url="https://mock.api.url/search-resource-configurations",
            method="POST",
            auth_token="mock_token",
            verify_ssl=True,
            timeout=60,
            max_retries=3,
            payload={"searchParameters": [{"resourceType": "AWS::EC2::Instance"}]}
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content), API_RESPONSE_MIXED)

    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.transform.requests.post")
    def test_make_api_request_success_with_pagination(self, mock_post):
        mock_post.side_effect = [
            generate_mock_api_response(API_RESPONSE_PAGE_1),
            generate_mock_api_response(API_RESPONSE_PAGE_2)
        ]
        response = transform._make_api_request(
            url="https://mock.api.url/search-resource-configurations",
            method="POST",
            auth_token="mock_token",
            verify_ssl=True,
            timeout=60,
            max_retries=3,
            payload={"searchParameters": [{"resourceType": "AWS::EC2::Instance"}]},
            params={"limit": 1}
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content), API_RESPONSE_PAGE_2)

    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.transform.requests.post")
    def test_make_api_request_http_error(self, mock_post):
        mock_response = generate_mock_api_response(status_code=500)
        mock_post.return_value = mock_response
        with pytest.raises(RequestException):
            transform._make_api_request(
                url="https://mock.api.url/search-resource-configurations",
                method="POST",
                auth_token="mock_token",
                verify_ssl=True,
                timeout=60,
                max_retries=3
            )

    @freeze_time("2024-11-05 12:09:00.123456")
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.transform._make_api_request")
    def test_transform_logic_mixed_compliance(self, mock_make_api_request, spark_session):
        mock_make_api_request.return_value = generate_mock_api_response(API_RESPONSE_MIXED)
        thresholds_df = _mock_threshold_df_spark(spark_session)
        context = {
            "api_auth_token": "mock_token",
            "cloud_tooling_api_url": "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations",
            "api_verify_ssl": True
        }
        result_df = transform.calculate_ctrl1077231_metrics(
            spark=spark_session,
            thresholds_raw=thresholds_df,
            context=context,
            resource_type="AWS::EC2::Instance",
            config_key="metadataOptions.httpTokens",
            config_value="required",
            ctrl_id="CTRL-1077231",
            tier1_metric_id="MNTR-1077231-T1",
            tier2_metric_id="MNTR-1077231-T2"
        )
        expected_df = _expected_output_mixed_df(spark_session)
        result_list = sorted(result_df.collect(), key=lambda r: r['monitoring_metric_id'])
        expected_list = sorted(expected_df.collect(), key=lambda r: r['monitoring_metric_id'])
        self.assertEqual(result_list, expected_list)

    @freeze_time("2024-11-05 12:09:00.123456")
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.transform._make_api_request")
    def test_transform_logic_empty_api_response(self, mock_make_api_request, spark_session):
        mock_make_api_request.return_value = generate_mock_api_response(API_RESPONSE_EMPTY)
        thresholds_df = _mock_threshold_df_spark(spark_session)
        context = {
            "api_auth_token": "mock_token",
            "cloud_tooling_api_url": "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations",
            "api_verify_ssl": True
        }
        result_df = transform.calculate_ctrl1077231_metrics(
            spark=spark_session,
            thresholds_raw=thresholds_df,
            context=context,
            resource_type="AWS::EC2::Instance",
            config_key="metadataOptions.httpTokens",
            config_value="required",
            ctrl_id="CTRL-1077231",
            tier1_metric_id="MNTR-1077231-T1",
            tier2_metric_id="MNTR-1077231-T2"
        )
        expected_df = _expected_output_empty_df(spark_session)
        self.assertEqual(result_df.collect(), expected_df.collect())

    @freeze_time("2024-11-05 12:09:00.123456")
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.refresh_oauth_token", return_value="mock_token_value")
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.ConfigPipeline.read")
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.ConfigPipeline.write")
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.transform._make_api_request")
    def test_full_run_mixed_compliance(self, mock_make_api_req, mock_write, mock_read, mock_refresh, spark_session):
        mock_read.return_value = _mock_threshold_df_spark(spark_session)
        mock_make_api_req.return_value = generate_mock_api_response(API_RESPONSE_MIXED)
        mock_env = MockEnv()
        pipe = pipeline.PLAutomatedMonitoringCtrl1077231(mock_env)
        pipe.configure_from_filename("pl_automated_monitoring_CTRL_1077231/config.yml")
        pipe.run()
        mock_write.assert_called_once()
        written_df = mock_write.call_args[0][0]
        expected_df = _expected_output_mixed_df(spark_session)
        written_list = sorted(written_df.collect(), key=lambda r: r['monitoring_metric_id'])
        expected_list = sorted(expected_df.collect(), key=lambda r: r['monitoring_metric_id'])
        self.assertEqual(written_list, expected_list)

    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.PLAutomatedMonitoringCtrl1077231")
    def test_run_entrypoint_defaults(self, mock_pipeline_class, spark_session):
        mock_pipeline_instance = mock_pipeline_class.return_value
        mock_pipeline_instance.run.return_value = None
        pipeline.run()
        mock_pipeline_class.assert_called_once()
        mock_pipeline_instance.run.assert_called_once()

    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.PLAutomatedMonitoringCtrl1077231")
    def test_run_entrypoint_no_load_no_dq(self, mock_pipeline_class, spark_session):
        mock_pipeline_instance = mock_pipeline_class.return_value
        mock_pipeline_instance.run.return_value = None
        pipeline.run(load=False, dq=False)
        mock_pipeline_class.assert_called_once()
        mock_pipeline_instance.run.assert_called_once_with(load=False, dq=False)

    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.pipeline.PLAutomatedMonitoringCtrl1077231")
    def test_run_entrypoint_export_test_data(self, mock_pipeline_class, spark_session):
        mock_pipeline_instance = mock_pipeline_class.return_value
        mock_pipeline_instance.run.return_value = None
        pipeline.run(export_test_data=True)
        mock_pipeline_class.assert_called_once()
        mock_pipeline_instance.run.assert_called_once_with(export_test_data=True)

    @freeze_time("2024-11-05 12:09:00.123456")
    @mock.patch("pipelines.pl_automated_monitoring_CTRL_1077231.transform._make_api_request")
    def test_transform_logic_invalid_thresholds(self, mock_make_api_request, spark_session):
        mock_make_api_request.return_value = generate_mock_api_response(API_RESPONSE_MIXED)
        thresholds_df = _mock_invalid_threshold_df_spark(spark_session)
        context = {
            "api_auth_token": "mock_token",
            "cloud_tooling_api_url": "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations",
            "api_verify_ssl": True
        }
        
        result_df = transform.calculate_ctrl1077231_metrics(
            spark=spark_session,
            thresholds_raw=thresholds_df,
            context=context,
            resource_type="AWS::EC2::Instance",
            config_key="metadataOptions.httpTokens",
            config_value="required",
            ctrl_id="CTRL-1077231",
            tier1_metric_id="MNTR-1077231-T1",
            tier2_metric_id="MNTR-1077231-T2"
        )
        
        expected_df_invalid = _expected_output_mixed_df_invalid(spark_session)

        result_list = sorted(result_df.collect(), key=lambda r: r['monitoring_metric_id'])
        expected_list = sorted(expected_df_invalid.collect(), key=lambda r: r['monitoring_metric_id'])

        self.assertEqual(result_list, expected_list)

        self.assertEqual(len(result_list), 2)
        self.assertEqual(result_list[0]["monitoring_metric_id"], 1)
        self.assertEqual(result_list[0]["monitoring_metric_value"], 80.0)
        self.assertEqual(result_list[0]["compliance_status"], "Red")
        self.assertEqual(result_list[1]["monitoring_metric_id"], 2)
        self.assertEqual(result_list[1]["monitoring_metric_value"], 75.0)
        self.assertEqual(result_list[1]["compliance_status"], "Red")

@freeze_time("2024-11-05 12:09:00.123456")
def _expected_output_mixed_df_invalid(spark: SparkSession) -> DataFrame:
    current_timestamp = datetime.datetime.now(timezone.utc)

    fields_to_keep = [
        "resourceId", "amazonResourceName", "resourceType", "awsRegion",
        "accountName", "awsAccountId", "configuration.metadataOptions.httpTokens"
    ]
    t1_non_compliant_details = {
        f: API_RESPONSE_MIXED["resourceConfigurations"][1].get(f, "N/A") for f in fields_to_keep
        if f != "configuration.metadataOptions.httpTokens"
    }
    t1_non_compliant_details["configuration.metadataOptions.httpTokens"] = ""
    t1_non_compliant_json = [json.dumps(t1_non_compliant_details)]
    t2_non_compliant_details = {
        f: API_RESPONSE_MIXED["resourceConfigurations"][0].get(f, "N/A") for f in fields_to_keep
        if f != "configuration.metadataOptions.httpTokens"
    }
    t2_non_compliant_details["configuration.metadataOptions.httpTokens"] = "optional"
    t2_non_compliant_json = [json.dumps(t2_non_compliant_details)]

    output_data = [
        (current_timestamp, "CTRL-1077231", 1, 80.0, "Red", 4, 5, t1_non_compliant_json),
        (current_timestamp, "CTRL-1077231", 2, 75.0, "Red", 3, 4, t2_non_compliant_json),
    ]
    return spark.createDataFrame(output_data, schema=transform.OUTPUT_SCHEMA) 
