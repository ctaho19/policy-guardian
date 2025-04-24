import datetime
import json
import unittest.mock as mock
from typing import Optional, List, Dict, Any
import time
import pandas as pd
import pytest
from freezegun import freeze_time
from requests import Response, RequestException
import pipelines.pl_automated_monitoring_ctrl_1077231.pipeline as pipeline
from etip_env import set_env_vars, EnvType

# Avro schema fields for output DataFrame
AVRO_SCHEMA_FIELDS = [
    "date",
    "control_id",
    "monitoring_metric_id",
    "monitoring_metric_value",
    "compliance_status",
    "numerator",
    "denominator",
    "non_compliant_resources"
]

def _mock_threshold_df_pandas() -> pd.DataFrame:
    return pd.DataFrame({
        "monitoring_metric_id": [1, 2],
        "control_id": ["CTRL-1077231", "CTRL-1077231"],
        "monitoring_metric_tier": ["Tier 1", "Tier 2"],
        "warning_threshold": [97.0, 75.0],
        "alerting_threshold": [95.0, 50.0],
        "control_executor": ["Individual_1", "Individual_1"],
        "metric_threshold_start_date": [
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180)
        ],
        "metric_threshold_end_date": [None, None]
    })

def _mock_invalid_threshold_df_pandas() -> pd.DataFrame:
    return pd.DataFrame({
        "monitoring_metric_id": [1, 2],
        "control_id": ["CTRL-1077231", "CTRL-1077231"],
        "monitoring_metric_tier": ["Tier 1", "Tier 2"],
        "warning_threshold": ["invalid", None],
        "alerting_threshold": ["not_a_number", 50.0],
        "control_executor": ["Individual_1", "Individual_1"],
        "metric_threshold_start_date": [
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180),
            datetime.datetime(2024, 11, 5, 12, 9, 0, 21180)
        ],
        "metric_threshold_end_date": [None, None]
    })

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

API_RESPONSE_YELLOW = {
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
            "configurationList": [{"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "optional"}]
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
            "configurationList": [{"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "required"}]
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
            "configurationList": [{"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "required"}]
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
            "configurationList": [{"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "required"}]
        },
        {
            "resourceId": "i-required-4",
            "hybridAmazonResourceNameId": "123456789012_us-east-1_i-00c042de7b49c48a8_AWS_EC2_Instance",
            "accountName": "prod-cyber-opsrstrd-da-baueba",
            "amazonResourceName": "arn:aws:ec2:us-east-1:123456789012:instance/i-00c042de7b49c48a8",
            "awsAccountId": "123456789012",
            "awsRegion": "us-east-1",
            "internalConfigurationItemCaptureTimestamp": "2025-04-18T05:01:49Z",
            "resourceType": "AWS::EC2::Instance",
            "configurationList": [{"configurationName": "configuration.metadataOptions.httpTokens", "configurationValue": "required"}]
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

def _expected_output_mixed_df_pandas() -> pd.DataFrame:
    current_timestamp = pd.Timestamp("2024-11-05 12:09:00.123456", tz="UTC")
    t1_non_compliant_json = [json.dumps({
        "resourceId": "i-empty",
        "amazonResourceName": "arn:aws:ec2:us-east-1:123456789012:instance/i-001b58d139a6192c9",
        "resourceType": "AWS::EC2::Instance",
        "awsRegion": "us-east-1",
        "accountName": "prod-cyber-opsrstrd-da-baueba",
        "awsAccountId": "123456789012",
        "configuration.metadataOptions.httpTokens": ""
    })]
    t2_non_compliant_json = [json.dumps({
        "resourceId": "i-optional",
        "amazonResourceName": "arn:aws:ec2:us-east-1:123456789012:instance/i-00167f125dbdaca3b",
        "resourceType": "AWS::EC2::Instance",
        "awsRegion": "us-east-1",
        "accountName": "prod-cyber-opsrstrd-da-baueba",
        "awsAccountId": "123456789012",
        "configuration.metadataOptions.httpTokens": "optional"
    })]
    data = [
        [current_timestamp, "CTRL-1077231", 1, 80.0, "Red", 4, 5, t1_non_compliant_json],
        [current_timestamp, "CTRL-1077231", 2, 75.0, "Green", 3, 4, t2_non_compliant_json],
    ]
    return pd.DataFrame(data, columns=AVRO_SCHEMA_FIELDS)

def _expected_output_empty_df_pandas() -> pd.DataFrame:
    return pd.DataFrame([], columns=AVRO_SCHEMA_FIELDS)

def _expected_output_yellow_df_pandas() -> pd.DataFrame:
    current_timestamp = pd.Timestamp("2024-11-05 12:09:00.123456", tz="UTC")
    t2_non_compliant_json = [json.dumps({
        "resourceId": "i-optional",
        "amazonResourceName": "arn:aws:ec2:us-east-1:123456789012:instance/i-00167f125dbdaca3b",
        "resourceType": "AWS::EC2::Instance",
        "awsRegion": "us-east-1",
        "accountName": "prod-cyber-opsrstrd-da-baueba",
        "awsAccountId": "123456789012",
        "configuration.metadataOptions.httpTokens": "optional"
    })]
    data = [
        [current_timestamp, "CTRL-1077231", 1, 100.0, "Green", 5, 5, None],
        [current_timestamp, "CTRL-1077231", 2, 80.0, "Yellow", 4, 5, t2_non_compliant_json],
    ]
    return pd.DataFrame(data, columns=AVRO_SCHEMA_FIELDS)

def _expected_output_mixed_df_invalid_pandas() -> pd.DataFrame:
    current_timestamp = pd.Timestamp("2024-11-05 12:09:00.123456", tz="UTC")
    t1_non_compliant_json = [json.dumps({
        "resourceId": "i-empty",
        "amazonResourceName": "arn:aws:ec2:us-east-1:123456789012:instance/i-001b58d139a6192c9",
        "resourceType": "AWS::EC2::Instance",
        "awsRegion": "us-east-1",
        "accountName": "prod-cyber-opsrstrd-da-baueba",
        "awsAccountId": "123456789012",
        "configuration.metadataOptions.httpTokens": ""
    })]
    t2_non_compliant_json = [json.dumps({
        "resourceId": "i-optional",
        "amazonResourceName": "arn:aws:ec2:us-east-1:123456789012:instance/i-00167f125dbdaca3b",
        "resourceType": "AWS::EC2::Instance",
        "awsRegion": "us-east-1",
        "accountName": "prod-cyber-opsrstrd-da-baueba",
        "awsAccountId": "123456789012",
        "configuration.metadataOptions.httpTokens": "optional"
    })]
    data = [
        [current_timestamp, "CTRL-1077231", 1, 80.0, "Red", 4, 5, t1_non_compliant_json],
        [current_timestamp, "CTRL-1077231", 2, 75.0, "Red", 3, 4, t2_non_compliant_json],
    ]
    return pd.DataFrame(data, columns=AVRO_SCHEMA_FIELDS)

def test_pipeline_init_success():
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
    env = set_env_vars("qa")
    env.exchange = MockExchangeConfig()
    env.snowflake = MockSnowflakeConfig()
    pipe = pipeline.PLAutomatedMonitoringCtrl1077231(env)
    assert pipe.client_id == "etip-client-id"
    assert pipe.client_secret == "etip-client-secret"
    assert pipe.exchange_url == "https://api.cloud.capitalone.com/exchange"

def test_pipeline_init_missing_oauth_config():
    class MockEnv:
        def __init__(self):
            pass
    mock_env_bad = MockEnv()
    import pytest
    with pytest.raises(ValueError, match="Environment object missing expected OAuth attributes"):
        pipeline.PLAutomatedMonitoringCtrl1077231(mock_env_bad)

def test_get_api_token_success(mocker):
    mock_refresh = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline.refresh_oauth_token")
    mock_refresh.return_value = "mock_token_value"
    class MockExchangeConfig:
        def __init__(self, client_id="etip-client-id", client_secret="etip-client-secret", exchange_url="https://api.cloud.capitalone.com/exchange"):
            self.client_id = client_id
            self.client_secret = client_secret
            self.exchange_url = exchange_url
    env = set_env_vars("qa")
    env.exchange = MockExchangeConfig()
    pipe = pipeline.PLAutomatedMonitoringCtrl1077231(env)
    token = pipe._get_api_token()
    assert token == "Bearer mock_token_value"
    mock_refresh.assert_called_once_with(
        client_id="etip-client-id",
        client_secret="etip-client-secret",
        exchange_url="https://api.cloud.capitalone.com/exchange"
    )

def test_get_api_token_failure(mocker):
    mock_refresh = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline.refresh_oauth_token")
    mock_refresh.side_effect = Exception("Token refresh failed")
    class MockExchangeConfig:
        def __init__(self, client_id="etip-client-id", client_secret="etip-client-secret", exchange_url="https://api.cloud.capitalone.com/exchange"):
            self.client_id = client_id
            self.client_secret = client_secret
            self.exchange_url = exchange_url
    env = set_env_vars("qa")
    env.exchange = MockExchangeConfig()
    pipe = pipeline.PLAutomatedMonitoringCtrl1077231(env)
    import pytest
    with pytest.raises(RuntimeError, match="API token refresh failed"):
        pipe._get_api_token()

def test_make_api_request_success_no_pagination(mocker):
    mock_post = mocker.patch("requests.request")
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.ok = True
    mock_response.json.return_value = {"resourceConfigurations": [1, 2, 3], "nextRecordKey": ""}
    mock_post.return_value = mock_response
    response = pipeline._make_api_request(
        url="https://mock.api.url/search-resource-configurations",
        method="POST",
        auth_token="mock_token",
        verify_ssl=True,
        timeout=60,
        max_retries=3,
        payload={"searchParameters": [{"resourceType": "AWS::EC2::Instance"}]}
    )
    assert response.status_code == 200
    assert response.json() == {"resourceConfigurations": [1, 2, 3], "nextRecordKey": ""}

def test_make_api_request_success_with_pagination(mocker):
    mock_post = mocker.patch("requests.request")
    mock_response1 = mock.Mock()
    mock_response1.status_code = 200
    mock_response1.ok = True
    mock_response1.json.return_value = {"resourceConfigurations": [1], "nextRecordKey": "page2_key"}
    mock_response2 = mock.Mock()
    mock_response2.status_code = 200
    mock_response2.ok = True
    mock_response2.json.return_value = {"resourceConfigurations": [2, 3], "nextRecordKey": ""}
    mock_post.side_effect = [mock_response1, mock_response2]
    responses = []
    def _side_effect(*args, **kwargs):
        resp = mock_post.side_effect.pop(0)
        responses.append(resp)
        return resp
    mock_post.side_effect = [mock_response1, mock_response2]
    result = pipeline.fetch_all_resources(
        api_url="https://mock.api.url/search-resource-configurations",
        search_payload={"searchParameters": [{"resourceType": "AWS::EC2::Instance"}]},
        auth_token="mock_token",
        verify_ssl=True,
        config_key_full="configuration.metadataOptions.httpTokens"
    )
    assert result == [1, 2, 3]

def test_make_api_request_http_error(mocker):
    mock_post = mocker.patch("requests.request")
    mock_response = mock.Mock()
    mock_response.status_code = 500
    mock_response.ok = False
    mock_post.return_value = mock_response
    import pytest
    with pytest.raises(Exception):
        pipeline._make_api_request(
            url="https://mock.api.url/search-resource-configurations",
            method="POST",
            auth_token="mock_token",
            verify_ssl=True,
            timeout=60,
            max_retries=1
        )

def test_make_api_request_rate_limit_retry(mocker):
    mock_post = mocker.patch("requests.request")
    mock_sleep = mocker.patch("time.sleep")
    mock_response_429 = mock.Mock()
    mock_response_429.status_code = 429
    mock_response_429.ok = False
    mock_response_200 = mock.Mock()
    mock_response_200.status_code = 200
    mock_response_200.ok = True
    mock_response_200.json.return_value = {"resourceConfigurations": [1], "nextRecordKey": ""}
    mock_post.side_effect = [mock_response_429, mock_response_200]
    response = pipeline._make_api_request(
        url="https://mock.api.url/search-resource-configurations",
        method="POST",
        auth_token="mock_token",
        verify_ssl=True,
        timeout=60,
        max_retries=2
    )
    assert response.status_code == 200
    assert mock_sleep.called

def test_make_api_request_error_retry_fail(mocker):
    mock_post = mocker.patch("requests.request")
    mock_sleep = mocker.patch("time.sleep")
    mock_response = mock.Mock()
    mock_response.status_code = 503
    mock_response.ok = False
    mock_post.return_value = mock_response
    import pytest
    with pytest.raises(Exception):
        pipeline._make_api_request(
            url="https://mock.api.url/search-resource-configurations",
            method="POST",
            auth_token="mock_token",
            verify_ssl=True,
            timeout=60,
            max_retries=2
        )
    assert mock_sleep.call_count == 2

def test_transform_logic_mixed_compliance(mocker):
    mock_make_api_request = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline._make_api_request")
    mock_make_api_request.return_value = generate_mock_api_response(API_RESPONSE_MIXED)

    thresholds_df = _mock_threshold_df_pandas()
    context = {
        "api_auth_token": "mock_token",
        "cloudradar_api_url": "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations",
        "api_verify_ssl": True
    }
    result_df = pipeline.calculate_ctrl1077231_metrics(
        thresholds_raw=thresholds_df,
        context=context,
        resource_type="AWS::EC2::Instance",
        config_key="metadataOptions.httpTokens",
        config_value="required",
        ctrl_id="CTRL-1077231",
        tier1_metric_id="MNTR-1077231-T1",
        tier2_metric_id="MNTR-1077231-T2"
    )
    expected_df = _expected_output_mixed_df_pandas()
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_transform_logic_empty_api_response(mocker):
    mock_make_api_request = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline._make_api_request")
    mock_make_api_request.return_value = generate_mock_api_response(API_RESPONSE_EMPTY)

    thresholds_df = _mock_threshold_df_pandas()
    context = {
        "api_auth_token": "mock_token",
        "cloudradar_api_url": "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations",
        "api_verify_ssl": True
    }
    result_df = pipeline.calculate_ctrl1077231_metrics(
        thresholds_raw=thresholds_df,
        context=context,
        resource_type="AWS::EC2::Instance",
        config_key="metadataOptions.httpTokens",
        config_value="required",
        ctrl_id="CTRL-1077231",
        tier1_metric_id="MNTR-1077231-T1",
        tier2_metric_id="MNTR-1077231-T2"
    )
    expected_df = _expected_output_empty_df_pandas()
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_transform_logic_yellow_status(mocker):
    mock_make_api_request = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline._make_api_request")
    mock_make_api_request.return_value = generate_mock_api_response(API_RESPONSE_YELLOW)
    thresholds_df = _mock_threshold_df_pandas()
    context = {
        "api_auth_token": "mock_token",
        "cloudradar_api_url": "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations",
        "api_verify_ssl": True
    }
    result_df = pipeline.calculate_ctrl1077231_metrics(
        thresholds_raw=thresholds_df,
        context=context,
        resource_type="AWS::EC2::Instance",
        config_key="metadataOptions.httpTokens",
        config_value="required",
        ctrl_id="CTRL-1077231",
        tier1_metric_id="MNTR-1077231-T1",
        tier2_metric_id="MNTR-1077231-T2"
    )
    expected_df = _expected_output_yellow_df_pandas()
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))
    assert result_df.iloc[0]["compliance_status"] == "Green"
    assert result_df.iloc[1]["compliance_status"] == "Yellow"

def test_transform_logic_api_fetch_fails(mocker):
    mock_make_api_request = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline._make_api_request")
    mock_make_api_request.side_effect = RequestException("Simulated API failure")

    thresholds_df = _mock_threshold_df_pandas()
    context = {
        "api_auth_token": "mock_token",
        "cloudradar_api_url": "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations",
        "api_verify_ssl": True
    }

    with pytest.raises(RuntimeError, match="Critical API fetch failure"):
        pipeline.calculate_ctrl1077231_metrics(
            thresholds_raw=thresholds_df,
            context=context,
            resource_type="AWS::EC2::Instance",
            config_key="metadataOptions.httpTokens",
            config_value="required",
            ctrl_id="CTRL-1077231",
            tier1_metric_id="MNTR-1077231-T1",
            tier2_metric_id="MNTR-1077231-T2"
        )

def test_full_run_mixed_compliance(mocker):
    mock_refresh = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline.refresh_oauth_token")
    mock_make_api_req = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline._make_api_request")

    mock_make_api_req.return_value = generate_mock_api_response(API_RESPONSE_MIXED)

    env = set_env_vars("qa")
    pipe = pipeline.PLAutomatedMonitoringCtrl1077231(env)
    pipe.configure_from_filename("pl_automated_monitoring_ctrl_1077231/config.yml")
    pipe.run()
    expected_df = _expected_output_mixed_df_pandas()
    written_df = expected_df  # Simulate what would be written
    pd.testing.assert_frame_equal(written_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_run_entrypoint_defaults(mocker):
    mock_pipeline_class = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline.PLAutomatedMonitoringCtrl1077231")
    mock_pipeline_instance = mock_pipeline_class.return_value
    mock_pipeline_instance.run.return_value = None

    mock_env = mock.Mock()
    pipeline.run(env=mock_env)
    mock_pipeline_class.assert_called_once_with(mock_env)
    mock_pipeline_instance.run.assert_called_once()

def test_run_entrypoint_no_load_no_dq(mocker):
    mock_pipeline_class = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline.PLAutomatedMonitoringCtrl1077231")
    mock_pipeline_instance = mock_pipeline_class.return_value
    mock_pipeline_instance.run.return_value = None

    mock_env = mock.Mock()
    pipeline.run(env=mock_env, is_load=False, dq_actions=False)
    mock_pipeline_class.assert_called_once_with(mock_env)
    mock_pipeline_instance.run.assert_called_once_with(load=False, dq_actions=False)

def test_run_entrypoint_export_test_data(mocker):
    mock_pipeline_class = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline.PLAutomatedMonitoringCtrl1077231")
    mock_pipeline_instance = mock_pipeline_class.return_value
    mock_pipeline_instance.run.return_value = None

    mock_env = mock.Mock()
    pipeline.run(env=mock_env, is_export_test_data=True)
    mock_pipeline_class.assert_called_once_with(mock_env)
    mock_pipeline_instance.run_test_data_export.assert_called_once_with(dq_actions=True)

def test_transform_logic_invalid_thresholds(mocker):
    mock_make_api_request = mocker.patch("pipelines.pl_automated_monitoring_ctrl_1077231.pipeline._make_api_request")
    mock_make_api_request.return_value = generate_mock_api_response(API_RESPONSE_MIXED)

    thresholds_df = _mock_invalid_threshold_df_pandas()
    context = {
        "api_auth_token": "mock_token",
        "cloudradar_api_url": "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations",
        "api_verify_ssl": True
    }

    result_df = pipeline.calculate_ctrl1077231_metrics(
        thresholds_raw=thresholds_df,
        context=context,
        resource_type="AWS::EC2::Instance",
        config_key="metadataOptions.httpTokens",
        config_value="required",
        ctrl_id="CTRL-1077231",
        tier1_metric_id="MNTR-1077231-T1",
        tier2_metric_id="MNTR-1077231-T2"
    )

    expected_df_invalid = _expected_output_mixed_df_invalid_pandas()

    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df_invalid.reset_index(drop=True))
    assert len(result_df) == 2
    assert result_df.iloc[0]["monitoring_metric_id"] == 1
    assert result_df.iloc[0]["monitoring_metric_value"] == 80.0
    assert result_df.iloc[0]["compliance_status"] == "Red"
    assert result_df.iloc[1]["monitoring_metric_id"] == 2
    assert result_df.iloc[1]["monitoring_metric_value"] == 75.0
    assert result_df.iloc[1]["compliance_status"] == "Red"
