import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
import requests
import json
import time
from datetime import datetime
from config_pipeline import ConfigPipeline
from etip_env import Env
from connectors.exchange.oauth_token import refresh as refresh_oauth_token
from connectors.ca_certs import C1_CERT_FILE
from transform_library import transformer

logger = logging.getLogger(__name__)

def run(
    env: Env,
    is_export_test_data: bool = False,
    is_load: bool = True,
    dq_actions: bool = True,
):
    pipeline = PLAutomatedMonitoringCtrl1077231(env)
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    logger.info(f"Running pipeline: {pipeline.pipeline_name}")
    return (
        pipeline.run_test_data_export(dq_actions=dq_actions)
        if is_export_test_data
        else pipeline.run(load=is_load, dq_actions=dq_actions)
    )

class PLAutomatedMonitoringCtrl1077231(ConfigPipeline):
    def __init__(self,
        env: Env,
    ) -> None:
        super().__init__(env)
        self.env = env

        self.cloudradar_api_url = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
        try:
            self.client_id = self.env.exchange.client_id
            self.client_secret = self.env.exchange.client_secret
            self.exchange_url = self.env.exchange.exchange_url
        except AttributeError as e:
            logger.error(f"Missing OAuth configuration in environment: {e}")
            raise ValueError(f"Environment object missing expected OAuth attributes: {e}") from e

    def _get_api_token(self) -> str:
        logger.info(f"Refreshing API token from {self.exchange_url}...")
        try:
            token = refresh_oauth_token(
                client_id=self.client_id,
                client_secret=self.client_secret,
                exchange_url=self.exchange_url,
            )
            logger.info("API token refreshed successfully.")
            return f"Bearer {token}"
        except Exception as e:
            logger.error(f"Failed to refresh API token: {e}")
            raise RuntimeError("API token refresh failed") from e

    def transform(self) -> None:
        logger.info("Preparing transform stage: Fetching API token...")
        api_auth_token = self._get_api_token()

        self.context["api_auth_token"] = api_auth_token
        self.context["cloud_tooling_api_url"] = self.cloud_tooling_api_url
        self.context["api_verify_ssl"] = C1_CERT_FILE

        logger.info("API context injected. Proceeding with config-defined transforms.")
        super().transform()

def _make_api_request(url: str, method: str, auth_token: str, verify_ssl: Any, timeout: int, max_retries: int, payload: Optional[Dict] = None, params: Optional[Dict] = None) -> requests.Response:
    headers = {"Accept": "application/json;v=1.0", "Authorization": auth_token, "Content-Type": "application/json"}
    for retry in range(max_retries + 1):
        try:
            response = requests.request(method=method, url=url, headers=headers, json=payload, params=params, verify=verify_ssl, timeout=timeout)
            if response.status_code == 429:
                wait_time = min(2 ** retry, 30)
                time.sleep(wait_time)
                if retry == max_retries:
                    response.raise_for_status()
                continue
            elif response.ok:
                return response
            else:
                if retry < max_retries:
                    wait_time = min(2 ** retry, 15)
                    time.sleep(wait_time)
                else:
                    response.raise_for_status()
        except requests.exceptions.Timeout:
            if retry < max_retries:
                wait_time = min(2 ** retry, 15)
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            if retry < max_retries:
                wait_time = min(2 ** retry, 15)
                time.sleep(wait_time)
            else:
                raise
    raise Exception(f"API request failed after {max_retries} retries to {url}")

def fetch_all_resources(api_url: str, search_payload: Dict, auth_token: str, verify_ssl: Any, config_key_full: str, limit: Optional[int] = None, timeout: int = 60, max_retries: int = 3) -> List[Dict]:
    all_resources = []
    next_record_key = ""
    response_fields = ["resourceId", "amazonResourceName", "resourceType", "awsRegion", "accountName", "awsAccountId", "configurationList", config_key_full]
    fetch_payload = {"searchParameters": search_payload.get("searchParameters", [{}]), "responseFields": response_fields}
    while True:
        params = {"limit": min(limit, 10000) if limit else 10000}
        if next_record_key:
            params["nextRecordKey"] = next_record_key
        response = _make_api_request(url=api_url, method="POST", auth_token=auth_token, verify_ssl=verify_ssl, timeout=timeout, max_retries=max_retries, payload=fetch_payload, params=params)
        data = response.json()
        resources = data.get("resourceConfigurations", [])
        new_next_record_key = data.get("nextRecordKey", "")
        all_resources.extend(resources)
        next_record_key = new_next_record_key
        if not next_record_key or (limit and len(all_resources) >= limit):
            break
    return all_resources

def get_compliance_status(metric: float, alert_threshold: float, warning_threshold: Optional[float] = None) -> str:
    metric_percentage = metric * 100
    if metric_percentage >= alert_threshold:
        return "Green"
    elif warning_threshold is not None and metric_percentage >= warning_threshold:
        return "Yellow"
    else:
        return "Red"

def _filter_resources(resources: List[Dict], config_key: str, config_value: str) -> Tuple[int, int, List[Dict], List[Dict]]:
    tier1_numerator = 0
    tier2_numerator = 0
    tier1_non_compliant = []
    tier2_non_compliant = []
    config_key_full = f"configuration.{config_key}"
    fields_to_keep = ["resourceId", "amazonResourceName", "resourceType", "awsRegion", "accountName", "awsAccountId", config_key_full]
    for resource in resources:
        config_list = resource.get("configurationList", [])
        config_item = next((c for c in config_list if c.get("configurationName") == config_key_full), None)
        actual_value = config_item.get("configurationValue") if config_item else None
        if actual_value and str(actual_value).strip():
            tier1_numerator += 1
            if str(actual_value) == config_value:
                tier2_numerator += 1
            else:
                non_compliant_info = {f: resource.get(f, "N/A") for f in fields_to_keep}
                non_compliant_info[config_key_full] = actual_value
                tier2_non_compliant.append(non_compliant_info)
        else:
            non_compliant_info = {f: resource.get(f, "N/A") for f in fields_to_keep}
            non_compliant_info[config_key_full] = actual_value if actual_value is not None else "MISSING"
            tier1_non_compliant.append(non_compliant_info)
    return tier1_numerator, tier2_numerator, tier1_non_compliant, tier2_non_compliant

@transformer
def calculate_ctrl1077231_metrics(thresholds_raw: pd.DataFrame, context: Dict[str, Any], resource_type: str, config_key: str, config_value: str, ctrl_id: str, tier1_metric_id: int, tier2_metric_id: int) -> pd.DataFrame:
    api_url = context["cloudradar_api_url"]
    auth_token = context["api_auth_token"]
    verify_ssl = context["api_verify_ssl"]
    config_key_full = f"configuration.{config_key}"
    search_payload = {"searchParameters": [{"resourceType": resource_type}]}
    resources = fetch_all_resources(api_url, search_payload, auth_token, verify_ssl, config_key_full)
    tier1_numerator, tier2_numerator, tier1_non_compliant, tier2_non_compliant = _filter_resources(resources, config_key, config_value)
    total_resources = len(resources)
    tier1_metric = tier1_numerator / total_resources if total_resources > 0 else 0
    tier2_metric = tier2_numerator / tier1_numerator if tier1_numerator > 0 else 0
    t1_threshold = thresholds_raw[thresholds_raw["monitoring_metric_id"] == tier1_metric_id].iloc[0]
    t2_threshold = thresholds_raw[thresholds_raw["monitoring_metric_id"] == tier2_metric_id].iloc[0]
    t1_status = get_compliance_status(tier1_metric, t1_threshold["alerting_threshold"], t1_threshold["warning_threshold"])
    t2_status = get_compliance_status(tier2_metric, t2_threshold["alerting_threshold"], t2_threshold["warning_threshold"])
    now = int(datetime.utcnow().timestamp() * 1000)
    results = [
        {"date": now, "control_id": ctrl_id, "monitoring_metric_id": tier1_metric_id, "monitoring_metric_value": float(tier1_metric * 100), "compliance_status": t1_status, "numerator": int(tier1_numerator), "denominator": int(total_resources), "non_compliant_resources": [json.dumps(x) for x in tier1_non_compliant] if tier1_non_compliant else None},
        {"date": now, "control_id": ctrl_id, "monitoring_metric_id": tier2_metric_id, "monitoring_metric_value": float(tier2_metric * 100), "compliance_status": t2_status, "numerator": int(tier2_numerator), "denominator": int(tier1_numerator), "non_compliant_resources": [json.dumps(x) for x in tier2_non_compliant] if tier2_non_compliant else None}
    ]
    df = pd.DataFrame(results)
    df["date"] = df["date"].astype("int64")
    df["numerator"] = df["numerator"].astype("int64")
    df["denominator"] = df["denominator"].astype("int64")
    return df

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    logger = logging.getLogger(__name__)
    
    from etip_env import set_env_vars
    env = set_env_vars()
    
    logger.info("Starting local pipeline run...")
    try:
        run(env=env, is_load=False, dq_actions=False)
        logger.info("Pipeline run completed successfully")
    except Exception as e:
        logger.exception("Pipeline run failed")
        exit(1)
