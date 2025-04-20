import requests
import json
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timezone
import time
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                             LongType, TimestampType, ArrayType, IntegerType)

from transform_library import transformer

logger = logging.getLogger(__name__)

DEFAULT_RESOURCE_TYPE = "AWS::EC2::Instance"
DEFAULT_CONFIG_KEY = "metadataOptions.httpTokens"
DEFAULT_CONFIG_VALUE = "required"
DEFAULT_TIER1_METRIC_ID = "MNTR-1077231-T1"
DEFAULT_TIER2_METRIC_ID = "MNTR-1077231-T2"
DEFAULT_CTRL_ID = "CTRL-1077231"
DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 3

NON_COMPLIANT_FIELDS = [
    "resourceId",
    "amazonResourceName",
    "resourceType",
    "awsRegion",
    "accountName",
    "awsAccountId",
]

OUTPUT_SCHEMA = StructType([
    StructField("date", TimestampType(), False),
    StructField("control_id", StringType(), False),
    StructField("monitoring_metric_id", IntegerType(), False),
    StructField("monitoring_metric_value", DoubleType(), False),
    StructField("compliance_status", StringType(), False),
    StructField("numerator", LongType(), False),
    StructField("denominator", LongType(), False),
    StructField("non_compliant_resources", ArrayType(StringType()), True)
])

def _make_api_request(
    url: str,
    method: str,
    auth_token: str,
    verify_ssl: Any,
    timeout: int,
    max_retries: int,
    payload: Optional[Dict] = None,
    params: Optional[Dict] = None,
) -> requests.Response:
    headers = {
        "Accept": "application/json;v=1.0",
        "Authorization": auth_token,
        "Content-Type": "application/json",
    }

    for retry in range(max_retries + 1):
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=payload,
                params=params,
                verify=verify_ssl,
                timeout=timeout,
            )

            if response.status_code == 429:
                wait_time = min(2 ** retry, 30)
                logger.warning(
                    f"Rate limited ({response.status_code}) on {url}. Waiting {wait_time}s before retry {retry + 1}/{max_retries}."
                )
                time.sleep(wait_time)
                if retry == max_retries:
                    logger.error(f"Max retries reached for rate limiting on {url}.")
                    response.raise_for_status()
                continue

            elif response.ok:
                return response

            else:
                logger.error(f"API Error on {url}: {response.status_code} - {response.text}")
                if retry < max_retries:
                    wait_time = min(2 ** retry, 15)
                    logger.info(f"Retrying in {wait_time}s... (Attempt {retry + 1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    response.raise_for_status()

        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout after {timeout}s on {url}")
            if retry < max_retries:
                wait_time = min(2 ** retry, 15)
                logger.info(f"Retrying in {wait_time}s... (Attempt {retry + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"Max retries reached after timeouts on {url}.")
                raise

        except Exception as e:
            logger.error(f"Exception during API request to {url}: {str(e)}")
            if retry < max_retries:
                wait_time = min(2 ** retry, 15)
                logger.info(f"Retrying in {wait_time}s... (Attempt {retry + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"Max retries reached after exception on {url}.")
                raise

    raise Exception(f"API request failed after {max_retries} retries to {url}")

def fetch_all_resources(
    api_url: str,
    search_payload: Dict,
    auth_token: str,
    verify_ssl: Any,
    config_key_full: str,
    limit: Optional[int] = None,
    timeout: int = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> List[Dict]:
    all_resources = []
    total_fetched = 0
    next_record_key = ""
    page_count = 0
    start_time = datetime.now()

    response_fields = list(set(NON_COMPLIANT_FIELDS + ["configurationList", config_key_full]))

    fetch_payload = {
        "searchParameters": search_payload.get("searchParameters", [{}]),
        "responseFields": response_fields,
    }

    logger.info(f"Fetching resources from {api_url} with payload: {json.dumps(fetch_payload, indent=2)}")

    while True:
        params = {"limit": min(limit, 10000) if limit else 10000}
        if next_record_key:
            params["nextRecordKey"] = next_record_key

        try:
            logger.info(
                f"Requesting page {page_count + 1} with nextRecordKey: '{next_record_key or 'None'}'"
            )
            response = _make_api_request(
                url=api_url,
                method="POST",
                auth_token=auth_token,
                verify_ssl=verify_ssl,
                timeout=timeout,
                max_retries=max_retries,
                payload=fetch_payload,
                params=params,
            )

            data = response.json()
            resources = data.get("resourceConfigurations", [])
            new_next_record_key = data.get("nextRecordKey", "")

            all_resources.extend(resources)
            total_fetched += len(resources)

            logger.info(
                f"Page {page_count + 1}: Fetched {len(resources)} resources, total: {total_fetched}, "
                f"nextRecordKey: '{new_next_record_key or 'None'}'"
            )

            next_record_key = new_next_record_key
            page_count += 1

        except Exception as e:
            logger.error(f"Failed to fetch resources page {page_count + 1}: {e}")
            raise RuntimeError(f"Critical API fetch failure: {e}") from e

        if not next_record_key or (limit and total_fetched >= limit):
            break

    total_time = (datetime.now() - start_time).total_seconds()
    logger.info(
        f"Fetch complete: {total_fetched} resources in {page_count} pages, {total_time:.1f} seconds"
    )
    return all_resources

def get_compliance_status(
    metric: float, alert_threshold: float, warning_threshold: Optional[float] = None
) -> str:
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

    fields_to_keep = list(set(NON_COMPLIANT_FIELDS + [config_key_full]))

    for resource in resources:
        config_list = resource.get("configurationList", [])
        config_item = next((
            c for c in config_list if c.get("configurationName") == config_key_full
        ), None)
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
def calculate_ctrl1077231_metrics(
    spark: SparkSession,
    thresholds_raw: DataFrame,
    context: Dict[str, Any],
    resource_type: str,
    config_key: str,
    config_value: str,
    ctrl_id: str,
    tier1_metric_id: str,
    tier2_metric_id: str,
) -> DataFrame:
    logger.info(f"Starting CTRL-1077231 metric calculation for {resource_type}...")

    api_url = context.get("cloud_tooling_api_url")
    auth_token = context.get("api_auth_token")
    verify_ssl = context.get("api_verify_ssl", True)
    timeout = context.get("api_timeout", DEFAULT_TIMEOUT)
    max_retries = context.get("api_max_retries", DEFAULT_MAX_RETRIES)

    if not api_url or not auth_token:
        logger.error("Missing API URL or Auth Token in context.")
        raise ValueError("API URL and Auth Token must be provided in context.")

    id_map = {
        1: tier1_metric_id,
        2: tier2_metric_id,
    }

    mapping_expr = F.create_map([F.lit(x) for x in sum(id_map.items(), ())])

    thresholds_processed = thresholds_raw.withColumn(
        "_string_metric_id", mapping_expr[F.col("monitoring_metric_id")]
    )

    thresholds_df = thresholds_processed.filter(
        (F.col("_string_metric_id") == tier1_metric_id) |
        (F.col("_string_metric_id") == tier2_metric_id)
    ).select(
        F.col("monitoring_metric_id"),
        F.col("_string_metric_id").alias("string_metric_id"),
        F.col("warning_threshold"),
        F.col("alerting_threshold")
    ).persist()

    if thresholds_df.isEmpty():
        logger.warning("No matching threshold data found for the provided metric IDs.")
        return spark.createDataFrame([], schema=OUTPUT_SCHEMA)

    threshold_map = {
        row["string_metric_id"]: {
            "warning": row["warning_threshold"],
            "alert": row["alerting_threshold"]
        } for row in thresholds_df.collect()
    }
    thresholds_df.unpersist()

    tier1_alert = threshold_map.get(tier1_metric_id, {}).get("alert", 100.0)
    tier1_warn = threshold_map.get(tier1_metric_id, {}).get("warning")
    tier2_alert = threshold_map.get(tier2_metric_id, {}).get("alert", 100.0)
    tier2_warn = threshold_map.get(tier2_metric_id, {}).get("warning")

    logger.info(f"Thresholds - Tier 1: Alert={tier1_alert}, Warn={tier1_warn} | Tier 2: Alert={tier2_alert}, Warn={tier2_warn}")

    search_payload = {"searchParameters": [{"resourceType": resource_type}]}
    config_key_full = f"configuration.{config_key}"

    all_resources = fetch_all_resources(
        api_url=api_url,
        search_payload=search_payload,
        auth_token=auth_token,
        verify_ssl=verify_ssl,
        config_key_full=config_key_full,
        timeout=timeout,
        max_retries=max_retries,
    )

    if not all_resources:
        logger.warning(f"No resources found for type {resource_type}. Returning empty DataFrame.")
        return spark.createDataFrame([], schema=OUTPUT_SCHEMA)

    tier1_numerator, tier2_numerator, tier1_non_compliant, tier2_non_compliant = _filter_resources(
        all_resources, config_key, config_value
    )

    denominator = len(all_resources)
    tier2_denominator = tier1_numerator

    logger.info(f"Calculation: Tier 1: {tier1_numerator}/{denominator}, Tier 2: {tier2_numerator}/{tier2_denominator}")
    logger.info(f"Non-compliant counts: Tier 1: {len(tier1_non_compliant)}, Tier 2: {len(tier2_non_compliant)}")

    tier1_metric = (tier1_numerator / denominator) if denominator > 0 else 0.0
    tier2_metric = (tier2_numerator / tier2_denominator) if tier2_denominator > 0 else 0.0

    tier1_status = get_compliance_status(tier1_metric, tier1_alert, tier1_warn)
    tier2_status = get_compliance_status(tier2_metric, tier2_alert, tier2_warn)

    string_to_int_id_map = {v: k for k, v in id_map.items()}
    tier1_int_id = string_to_int_id_map.get(tier1_metric_id)
    tier2_int_id = string_to_int_id_map.get(tier2_metric_id)

    current_timestamp = datetime.now(timezone.utc)

    output_data = []
    if tier1_int_id is not None:
         output_data.append((
            current_timestamp,
            ctrl_id,
            tier1_int_id,
            round(tier1_metric * 100, 2),
            tier1_status,
            tier1_numerator,
            denominator,
            [json.dumps(res) for res in tier1_non_compliant] if tier1_non_compliant else None
        ))
    else:
        logger.warning(f"Could not find integer ID mapping for string ID: {tier1_metric_id}. Tier 1 output skipped.")

    if tier2_denominator > 0:
        if tier2_int_id is not None:
             output_data.append((
                current_timestamp,
                ctrl_id,
                tier2_int_id,
                round(tier2_metric * 100, 2),
                tier2_status,
                tier2_numerator,
                tier2_denominator,
                [json.dumps(res) for res in tier2_non_compliant] if tier2_non_compliant else None
            ))
        else:
             logger.warning(f"Could not find integer ID mapping for string ID: {tier2_metric_id}. Tier 2 output skipped.")
    else:
        logger.info("Tier 2 metric skipped as denominator is 0 (no resources had httpTokens set).")

    logger.info("Metric calculation finished. Creating output DataFrame with snake_case schema.")
    return spark.createDataFrame(output_data, schema=OUTPUT_SCHEMA)

def transform_function(name):
    def decorator(func):
        logger.debug(f"Registering transform function: {name}")
        func._transform_name = name
        return func
    return decorator 
