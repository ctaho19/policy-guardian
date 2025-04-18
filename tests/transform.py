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
                             LongType, TimestampType, ArrayType)

from transform_library import transformer

# Assuming logger is configured by the ETIP framework or pipeline.py
logger = logging.getLogger(__name__)

# --- Constants (Mostly sourced from config/context now) ---
DEFAULT_RESOURCE_TYPE = "AWS::EC2::Instance"
DEFAULT_CONFIG_KEY = "metadataOptions.httpTokens"
DEFAULT_CONFIG_VALUE = "required"
DEFAULT_TIER1_METRIC_ID = "MNTR-1077231-T1"
DEFAULT_TIER2_METRIC_ID = "MNTR-1077231-T2"
DEFAULT_CTRL_ID = "CTRL-1077231"
DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 3

# Fields needed for non-compliant reporting
NON_COMPLIANT_FIELDS = [
    "resourceId",
    "amazonResourceName",
    "resourceType",
    "awsRegion",
    "accountName",
    "awsAccountId",
    # Specific config field added dynamically based on config_key
]

# Output schema structure matching avro_schema.json
OUTPUT_SCHEMA = StructType([
    StructField("DATE", TimestampType(), False),
    StructField("CTRL_ID", StringType(), False),
    StructField("MONITORING_METRIC_NUMBER", StringType(), False),
    StructField("MONITORING_METRIC", DoubleType(), False),
    StructField("COMPLIANCE_STATUS", StringType(), False),
    StructField("NUMERATOR", LongType(), False),
    StructField("DENOMINATOR", LongType(), False),
    StructField("NON_COMPLIANT_RESOURCES", ArrayType(StringType()), True)
])

# --- API Interaction Functions (Modified to accept auth context) ---

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
    """Helper function to make requests with retry logic."""
    headers = {
        "Accept": "application/json;v=1.0",
        "Authorization": auth_token, # Use passed token
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
                verify=verify_ssl, # Use passed verification setting
                timeout=timeout,
            )

            if response.status_code == 429: # Rate limited
                wait_time = min(2 ** retry, 30)
                logger.warning(
                    f"Rate limited ({response.status_code}) on {url}. Waiting {wait_time}s before retry {retry + 1}/{max_retries}."
                )
                time.sleep(wait_time)
                if retry == max_retries:
                    logger.error(f"Max retries reached for rate limiting on {url}.")
                    response.raise_for_status() # Raise final error
                continue # Go to next retry iteration

            elif response.ok: # Success (2xx)
                return response

            else: # Other non-success status codes
                logger.error(f"API Error on {url}: {response.status_code} - {response.text}")
                if retry < max_retries:
                    wait_time = min(2 ** retry, 15)
                    logger.info(f"Retrying in {wait_time}s... (Attempt {retry + 1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    response.raise_for_status() # Raise final error after retries

        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout after {timeout}s on {url}")
            if retry < max_retries:
                wait_time = min(2 ** retry, 15)
                logger.info(f"Retrying in {wait_time}s... (Attempt {retry + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"Max retries reached after timeouts on {url}.")
                raise # Reraise the timeout exception

        except Exception as e:
            logger.error(f"Exception during API request to {url}: {str(e)}")
            if retry < max_retries:
                wait_time = min(2 ** retry, 15)
                logger.info(f"Retrying in {wait_time}s... (Attempt {retry + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"Max retries reached after exception on {url}.")
                raise # Reraise the exception

    # Should not be reached if logic is correct, but added for safety
    raise Exception(f"API request failed after {max_retries} retries to {url}")


def fetch_all_resources(
    api_url: str,
    search_payload: Dict,
    auth_token: str,
    verify_ssl: Any,
    config_key_full: str, # Pass full config key like 'configuration.metadataOptions.httpTokens'
    limit: Optional[int] = None,
    timeout: int = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> List[Dict]:
    """Fetch all cloud resources using pagination."""
    all_resources = []
    total_fetched = 0
    next_record_key = ""
    page_count = 0
    start_time = datetime.now()

    # Define standard fields plus the specific config key being checked
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


# --- Compliance Calculation Function ---

def get_compliance_status(
    metric: float, alert_threshold: float, warning_threshold: Optional[float] = None
) -> str:
    """Determine compliance status (Green/Yellow/Red)."""
    metric_percentage = metric * 100

    # Ensure thresholds are treated as percentages (0-100)
    if metric_percentage >= alert_threshold:
        return "Green"
    elif warning_threshold is not None and metric_percentage >= warning_threshold:
        return "Yellow"
    else:
        return "Red"

def _filter_resources(resources: List[Dict], config_key: str, config_value: str) -> Tuple[int, int, List[Dict], List[Dict]]:
    """Filters resources for Tier 1 and Tier 2 compliance."""
    tier1_numerator = 0
    tier2_numerator = 0
    tier1_non_compliant = []
    tier2_non_compliant = []
    config_key_full = f"configuration.{config_key}"

    # Dynamically add the specific config field to the list of fields to keep for non-compliant reporting
    fields_to_keep = list(set(NON_COMPLIANT_FIELDS + [config_key_full]))

    for resource in resources:
        config_list = resource.get("configurationList", [])
        # Find the specific configuration item
        config_item = next((
            c for c in config_list if c.get("configurationName") == config_key_full
        ), None)
        actual_value = config_item.get("configurationValue") if config_item else None

        # Tier 1 Check: Does the config value exist and is non-empty?
        if actual_value and str(actual_value).strip():
            tier1_numerator += 1
            # Tier 2 Check (only if Tier 1 passed): Does the value match the desired value?
            if str(actual_value) == config_value:
                tier2_numerator += 1
            else:
                # Tier 2 Non-compliant (exists but wrong value)
                non_compliant_info = {f: resource.get(f, "N/A") for f in fields_to_keep}
                non_compliant_info[config_key_full] = actual_value # Ensure actual value is captured
                tier2_non_compliant.append(non_compliant_info)
        else:
            # Tier 1 Non-compliant (missing or empty value)
            non_compliant_info = {f: resource.get(f, "N/A") for f in fields_to_keep}
            non_compliant_info[config_key_full] = actual_value if actual_value is not None else "MISSING" # Indicate if missing
            tier1_non_compliant.append(non_compliant_info)

    return tier1_numerator, tier2_numerator, tier1_non_compliant, tier2_non_compliant

# --- Main Transformation Function (called by config.yml) ---

@transformer
def calculate_ctrl1077231_metrics(
    spark: SparkSession, # SparkSession is typically available
    thresholds_raw: DataFrame, # Input DataFrame from config.yml
    context: Dict[str, Any], # Context from pipeline.py and config.yml
    # Options passed from config.yml
    resource_type: str,
    config_key: str,
    config_value: str,
    ctrl_id: str,
    tier1_metric_id: str,
    tier2_metric_id: str,
) -> DataFrame:
    """
    Calculates Tier 1 and Tier 2 metrics for CTRL-1077231.
    Fetches EC2 data, filters based on httpTokens, calculates compliance.
    """
    logger.info(f"Starting CTRL-1077231 metric calculation for {resource_type}...")

    # --- Get API Credentials from Context ---
    try:
        api_auth_token = context["api_auth_token"]
        api_url = context["cloud_tooling_api_url"]
        api_verify_ssl = context.get("api_verify_ssl", True)
    except KeyError as e:
        logger.error(f"Missing required API context key: {e}")
        raise

    # --- Fetch Resources ---
    api_payload = {
        "searchParameters": [{
            "resourceType": resource_type,
            # Add other necessary search parameters if needed
        }]
    }
    config_key_full = f"configuration.{config_key}"
    all_resources = fetch_all_resources(
        api_url=api_url,
        search_payload=api_payload,
        auth_token=api_auth_token,
        verify_ssl=api_verify_ssl,
        config_key_full=config_key_full,
        # Add timeout/retries from config/context if needed
    )

    total_denominator = len(all_resources)
    if total_denominator == 0:
        logger.warning(f"No resources found for {resource_type}. Returning empty metrics.")
        return spark.createDataFrame([], OUTPUT_SCHEMA)

    # --- Filter Resources ---
    logger.info(f"Filtering {total_denominator} resources based on {config_key}...")
    tier1_numerator, tier2_numerator, tier1_non_compliant_list, tier2_non_compliant_list = _filter_resources(
        all_resources, config_key, config_value
    )

    logger.info(f"Filtering complete: Tier1 Num={tier1_numerator}, Tier2 Num={tier2_numerator}, Denom={total_denominator}")

    # --- Load and Process Thresholds ---
    try:
        thresholds_pd = thresholds_raw.toPandas()
        tier1_thresh_row = thresholds_pd[thresholds_pd['MONITORING_METRIC_ID'] == tier1_metric_id]
        tier2_thresh_row = thresholds_pd[thresholds_pd['MONITORING_METRIC_ID'] == tier2_metric_id]

        t1_alert = float(tier1_thresh_row.iloc[0]['ALERT_THRESHOLD']) if not tier1_thresh_row.empty else 100.0
        t1_warn_str = tier1_thresh_row.iloc[0]['WARNING_THRESHOLD'] if not tier1_thresh_row.empty else None
        t1_warn = float(t1_warn_str) if t1_warn_str not in [None, 'NULL', 'None'] else None

        t2_alert = float(tier2_thresh_row.iloc[0]['ALERT_THRESHOLD']) if not tier2_thresh_row.empty else 100.0
        t2_warn_str = tier2_thresh_row.iloc[0]['WARNING_THRESHOLD'] if not tier2_thresh_row.empty else None
        t2_warn = float(t2_warn_str) if t2_warn_str not in [None, 'NULL', 'None'] else None
        logger.info(f"Thresholds loaded: T1 Alert={t1_alert}, T1 Warn={t1_warn}, T2 Alert={t2_alert}, T2 Warn={t2_warn}")
    except Exception as e:
        logger.error(f"Failed to process thresholds DataFrame: {e}. Using default thresholds.")
        t1_alert, t1_warn, t2_alert, t2_warn = 100.0, None, 100.0, None

    # --- Calculate Metrics and Compliance Status ---
    tier1_metric = tier1_numerator / total_denominator if total_denominator > 0 else 0.0
    tier1_status = get_compliance_status(tier1_metric, t1_alert, t1_warn)

    tier2_denominator = tier1_numerator # Tier 2 is % of Tier 1 compliant
    tier2_metric = tier2_numerator / tier2_denominator if tier2_denominator > 0 else 0.0
    tier2_status = get_compliance_status(tier2_metric, t2_alert, t2_warn)

    # --- Prepare Non-Compliant Data (Convert dicts to JSON strings) ---
    # Tier 1 non-compliant are those missing the value
    tier1_non_compliant_json = [json.dumps(item) for item in tier1_non_compliant_list] if tier1_status != "Green" else None
    # Tier 2 non-compliant are those with the wrong value
    tier2_non_compliant_json = [json.dumps(item) for item in tier2_non_compliant_list] if tier2_status != "Green" else None

    # --- Create Output DataFrame ---
    current_timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    output_data = [
        (current_timestamp_ms, ctrl_id, tier1_metric_id, round(tier1_metric * 100, 4), tier1_status, tier1_numerator, total_denominator, tier1_non_compliant_json),
        (current_timestamp_ms, ctrl_id, tier2_metric_id, round(tier2_metric * 100, 4), tier2_status, tier2_numerator, tier2_denominator, tier2_non_compliant_json)
    ]

    results_df = spark.createDataFrame(output_data, schema=OUTPUT_SCHEMA)
    logger.info(f"Created metrics DataFrame with {results_df.count()} rows.")
    results_df.show(truncate=False) # Log output for debugging

    return results_df

# --- Helper Decorator (Example - adapt to actual framework registration) ---
def transform_function(name):
    """Decorator to register the function with the framework (example)."""
    def decorator(func):
        # In a real scenario, this would likely interact with a registry
        logger.debug(f"Registering transform function: {name}")
        func._transform_name = name
        return func
    return decorator

# Removed original get_summary_count and old filter_resources_and_calculate_metrics
# Removed AUTH_TOKEN constant and direct HEADERS creation

# Removed setup_logging, load_thresholds, main, spark interactions, dbutils 