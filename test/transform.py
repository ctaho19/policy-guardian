import requests
import json
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timezone
import time
import sys

# Assuming logger is configured by the ETIP framework or pipeline.py
logger = logging.getLogger(__name__)

# --- Constants (moved from original script, can be overridden by config/context) ---
DEFAULT_RESOURCE_TYPE = "AWS::EC2::Instance"
DEFAULT_CONFIG_KEY = "metadataOptions.httpTokens"
DEFAULT_CONFIG_VALUE = "required"
DEFAULT_TIER1_METRIC_ID = "MNTR-1077231-T1"
DEFAULT_TIER2_METRIC_ID = "MNTR-1077231-T2"
DEFAULT_CTRL_ID = "CTRL-1077231"
DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 3

# Fields needed for non-compliant reporting (subset of fetch_all_resources responseFields)
# Ensure these are included in the fetch_all_resources responseFields list
NON_COMPLIANT_FIELDS = [
    "resourceId",
    "amazonResourceName",
    "resourceType",
    "awsRegion",
    "accountName",
    "awsAccountId",
    f"configuration.{DEFAULT_CONFIG_KEY}", # Include the specific config being checked
]

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

    # Define standard fields needed for processing and non-compliant reporting
    response_fields = [
        "accountName", "accountResourceId", "amazonResourceName", "asvName",
        "awsAccountId", "awsRegion", "businessApplicationName", "environment",
        "resourceCreationTimestamp", "resourceId", "resourceType",
        "configurationList", f"configuration.{DEFAULT_CONFIG_KEY}", # Ensure specific key is requested
    ]

    fetch_payload = {
        "searchParameters": search_payload.get("searchParameters", [{}]),
        "responseFields": response_fields,
    }

    logger.info(f"Fetching resources from {api_url} with initial payload: {json.dumps(fetch_payload, indent=2)}")

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
            # Depending on requirements, either raise error or return partial results
            raise # Stop processing if API fetch fails critically

        if not next_record_key or (limit and total_fetched >= limit):
            break

    total_time = (datetime.now() - start_time).total_seconds()
    logger.info(
        f"Fetch complete: {total_fetched} resources in {page_count} pages, {total_time:.1f} seconds"
    )
    # The original script returned total_count, but it seems we only need the list
    return all_resources


# --- Compliance Calculation Function ---

def get_compliance_status(
    metric: float, alert_threshold: float, warning_threshold: Optional[float] = None
) -> str:
    """Determine compliance status based on metric and thresholds."""
    metric_percentage = metric * 100

    # Thresholds from the standard table are usually 0-100
    if metric_percentage >= alert_threshold:
        return "Green" # Match status values from reference pipeline
    elif warning_threshold is not None and metric_percentage >= warning_threshold:
        return "Yellow"
    else:
        return "Red"

# --- Main Transformation Function (called by config.yml) ---

@transform_function("custom/calculate_ctrl1077231_metrics")
def calculate_ctrl1077231_metrics(
    threshold_df: pd.DataFrame,
    context: Dict[str, Any], # Receive context from pipeline.py
    # Add other specific parameters if needed, passed via config.yml options
) -> pd.DataFrame:
    """
    Main transformation logic:
    1. Fetches EC2 instance data from Cloud Tooling API.
    2. Filters resources based on metadataOptions.httpTokens config.
    3. Calculates Tier 1 and Tier 2 compliance metrics.
    4. Formats the output according to the standard Avro schema.
    """
    logger.info("Starting CTRL-1077231 metric calculation...")

    # --- Get API Credentials and Config from Context ---
    try:
        api_auth_token = context["api_auth_token"]
        api_url = context["cloud_tooling_api_url"]
        api_verify_ssl = context.get("api_verify_ssl", True) # Default to True if not set
        # Get resource-specific config (could be passed via context or use defaults)
        resource_type = context.get("resource_type", DEFAULT_RESOURCE_TYPE)
        config_key = context.get("config_key", DEFAULT_CONFIG_KEY)
        config_value = context.get("config_value", DEFAULT_CONFIG_VALUE)
        ctrl_id = context.get("ctrl_id", DEFAULT_CTRL_ID)
        tier1_metric_id_str = context.get("tier1_metric_id", DEFAULT_TIER1_METRIC_ID)
        tier2_metric_id_str = context.get("tier2_metric_id", DEFAULT_TIER2_METRIC_ID)

    except KeyError as e:
        logger.error(f"Missing required context key: {e}")
        raise

    # --- Fetch Resources --- #
    api_payload = {
        "searchParameters": [{
            "resourceType": resource_type,
            # Add other filters if necessary
        }]
    }
    logger.info(f"Fetching resources of type: {resource_type}")
    all_resources = fetch_all_resources(
        api_url=api_url,
        search_payload=api_payload,
        auth_token=api_auth_token,
        verify_ssl=api_verify_ssl,
        timeout=DEFAULT_TIMEOUT, # Or get from context/config
        max_retries=DEFAULT_MAX_RETRIES # Or get from context/config
    )

    # --- Process Thresholds --- #
    logger.info("Processing thresholds...")
    if threshold_df.empty:
        logger.error("Thresholds DataFrame is empty. Cannot calculate compliance.")
        raise ValueError("Thresholds DataFrame is empty.")

    try:
        # Extract T1 and T2 thresholds using standard column names
        tier1_threshold_row = threshold_df[threshold_df["monitoring_metric_id"] == tier1_metric_id_str].iloc[0]
        tier2_threshold_row = threshold_df[threshold_df["monitoring_metric_id"] == tier2_metric_id_str].iloc[0]

        # Use 'alerting_threshold' and 'warning_threshold' as per standard table
        tier1_alert_th = float(tier1_threshold_row["alerting_threshold"])
        tier1_warn_th = float(wt) if (wt := tier1_threshold_row.get("warning_threshold")) is not None and pd.notna(wt) else None

        tier2_alert_th = float(tier2_threshold_row["alerting_threshold"])
        tier2_warn_th = float(wt) if (wt := tier2_threshold_row.get("warning_threshold")) is not None and pd.notna(wt) else None

        # Get the integer monitoring_metric_id values for the output schema
        tier1_metric_id_int = int(tier1_threshold_row["monitoring_metric_id"])
        tier2_metric_id_int = int(tier2_threshold_row["monitoring_metric_id"])

        logger.info(f"Thresholds processed: T1 Alert={tier1_alert_th}, T1 Warn={tier1_warn_th}, T2 Alert={tier2_alert_th}, T2 Warn={tier2_warn_th}")

    except (IndexError, KeyError, ValueError) as e:
        logger.error(f"Error processing thresholds for {tier1_metric_id_str}/{tier2_metric_id_str}: {e}")
        raise

    # --- Filter Resources and Calculate Numerators --- #
    logger.info("Filtering resources and calculating numerators...")
    tier1_numerator = 0
    tier2_numerator = 0
    non_compliant_resources_t1 = [] # Resources failing T1
    non_compliant_resources_t2 = [] # Resources passing T1 but failing T2
    config_total_count = len(all_resources)

    for resource in all_resources:
        config_list = resource.get("configurationList", [])
        target_config = next(
            (c for c in config_list if c["configurationName"] == f"configuration.{config_key}"),
            None,
        )
        config_value_actual = target_config.get("configurationValue") if target_config else None

        # Tier 1 Check: Non-empty configuration value
        tier1_pass = config_value_actual and str(config_value_actual).strip()

        resource_details_for_reporting = {field: resource.get(field) for field in NON_COMPLIANT_FIELDS}
        # Ensure the actual config value is captured correctly
        resource_details_for_reporting[f"configuration.{config_key}"] = config_value_actual

        if tier1_pass:
            tier1_numerator += 1
            # Tier 2 Check: Specific configuration value
            if str(config_value_actual) == config_value:
                tier2_numerator += 1
            else:
                # Failed Tier 2 (but passed Tier 1)
                non_compliant_resources_t2.append(resource_details_for_reporting)
        else:
            # Failed Tier 1
            non_compliant_resources_t1.append(resource_details_for_reporting)

    logger.info(f"Filtering Complete: Total={config_total_count}, Tier1 Pass={tier1_numerator}, Tier2 Pass={tier2_numerator}")

    # --- Calculate Metrics and Compliance Status --- #
    logger.info("Calculating metrics and status...")
    tier1_metric_value = (tier1_numerator / config_total_count * 100) if config_total_count > 0 else 0.0
    tier1_status = get_compliance_status(tier1_metric_value / 100, tier1_alert_th, tier1_warn_th)

    tier2_denominator = tier1_numerator # Denominator for T2 is count passing T1
    tier2_metric_value = (tier2_numerator / tier2_denominator * 100) if tier2_denominator > 0 else 0.0
    tier2_status = get_compliance_status(tier2_metric_value / 100, tier2_alert_th, tier2_warn_th)

    # --- Format Non-Compliant Resources for Output --- #
    # Combine T1 and T2 non-compliant lists for the single 'resources_info' field
    all_non_compliant_resources = non_compliant_resources_t1 + non_compliant_resources_t2
    # Convert each resource dict to a JSON string as expected by the schema
    resources_info_list = [json.dumps(res, default=str) for res in all_non_compliant_resources] if all_non_compliant_resources else None

    # --- Create Final DataFrame --- #
    logger.info("Creating final DataFrame...")
    current_timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    output_data = [
        {
            # Match Avro schema field names exactly
            "monitoring_metric_id": tier1_metric_id_int,
            "control_id": ctrl_id,
            "monitoring_metric_value": round(tier1_metric_value, 4),
            "monitoring_metric_status": tier1_status,
            "metric_value_numerator": tier1_numerator,
            "metric_value_denominator": config_total_count,
            "resources_info": resources_info_list, # Include combined list for T1 metric row
            "control_monitoring_utc_timestamp": current_timestamp_ms,
        },
        {
            "monitoring_metric_id": tier2_metric_id_int,
            "control_id": ctrl_id,
            "monitoring_metric_value": round(tier2_metric_value, 4),
            "monitoring_metric_status": tier2_status,
            "metric_value_numerator": tier2_numerator,
            "metric_value_denominator": tier2_denominator,
            "resources_info": resources_info_list, # Include combined list for T2 metric row
            "control_monitoring_utc_timestamp": current_timestamp_ms,
        },
    ]

    monitoring_results_df = pd.DataFrame(output_data)

    # --- Final Type Casting (Ensure types match Avro schema) --- #
    try:
        monitoring_results_df = monitoring_results_df.astype({
            "monitoring_metric_id": "int64", # Avro 'int' maps to int64 usually
            "control_id": "string",
            "monitoring_metric_value": "float64",
            "monitoring_metric_status": "string",
            "metric_value_numerator": "int64",
            "metric_value_denominator": "int64",
            # "resources_info" is handled by object type for list/None
            "control_monitoring_utc_timestamp": "int64",
        })
        # Handle nullable string for status
        monitoring_results_df['monitoring_metric_status'] = monitoring_results_df['monitoring_metric_status'].fillna(pd.NA).astype("string")
        # Handle nullable array for resources
        # Pandas doesn't have a direct nullable list type easily, object is usually fine here
        # Ensure None is used where appropriate
        monitoring_results_df['resources_info'] = monitoring_results_df['resources_info'].apply(lambda x: x if x is not None else None)

    except Exception as e:
        logger.error(f"Error during final type casting: {e}")
        raise

    logger.info(f"Successfully created monitoring DataFrame with {len(monitoring_results_df)} rows.")
    return monitoring_results_df

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