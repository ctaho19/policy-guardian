import requests
import json
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import time
from IPython.display import display, HTML
import sys
from pyspark.sql import (
    SparkSession,
)  # Configure logging for both file and console output in Databricks environment.


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(console_handler)

    return logging.getLogger(__name__)


logger = setup_logging()  # Connecting to snowflake
username = dbutils.secrets.get(scope="uno201", key="eid")  # NOTE: Using Chris' scope
password = dbutils.secrets.get(
    scope="uno201", key="password"
)  # username = dbutils.secrets.get(scope="sdd802_scope", key="eid") #NOTE: Using Aditya's scope
# password = dbutils.secrets.get(scope="sdd802_scope", key="password")# Define Snowflake connection options
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = dict(
    sfUrl="prod.us-east-1.capitalone.snowflakecomputing.com",
    sfUser=username,
    sfPassword=password,
    sfDatabase="SB",
    sfSchema="USER_UNO201",
    sfWarehouse="CYBR_Q_DI_WH",
)  # API Endpoints and Auth
AUTH_TOKEN = ""
BASE_URL = (
    "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling"
)
SUMMARY_URL = f"{BASE_URL}/summary-view"
CONFIG_URL = f"{BASE_URL}/search-resource-configurations"

HEADERS = {
    "Accept": "application/json;v=1.0",
    "Authorization": f"Bearer {AUTH_TOKEN}",
    "Content-Type": "application/json",
}


def get_summary_count(
    payload: Dict, timeout: int = 30, max_retries: int = 2
) -> Optional[int]:
    """Fetch the total count of resources from the summary-view API.

    Args:
        payload: API request payload containing search parameters
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts

    Returns:
        Total resource count or None if request fails
    """


logger.info(f"Calling Summary View API with payload: {json.dumps(payload, indent=2)}")

for retry in range(max_retries + 1):
    try:
        response = requests.post(
            SUMMARY_URL,
            headers=HEADERS,
            json=payload,  # Use json= for consistency
            verify=False,
            timeout=timeout,
        )

        if response.status_code == 200:
            data = response.json()
            level1_list = data.get("level1List", [])
            if not level1_list:
                logger.warning("Empty level1List in response")
                return 0

            count = level1_list[0].get("level1ResourceCount", 0)
            logger.info(f"Summary count: {count}")
            return count

        elif response.status_code == 429:
            wait_time = min(2**retry, 30)
            logger.warning(
                f"Rate limited (429). Waiting {wait_time}s before retry {retry+1}/{max_retries}."
            )
            time.sleep(wait_time)
            if retry == max_retries:
                logger.error("Max retries reached for rate limiting.")
                return None
        else:
            logger.error(
                f"Summary API failed: {response.status_code} - {response.text}"
            )
            if retry < max_retries:
                wait_time = min(2**retry, 15)
                logger.info(
                    f"Retrying in {wait_time}s... (Attempt {retry+1}/{max_retries})"
                )
                time.sleep(wait_time)
            else:
                return None

    except requests.exceptions.Timeout:
        logger.warning(f"Request timeout after {timeout}s")
        if retry < max_retries:
            wait_time = min(2**retry, 15)
            logger.info(
                f"Retrying in {wait_time}s... (Attempt {retry+1}/{max_retries})"
            )
            time.sleep(wait_time)
        else:
            logger.error("Max retries reached after timeouts.")
            return None

    except Exception as e:
        logger.error(f"Exception in Summary API: {str(e)}")
        if retry < max_retries:
            wait_time = min(2**retry, 15)
            logger.info(
                f"Retrying in {wait_time}s... (Attempt {retry+1}/{max_retries})"
            )
            time.sleep(wait_time)
        else:
            return None

    return None


def fetch_all_resources(
    payload: Dict,
    limit: Optional[int] = None,
    validate_only: bool = False,
    timeout: int = 60,
    max_retries: int = 3,
) -> Tuple[List[Dict], int]:
    """Fetch all cloud resources using pagination with nextRecordKey as query param.

    Args:
        payload: API request payload with search parameters
        limit: Maximum number of resources to fetch (optional)
        validate_only: If True, fetch only for validation purposes
        timeout: Request timeout in seconds
        max_retries: Maximum number of retry attempts

    Returns:
        Tuple of (list of resources, total count)
    """


all_resources = []
total_count = 0
next_record_key = ""
page_count = 0
start_time = datetime.now()

# Base payload (no limit or nextRecordKey here)
fetch_payload = {
    "searchParameters": payload.get("searchParameters", [{}]),
    "responseFields": [
        "accountName",
        "accountResourceId",
        "amazonResourceName",
        "asvName",
        "awsAccountId",
        "awsRegion",
        "businessApplicationName",
        "environment",
        "resourceCreationTimestamp",
        "resourceId",
        "resourceType",
        "configurationList",
        "configuration.metadataOptions.httpTokens",
    ],
}

display_log(
    f"Fetching resources with initial payload: {json.dumps(fetch_payload, indent=2)}"
)

while True:
    # Fresh params for each request
    params = {"limit": min(limit, 10000) if limit else 10000}
    if next_record_key:
        params["nextRecordKey"] = next_record_key

    for retry in range(max_retries + 1):
        try:
            logger.info(
                f"Requesting page {page_count + 1} with nextRecordKey: '{next_record_key or 'None'}'"
                + (f" (retry {retry})" if retry > 0 else "")
            )
            full_url = (
                requests.Request(
                    "POST",
                    CONFIG_URL,
                    headers=HEADERS,
                    json=fetch_payload,
                    params=params,
                )
                .prepare()
                .url
            )
            logger.debug(f"Full URL: {full_url}")

            response = requests.post(
                CONFIG_URL,
                headers=HEADERS,
                json=fetch_payload,
                params=params,  # Send limit and nextRecordKey as query params
                verify=False,
                timeout=timeout,
            )

            if response.status_code == 200:
                data = response.json()
                resources = data.get("resourceConfigurations", [])
                new_next_record_key = data.get("nextRecordKey", "")

                if page_count == 0 and resources:
                    logger.debug(
                        f"First page sample resource: {json.dumps(resources[0], indent=2)}"
                    )

                all_resources.extend(resources)
                total_count += len(resources)

                logger.info(
                    f"Page {page_count + 1}: Fetched {len(resources)} resources, total: {total_count}, "
                    f"nextRecordKey: '{new_next_record_key or 'None'}'"
                )

                next_record_key = new_next_record_key
                page_count += 1
                break  # Exit retry loop on success

            elif response.status_code == 502:
                logger.error(f"Bad Gateway (502) error. Response: {response.text}")
                if retry == max_retries:
                    raise Exception(
                        f"Failed after {max_retries} retries due to Bad Gateway"
                    )
                wait_time = min(2**retry, 30)
                logger.warning(f"Waiting {wait_time}s before retry...")
                time.sleep(wait_time)

            elif response.status_code != 200:
                logger.error(f"API error: {response.status_code} - {response.text}")
                if retry == max_retries:
                    raise Exception(f"Failed after {max_retries} retries")
                time.sleep(min(2**retry, 30))

        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            if retry == max_retries:
                raise Exception(f"Failed after {max_retries} retries")
            time.sleep(min(2**retry, 30))

    if validate_only or not next_record_key or (limit and total_count >= limit):
        break

total_time = (datetime.now() - start_time).total_seconds()
logger.info(
    f"Fetch complete: {total_count} resources in {page_count} pages, {total_time:.1f} seconds"
)
return all_resources, total_count


def filter_tier1_resources(
    resources: List[Dict], config_key: str, fields: List[str]
) -> Tuple[int, pd.DataFrame]:
    """Filter resources based on Tier 1 criteria (non-empty configuration value)."""
    matching_count = 0
    non_matching_resources = []
    for resource in resources:
        config_list = resource.get("configurationList", [])
        imds_config = next(
            (
                c
                for c in config_list
                if c["configurationName"] == f"configuration.{config_key}"
            ),
            None,
        )
        config_value = imds_config.get("configurationValue") if imds_config else None

    if config_value and config_value.strip():
        matching_count += 1
    else:
        filtered_resource = {field: resource.get(field, "N/A") for field in fields}
        filtered_resource["configuration.origin"] = (
            config_value if config_value else "N/A"
        )
        non_matching_resources.append(filtered_resource)


logger.info(f"Tier 1: {matching_count} EC2 instances with non-empty {config_key}")
return matching_count, (
    pd.DataFrame(non_matching_resources)
    if non_matching_resources
    else pd.DataFrame(columns=fields)
)


def filter_tier2_resources(
    resources: List[Dict], config_key: str, config_value: str, fields: List[str]
) -> Tuple[int, pd.DataFrame]:
    """Filter resources based on Tier 2 criteria (specific configuration value).
    Only processes resources that passed Tier 1 criteria (have non-empty configuration value).
    """
    matching_count = 0
    non_matching_resources = []
    for resource in resources:
        config_list = resource.get("configurationList", [])
        origin_config = next(
            (
                c
                for c in config_list
                if c["configurationName"] == f"configuration.{config_key}"
            ),
            None,
        )
        config_value_actual = (
            origin_config.get("configurationValue") if origin_config else None
        )

    # Only process resources that pass Tier 1 criteria (have non-empty configuration value)
    if config_value_actual and config_value_actual.strip():
        if config_value_actual == config_value:
            matching_count += 1
        else:
            filtered_resource = {field: resource.get(field, "N/A") for field in fields}
            filtered_resource["configuration.metadataOptions.httpTokens"] = (
                config_value_actual
            )
            non_matching_resources.append(filtered_resource)


logger.info(
    f"Tier 2: {matching_count} EC2 Instances with {config_key} = {config_value}"
)
return matching_count, (
    pd.DataFrame(non_matching_resources)
    if non_matching_resources
    else pd.DataFrame(columns=fields)
)


def load_thresholds(spark: SparkSession) -> pd.DataFrame:
    """Load compliance thresholds from Snowflake Table."""
    query = "SELECT MONITORING_METRIC_ID, ALERT_THRESHOLD, WARNING_THRESHOLD FROM CYBR_DB_COLLAB.LAB_ESRA_TCRD.CYBER_CONTROLS_MONITORING_THRESHOLD WHERE MONITORING_METRIC_ID IN ('MNTR-1077125-T1', 'MNTR-1077125-T2')"
    logger.info("Loading Tier 1 and Tier 2 thresholds from Snowflake")
    thresholds_df = (
        spark.read.format(SNOWFLAKE_SOURCE_NAME)
        .options(**sfOptions)
        .option("query", query)
        .load()
    )
    threshold_data = thresholds_df.collect()
    logger.info(
        f"Threshold data collected: {threshold_data}, type: {type(threshold_data)}"
    )


if threshold_data:
    data = [
        {
            "MONITORING_METRIC_ID": row["MONITORING_METRIC_ID"],
            "ALERT_THRESHOLD": row["ALERT_THRESHOLD"],
            "WARNING_THRESHOLD": row["WARNING_THRESHOLD"],
        }
        for row in threshold_data
    ]
    return pd.DataFrame(data)
else:
    logger.warning("No threshold data found; using fallback values")
    return pd.DataFrame(
        [
            {
                "MONITORING_METRIC_ID": "MNTR-1077125-T1",
                "ALERT_THRESHOLD": 100,
                "WARNING_THRESHOLD": "NULL",
            },
            {
                "MONITORING_METRIC_ID": "MNTR-1077125-T2",
                "ALERT_THRESHOLD": 99,
                "WARNING_THRESHOLD": "NULL",
            },
        ]
    )


def get_compliance_status(
    metric: float, alert_threshold: float, warning_threshold: Optional[float] = None
) -> str:
    """Determine compliance status based on metric and thresholds.

    Args:
        metric: The compliance metric as a decimal (0-1)
        alert_threshold: The alert threshold as a whole number percentage (0-100)
        warning_threshold: Optional warning threshold as a whole number percentage (0-100)
    """


metric_percentage = metric * 100

if metric_percentage >= alert_threshold:
    return "GREEN"
elif warning_threshold is not None and metric_percentage >= warning_threshold:
    return "YELLOW"
else:
    return "RED"


def main():
    # Connecting to snowflake
    username = dbutils.secrets.get(
        scope="uno201", key="eid"
    )  # NOTE: Using Chris' scope
    password = dbutils.secrets.get(scope="uno201", key="password")

    # username = dbutils.secrets.get(scope="sdd802_scope", key="eid") #NOTE: Using Aditya's scope
    # password = dbutils.secrets.get(scope="sdd802_scope", key="password")

    # Define Snowflake connection options
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    sfOptions = dict(
        sfUrl="prod.us-east-1.capitalone.snowflakecomputing.com",
        sfUser=username,
        sfPassword=password,
        sfDatabase="SB",
        sfSchema="USER_UNO201",
        sfWarehouse="CYBR_Q_DI_WH",
    )

    # Initialize Spark session for Databricks
    spark = SparkSession.builder.appName("MNTR-1077231").getOrCreate()
    logger.info("Spark session initialized")

    # Configuration for this specific metric - can be easily modified for other resource types
    resource_config = {
        "resource_type": "AWS::EC2::Instance",
        "config_key": "metadataOptions.httpTokens",
        "config_value": "required",
        "desired_fields": [
            "resourceId",
            "amazonResourceName",
            "resourceType",
            "awsRegion",
            "configuration.metadataOptions.httpTokens",
        ],
        "tier1_metric_id": "MNTR-1077231-T1",
        "tier2_metric_id": "MNTR-1077231-T2",
        "validation_limit": 1000,  # Number of resources to fetch for validation
        "full_limit": None,  # None means fetch all, or set a number for testing
        "timeout": 60,  # API request timeout in seconds
        "max_retries": 3,  # Maximum retries for failed API requests
    }

    # Define payloads based on configuration
    summary_payload_all = {
        "searchParameters": {
            "resourceType": resource_config["resource_type"],
            "aggregations": ["resourceType"],
        }
    }

    summary_payload_filtered = {
        "searchParameters": {
            "resourceType": resource_config["resource_type"],
            "configurationItems": [
                {
                    "configurationName": resource_config["config_key"],
                    "configurationValue": resource_config["config_value"],
                }
            ],
            "aggregations": ["resourceType"],
        }
    }

    # In main(), update the config_payload structure:
    config_payload = {
        "searchParameters": {"resourceType": resource_config["resource_type"]}
    }

    # Update the summary_payload_all and summary_payload_filtered to match the new structure:
    summary_payload_all = {
        "searchParameters": [
            {
                "resourceType": resource_config["resource_type"],
                "aggregations": ["resourceType"],
            }
        ]
    }

    summary_payload_filtered = {
        "searchParameters": [
            {
                "resourceType": resource_config["resource_type"],
                "configurationItems": [
                    {
                        "configurationName": resource_config["config_key"],
                        "configurationValue": resource_config["config_value"],
                    }
                ],
                "aggregations": ["resourceType"],
            }
        ]
    }

    config_payload = {
        "searchParameters": [{"resourceType": resource_config["resource_type"]}]
    }

    # Step 1: Get summary counts
    logger.info("Step 1: Getting summary counts...")
    total_summary_count = get_summary_count(summary_payload_all)
    filtered_summary_count = get_summary_count(summary_payload_filtered)

    if total_summary_count is None or filtered_summary_count is None:
        logger.error("Failed to get summary counts. Exiting.")
        return

    logger.info(
        f"Total {resource_config['resource_type']} resources: {total_summary_count}, "
        f"Filtered by {resource_config['config_key']}={resource_config['config_value']}: {filtered_summary_count}"
    )

    # Step 2: Early validation with limited resources
    logger.info(
        f"Step 2: Validating with first {resource_config['validation_limit']} resources..."
    )
    validation_resources, validation_count = fetch_all_resources(
        config_payload,
        limit=resource_config["validation_limit"],
        validate_only=True,
        timeout=resource_config["timeout"],
        max_retries=resource_config["max_retries"],
    )

    if validation_count == 0:
        logger.error("No resources fetched in validation. Check API or payload.")
        return

    # Sample and validate configuration extraction
    config_values = []
    for i, resource in enumerate(validation_resources[:5]):
        config_list = resource.get("configurationList", [])
        config_item = next(
            (
                c
                for c in config_list
                if c["configurationName"]
                == f"configuration.{resource_config['config_key']}"
            ),
            None,
        )
        config_value = config_item.get("configurationValue") if config_item else "N/A"
        config_values.append(config_value)
        logger.debug(
            f"Sample resource {i}: {resource_config['config_key']} = {config_value}"
        )

    # Perform validation filtering
    tier1_count, tier1_non_compliant_df = filter_tier1_resources(
        validation_resources,
        resource_config["config_key"],
        resource_config["desired_fields"],
    )

    tier2_count, tier2_non_compliant_df = filter_tier2_resources(
        validation_resources,
        resource_config["config_key"],
        resource_config["config_value"],
        resource_config["desired_fields"],
    )

    logger.info(
        f"Validation: Total={validation_count}, Tier 1={tier1_count}, Tier 2={tier2_count}"
    )

    # Validation checks
    configs_found = sum(1 for v in config_values if v != "N/A")
    if configs_found == 0:
        logger.error(
            f"No {resource_config['config_key']} fields extracted from sample. Check configurationList structure or key name."
        )
        print(
            f"Validation Error: No configuration.{resource_config['config_key']} found in first {resource_config['validation_limit']} resources."
        )
        print(f"Sample values: {config_values}")
        return

    if tier1_count == 0:
        logger.error(
            f"No resources have a non-empty {resource_config['config_key']}. Filtering logic or data issue."
        )
        print(
            f"Validation Error: Tier 1 count is 0. Expected most resources to have a {resource_config['config_key']}."
        )
        print(f"Sample values: {config_values}")
        return

    if tier1_count == validation_count and tier2_count == validation_count:
        logger.warning(
            "All resources match both tiers. Filtering may not be distinguishing values."
        )
        print("Validation Warning: No variation in configuration values detected.")
        print(f"Sample values: {config_values}")

    # Step 3: Fetch all resources (or limited set for testing)
    logger.info("Step 3: Validation passed or overridden. Fetching all resources...")
    all_resources, config_total_count = fetch_all_resources(
        config_payload,
        limit=resource_config["full_limit"],
        timeout=resource_config["timeout"],
        max_retries=resource_config["max_retries"],
    )

    if (
        total_summary_count != config_total_count
        and resource_config["full_limit"] is None
    ):
        logger.warning(
            f"Mismatch: Summary count ({total_summary_count}) != Config count ({config_total_count})"
        )

    # Step 4: Full filtering
    logger.info("Step 4: Filtering all resources...")
    tier1_numerator, tier1_non_compliant_df = filter_tier1_resources(
        all_resources, resource_config["config_key"], resource_config["desired_fields"]
    )

    tier2_numerator, tier2_non_compliant_df = filter_tier2_resources(
        all_resources,
        resource_config["config_key"],
        resource_config["config_value"],
        resource_config["desired_fields"],
    )

    # Step 5: Load thresholds and calculate compliance
    logger.info("Step 5: Loading thresholds and calculating compliance...")
    thresholds_df = load_thresholds(spark)

    # Default thresholds
    default_thresholds = {
        "MNTR-1077231-T1": {"ALERT_THRESHOLD": 100, "WARNING_THRESHOLD": None},
        "MNTR-1077231-T2": {"ALERT_THRESHOLD": 100, "WARNING_THRESHOLD": None},
    }

    if thresholds_df.empty:
        logger.warning("No thresholds loaded. Using defaults.")
        tier1_threshold = default_thresholds[resource_config["tier1_metric_id"]]
        tier2_threshold = default_thresholds[resource_config["tier2_metric_id"]]
    else:
        # Safely get thresholds with fallback to defaults
        tier1_df = thresholds_df[
            thresholds_df["MONITORING_METRIC_ID"] == resource_config["tier1_metric_id"]
        ]
        tier2_df = thresholds_df[
            thresholds_df["MONITORING_METRIC_ID"] == resource_config["tier2_metric_id"]
        ]

        tier1_threshold = (
            tier1_df.iloc[0].to_dict()
            if not tier1_df.empty
            else default_thresholds[resource_config["tier1_metric_id"]]
        )
        tier2_threshold = (
            tier2_df.iloc[0].to_dict()
            if not tier2_df.empty
            else default_thresholds[resource_config["tier2_metric_id"]]
        )

        logger.info(
            f"Loaded thresholds - Tier1: {tier1_threshold}, Tier2: {tier2_threshold}"
        )

    # Calculate metrics and compliance status
    tier1_metric = tier1_numerator / config_total_count if config_total_count > 0 else 0
    tier1_status = get_compliance_status(
        tier1_metric,
        float(tier1_threshold["ALERT_THRESHOLD"]),
        (
            float(tier1_threshold["WARNING_THRESHOLD"])
            if tier1_threshold["WARNING_THRESHOLD"] not in ["NULL", None, "None"]
            else None
        ),
    )

    tier2_denominator = tier1_numerator
    tier2_metric = tier2_numerator / tier2_denominator if tier2_denominator > 0 else 0
    tier2_status = get_compliance_status(
        tier2_metric,
        float(tier2_threshold["ALERT_THRESHOLD"]),
        (
            float(tier2_threshold["WARNING_THRESHOLD"])
            if tier2_threshold["WARNING_THRESHOLD"] not in ["NULL", None, "None"]
            else None
        ),
    )

    # Step 6: Create monitoring DataFrame
    logger.info("Step 6: Creating monitoring DataFrame...")
    monitoring_df = pd.DataFrame(
        [
            {
                "DATE": datetime.now().strftime("%Y-%m-%d"),
                "CTRL_ID": "CTRL-1077231",
                "MONITORING_METRIC_NUMBER": resource_config["tier1_metric_id"],
                "MONITORING_METRIC": round(tier1_metric * 100, 4),
                "COMPLIANCE_STATUS": tier1_status,
                "NUMERATOR": tier1_numerator,
                "DENOMINATOR": config_total_count,
            },
            {
                "DATE": datetime.now().strftime("%Y-%m-%d"),
                "CTRL_ID": "CTRL-1077231",
                "MONITORING_METRIC_NUMBER": resource_config["tier2_metric_id"],
                "MONITORING_METRIC": round(tier2_metric * 100, 4),
                "COMPLIANCE_STATUS": tier2_status,
                "NUMERATOR": tier2_numerator,
                "DENOMINATOR": tier2_denominator,
            },
        ]
    )

    # Step 7: Report results
    logger.info("Step 7: Reporting results...")
    print(
        f"\nTier 1 Non-compliant Resources ({resource_config['config_key']} missing or empty):"
    )
    display(
        tier1_non_compliant_df if not tier1_non_compliant_df.empty else "None found"
    )
    print(
        f"\nTier 2 Non-compliant Resources ({resource_config['config_key']} != {resource_config['config_value']}):"
    )
    display(
        tier2_non_compliant_df if not tier2_non_compliant_df.empty else "None found"
    )
    print("\nMonitoring Metrics:")
    monitoring_spark_df = spark.createDataFrame(monitoring_df)
    display(monitoring_spark_df)

    # Explicitly set the schema before writing to Snowflake

    #  spark.sql(f"USE SCHEMA {sfOptions['sfSchema']}")
    monitoring_spark_df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option(
        "dbtable", "CYBR_DB_COLLAB.LAB_ESRA_TCRD.CYBER_CONTROLS_MONITORING"
    ).mode("APPEND").save()


if __name__ == "__main__":
    main()
