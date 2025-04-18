import logging
import os
from pathlib import Path
from typing import Dict, Any

# Core ETIP/AxDP imports (adjust based on actual framework)
from config_pipeline import ConfigPipeline # Assuming this base class exists
from etip_env import Env # Assuming Env class provides config/secrets

# Imports for API interaction (similar to reference)
from connectors.exchange.oauth_token import refresh as refresh_oauth_token
from connectors.ca_certs import C1_CERT_FILE # Example cert file path

# Import transforms to register custom functions
import pipelines.pl_automated_monitoring_CTRL_1077231.transform # noqa: F401

logger = logging.getLogger(__name__)

# --- Constants/Configuration (should match transform.py or be passed) ---
# These might be better defined centrally or passed via context/config
# API_BASE_URL = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling"
# CONFIG_URL = f"{API_BASE_URL}/search-resource-configurations"
# Note: The specific API endpoint URL might be passed directly to the transform function

def run(
    env: Env, # Pass environment context containing secrets/config
    is_export_test_data: bool = False,
    is_load: bool = True,
    dq_actions: bool = True,
):
    """Main entry point called by the ETIP framework."""
    pipeline = PLAutomatedMonitoringCtrl1077231(env) # Instantiate the specific pipeline class
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    logger.info(f"Running pipeline: {pipeline.pipeline_name}")
    return (
        pipeline.run_test_data_export(dq_actions=dq_actions)
        if is_export_test_data
        else pipeline.run(load=is_load, dq_actions=dq_actions)
    )

# Define a pipeline class inheriting from the framework's base class
class PLAutomatedMonitoringCtrl1077231(ConfigPipeline):
    def __init__(self,
        env: Env, # Receive environment context
    ) -> None:
        super().__init__(env)
        # Store env for later use (e.g., fetching secrets)
        self.env = env

        # API Configuration (URL could also be in env/config)
        self.cloud_tooling_api_url = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
        # Fetch OAuth credentials securely from the environment
        # These must be configured for the pipeline's service account
        try:
            self.client_id = self.env.exchange.client_id
            self.client_secret = self.env.exchange.client_secret
            self.exchange_url = self.env.exchange.exchange_url
        except AttributeError as e:
            logger.error(f"Missing OAuth configuration in environment: {e}")
            raise ValueError(f"Environment object missing expected OAuth attributes: {e}") from e

    def _get_api_token(self) -> str:
        """Refreshes and returns the API OAuth token string (including 'Bearer')."""
        logger.info(f"Refreshing API token from {self.exchange_url}...")
        try:
            # Call the token refresh function from the framework's library
            token = refresh_oauth_token(
                client_id=self.client_id,
                client_secret=self.client_secret,
                exchange_url=self.exchange_url,
            )
            logger.info("API token refreshed successfully.")
            return f"Bearer {token}"
        except Exception as e:
            logger.error(f"Failed to refresh API token: {e}")
            # Propagate the error to halt the pipeline if auth fails
            raise RuntimeError("API token refresh failed") from e

    # Override the transform method to inject API context
    def transform(self) -> None:
        """Injects API authentication token and config into context before running transforms."""
        logger.info("Preparing transform stage: Fetching API token...")
        api_auth_token = self._get_api_token()

        # Add necessary context for the transform function(s)
        # The transform function expects these keys in its 'context' argument
        self.context["api_auth_token"] = api_auth_token
        self.context["cloud_tooling_api_url"] = self.cloud_tooling_api_url
        self.context["api_verify_ssl"] = C1_CERT_FILE # Pass CA cert bundle path

        logger.info("API context injected. Proceeding with config-defined transforms.")
        # Call the parent class's transform method to execute steps from config.yml
        super().transform()


# --- Main Execution Block (for local testing) ---
if __name__ == "__main__":
    # Setup basic logging for local run
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    logger = logging.getLogger(__name__)

    logger.warning("--- Running Pipeline Locally --- ")
    logger.warning("Ensure MOCK environment variables for OAuth are set correctly!")

    # Mock the Env object needed by the pipeline
    # Fetch required secrets from environment variables for local run
    class MockExchangeConfig:
        client_id = os.environ.get("MOCK_CLIENT_ID")
        client_secret = os.environ.get("MOCK_CLIENT_SECRET")
        exchange_url = os.environ.get("MOCK_EXCHANGE_URL")

    class MockSnowflakeConfig:
        # Add required Snowflake mock attributes if base class needs them
        account = os.environ.get("MOCK_SNOWFLAKE_ACCOUNT")
        user = os.environ.get("MOCK_SNOWFLAKE_USER")
        password = os.environ.get("MOCK_SNOWFLAKE_PASSWORD")
        role = os.environ.get("MOCK_SNOWFLAKE_ROLE")
        warehouse = os.environ.get("MOCK_SNOWFLAKE_WAREHOUSE")
        database = os.environ.get("MOCK_SNOWFLAKE_DATABASE")
        schema = os.environ.get("MOCK_SNOWFLAKE_SCHEMA")

    class MockEnv:
        exchange = MockExchangeConfig()
        snowflake = MockSnowflakeConfig()
        # Add any other attributes ConfigPipeline might expect from Env

    mock_env = MockEnv()

    # Basic validation for mock secrets
    missing_secrets = False
    if not mock_env.exchange.client_id:
        logger.error("Missing MOCK_CLIENT_ID environment variable.")
        missing_secrets = True
    if not mock_env.exchange.client_secret:
        logger.error("Missing MOCK_CLIENT_SECRET environment variable.")
        missing_secrets = True
    if not mock_env.exchange.exchange_url:
        logger.error("Missing MOCK_EXCHANGE_URL environment variable.")
        missing_secrets = True
    # Add checks for Snowflake mock vars if needed

    if missing_secrets:
        logger.error("Halting local execution due to missing mock secrets.")
        exit(1)

    logger.info("Mock environment configured. Starting local pipeline run...")
    try:
        # Run the pipeline, typically without loading to the actual destination
        run(env=mock_env, is_load=False, dq_actions=False)
        logger.info("--- Local Pipeline Run Finished Successfully ---")
    except Exception as e:
        logger.exception(f"--- Local Pipeline Run Failed: {e} ---") 