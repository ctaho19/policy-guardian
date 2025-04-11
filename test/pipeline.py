import logging
import os
from pathlib import Path

# Core ETIP/AxDP imports (adjust based on actual framework)
from config_pipeline import ConfigPipeline # Assuming this base class exists
from etip_env import Env # Assuming Env class provides config/secrets

# Imports for API interaction (similar to reference)
from connectors.api import OauthApi # Example API connector class
from connectors.exchange.oauth_token import refresh # Example token refresh function
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
    pipeline = PLAmCtrl1077231Pipeline(env) # Instantiate the specific pipeline class
    pipeline.configure_from_filename(str(Path(__file__).parent / "config.yml"))
    logger.info(f"Running pipeline: {pipeline.pipeline_name}")
    return (
        pipeline.run_test_data_export(dq_actions=dq_actions)
        if is_export_test_data
        else pipeline.run(load=is_load, dq_actions=dq_actions)
    )

# Define a pipeline class inheriting from the framework's base class
class PLAmCtrl1077231Pipeline(ConfigPipeline):
    def __init__(self,
        env: Env, # Receive environment context
    ) -> None:
        super().__init__(env)
        # Setup API connection details here, using env for secrets
        self.cloud_tooling_api_url = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
        # Fetch client_id and client_secret securely from the environment
        # Ensure these secrets are configured in the ETIP environment for this pipeline
        self.client_id = env.exchange.client_id # Example access
        self.client_secret = env.exchange.client_secret # Example access
        self.exchange_url = env.exchange.exchange_url # Example access

    def _get_api_token(self) -> str:
        """Refreshes and returns the API OAuth token."""
        logger.info("Refreshing API token...")
        try:
            token = refresh(
                client_id=self.client_id,
                client_secret=self.client_secret,
                exchange_url=self.exchange_url,
            )
            logger.info("API token refreshed successfully.")
            return f"Bearer {token}"
        except Exception as e:
            logger.error(f"Failed to refresh API token: {e}")
            raise

    # Override or extend transform stage if needed to inject API token/client
    # This depends heavily on how ConfigPipeline allows context injection
    def transform(self) -> None:
        """Extends base transform to inject API context."""
        logger.info("Preparing transform stage...")
        # Get fresh API token
        api_auth_token = self._get_api_token()

        # Inject the token into the pipeline context so transform functions can access it.
        # The exact mechanism depends on the ConfigPipeline implementation.
        # Option 1: Update a shared context dictionary
        self.context["api_auth_token"] = api_auth_token
        self.context["cloud_tooling_api_url"] = self.cloud_tooling_api_url
        self.context["api_verify_ssl"] = C1_CERT_FILE # Pass cert path if needed

        # Option 2: Pass it explicitly if the framework allows modifying kwargs
        # (This might require changes to how functions are called in config.yml)

        logger.info("API context injected. Proceeding with standard transform.")
        super().transform() # Call the base transform method which executes steps from config.yml


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

    logger.warning("Running pipeline script directly for local testing.")
    logger.warning("Ensure mock environment/secrets are correctly configured.")

    # Create a mock Env object for local testing
    # IMPORTANT: Replace with realistic mock data or a proper local config setup
    class MockExchangeConfig:
        client_id = os.environ.get("MOCK_CLIENT_ID", "your_mock_client_id")
        client_secret = os.environ.get("MOCK_CLIENT_SECRET", "your_mock_client_secret")
        exchange_url = os.environ.get("MOCK_EXCHANGE_URL", "https://mock.exchange.url")

    class MockEnv:
        exchange = MockExchangeConfig()
        # Add other necessary env attributes if needed by ConfigPipeline base class

    mock_env = MockEnv()

    # Check if mock secrets are set (simple example)
    if mock_env.exchange.client_id == "your_mock_client_id":
        logger.error("Mock client ID not set. Use environment variables or update script.")
        exit(1)
    if mock_env.exchange.client_secret == "your_mock_client_secret":
        logger.error("Mock client secret not set. Use environment variables or update script.")
        exit(1)

    # Run the pipeline
    try:
        run(env=mock_env, is_load=False) # Typically don't load in local test
        logger.info("Local pipeline run finished.")
    except Exception as e:
        logger.exception(f"Local pipeline run failed: {e}") 