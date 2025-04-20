import logging
import os
from pathlib import Path
from typing import Dict, Any

from config_pipeline import ConfigPipeline
from etip_env import Env

from connectors.exchange.oauth_token import refresh as refresh_oauth_token
from connectors.ca_certs import C1_CERT_FILE

import pipelines.pl_automated_monitoring_CTRL_1077231.transform

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

        self.cloud_tooling_api_url = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
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
