import logging
from pathlib import Path
from google.cloud import bigquery

from gustavo_sdk.infrastructure.integration.gcp.gcp import GCP


logger = logging.getLogger(__name__)


class Silver:
    def __init__(
        self,
        sql_path: str,
        credentials_json: dict,
    ) -> None:
        self.sql_path = Path(sql_path)

        self.gcp_bigquery = GCP(
            service=bigquery,
            credentials_json=credentials_json,
        )

    def build_query(self) -> str:
        with open(
            self.sql_path,
            "r",
            encoding="utf-8",
        ) as file:
            return file.read()

    def execute_query(
        self,
        query: str,
    ) -> None:
        job = self.gcp_bigquery.client.query(query)
        job.result()

    def run(self) -> None:
        logger.info(
            f"Starting SILVER pipeline: {self.sql_path}"
        )

        query = self.build_query()

        logger.info(
            "Executing SILVER query"
        )

        self.execute_query(query)

        logger.info(
            "SILVER table created successfully"
        )