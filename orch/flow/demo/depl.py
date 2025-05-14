from time import sleep
from typing import Any, ClassVar

from prefect import get_run_logger, task

from orch.core.base import BaseDepl


@task
def get_secret_from_aws(secret_name: str) -> str:
    # imitate getting secret from AWS Secret Manager
    sleep(2)
    if secret_name:
        return "something"
    else:
        raise ValueError("secret_name is missing")
    
    
@task
def get_data_from_snowflake(query: str, secret: str) -> list[dict[str, Any]]:
    # imitate querying the Snowflake
    sleep(2)
    logger = get_run_logger()
    if query and secret:
        logger.info("Snowflake returned 3 rows")
        return [
            {"a":5, "b": 6},
            {"a":7, "b": 8},
            {"a":9, "b": 10},
        ]

@task
def produce_to_kafka(data: list[dict[str, Any]]) -> None:
    logger = get_run_logger()
    sleep()
    # imitate producing to kafka
    logger.info(f"Produced {len(data)} rows to ")


class Demo(BaseDepl):
    description: ClassVar[str] = "Simple Demo deployment"

    def flow() -> None:  # type: ignore
        secret_name = "secret_name"
        query = "select 1 as a"

        secret = get_secret_from_aws(secret_name=secret_name)

        data = get_data_from_snowflake(query=query, secret=secret)
         
        produce_to_kafka(data)