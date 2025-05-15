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
    else:
        return []

@task
def produce_to_kafka(data: list[dict[str, Any]]) -> None:
    logger = get_run_logger()
    sleep(3)
    # imitate producing to kafka
    logger.info(f"Produced {len(data)} rows to topic_name")


class Demo(BaseDepl):
    description: ClassVar[str] = "Simple Demo deployment"

    def flow(anything: list[str]) -> None:  # type: ignore
        list_ = [("secret_name", "select 1 as a"),("secret_name_", "select 2 as a")]
        task_links = []
        for secret_name, query in list_:
            secret = get_secret_from_aws.submit(secret_name=secret_name)
            task_links.append(secret)

            data = get_data_from_snowflake.with_options(name="data_from_sf").submit(query=query, secret=secret)
            task_links.append(data)

            task_links.append(produce_to_kafka.submit(data))

        [task.result() for task in task_links]
