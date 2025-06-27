from time import sleep
from typing import ClassVar

from prefect import flow, task

from orch.core.base import BaseDepl


@task(log_prints=True)
def say_hello(name: str) -> None:
    if name == "Error":
        raise ValueError("This is expected to fail.")
    if name == "Sleep":
        sleep(120)
    print(f"Hello, {name}!")


class HelloWorld(BaseDepl):
    description: ClassVar[str] = "Simple Hello World deployment"
    a: ClassVar[str] = "aaaaaaaa"

    @classmethod
    def flow(cls, names: list[str]) -> None:  # type: ignore
        names = [f"{name}{cls.a}" for name in names]
        parallel_tasks = say_hello.map(names)  # most common parallel task

        [task.result() for task in parallel_tasks]  # catch the result


