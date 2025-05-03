import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Type
from prefect import Flow
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret
from prefect.schedules import Cron


from orch.flow.hello_world.depl import HelloWorld
from orch.flow.inherit_test.bar.depl import InheritBar
from orch.flow.inherit_test.foo.depl import InheritFoo
from orch.core.base import BaseDepl


@dataclass
class DeploymentConfig:
    flow_class: Type[BaseDepl]
    entrypoint: str

async def deploy() -> None:

    gitlab_repo = GitRepository(
        url="https://github.com/vyagubov/orch_sandbox.git", #todo: to config
        branch="develop", #todo: to config
    )

    global_config = {
        HelloWorld.deployment_name: DeploymentConfig(
            flow_class=HelloWorld,
            entrypoint="orch/flow/hello_world/depl.py:link",
        ),
        InheritBar.deployment_name: DeploymentConfig(
            flow_class=HelloWorld,
            entrypoint="orch/flow/inherit_test/bar/depl.py:link",
        ),
        InheritFoo.deployment_name: DeploymentConfig(
            flow_class=HelloWorld,
            entrypoint="orch/flow/inherit_test/foo/depl.py:link",
        ),
    }

    for (deployment_name,
        deployment_config,
    ) in global_config.items():
        klass = deployment_config.flow_class

        cron_setting = {
            "cron": klass.cron,
            "day_or": True,
            "timezone": "Europe/Berlin",
        }

        schedule = (
            Cron(
                cron_setting["cron"],
                day_or=cron_setting["day_or"],
                timezone=cron_setting["timezone"],
            )
            if klass.cron and klass.env == "dev"
            else None
        )

        deployment_dict = {
            "name": deployment_name,
            "work_pool_name": klass.work_pool_name,
            "work_queue_name": klass.work_queue_name,
            "job_variables": klass.job_variables,
            "rrule": klass.rrule,
            "paused": klass.paused,
            "build": klass.build,
            "push": klass.push,
            "image": klass.image,
            "interval": klass.interval,
            "triggers": klass.triggers,
            "parameters": klass.parameters,
            "description": klass.description,
            "tags": klass.tags,
            "enforce_parameter_schema": klass.enforce_parameter_schema,
            "print_next_steps": klass.print_next_steps,
            "ignore_warnings": klass.ignore_warnings,
            "schedule": schedule,
            "concurrency_limit": klass.concurrency_limit,
        }

        flow = await Flow.afrom_source(
            source=gitlab_repo,
            entrypoint=deployment_config.entrypoint
        )
        await flow.deploy(
            **deployment_dict,
        )


if __name__=="__main__":
    asyncio.run(deploy())