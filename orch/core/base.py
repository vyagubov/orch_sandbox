import datetime

from typing import ClassVar
from prefect import flow
from prefect.client.schemas.objects import (
    ConcurrencyLimitConfig,
    ConcurrencyLimitStrategy,
)
from prefect.events import DeploymentTriggerTypes, TriggerTypes

class classproperty:
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        return self.func(owner)


class BaseDepl:
    env: ClassVar[str] = "dev"
    work_pool_name: ClassVar[str] = "managed_worker"
    flow_name: ClassVar[str | None] = None
    work_queue_name: ClassVar[str | None] = None
    job_variables: ClassVar[dict | None] = None
    cron: ClassVar[str | None] = None
    rrule: ClassVar[str | None] = None
    paused: ClassVar[bool | None] = None
    build: ClassVar[bool | None] = None
    push: ClassVar[bool | None] = None
    image: ClassVar[str | None] = None
    interval: ClassVar[int | float | datetime.timedelta | None] = None
    triggers: ClassVar[list[DeploymentTriggerTypes | TriggerTypes] | None] = None
    parameters: ClassVar[dict | None] = None
    description: ClassVar[str | None] = None
    tags: ClassVar[list[str] | None] = None
    version: ClassVar[str | None] = None
    enforce_parameter_schema: ClassVar[bool] = True
    print_next_steps: ClassVar[bool] = True
    ignore_warnings: ClassVar[bool] = False
    concurrency_limit: ClassVar[
        int | ConcurrencyLimitConfig | None
    ] = ConcurrencyLimitConfig(
        limit=1, #todo: config
        collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
    )


    @classproperty
    def deployment_name(cls) -> str:
        return f"{cls.__name__}"  # type: ignore


    @classmethod
    def flow(*args, **kwargs):
        # Placeholder for child class flow logic
        raise NotImplementedError("Subclasses must implement the flow logic.")


    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Wrap run method and make it Flow
        # Check if "flow" method was implemented in current class, not in parent
        if "flow" in cls.__dict__:
            original_run = getattr(cls, "flow", None)
            decorated_run = flow(
                name=cls.flow_name or cls.__name__,
            )(original_run)
            setattr(cls, "flow", decorated_run)