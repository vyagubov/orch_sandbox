from typing import ClassVar
from orch.flow.inherit_test.base import InheritBase


class InheritBar(InheritBase):
    description: ClassVar[str] = "Bar deployment"
    parameters: ClassVar[dict] = {"repo_name": "PrefectHQ/prefect"}
