from typing import ClassVar
from orch.flow.inherit_test.base import InheritBase


class InheritFoo(InheritBase):
    description: ClassVar[str] = "Foo deployment"
