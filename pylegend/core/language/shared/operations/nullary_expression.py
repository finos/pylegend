# Copyright 2023 Goldman Sachs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import ABCMeta
from pylegend._typing import (
    PyLegendSequence,
    PyLegendCallable,
)
from pylegend.core.language.shared.expression import (
    PyLegendExpression,
)
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendNullaryExpression"
]


class PyLegendNullaryExpression(PyLegendExpression, metaclass=ABCMeta):
    __to_pure_func: PyLegendCallable[[FrameToPureConfig], str]

    def __init__(
            self,
            to_pure_func: PyLegendCallable[[FrameToPureConfig], str],
            non_nullable: bool = False,
    ) -> None:
        self.__to_pure_func = to_pure_func
        self.__non_nullable = non_nullable

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return self.__to_pure_func(config)

    def is_non_nullable(self) -> bool:
        return self.__non_nullable

    def get_leaf_expressions(self) -> PyLegendSequence["PyLegendExpression"]:
        return [self]
