# Copyright 2026 Goldman Sachs
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

from abc import ABC, abstractmethod
from pylegend._typing import (
    PyLegendList,
    PyLegendTuple,
)


class PureExpression(ABC):
    @abstractmethod
    def compile(self, tds_row_alias: str) -> PyLegendTuple[PyLegendList["PrerequisitePureExpression"], str]:
        pass  # pragma: no cover

    @staticmethod
    def from_prerequisite_expr(prerequisite_expr: str, column_name: str) -> "PureExpression":
        return PrerequisitePureExpression(prerequisite_expr, column_name)


class PrerequisitePureExpression(PureExpression):
    _prerequisite_expr: str
    _column_name: str

    def __init__(self, prerequisite_expr: str, column_name: str):
        self._prerequisite_expr = prerequisite_expr
        self._column_name = column_name

    @property
    def prerequisite_expr(self) -> str:
        return self._prerequisite_expr

    def compile(self, tds_row_alias: str) -> PyLegendTuple[PyLegendList["PrerequisitePureExpression"], str]:
        final_expr = f"${tds_row_alias}.{self._column_name}"
        return [self], final_expr


class CompositePureExpression(PureExpression):
    _prerequisites: PyLegendList[PrerequisitePureExpression]
    _final_expr: str

    def __init__(self, final_expr: str):
        self._prerequisites = []
        self._final_expr = final_expr

    def add_prerequisites(self, new_prerequisites: PyLegendList[PrerequisitePureExpression]) -> None:
        self._prerequisites.extend(new_prerequisites)

    def compile(self, tds_row_alias: str) -> PyLegendTuple[PyLegendList[PrerequisitePureExpression], str]:
        return self._prerequisites, self._final_expr
