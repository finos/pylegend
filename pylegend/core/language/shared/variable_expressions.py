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

from abc import ABCMeta
from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
)
from pylegend.core.language.shared.expression import (
    PyLegendExpression,
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendVariableExpression",
    "PyLegendBooleanVariableExpression",
    "PyLegendStringVariableExpression",
    "PyLegendNumberVariableExpression",
    "PyLegendIntegerVariableExpression",
    "PyLegendFloatVariableExpression",
    "PyLegendDateVariableExpression",
    "PyLegendDateTimeVariableExpression",
    "PyLegendStrictDateVariableExpression"
]


class PyLegendVariableExpression(PyLegendExpression, metaclass=ABCMeta):
    __name: str

    def __init__(self, name: str) -> None:
        if not name.isidentifier():
            raise ValueError(f"Invalid variable name: '{name}'. Should be a valid identifier")
        self.__name = name

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        raise RuntimeError("SQL translation for variable expression not supported!")

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"${self.__name}"


class PyLegendBooleanVariableExpression(PyLegendVariableExpression, PyLegendExpressionBooleanReturn):
    def __init__(self, name: str) -> None:
        super().__init__(name=name)


class PyLegendStringVariableExpression(PyLegendVariableExpression, PyLegendExpressionStringReturn):
    def __init__(self, name: str) -> None:
        super().__init__(name=name)


class PyLegendNumberVariableExpression(PyLegendVariableExpression, PyLegendExpressionNumberReturn):
    def __init__(self, name: str) -> None:
        super().__init__(name=name)


class PyLegendIntegerVariableExpression(PyLegendNumberVariableExpression, PyLegendExpressionIntegerReturn):
    def __init__(self, name: str) -> None:
        super().__init__(name=name)


class PyLegendFloatVariableExpression(PyLegendNumberVariableExpression, PyLegendExpressionFloatReturn):
    def __init__(self, name: str) -> None:
        super().__init__(name=name)


class PyLegendDateVariableExpression(PyLegendVariableExpression, PyLegendExpressionDateReturn):
    def __init__(self, name: str) -> None:
        super().__init__(name=name)


class PyLegendDateTimeVariableExpression(PyLegendDateVariableExpression, PyLegendExpressionDateTimeReturn):
    def __init__(self, name: str) -> None:
        super().__init__(name=name)


class PyLegendStrictDateVariableExpression(PyLegendDateVariableExpression, PyLegendExpressionStrictDateReturn):
    def __init__(self, name: str) -> None:
        super().__init__(name=name)
