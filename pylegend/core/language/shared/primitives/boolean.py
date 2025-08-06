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

from pylegend._typing import (
    PyLegendSequence,
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.language.shared.expression import PyLegendExpressionBooleanReturn
from pylegend.core.language.shared.literal_expressions import PyLegendBooleanLiteralExpression
from pylegend.core.language.shared.operations.boolean_operation_expressions import (
    PyLegendBooleanOrExpression,
    PyLegendBooleanAndExpression,
    PyLegendBooleanNotExpression,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "PyLegendBoolean"
]


class PyLegendBoolean(PyLegendPrimitive):
    __value: PyLegendExpressionBooleanReturn

    def __init__(
            self,
            value: PyLegendExpressionBooleanReturn
    ) -> None:
        self.__value = value

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__value.to_sql_expression(frame_name_to_base_query_map, config)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return self.__value.to_pure_expression(config)

    def value(self) -> PyLegendExpressionBooleanReturn:
        return self.__value

    def __or__(self, other: PyLegendUnion[bool, "PyLegendBoolean"]) -> "PyLegendBoolean":
        PyLegendBoolean.__validate__param_to_be_bool(other, "Boolean OR (|) parameter")
        other_op = PyLegendBooleanLiteralExpression(other) if isinstance(other, bool) else other.__value
        return PyLegendBoolean(PyLegendBooleanOrExpression(self.__value, other_op))

    def __ror__(self, other: PyLegendUnion[bool, "PyLegendBoolean"]) -> "PyLegendBoolean":
        PyLegendBoolean.__validate__param_to_be_bool(other, "Boolean OR (|) parameter")
        other_op = PyLegendBooleanLiteralExpression(other) if isinstance(other, bool) else other.__value
        return PyLegendBoolean(PyLegendBooleanOrExpression(other_op, self.__value))

    def __and__(self, other: PyLegendUnion[bool, "PyLegendBoolean"]) -> "PyLegendBoolean":
        PyLegendBoolean.__validate__param_to_be_bool(other, "Boolean AND (&) parameter")
        other_op = PyLegendBooleanLiteralExpression(other) if isinstance(other, bool) else other.__value
        return PyLegendBoolean(PyLegendBooleanAndExpression(self.__value, other_op))

    def __rand__(self, other: PyLegendUnion[bool, "PyLegendBoolean"]) -> "PyLegendBoolean":
        PyLegendBoolean.__validate__param_to_be_bool(other, "Boolean AND (&) parameter")
        other_op = PyLegendBooleanLiteralExpression(other) if isinstance(other, bool) else other.__value
        return PyLegendBoolean(PyLegendBooleanAndExpression(other_op, self.__value))

    def __invert__(self) -> "PyLegendBoolean":
        return PyLegendBoolean(PyLegendBooleanNotExpression(self.__value))

    @staticmethod
    def __validate__param_to_be_bool(param: PyLegendUnion[bool, "PyLegendBoolean"], desc: str) -> None:
        if not isinstance(param, (bool, PyLegendBoolean)):
            raise TypeError(desc + " should be a bool or a boolean expression (PyLegendBoolean)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))
