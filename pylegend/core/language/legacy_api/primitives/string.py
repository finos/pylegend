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
from pylegend.core.language.legacy_api.primitives.primitive import LegacyApiPrimitive
from pylegend.core.language.legacy_api.primitives.integer import LegacyApiInteger
from pylegend.core.language.legacy_api.primitives.float import LegacyApiFloat
from pylegend.core.language.legacy_api.primitives.boolean import LegacyApiBoolean
from pylegend.core.language.shared.expression import PyLegendExpressionStringReturn, PyLegendExpression
from pylegend.core.language.shared.literal_expressions import PyLegendStringLiteralExpression
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.language.shared.operations.string_operation_expressions import (
    PyLegendStringLengthExpression,
    PyLegendStringStartsWithExpression,
    PyLegendStringEndsWithExpression,
    PyLegendStringContainsExpression,
    PyLegendStringUpperExpression,
    PyLegendStringLowerExpression,
    PyLegendStringLTrimExpression,
    PyLegendStringRTrimExpression,
    PyLegendStringBTrimExpression,
    PyLegendStringPosExpression,
    PyLegendStringParseIntExpression,
    PyLegendStringParseFloatExpression,
    PyLegendStringConcatExpression,
    PyLegendStringLessThanExpression,
    PyLegendStringLessThanEqualExpression,
    PyLegendStringGreaterThanExpression,
    PyLegendStringGreaterThanEqualExpression,
)


__all__: PyLegendSequence[str] = [
    "LegacyApiString"
]


class LegacyApiString(LegacyApiPrimitive):
    __value: PyLegendExpressionStringReturn

    def __init__(
            self,
            value: PyLegendExpressionStringReturn
    ) -> None:
        self.__value = value

    def len(self) -> LegacyApiInteger:
        return LegacyApiInteger(PyLegendStringLengthExpression(self.__value))

    def length(self) -> LegacyApiInteger:
        return self.len()

    def startswith(self, prefix: str) -> LegacyApiBoolean:
        LegacyApiString.__validate_param_to_be_str(prefix, "startswith prefix parameter")
        return LegacyApiBoolean(
            PyLegendStringStartsWithExpression(self.__value, PyLegendStringLiteralExpression(prefix))
        )

    def endswith(self, suffix: str) -> LegacyApiBoolean:
        LegacyApiString.__validate_param_to_be_str(suffix, "endswith suffix parameter")
        return LegacyApiBoolean(
            PyLegendStringEndsWithExpression(self.__value, PyLegendStringLiteralExpression(suffix))
        )

    def contains(self, other: str) -> LegacyApiBoolean:
        LegacyApiString.__validate_param_to_be_str(other, "contains/in other parameter")
        return LegacyApiBoolean(
            PyLegendStringContainsExpression(self.__value, PyLegendStringLiteralExpression(other))
        )

    def upper(self) -> "LegacyApiString":
        return LegacyApiString(PyLegendStringUpperExpression(self.__value))

    def lower(self) -> "LegacyApiString":
        return LegacyApiString(PyLegendStringLowerExpression(self.__value))

    def lstrip(self) -> "LegacyApiString":
        return LegacyApiString(PyLegendStringLTrimExpression(self.__value))

    def rstrip(self) -> "LegacyApiString":
        return LegacyApiString(PyLegendStringRTrimExpression(self.__value))

    def strip(self) -> "LegacyApiString":
        return LegacyApiString(PyLegendStringBTrimExpression(self.__value))

    def index_of(self, other: PyLegendUnion[str, "LegacyApiString"]) -> "LegacyApiInteger":
        LegacyApiString.__validate_param_to_be_str_or_str_expr(other, "Index_of parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegacyApiInteger(PyLegendStringPosExpression(self.__value, other_op))

    def index(self, other: PyLegendUnion[str, "LegacyApiString"]) -> "LegacyApiInteger":
        return self.index_of(other)

    def parse_int(self) -> "LegacyApiInteger":
        return LegacyApiInteger(PyLegendStringParseIntExpression(self.__value))

    def parse_integer(self) -> "LegacyApiInteger":
        return self.parse_int()

    def parse_float(self) -> "LegacyApiFloat":
        return LegacyApiFloat(PyLegendStringParseFloatExpression(self.__value))

    def __add__(self, other: PyLegendUnion[str, "LegacyApiString"]) -> "LegacyApiString":
        LegacyApiString.__validate_param_to_be_str_or_str_expr(other, "String plus (+) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegacyApiString(PyLegendStringConcatExpression(self.__value, other_op))

    def __radd__(self, other: PyLegendUnion[str, "LegacyApiString"]) -> "LegacyApiString":
        LegacyApiString.__validate_param_to_be_str_or_str_expr(other, "String plus (+) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegacyApiString(PyLegendStringConcatExpression(other_op, self.__value))

    def __lt__(self, other: PyLegendUnion[str, "LegacyApiString"]) -> "LegacyApiBoolean":
        LegacyApiString.__validate_param_to_be_str_or_str_expr(other, "String less than (<) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegacyApiBoolean(PyLegendStringLessThanExpression(self.__value, other_op))

    def __le__(self, other: PyLegendUnion[str, "LegacyApiString"]) -> "LegacyApiBoolean":
        LegacyApiString.__validate_param_to_be_str_or_str_expr(other, "String less than equal (<=) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegacyApiBoolean(PyLegendStringLessThanEqualExpression(self.__value, other_op))

    def __gt__(self, other: PyLegendUnion[str, "LegacyApiString"]) -> "LegacyApiBoolean":
        LegacyApiString.__validate_param_to_be_str_or_str_expr(other, "String greater than (>) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegacyApiBoolean(PyLegendStringGreaterThanExpression(self.__value, other_op))

    def __ge__(self, other: PyLegendUnion[str, "LegacyApiString"]) -> "LegacyApiBoolean":
        LegacyApiString.__validate_param_to_be_str_or_str_expr(other, "String greater than equal (>=) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegacyApiBoolean(PyLegendStringGreaterThanEqualExpression(self.__value, other_op))

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__value.to_sql_expression(frame_name_to_base_query_map, config)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return self.__value.to_pure_expression(config)

    def value(self) -> PyLegendExpression:
        return self.__value

    @staticmethod
    def __validate_param_to_be_str_or_str_expr(param: PyLegendUnion[str, "LegacyApiString"], desc: str) -> None:
        if not isinstance(param, (str, LegacyApiString)):
            raise TypeError(desc + " should be a str or a string expression (PyLegendString)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

    @staticmethod
    def __validate_param_to_be_str(param: str, desc: str) -> None:
        if not isinstance(param, str):
            raise TypeError(desc + " should be a str."
                                   " Got value " + str(param) + " of type: " + str(type(param)))
