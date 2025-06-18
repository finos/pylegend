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
from pylegend.core.language.legend_api.primitives.primitive import LegendApiPrimitive
from pylegend.core.language.legend_api.primitives.integer import LegendApiInteger
from pylegend.core.language.legend_api.primitives.float import LegendApiFloat
from pylegend.core.language.legend_api.primitives.boolean import LegendApiBoolean
from pylegend.core.language.shared.expression import PyLegendExpressionStringReturn, PyLegendExpression
from pylegend.core.language.shared.literal_expressions import PyLegendStringLiteralExpression
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.language.shared.operations.string_operation_expressions import (
    PyLegendStringLengthExpression,
    PyLegendStringLikeExpression,
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
    "LegendApiString"
]


class LegendApiString(LegendApiPrimitive):
    __value: PyLegendExpressionStringReturn

    def __init__(
            self,
            value: PyLegendExpressionStringReturn
    ) -> None:
        self.__value = value

    def len(self) -> LegendApiInteger:
        return LegendApiInteger(PyLegendStringLengthExpression(self.__value))

    def length(self) -> LegendApiInteger:
        return self.len()

    def startswith(self, prefix: str) -> LegendApiBoolean:
        LegendApiString.__validate_param_to_be_str(prefix, "startswith prefix parameter")
        escaped_prefix = LegendApiString.__escape_like_param(prefix)
        return LegendApiBoolean(
            PyLegendStringLikeExpression(self.__value, PyLegendStringLiteralExpression(escaped_prefix + '%'))
        )

    def endswith(self, suffix: str) -> LegendApiBoolean:
        LegendApiString.__validate_param_to_be_str(suffix, "endswith suffix parameter")
        escaped_suffix = LegendApiString.__escape_like_param(suffix)
        return LegendApiBoolean(
            PyLegendStringLikeExpression(self.__value, PyLegendStringLiteralExpression("%" + escaped_suffix))
        )

    def contains(self, other: str) -> LegendApiBoolean:
        LegendApiString.__validate_param_to_be_str(other, "contains/in other parameter")
        escaped_other = LegendApiString.__escape_like_param(other)
        return LegendApiBoolean(
            PyLegendStringLikeExpression(self.__value, PyLegendStringLiteralExpression("%" + escaped_other + "%"))
        )

    def upper(self) -> "LegendApiString":
        return LegendApiString(PyLegendStringUpperExpression(self.__value))

    def lower(self) -> "LegendApiString":
        return LegendApiString(PyLegendStringLowerExpression(self.__value))

    def lstrip(self) -> "LegendApiString":
        return LegendApiString(PyLegendStringLTrimExpression(self.__value))

    def rstrip(self) -> "LegendApiString":
        return LegendApiString(PyLegendStringRTrimExpression(self.__value))

    def strip(self) -> "LegendApiString":
        return LegendApiString(PyLegendStringBTrimExpression(self.__value))

    def index_of(self, other: PyLegendUnion[str, "LegendApiString"]) -> "LegendApiInteger":
        LegendApiString.__validate_param_to_be_str_or_str_expr(other, "Index_of parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegendApiInteger(PyLegendStringPosExpression(self.__value, other_op))

    def index(self, other: PyLegendUnion[str, "LegendApiString"]) -> "LegendApiInteger":
        return self.index_of(other)

    def parse_int(self) -> "LegendApiInteger":
        return LegendApiInteger(PyLegendStringParseIntExpression(self.__value))

    def parse_integer(self) -> "LegendApiInteger":
        return self.parse_int()

    def parse_float(self) -> "LegendApiFloat":
        return LegendApiFloat(PyLegendStringParseFloatExpression(self.__value))

    def __add__(self, other: PyLegendUnion[str, "LegendApiString"]) -> "LegendApiString":
        LegendApiString.__validate_param_to_be_str_or_str_expr(other, "String plus (+) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegendApiString(PyLegendStringConcatExpression(self.__value, other_op))

    def __radd__(self, other: PyLegendUnion[str, "LegendApiString"]) -> "LegendApiString":
        LegendApiString.__validate_param_to_be_str_or_str_expr(other, "String plus (+) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegendApiString(PyLegendStringConcatExpression(other_op, self.__value))

    def __lt__(self, other: PyLegendUnion[str, "LegendApiString"]) -> "LegendApiBoolean":
        LegendApiString.__validate_param_to_be_str_or_str_expr(other, "String less than (<) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegendApiBoolean(PyLegendStringLessThanExpression(self.__value, other_op))

    def __le__(self, other: PyLegendUnion[str, "LegendApiString"]) -> "LegendApiBoolean":
        LegendApiString.__validate_param_to_be_str_or_str_expr(other, "String less than equal (<=) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegendApiBoolean(PyLegendStringLessThanEqualExpression(self.__value, other_op))

    def __gt__(self, other: PyLegendUnion[str, "LegendApiString"]) -> "LegendApiBoolean":
        LegendApiString.__validate_param_to_be_str_or_str_expr(other, "String greater than (>) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegendApiBoolean(PyLegendStringGreaterThanExpression(self.__value, other_op))

    def __ge__(self, other: PyLegendUnion[str, "LegendApiString"]) -> "LegendApiBoolean":
        LegendApiString.__validate_param_to_be_str_or_str_expr(other, "String greater than equal (>=) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return LegendApiBoolean(PyLegendStringGreaterThanEqualExpression(self.__value, other_op))

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__value.to_sql_expression(frame_name_to_base_query_map, config)

    def value(self) -> PyLegendExpression:
        return self.__value

    @staticmethod
    def __escape_like_param(param: str) -> str:
        return param.replace("_", "\\_").replace("%", "\\%")

    @staticmethod
    def __validate_param_to_be_str_or_str_expr(param: PyLegendUnion[str, "LegendApiString"], desc: str) -> None:
        if not isinstance(param, (str, LegendApiString)):
            raise TypeError(desc + " should be a str or a string expression (PyLegendString)."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

    @staticmethod
    def __validate_param_to_be_str(param: str, desc: str) -> None:
        if not isinstance(param, str):
            raise TypeError(desc + " should be a str."
                                   " Got value " + str(param) + " of type: " + str(type(param)))
