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
from pylegend.core.language.shared.literal_expressions import PyLegendIntegerLiteralExpression
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.language.shared.primitives.float import PyLegendFloat
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.datetime import PyLegendDate
from pylegend.core.language.shared.expression import PyLegendExpressionStringReturn
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
    PyLegendStringParseBooleanExpression,
    PyLegendStringParseDateTimeExpression,
    PyLegendStringOrdExpression,
    PyLegendStringDecodeBase64Expression,
    PyLegendStringEncodeBase64Expression,
    PyLegendStringReverseExpression,
    PyLegendStringToLowerFirstCharacterExpression,
    PyLegendStringToUpperFirstCharacterExpression,
    PyLegendStringLeftExpression,
    PyLegendStringRightExpression,
    PyLegendStringSubStringExpression,
    PyLegendStringJaroWinklerSimilarityExpression,
    PyLegendStringLevenshteinDistanceExpression,
    PyLegendStringReplaceExpression
)

from pylegend.core.language.shared.primitives.integer import PyLegendInteger

__all__: PyLegendSequence[str] = [
    "PyLegendString"
]


class PyLegendString(PyLegendPrimitive):
    __value: PyLegendExpressionStringReturn

    def __init__(
            self,
            value: PyLegendExpressionStringReturn
    ) -> None:
        self.__value = value

    def len(self) -> PyLegendInteger:
        return PyLegendInteger(PyLegendStringLengthExpression(self.__value))

    def length(self) -> PyLegendInteger:
        return self.len()

    def startswith(self, prefix: str) -> PyLegendBoolean:
        PyLegendString.__validate_param_to_be_str(prefix, "startswith prefix parameter")
        return PyLegendBoolean(
            PyLegendStringStartsWithExpression(self.__value, PyLegendStringLiteralExpression(prefix))
        )

    def endswith(self, suffix: str) -> PyLegendBoolean:
        PyLegendString.__validate_param_to_be_str(suffix, "endswith suffix parameter")
        return PyLegendBoolean(
            PyLegendStringEndsWithExpression(self.__value, PyLegendStringLiteralExpression(suffix))
        )

    def contains(self, other: str) -> PyLegendBoolean:
        PyLegendString.__validate_param_to_be_str(other, "contains/in other parameter")
        return PyLegendBoolean(
            PyLegendStringContainsExpression(self.__value, PyLegendStringLiteralExpression(other))
        )

    def upper(self) -> "PyLegendString":
        return PyLegendString(PyLegendStringUpperExpression(self.__value))

    def lower(self) -> "PyLegendString":
        return PyLegendString(PyLegendStringLowerExpression(self.__value))

    def lstrip(self) -> "PyLegendString":
        return PyLegendString(PyLegendStringLTrimExpression(self.__value))

    def rstrip(self) -> "PyLegendString":
        return PyLegendString(PyLegendStringRTrimExpression(self.__value))

    def strip(self) -> "PyLegendString":
        return PyLegendString(PyLegendStringBTrimExpression(self.__value))

    # def index_of(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendInteger":
    #     PyLegendString.__validate_param_to_be_str_or_str_expr(other, "Index_of parameter")
    #     other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
    #     return PyLegendInteger(PyLegendStringPosExpression(self.__value, other_op))

    def index(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendInteger":
        return self.index_of(other)

    def index_of(self, other: PyLegendUnion[str, "PyLegendString"], index_from: PyLegendUnion[int, "PyLegendInteger"] = 0) -> "PyLegendInteger":
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "Index_of parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        index_from = PyLegendIntegerLiteralExpression(index_from) if isinstance(index_from, int) else index_from.value()
        return PyLegendInteger(PyLegendStringPosExpression([self.__value, other_op, index_from]))

    def parse_int(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendStringParseIntExpression(self.__value))

    def parse_integer(self) -> "PyLegendInteger":
        return self.parse_int()

    def parse_float(self) -> "PyLegendFloat":
        return PyLegendFloat(PyLegendStringParseFloatExpression(self.__value))

    def parse_boolean(self) -> "PyLegendBoolean":
        return PyLegendBoolean(PyLegendStringParseBooleanExpression(self.__value))

    def parse_date(self) -> "PyLegendDate":
        return PyLegendDate(PyLegendStringParseDateTimeExpression(self.__value))

    def ord(self) -> "PyLegendInteger":
        return PyLegendInteger(PyLegendStringOrdExpression(self.__value))

    def decode_base64(self) -> "PyLegendString":
        return PyLegendString(PyLegendStringDecodeBase64Expression(self.__value))

    def encode_base64(self) -> "PyLegendString":
        return PyLegendString(PyLegendStringEncodeBase64Expression(self.__value))

    def reverse(self) -> "PyLegendString":
        return PyLegendString(PyLegendStringReverseExpression(self.__value))

    def to_lower_first_character(self) -> "PyLegendString":
        return PyLegendString(PyLegendStringToLowerFirstCharacterExpression(self.__value))

    def to_upper_first_character(self) -> "PyLegendString":
         return PyLegendString(PyLegendStringToUpperFirstCharacterExpression(self.__value))

    def left(self, count: PyLegendUnion[int, "PyLegendInteger"]) -> "PyLegendString":
        PyLegendString.__validate_param_to_be_integer(count, "left parameter")
        cnt = PyLegendIntegerLiteralExpression(count) if isinstance(count, int) else count.value()
        return PyLegendString(PyLegendStringLeftExpression(self.__value, cnt))

    def right(self, count: PyLegendUnion[int, "PyLegendInteger"]) -> "PyLegendString":
        PyLegendString.__validate_param_to_be_integer(count, "right parameter")
        cnt = PyLegendIntegerLiteralExpression(count) if isinstance(count, int) else count.value()
        return PyLegendString(PyLegendStringRightExpression(self.__value, cnt))

    def substring(self, start: PyLegendUnion[int, "PyLegendInteger"]) -> "PyLegendString":
        PyLegendString.__validate_param_to_be_integer(start, "substring parameter")
        strt = PyLegendIntegerLiteralExpression(start) if isinstance(start, int) else start.value()
        return PyLegendString(PyLegendStringSubStringExpression(self.__value, strt))

    def jaro_winkler_similarity(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendFloat":
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "JaroWinkler similarity parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendFloat(PyLegendStringJaroWinklerSimilarityExpression(self.__value, other_op))

    def levenshtein_distance(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendInteger":
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "JaroWinkler similarity parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendInteger(PyLegendStringLevenshteinDistanceExpression(self.__value, other_op))

    def replace(self,to_replace: PyLegendUnion[str, "PyLegendString"], replacement: PyLegendUnion[str , "PyLegendString"]) -> "PyLegendString":
        PyLegendString.__validate_param_to_be_str_or_str_expr(to_replace, "replace parameter")
        to_replace_op = PyLegendStringLiteralExpression(to_replace) if isinstance(to_replace, str) else to_replace.__value
        PyLegendString.__validate_param_to_be_str_or_str_expr(replacement, "replace parameter")
        replacement_op = PyLegendStringLiteralExpression(replacement) if isinstance(replacement,str) else replacement.__value
        return PyLegendString(PyLegendStringReplaceExpression([self.__value, to_replace_op,replacement_op]))

    def __add__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendString":
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String plus (+) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendString(PyLegendStringConcatExpression(self.__value, other_op))

    def __radd__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendString":
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String plus (+) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendString(PyLegendStringConcatExpression(other_op, self.__value))

    def __lt__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendBoolean":
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String less than (<) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendBoolean(PyLegendStringLessThanExpression(self.__value, other_op))

    def __le__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendBoolean":
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String less than equal (<=) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendBoolean(PyLegendStringLessThanEqualExpression(self.__value, other_op))

    def __gt__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendBoolean":
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String greater than (>) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendBoolean(PyLegendStringGreaterThanExpression(self.__value, other_op))

    def __ge__(self, other: PyLegendUnion[str, "PyLegendString"]) -> "PyLegendBoolean":
        PyLegendString.__validate_param_to_be_str_or_str_expr(other, "String greater than equal (>=) parameter")
        other_op = PyLegendStringLiteralExpression(other) if isinstance(other, str) else other.__value
        return PyLegendBoolean(PyLegendStringGreaterThanEqualExpression(self.__value, other_op))

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__value.to_sql_expression(frame_name_to_base_query_map, config)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return self.__value.to_pure_expression(config)

    def value(self) -> PyLegendExpressionStringReturn:
        return self.__value

    @staticmethod
    def __validate_param_to_be_str_or_str_expr(param: PyLegendUnion[str, "PyLegendString"], desc: str) -> None:
        if not isinstance(param, (str, PyLegendString)):
            raise TypeError(desc + " should be a str or a string expression (PyLegendString)."
                                   
                                   " Got value " + str(param) + " of type: " + str(type(param)))

    @staticmethod
    def __validate_param_to_be_str(param: str, desc: str) -> None:
        if not isinstance(param, str):
            raise TypeError(desc + " should be a str."
                                   " Got value " + str(param) + " of type: " + str(type(param)))

    @staticmethod
    def __validate_param_to_be_integer(param: int, desc: str) -> None:
        if not isinstance(param, int):
            raise TypeError(desc + " should be a int."
                                   " Got value " + str(param) + " of type: " + str(type(param)))
