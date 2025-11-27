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
)
from pylegend.core.language.shared.expression import (
    PyLegendExpressionStringReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionDateTimeReturn,
)
from pylegend.core.language.shared.expression import PyLegendExpression
from pylegend.core.language.shared.operations.binary_expression import PyLegendBinaryExpression
from pylegend.core.language.shared.operations.nullary_expression import PyLegendNullaryExpression
from pylegend.core.language.shared.operations.nary_expression import PyLegendNaryExpression
from pylegend.core.language.shared.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.language.shared.helpers import generate_pure_functional_call
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
    ColumnType,
    Cast,
    ComparisonOperator,
    ComparisonExpression,
    StringLiteral,
    FunctionCall,
    QualifiedName,
    IntegerLiteral
)
from pylegend.core.sql.metamodel_extension import (
    StringLengthExpression,
    StringLikeExpression,
    StringUpperExpression,
    StringLowerExpression,
    TrimType,
    StringTrimExpression,
    StringPosExpression,
    StringConcatExpression,
    ConstantExpression,
    StringSubStringExpression
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig

__all__: PyLegendSequence[str] = [
    "PyLegendStringLengthExpression",
    "PyLegendStringStartsWithExpression",
    "PyLegendStringEndsWithExpression",
    "PyLegendStringContainsExpression",
    "PyLegendStringUpperExpression",
    "PyLegendStringLowerExpression",
    "PyLegendStringLTrimExpression",
    "PyLegendStringRTrimExpression",
    "PyLegendStringBTrimExpression",
    "PyLegendStringPosExpression",
    "PyLegendStringParseIntExpression",
    "PyLegendStringParseFloatExpression",
    "PyLegendStringConcatExpression",
    "PyLegendStringLessThanExpression",
    "PyLegendStringLessThanEqualExpression",
    "PyLegendStringGreaterThanExpression",
    "PyLegendStringGreaterThanEqualExpression",
    "PyLegendCurrentUserExpression",
    "PyLegendStringParseBooleanExpression",
    "PyLegendStringParseDateTimeExpression",
    "PyLegendStringAsciiExpression",
    "PyLegendStringDecodeBase64Expression",
    "PyLegendStringEncodeBase64Expression",
    "PyLegendStringReverseExpression",
    "PyLegendStringToLowerFirstCharacterExpression",
    "PyLegendStringToUpperFirstCharacterExpression",
    "PyLegendStringLeftExpression",
    "PyLegendStringRightExpression",
    "PyLegendStringSubStringExpression",
    "PyLegendStringReplaceExpression",
    "PyLegendStringLpadExpression",
    "PyLegendStringRpadExpression",
    "PyLegendStringSplitPartExpression",
    "PyLegendStringRepeatStringExpression",
    "PyLegendStringFullMatchExpression",
    "PyLegendStringMatchExpression"
]


class PyLegendStringLengthExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return StringLengthExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("length", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringLengthExpression.__to_sql_func,
            PyLegendStringLengthExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringStartsWithExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        assert isinstance(expression2, StringLiteral)
        escaped = _escape_like_param(expression2.value)
        return StringLikeExpression(expression1, StringLiteral(value=escaped + "%", quoted=False))

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("startsWith", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringStartsWithExpression.__to_sql_func,
            PyLegendStringStartsWithExpression.__to_pure_func
        )


class PyLegendStringEndsWithExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        assert isinstance(expression2, StringLiteral)
        escaped = _escape_like_param(expression2.value)
        return StringLikeExpression(expression1, StringLiteral(value="%" + escaped, quoted=False))

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("endsWith", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringEndsWithExpression.__to_sql_func,
            PyLegendStringEndsWithExpression.__to_pure_func
        )


class PyLegendStringContainsExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        assert isinstance(expression2, StringLiteral)
        escaped = _escape_like_param(expression2.value)
        return StringLikeExpression(expression1, StringLiteral(value="%" + escaped + "%", quoted=False))

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("contains", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringContainsExpression.__to_sql_func,
            PyLegendStringContainsExpression.__to_pure_func
        )


class PyLegendStringUpperExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return StringUpperExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("toUpper", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringUpperExpression.__to_sql_func,
            PyLegendStringUpperExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringLowerExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return StringLowerExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("toLower", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringLowerExpression.__to_sql_func,
            PyLegendStringLowerExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringLTrimExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return StringTrimExpression(expression, trim_type=TrimType.Left)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("ltrim", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringLTrimExpression.__to_sql_func,
            PyLegendStringLTrimExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringRTrimExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return StringTrimExpression(expression, trim_type=TrimType.Right)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("rtrim", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringRTrimExpression.__to_sql_func,
            PyLegendStringRTrimExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringBTrimExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return StringTrimExpression(expression, trim_type=TrimType.Both)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("trim", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringBTrimExpression.__to_sql_func,
            PyLegendStringBTrimExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringPosExpression(PyLegendBinaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return StringPosExpression(expression1, expression2)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("indexOf", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringPosExpression.__to_sql_func,
            PyLegendStringPosExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendStringParseIntExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return Cast(expression, ColumnType(name="INTEGER", parameters=[]))

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("parseInteger", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringParseIntExpression.__to_sql_func,
            PyLegendStringParseIntExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringParseFloatExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return Cast(expression, ColumnType(name="DOUBLE PRECISION", parameters=[]))

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("parseFloat", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringParseFloatExpression.__to_sql_func,
            PyLegendStringParseFloatExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringParseBooleanExpression(PyLegendUnaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return Cast(expression, ColumnType(name="BOOLEAN", parameters=[]))

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("parseBoolean", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringParseBooleanExpression.__to_sql_func,
            PyLegendStringParseBooleanExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringParseDateTimeExpression(PyLegendUnaryExpression, PyLegendExpressionDateTimeReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return Cast(expression, ColumnType(name="TIMESTAMP", parameters=[]))

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("parseDate", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn):
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringParseDateTimeExpression.__to_sql_func,
            PyLegendStringParseDateTimeExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringDecodeBase64Expression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["CONVERT_FROM"]),
            distinct=False,
            arguments=[
                FunctionCall(
                    name=QualifiedName(parts=["DECODE"]),
                    distinct=False,
                    arguments=[expression, StringLiteral("BASE64", quoted=False)],
                    filter_=None, window=None
                ), StringLiteral("UTF8", quoted=False)
            ],
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("decodeBase64", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn):
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringDecodeBase64Expression.__to_sql_func,
            PyLegendStringDecodeBase64Expression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringEncodeBase64Expression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["ENCODE"]),
            distinct=False,
            arguments=[
                FunctionCall(
                    name=QualifiedName(parts=["CONVERT_TO"]),
                    distinct=False,
                    arguments=[expression, StringLiteral("UTF8", quoted=False)],
                    filter_=None, window=None
                ), StringLiteral("BASE64", quoted=False)
            ],
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("encodeBase64", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn):
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringEncodeBase64Expression.__to_sql_func,
            PyLegendStringEncodeBase64Expression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringAsciiExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["ASCII"]),
            distinct=False,
            arguments=[expression],
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("ascii", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringAsciiExpression.__to_sql_func,
            PyLegendStringAsciiExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringReverseExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["REVERSE"]),
            distinct=False,
            arguments=[expression],
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("reverseString", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringReverseExpression.__to_sql_func,
            PyLegendStringReverseExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringToLowerFirstCharacterExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["CONCAT"]),
            distinct=False,
            arguments=[
                FunctionCall(
                    name=QualifiedName(parts=["LOWER"]),
                    distinct=False,
                    arguments=[FunctionCall(
                        name=QualifiedName(parts=["LEFT"]),
                        distinct=False,
                        arguments=[expression, IntegerLiteral(1)],
                        filter_=None, window=None
                    )],
                    filter_=None, window=None
                ),
                FunctionCall(
                    name=QualifiedName(parts=["SUBSTR"]),
                    distinct=False,
                    arguments=[expression, IntegerLiteral(2)],
                    filter_=None, window=None
                )],
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("toLowerFirstCharacter", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringToLowerFirstCharacterExpression.__to_sql_func,
            PyLegendStringToLowerFirstCharacterExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringToUpperFirstCharacterExpression(PyLegendUnaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["CONCAT"]),
            distinct=False,
            arguments=[
                FunctionCall(
                    name=QualifiedName(parts=["UPPER"]),
                    distinct=False,
                    arguments=[FunctionCall(
                        name=QualifiedName(parts=["LEFT"]),
                        distinct=False,
                        arguments=[expression, IntegerLiteral(1)],
                        filter_=None, window=None
                    )],
                    filter_=None, window=None
                ),
                FunctionCall(
                    name=QualifiedName(parts=["SUBSTR"]),
                    distinct=False,
                    arguments=[expression, IntegerLiteral(2)],
                    filter_=None, window=None
                )],
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("toUpperFirstCharacter", [op_expr])

    def __init__(self, operand: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendStringToUpperFirstCharacterExpression.__to_sql_func,
            PyLegendStringToUpperFirstCharacterExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True,
        )


class PyLegendStringConcatExpression(PyLegendBinaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return StringConcatExpression(expression1, expression2)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} + {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringConcatExpression.__to_sql_func,
            PyLegendStringConcatExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendStringFullMatchExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ComparisonExpression(expression1, expression2, ComparisonOperator.LIKE)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("matches", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringFullMatchExpression.__to_sql_func,
            PyLegendStringFullMatchExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendStringMatchExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ComparisonExpression(expression1, expression2, ComparisonOperator.REGEX_MATCH)  # pragma: no cover

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("regexpLike", [op1_expr, op2_expr])  # pragma: no cover

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)  # pragma: no cover
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringMatchExpression.__to_sql_func,
            PyLegendStringMatchExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )  # pragma: no cover


class PyLegendStringRepeatStringExpression(PyLegendBinaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["REPEAT"]),
            distinct=False,
            arguments=[expression1, expression2],
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("repeatString", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringRepeatStringExpression.__to_sql_func,
            PyLegendStringRepeatStringExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendStringLessThanExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ComparisonExpression(expression1, expression2, ComparisonOperator.LESS_THAN)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} < {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringLessThanExpression.__to_sql_func,
            PyLegendStringLessThanExpression.__to_pure_func
        )


class PyLegendStringLessThanEqualExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ComparisonExpression(expression1, expression2, ComparisonOperator.LESS_THAN_OR_EQUAL)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} <= {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringLessThanEqualExpression.__to_sql_func,
            PyLegendStringLessThanEqualExpression.__to_pure_func
        )


class PyLegendStringGreaterThanExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ComparisonExpression(expression1, expression2, ComparisonOperator.GREATER_THAN)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} > {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringGreaterThanExpression.__to_sql_func,
            PyLegendStringGreaterThanExpression.__to_pure_func
        )


class PyLegendStringGreaterThanEqualExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ComparisonExpression(expression1, expression2, ComparisonOperator.GREATER_THAN_OR_EQUAL)

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return f"({op1_expr} >= {op2_expr})"

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionStringReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringGreaterThanEqualExpression.__to_sql_func,
            PyLegendStringGreaterThanEqualExpression.__to_pure_func
        )


class PyLegendStringLeftExpression(PyLegendBinaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["LEFT"]),
            distinct=False,
            arguments=[expression1, expression2],
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("left", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringLeftExpression.__to_sql_func,
            PyLegendStringLeftExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendStringRightExpression(PyLegendBinaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expression1: Expression,
            expression2: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["RIGHT"]),
            distinct=False,
            arguments=[expression1, expression2],
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op1_expr: str, op2_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("right", [op1_expr, op2_expr])

    def __init__(self, operand1: PyLegendExpressionStringReturn, operand2: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendStringRightExpression.__to_sql_func,
            PyLegendStringRightExpression.__to_pure_func,
            non_nullable=True,
            first_operand_needs_to_be_non_nullable=True,
            second_operand_needs_to_be_non_nullable=True
        )


class PyLegendStringSubStringExpression(PyLegendNaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expressions: list[Expression],
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        if len(expressions) > 2:
            return StringSubStringExpression(expressions[0], expressions[1], expressions[2])
        else:
            return StringSubStringExpression(expressions[0], expressions[1])

    @staticmethod
    def __to_pure_func(op_expr: list[str], config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("substring", op_expr)

    def __init__(self, operands: list[PyLegendExpression]) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendNaryExpression.__init__(
            self,
            operands,
            PyLegendStringSubStringExpression.__to_sql_func,
            PyLegendStringSubStringExpression.__to_pure_func,
            non_nullable=False,
            operands_non_nullable_flags=[True, True, False]
        )


class PyLegendStringReplaceExpression(PyLegendNaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expressions: list[Expression],
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["REPLACE"]),
            distinct=False,
            arguments=expressions,
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op_expr: list[str], config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("replace", op_expr)

    def __init__(self, operands: list[PyLegendExpression]) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendNaryExpression.__init__(
            self,
            operands,
            PyLegendStringReplaceExpression.__to_sql_func,
            PyLegendStringReplaceExpression.__to_pure_func,
            non_nullable=True,
            operands_non_nullable_flags=[True, True, True]
        )


class PyLegendStringLpadExpression(PyLegendNaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expressions: list[Expression],
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["LPAD"]),
            distinct=False,
            arguments=expressions,
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op_expr: list[str], config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("lpad", op_expr)

    def __init__(self, operands: list[PyLegendExpression]) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendNaryExpression.__init__(
            self,
            operands,
            PyLegendStringLpadExpression.__to_sql_func,
            PyLegendStringLpadExpression.__to_pure_func,
            non_nullable=True,
            operands_non_nullable_flags=[True, True, True]
        )


class PyLegendStringRpadExpression(PyLegendNaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expressions: list[Expression],
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["RPAD"]),
            distinct=False,
            arguments=expressions,
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op_expr: list[str], config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("rpad", op_expr)

    def __init__(self, operands: list[PyLegendExpression]) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendNaryExpression.__init__(
            self,
            operands,
            PyLegendStringRpadExpression.__to_sql_func,
            PyLegendStringRpadExpression.__to_pure_func,
            non_nullable=True,
            operands_non_nullable_flags=[True, True, True]
        )


class PyLegendStringSplitPartExpression(PyLegendNaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            expressions: list[Expression],
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["SPLIT_PART"]),
            distinct=False,
            arguments=expressions,
            filter_=None, window=None
        )

    @staticmethod
    def __to_pure_func(op_expr: list[str], config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("splitPart", op_expr)

    def __init__(self, operands: list[PyLegendExpression]) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendNaryExpression.__init__(
            self,
            operands,
            PyLegendStringSplitPartExpression.__to_sql_func,
            PyLegendStringSplitPartExpression.__to_pure_func,
            non_nullable=True,
            operands_non_nullable_flags=[True, True, True]
        )


class PyLegendCurrentUserExpression(PyLegendNullaryExpression, PyLegendExpressionStringReturn):

    @staticmethod
    def __to_sql_func(
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return ConstantExpression('CURRENT_USER')

    @staticmethod
    def __to_pure_func(config: FrameToPureConfig) -> str:
        return "currentUserId()"

    def __init__(self) -> None:
        PyLegendExpressionStringReturn.__init__(self)
        PyLegendNullaryExpression.__init__(
            self,
            PyLegendCurrentUserExpression.__to_sql_func,
            PyLegendCurrentUserExpression.__to_pure_func,
            non_nullable=True
        )


def _escape_like_param(param: str) -> str:
    return param.replace("_", "\\_").replace("%", "\\%")
