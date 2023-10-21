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
from pylegend.core.language.expression import (
    PyLegendExpression,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionNumberReturn,
)
from pylegend.core.language.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
    FunctionCall,
    QualifiedName,
)


__all__: PyLegendSequence[str] = [
    "PyLegendCountExpression",
    "PyLegendAverageExpression",
    "PyLegendIntegerMaxExpression",
    "PyLegendIntegerMinExpression",
    "PyLegendFloatMaxExpression",
    "PyLegendFloatMinExpression",
    "PyLegendNumberMaxExpression",
]


class PyLegendCountExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["COUNT"]),
            arguments=[expression],
            distinct=False,
            filter_=None,
            window=None
        )

    def __init__(self, operand: PyLegendExpression) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendCountExpression.__to_sql_func
        )


class PyLegendAverageExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["AVG"]),
            arguments=[expression],
            distinct=False,
            filter_=None,
            window=None
        )

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendAverageExpression.__to_sql_func
        )


class PyLegendIntegerMaxExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["MAX"]),
            arguments=[expression],
            distinct=False,
            filter_=None,
            window=None
        )

    def __init__(self, operand: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIntegerMaxExpression.__to_sql_func
        )


class PyLegendIntegerMinExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["MIN"]),
            arguments=[expression],
            distinct=False,
            filter_=None,
            window=None
        )

    def __init__(self, operand: PyLegendExpressionIntegerReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendIntegerMinExpression.__to_sql_func
        )


class PyLegendFloatMaxExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["MAX"]),
            arguments=[expression],
            distinct=False,
            filter_=None,
            window=None
        )

    def __init__(self, operand: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFloatMaxExpression.__to_sql_func
        )


class PyLegendFloatMinExpression(PyLegendUnaryExpression, PyLegendExpressionFloatReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["MIN"]),
            arguments=[expression],
            distinct=False,
            filter_=None,
            window=None
        )

    def __init__(self, operand: PyLegendExpressionFloatReturn) -> None:
        PyLegendExpressionFloatReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFloatMinExpression.__to_sql_func
        )


class PyLegendNumberMaxExpression(PyLegendUnaryExpression, PyLegendExpressionNumberReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FunctionCall(
            name=QualifiedName(parts=["MAX"]),
            arguments=[expression],
            distinct=False,
            filter_=None,
            window=None
        )

    def __init__(self, operand: PyLegendExpressionNumberReturn) -> None:
        PyLegendExpressionNumberReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendNumberMaxExpression.__to_sql_func
        )
