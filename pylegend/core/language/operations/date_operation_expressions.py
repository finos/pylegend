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
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
)
from pylegend.core.language.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
)
from pylegend.core.sql.metamodel_extension import (
    FirstDayOfYearExpression,
    FirstDayOfQuarterExpression,
    FirstDayOfMonthExpression,
    FirstDayOfWeekExpression,
    FirstHourOfDayExpression,
    FirstMinuteOfHourExpression,
    FirstSecondOfMinuteExpression,
    FirstMillisecondOfSecondExpression,
)


__all__: PyLegendSequence[str] = [
    "PyLegendFirstDayOfYearExpression",
    "PyLegendFirstDayOfQuarterExpression",
    "PyLegendFirstDayOfMonthExpression",
    "PyLegendFirstDayOfWeekExpression",
    "PyLegendFirstHourOfDayExpression",
    "PyLegendFirstMinuteOfHourExpression",
    "PyLegendFirstSecondOfMinuteExpression",
    "PyLegendFirstMillisecondOfSecondExpression",
]


class PyLegendFirstDayOfYearExpression(PyLegendUnaryExpression, PyLegendExpressionDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstDayOfYearExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstDayOfYearExpression.__to_sql_func
        )


class PyLegendFirstDayOfQuarterExpression(PyLegendUnaryExpression, PyLegendExpressionDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstDayOfQuarterExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstDayOfQuarterExpression.__to_sql_func
        )


class PyLegendFirstDayOfMonthExpression(PyLegendUnaryExpression, PyLegendExpressionDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstDayOfMonthExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstDayOfMonthExpression.__to_sql_func
        )


class PyLegendFirstDayOfWeekExpression(PyLegendUnaryExpression, PyLegendExpressionDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstDayOfWeekExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstDayOfWeekExpression.__to_sql_func
        )


class PyLegendFirstHourOfDayExpression(PyLegendUnaryExpression, PyLegendExpressionDateTimeReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstHourOfDayExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstHourOfDayExpression.__to_sql_func
        )


class PyLegendFirstMinuteOfHourExpression(PyLegendUnaryExpression, PyLegendExpressionDateTimeReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstMinuteOfHourExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstMinuteOfHourExpression.__to_sql_func
        )


class PyLegendFirstSecondOfMinuteExpression(PyLegendUnaryExpression, PyLegendExpressionDateTimeReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstSecondOfMinuteExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstSecondOfMinuteExpression.__to_sql_func
        )


class PyLegendFirstMillisecondOfSecondExpression(PyLegendUnaryExpression, PyLegendExpressionDateTimeReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstMillisecondOfSecondExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstMillisecondOfSecondExpression.__to_sql_func
        )
