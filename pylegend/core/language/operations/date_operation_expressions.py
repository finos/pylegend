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
    PyLegendExpressionStrictDateReturn,
    PyLegendExpressionIntegerReturn,
)
from pylegend.core.language.operations.nullary_expression import PyLegendNullaryExpression
from pylegend.core.language.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
)
from pylegend.core.sql.metamodel import (
    CurrentTime,
    CurrentTimeType,
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
    YearExpression,
    QuarterExpression,
    MonthExpression,
    WeekOfYearExpression,
    DayOfYearExpression,
    DayOfMonthExpression,
    DayOfWeekExpression,
    HourExpression,
    MinuteExpression,
    SecondExpression,
    EpochExpression,
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
    "PyLegendYearExpression",
    "PyLegendQuarterExpression",
    "PyLegendMonthExpression",
    "PyLegendWeekOfYearExpression",
    "PyLegendDayOfYearExpression",
    "PyLegendDayOfMonthExpression",
    "PyLegendDayOfWeekExpression",
    "PyLegendHourExpression",
    "PyLegendMinuteExpression",
    "PyLegendSecondExpression",
    "PyLegendEpochExpression",
    "PyLegendTodayExpression",
    "PyLegendNowExpression",
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


class PyLegendYearExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return YearExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendYearExpression.__to_sql_func
        )


class PyLegendQuarterExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return QuarterExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendQuarterExpression.__to_sql_func
        )


class PyLegendMonthExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MonthExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendMonthExpression.__to_sql_func
        )


class PyLegendWeekOfYearExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return WeekOfYearExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendWeekOfYearExpression.__to_sql_func
        )


class PyLegendDayOfYearExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return DayOfYearExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDayOfYearExpression.__to_sql_func
        )


class PyLegendDayOfMonthExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return DayOfMonthExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDayOfMonthExpression.__to_sql_func
        )


class PyLegendDayOfWeekExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return DayOfWeekExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDayOfWeekExpression.__to_sql_func
        )


class PyLegendHourExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return HourExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendHourExpression.__to_sql_func
        )


class PyLegendMinuteExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MinuteExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendMinuteExpression.__to_sql_func
        )


class PyLegendSecondExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return SecondExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendSecondExpression.__to_sql_func
        )


class PyLegendEpochExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return EpochExpression(expression)

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendEpochExpression.__to_sql_func
        )


class PyLegendTodayExpression(PyLegendNullaryExpression, PyLegendExpressionStrictDateReturn):

    @staticmethod
    def __to_sql_func(
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return CurrentTime(type_=CurrentTimeType.DATE, precision=None)

    def __init__(self) -> None:
        PyLegendExpressionStrictDateReturn.__init__(self)
        PyLegendNullaryExpression.__init__(
            self,
            PyLegendTodayExpression.__to_sql_func
        )


class PyLegendNowExpression(PyLegendNullaryExpression, PyLegendExpressionDateTimeReturn):

    @staticmethod
    def __to_sql_func(
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return CurrentTime(type_=CurrentTimeType.TIMESTAMP, precision=None)

    def __init__(self) -> None:
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendNullaryExpression.__init__(
            self,
            PyLegendNowExpression.__to_sql_func
        )
