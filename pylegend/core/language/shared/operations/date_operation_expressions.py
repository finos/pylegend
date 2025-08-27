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
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionBooleanReturn,
)
from pylegend.core.language.shared.operations.binary_expression import PyLegendBinaryExpression
from pylegend.core.language.shared.operations.nullary_expression import PyLegendNullaryExpression
from pylegend.core.language.shared.operations.unary_expression import PyLegendUnaryExpression
from pylegend.core.language.shared.helpers import generate_pure_functional_call
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
)
from pylegend.core.sql.metamodel import (
    CurrentTime,
    CurrentTimeType,
    Cast,
    ColumnType,
    ComparisonExpression,
    ComparisonOperator,
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
    "PyLegendDatePartExpression",
    "PyLegendDateLessThanExpression",
    "PyLegendDateLessThanEqualExpression",
    "PyLegendDateGreaterThanExpression",
    "PyLegendDateGreaterThanEqualExpression",
]


class PyLegendFirstDayOfYearExpression(PyLegendUnaryExpression, PyLegendExpressionDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstDayOfYearExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("firstDayOfYear", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstDayOfYearExpression.__to_sql_func,
            PyLegendFirstDayOfYearExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendFirstDayOfQuarterExpression(PyLegendUnaryExpression, PyLegendExpressionDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstDayOfQuarterExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("firstDayOfQuarter", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstDayOfQuarterExpression.__to_sql_func,
            PyLegendFirstDayOfQuarterExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendFirstDayOfMonthExpression(PyLegendUnaryExpression, PyLegendExpressionDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstDayOfMonthExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("firstDayOfMonth", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstDayOfMonthExpression.__to_sql_func,
            PyLegendFirstDayOfMonthExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendFirstDayOfWeekExpression(PyLegendUnaryExpression, PyLegendExpressionDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstDayOfWeekExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("firstDayOfWeek", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstDayOfWeekExpression.__to_sql_func,
            PyLegendFirstDayOfWeekExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendFirstHourOfDayExpression(PyLegendUnaryExpression, PyLegendExpressionDateTimeReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstHourOfDayExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("firstHourOfDay", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstHourOfDayExpression.__to_sql_func,
            PyLegendFirstHourOfDayExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendFirstMinuteOfHourExpression(PyLegendUnaryExpression, PyLegendExpressionDateTimeReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstMinuteOfHourExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("firstMinuteOfHour", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstMinuteOfHourExpression.__to_sql_func,
            PyLegendFirstMinuteOfHourExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendFirstSecondOfMinuteExpression(PyLegendUnaryExpression, PyLegendExpressionDateTimeReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstSecondOfMinuteExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("firstSecondOfMinute", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstSecondOfMinuteExpression.__to_sql_func,
            PyLegendFirstSecondOfMinuteExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendFirstMillisecondOfSecondExpression(PyLegendUnaryExpression, PyLegendExpressionDateTimeReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return FirstMillisecondOfSecondExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("firstMillisecondOfSecond", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendFirstMillisecondOfSecondExpression.__to_sql_func,
            PyLegendFirstMillisecondOfSecondExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendYearExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return YearExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("year", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendYearExpression.__to_sql_func,
            PyLegendYearExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendQuarterExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return QuarterExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("quarter", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendQuarterExpression.__to_sql_func,
            PyLegendQuarterExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendMonthExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MonthExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("month", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendMonthExpression.__to_sql_func,
            PyLegendMonthExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendWeekOfYearExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return WeekOfYearExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("weekOfYear", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendWeekOfYearExpression.__to_sql_func,
            PyLegendWeekOfYearExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendDayOfYearExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return DayOfYearExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("dayOfYear", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDayOfYearExpression.__to_sql_func,
            PyLegendDayOfYearExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendDayOfMonthExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return DayOfMonthExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("dayOfMonth", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDayOfMonthExpression.__to_sql_func,
            PyLegendDayOfMonthExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendDayOfWeekExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return DayOfWeekExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("dayOfWeekNumber", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDayOfWeekExpression.__to_sql_func,
            PyLegendDayOfWeekExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendHourExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return HourExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("hour", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendHourExpression.__to_sql_func,
            PyLegendHourExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendMinuteExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return MinuteExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("minute", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendMinuteExpression.__to_sql_func,
            PyLegendMinuteExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendSecondExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return SecondExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("second", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendSecondExpression.__to_sql_func,
            PyLegendSecondExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendEpochExpression(PyLegendUnaryExpression, PyLegendExpressionIntegerReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return EpochExpression(expression)

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call("toEpochValue", [op_expr])

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionIntegerReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendEpochExpression.__to_sql_func,
            PyLegendEpochExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendTodayExpression(PyLegendNullaryExpression, PyLegendExpressionStrictDateReturn):

    @staticmethod
    def __to_sql_func(
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return CurrentTime(type_=CurrentTimeType.DATE, precision=None)

    @staticmethod
    def __to_pure_func(config: FrameToPureConfig) -> str:
        return "today()"

    def __init__(self) -> None:
        PyLegendExpressionStrictDateReturn.__init__(self)
        PyLegendNullaryExpression.__init__(
            self,
            PyLegendTodayExpression.__to_sql_func,
            PyLegendTodayExpression.__to_pure_func,
            non_nullable=True
        )


class PyLegendNowExpression(PyLegendNullaryExpression, PyLegendExpressionDateTimeReturn):

    @staticmethod
    def __to_sql_func(
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return CurrentTime(type_=CurrentTimeType.TIMESTAMP, precision=None)

    @staticmethod
    def __to_pure_func(config: FrameToPureConfig) -> str:
        return "now()"

    def __init__(self) -> None:
        PyLegendExpressionDateTimeReturn.__init__(self)
        PyLegendNullaryExpression.__init__(
            self,
            PyLegendNowExpression.__to_sql_func,
            PyLegendNowExpression.__to_pure_func,
            non_nullable=True
        )


class PyLegendDatePartExpression(PyLegendUnaryExpression, PyLegendExpressionStrictDateReturn):

    @staticmethod
    def __to_sql_func(
            expression: Expression,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return Cast(expression, ColumnType(name="DATE", parameters=[]))

    @staticmethod
    def __to_pure_func(op_expr: str, config: FrameToPureConfig) -> str:
        return generate_pure_functional_call(
            "cast",
            [
                generate_pure_functional_call("datePart", [op_expr]),
                "@StrictDate",
            ]
        )

    def __init__(self, operand: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionStrictDateReturn.__init__(self)
        PyLegendUnaryExpression.__init__(
            self,
            operand,
            PyLegendDatePartExpression.__to_sql_func,
            PyLegendDatePartExpression.__to_pure_func,
            non_nullable=True,
            operand_needs_to_be_non_nullable=True
        )


class PyLegendDateLessThanExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

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

    def __init__(self, operand1: PyLegendExpressionDateReturn, operand2: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendDateLessThanExpression.__to_sql_func,
            PyLegendDateLessThanExpression.__to_pure_func
        )

    def is_non_nullable(self) -> bool:
        return True


class PyLegendDateLessThanEqualExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

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

    def __init__(self, operand1: PyLegendExpressionDateReturn, operand2: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendDateLessThanEqualExpression.__to_sql_func,
            PyLegendDateLessThanEqualExpression.__to_pure_func
        )

    def is_non_nullable(self) -> bool:
        return True


class PyLegendDateGreaterThanExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

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

    def __init__(self, operand1: PyLegendExpressionDateReturn, operand2: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendDateGreaterThanExpression.__to_sql_func,
            PyLegendDateGreaterThanExpression.__to_pure_func
        )

    def is_non_nullable(self) -> bool:
        return True


class PyLegendDateGreaterThanEqualExpression(PyLegendBinaryExpression, PyLegendExpressionBooleanReturn):

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

    def __init__(self, operand1: PyLegendExpressionDateReturn, operand2: PyLegendExpressionDateReturn) -> None:
        PyLegendExpressionBooleanReturn.__init__(self)
        PyLegendBinaryExpression.__init__(
            self,
            operand1,
            operand2,
            PyLegendDateGreaterThanEqualExpression.__to_sql_func,
            PyLegendDateGreaterThanEqualExpression.__to_pure_func
        )

    def is_non_nullable(self) -> bool:
        return True
