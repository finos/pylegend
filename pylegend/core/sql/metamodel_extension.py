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

from enum import Enum
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
)
from pylegend.core.sql.metamodel import (
    Expression,
    Window,
)

__all__: PyLegendSequence[str] = [
    "StringLengthExpression",
    "StringLikeExpression",
    "StringUpperExpression",
    "StringLowerExpression",
    "TrimType",
    "StringTrimExpression",
    "StringPosExpression",
    "StringConcatExpression",
    "AbsoluteExpression",
    "PowerExpression",
    "CeilExpression",
    "FloorExpression",
    "SqrtExpression",
    "CbrtExpression",
    "ExpExpression",
    "LogExpression",
    "RemainderExpression",
    "RoundExpression",
    "SineExpression",
    "ArcSineExpression",
    "CosineExpression",
    "ArcCosineExpression",
    "TanExpression",
    "ArcTanExpression",
    "ArcTan2Expression",
    "CotExpression",
    "CountExpression",
    "DistinctCountExpression",
    "AverageExpression",
    "MaxExpression",
    "MinExpression",
    "SumExpression",
    "StdDevSampleExpression",
    "StdDevPopulationExpression",
    "VarianceSampleExpression",
    "VariancePopulationExpression",
    "JoinStringsExpression",
    "FirstDayOfYearExpression",
    "FirstDayOfQuarterExpression",
    "FirstDayOfMonthExpression",
    "FirstDayOfWeekExpression",
    "FirstHourOfDayExpression",
    "FirstMinuteOfHourExpression",
    "FirstSecondOfMinuteExpression",
    "FirstMillisecondOfSecondExpression",
    "YearExpression",
    "QuarterExpression",
    "MonthExpression",
    "WeekOfYearExpression",
    "DayOfYearExpression",
    "DayOfMonthExpression",
    "DayOfWeekExpression",
    "HourExpression",
    "MinuteExpression",
    "SecondExpression",
    "EpochExpression",
    "WindowExpression",
    "ConstantExpression",
]


class StringLengthExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression"
    ) -> None:
        super().__init__(_type="stringLengthExpression")
        self.value = value


class StringLikeExpression(Expression):
    value: "Expression"
    other: "Expression"

    def __init__(
        self,
        value: "Expression",
        other: "Expression"
    ) -> None:
        super().__init__(_type="stringLikeExpression")
        self.value = value
        self.other = other


class StringUpperExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression"
    ) -> None:
        super().__init__(_type="stringUpperExpression")
        self.value = value


class StringLowerExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression"
    ) -> None:
        super().__init__(_type="stringLowerExpression")
        self.value = value


class TrimType(Enum):
    Left = 1,
    Right = 2,
    Both = 3


class StringTrimExpression(Expression):
    value: "Expression"
    trim_type: TrimType

    def __init__(
        self,
        value: "Expression",
        trim_type: TrimType
    ) -> None:
        super().__init__(_type="stringTrimExpression")
        self.value = value
        self.trim_type = trim_type


class StringPosExpression(Expression):
    value: "Expression"
    other: "Expression"

    def __init__(
        self,
        value: "Expression",
        other: "Expression"
    ) -> None:
        super().__init__(_type="stringPosExpression")
        self.value = value
        self.other = other


class StringConcatExpression(Expression):
    first: "Expression"
    second: "Expression"

    def __init__(
        self,
        first: "Expression",
        second: "Expression"
    ) -> None:
        super().__init__(_type="stringConcatExpression")
        self.first = first
        self.second = second


class AbsoluteExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="absoluteExpression")
        self.value = value


class PowerExpression(Expression):
    first: "Expression"
    second: "Expression"

    def __init__(
        self,
        first: "Expression",
        second: "Expression"
    ) -> None:
        super().__init__(_type="powerExpression")
        self.first = first
        self.second = second


class CeilExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="ceilExpression")
        self.value = value


class FloorExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="floorExpression")
        self.value = value


class SqrtExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="sqrtExpression")
        self.value = value


class CbrtExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="cbrtExpression")
        self.value = value


class ExpExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="expExpression")
        self.value = value


class LogExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="logExpression")
        self.value = value


class RemainderExpression(Expression):
    first: "Expression"
    second: "Expression"

    def __init__(
        self,
        first: "Expression",
        second: "Expression"
    ) -> None:
        super().__init__(_type="remainderExpression")
        self.first = first
        self.second = second


class RoundExpression(Expression):
    first: "Expression"
    second: "PyLegendOptional[Expression]"

    def __init__(
        self,
        first: "Expression",
        second: "PyLegendOptional[Expression]"
    ) -> None:
        super().__init__(_type="roundExpression")
        self.first = first
        self.second = second


class SineExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="sineExpression")
        self.value = value


class ArcSineExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="arcSineExpression")
        self.value = value


class CosineExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="cosineExpression")
        self.value = value


class ArcCosineExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="arcCosineExpression")
        self.value = value


class TanExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="tanExpression")
        self.value = value


class ArcTanExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="arcTanExpression")
        self.value = value


class ArcTan2Expression(Expression):
    first: "Expression"
    second: "Expression"

    def __init__(
        self,
        first: "Expression",
        second: "Expression"
    ) -> None:
        super().__init__(_type="arcTan2Expression")
        self.first = first
        self.second = second


class CotExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="cotExpression")
        self.value = value


class CountExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="countExpression")
        self.value = value


class DistinctCountExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="distinctCountExpression")
        self.value = value


class AverageExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="averageExpression")
        self.value = value


class MaxExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="maxExpression")
        self.value = value


class MinExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="minExpression")
        self.value = value


class SumExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="sumExpression")
        self.value = value


class StdDevSampleExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="stdDevSampleExpression")
        self.value = value


class StdDevPopulationExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="stdDevPopulationExpression")
        self.value = value


class VarianceSampleExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="varianceSampleExpression")
        self.value = value


class VariancePopulationExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="variancePopulationExpression")
        self.value = value


class JoinStringsExpression(Expression):
    value: "Expression"
    other: "Expression"

    def __init__(
        self,
        value: "Expression",
        other: "Expression",
    ) -> None:
        super().__init__(_type="joinStringsExpression")
        self.value = value
        self.other = other


class FirstDayOfYearExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="firstDayOfYearExpression")
        self.value = value


class FirstDayOfQuarterExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="firstDayOfQuarterExpression")
        self.value = value


class FirstDayOfMonthExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="firstDayOfMonthExpression")
        self.value = value


class FirstDayOfWeekExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="firstDayOfWeekExpression")
        self.value = value


class FirstHourOfDayExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="firstHourOfDayExpression")
        self.value = value


class FirstMinuteOfHourExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="firstMinuteOfHourExpression")
        self.value = value


class FirstSecondOfMinuteExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="firstSecondOfMinuteExpression")
        self.value = value


class FirstMillisecondOfSecondExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="firstMillisecondOfSecondExpression")
        self.value = value


class YearExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="yearExpression")
        self.value = value


class QuarterExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="quarterExpression")
        self.value = value


class MonthExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="monthExpression")
        self.value = value


class WeekOfYearExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="weekOfYearExpression")
        self.value = value


class DayOfYearExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="dayOfYearExpression")
        self.value = value


class DayOfMonthExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="dayOfMonthExpression")
        self.value = value


class DayOfWeekExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="dayOfWeekExpression")
        self.value = value


class HourExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="hourExpression")
        self.value = value


class MinuteExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="minuteExpression")
        self.value = value


class SecondExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="secondExpression")
        self.value = value


class EpochExpression(Expression):
    value: "Expression"

    def __init__(
        self,
        value: "Expression",
    ) -> None:
        super().__init__(_type="epochExpression")
        self.value = value


class WindowExpression(Expression):
    nested: "Expression"
    window: "Window"

    def __init__(
        self,
        nested: "Expression",
        window: "Window",
    ) -> None:
        super().__init__(_type="windowExpression")
        self.nested = nested
        self.window = window


class ConstantExpression(Expression):
    name: str

    def __init__(
        self,
        name: str
    ) -> None:
        super().__init__(_type="constantExpression")
        self.name = name
