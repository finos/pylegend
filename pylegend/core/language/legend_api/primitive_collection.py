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

from abc import ABCMeta
from datetime import date, datetime
from pylegend._typing import (
    PyLegendSequence,
    PyLegendUnion,
)
from pylegend.core.language.legend_api.primitives.primitive import LegendApiPrimitiveOrPythonPrimitive
from pylegend.core.language import (
    LegendApiInteger,
    LegendApiFloat,
    LegendApiNumber,
    LegendApiString,
    LegendApiBoolean,
    LegendApiDate,
    LegendApiDateTime,
    LegendApiStrictDate,
    convert_literal_to_literal_expression,
)
from pylegend.core.language.shared.operations.collection_operation_expressions import (
    PyLegendCountExpression,
    PyLegendDistinctCountExpression,
    PyLegendAverageExpression,
    PyLegendIntegerMaxExpression,
    PyLegendIntegerMinExpression,
    PyLegendIntegerSumExpression,
    PyLegendFloatMaxExpression,
    PyLegendFloatMinExpression,
    PyLegendFloatSumExpression,
    PyLegendNumberMaxExpression,
    PyLegendNumberMinExpression,
    PyLegendNumberSumExpression,
    PyLegendStdDevSampleExpression,
    PyLegendStdDevPopulationExpression,
    PyLegendVarianceSampleExpression,
    PyLegendVariancePopulationExpression,
    PyLegendStringMaxExpression,
    PyLegendStringMinExpression,
    PyLegendJoinStringsExpression,
    PyLegendStrictDateMaxExpression,
    PyLegendStrictDateMinExpression,
    PyLegendDateMaxExpression,
    PyLegendDateMinExpression,
)


__all__: PyLegendSequence[str] = [
    "LegendApiPrimitiveCollection",
    "LegendApiIntegerCollection",
    "LegendApiFloatCollection",
    "LegendApiNumberCollection",
    "LegendApiStringCollection",
    "LegendApiBooleanCollection",
    "LegendApiDateCollection",
    "LegendApiDateTimeCollection",
    "LegendApiStrictDateCollection",
    "create_primitive_collection"
]


class LegendApiPrimitiveCollection(metaclass=ABCMeta):
    __nested: LegendApiPrimitiveOrPythonPrimitive

    def __init__(self, nested: LegendApiPrimitiveOrPythonPrimitive) -> None:
        self.__nested = nested

    def count(self) -> "LegendApiInteger":
        if isinstance(self.__nested, (bool, int, float, str, date, datetime)):
            nested_expr = convert_literal_to_literal_expression(self.__nested)
        else:
            nested_expr = self.__nested.value()
        return LegendApiInteger(PyLegendCountExpression(nested_expr))

    def distinct_count(self) -> "LegendApiInteger":
        if isinstance(self.__nested, (bool, int, float, str, date, datetime)):
            nested_expr = convert_literal_to_literal_expression(self.__nested)
        else:
            nested_expr = self.__nested.value()
        return LegendApiInteger(PyLegendDistinctCountExpression(nested_expr))


class LegendApiNumberCollection(LegendApiPrimitiveCollection):
    __nested: PyLegendUnion[int, float, LegendApiInteger, LegendApiFloat, LegendApiNumber]

    def __init__(self, nested: PyLegendUnion[int, float, LegendApiInteger, LegendApiFloat, LegendApiNumber]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def average(self) -> "LegendApiFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegendApiFloat(PyLegendAverageExpression(nested_expr))  # type: ignore

    def max(self) -> "LegendApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegendApiNumber(PyLegendNumberMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegendApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegendApiNumber(PyLegendNumberMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "LegendApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegendApiNumber(PyLegendNumberSumExpression(nested_expr))  # type: ignore

    def std_dev_sample(self) -> "LegendApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegendApiNumber(PyLegendStdDevSampleExpression(nested_expr))  # type: ignore

    def std_dev(self) -> "LegendApiNumber":
        return self.std_dev_sample()

    def std_dev_population(self) -> "LegendApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegendApiNumber(PyLegendStdDevPopulationExpression(nested_expr))  # type: ignore

    def variance_sample(self) -> "LegendApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegendApiNumber(PyLegendVarianceSampleExpression(nested_expr))  # type: ignore

    def variance(self) -> "LegendApiNumber":
        return self.variance_sample()

    def variance_population(self) -> "LegendApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegendApiNumber(PyLegendVariancePopulationExpression(nested_expr))  # type: ignore


class LegendApiIntegerCollection(LegendApiNumberCollection):
    __nested: PyLegendUnion[int, LegendApiInteger]

    def __init__(self, nested: PyLegendUnion[int, LegendApiInteger]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "LegendApiInteger":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return LegendApiInteger(PyLegendIntegerMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegendApiInteger":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return LegendApiInteger(PyLegendIntegerMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "LegendApiInteger":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return LegendApiInteger(PyLegendIntegerSumExpression(nested_expr))  # type: ignore


class LegendApiFloatCollection(LegendApiNumberCollection):
    __nested: PyLegendUnion[float, LegendApiFloat]

    def __init__(self, nested: PyLegendUnion[float, LegendApiFloat]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "LegendApiFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return LegendApiFloat(PyLegendFloatMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegendApiFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return LegendApiFloat(PyLegendFloatMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "LegendApiFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return LegendApiFloat(PyLegendFloatSumExpression(nested_expr))  # type: ignore


class LegendApiStringCollection(LegendApiPrimitiveCollection):
    __nested: PyLegendUnion[str, LegendApiString]

    def __init__(self, nested: PyLegendUnion[str, LegendApiString]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "LegendApiString":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        return LegendApiString(PyLegendStringMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegendApiString":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        return LegendApiString(PyLegendStringMinExpression(nested_expr))  # type: ignore

    def join(self, separator: str) -> "LegendApiString":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        separator_expr = convert_literal_to_literal_expression(separator)
        return LegendApiString(PyLegendJoinStringsExpression(nested_expr, separator_expr))  # type: ignore


class LegendApiBooleanCollection(LegendApiPrimitiveCollection):
    __nested: PyLegendUnion[bool, LegendApiBoolean]

    def __init__(self, nested: PyLegendUnion[bool, LegendApiBoolean]) -> None:
        super().__init__(nested)
        self.__nested = nested


class LegendApiDateCollection(LegendApiPrimitiveCollection):
    __nested: PyLegendUnion[date, datetime, LegendApiDate]

    def __init__(self, nested: PyLegendUnion[date, datetime, LegendApiDate]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "LegendApiDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (date, datetime))
            else self.__nested.value()
        )
        return LegendApiDate(PyLegendDateMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegendApiDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (date, datetime))
            else self.__nested.value()
        )
        return LegendApiDate(PyLegendDateMinExpression(nested_expr))  # type: ignore


class LegendApiDateTimeCollection(LegendApiDateCollection):
    __nested: PyLegendUnion[datetime, LegendApiDateTime]

    def __init__(self, nested: PyLegendUnion[datetime, LegendApiDateTime]) -> None:
        super().__init__(nested)
        self.__nested = nested


class LegendApiStrictDateCollection(LegendApiDateCollection):
    __nested: PyLegendUnion[date, LegendApiStrictDate]

    def __init__(self, nested: PyLegendUnion[date, LegendApiStrictDate]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "LegendApiStrictDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, date)
            else self.__nested.value()
        )
        return LegendApiStrictDate(PyLegendStrictDateMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegendApiStrictDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, date)
            else self.__nested.value()
        )
        return LegendApiStrictDate(PyLegendStrictDateMinExpression(nested_expr))  # type: ignore


def create_primitive_collection(nested: LegendApiPrimitiveOrPythonPrimitive) -> LegendApiPrimitiveCollection:
    if isinstance(nested, (int, LegendApiInteger)):
        return LegendApiIntegerCollection(nested)

    if isinstance(nested, (float, LegendApiFloat)):
        return LegendApiFloatCollection(nested)

    if isinstance(nested, LegendApiNumber):
        return LegendApiNumberCollection(nested)

    if isinstance(nested, (str, LegendApiString)):
        return LegendApiStringCollection(nested)

    if isinstance(nested, (bool, LegendApiBoolean)):
        return LegendApiBooleanCollection(nested)

    if isinstance(nested, (datetime, LegendApiDateTime)):
        return LegendApiDateTimeCollection(nested)

    if isinstance(nested, (date, LegendApiStrictDate)):
        return LegendApiStrictDateCollection(nested)

    if isinstance(nested, LegendApiDate):
        return LegendApiDateCollection(nested)

    raise RuntimeError(f"Not supported type - {type(nested)}")  # pragma: no cover
