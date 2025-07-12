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
from pylegend.core.language.legacy_api.primitives.primitive import LegacyApiPrimitiveOrPythonPrimitive
from pylegend.core.language import (
    LegacyApiInteger,
    LegacyApiFloat,
    LegacyApiNumber,
    LegacyApiString,
    LegacyApiBoolean,
    LegacyApiDate,
    LegacyApiDateTime,
    LegacyApiStrictDate,
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
    "LegacyApiPrimitiveCollection",
    "LegacyApiIntegerCollection",
    "LegacyApiFloatCollection",
    "LegacyApiNumberCollection",
    "LegacyApiStringCollection",
    "LegacyApiBooleanCollection",
    "LegacyApiDateCollection",
    "LegacyApiDateTimeCollection",
    "LegacyApiStrictDateCollection",
    "create_primitive_collection"
]


class LegacyApiPrimitiveCollection(metaclass=ABCMeta):
    __nested: LegacyApiPrimitiveOrPythonPrimitive

    def __init__(self, nested: LegacyApiPrimitiveOrPythonPrimitive) -> None:
        self.__nested = nested

    def count(self) -> "LegacyApiInteger":
        if isinstance(self.__nested, (bool, int, float, str, date, datetime)):
            nested_expr = convert_literal_to_literal_expression(self.__nested)
        else:
            nested_expr = self.__nested.value()
        return LegacyApiInteger(PyLegendCountExpression(nested_expr))

    def distinct_count(self) -> "LegacyApiInteger":
        if isinstance(self.__nested, (bool, int, float, str, date, datetime)):
            nested_expr = convert_literal_to_literal_expression(self.__nested)
        else:
            nested_expr = self.__nested.value()
        return LegacyApiInteger(PyLegendDistinctCountExpression(nested_expr))


class LegacyApiNumberCollection(LegacyApiPrimitiveCollection):
    __nested: PyLegendUnion[int, float, LegacyApiInteger, LegacyApiFloat, LegacyApiNumber]

    def __init__(self, nested: PyLegendUnion[int, float, LegacyApiInteger, LegacyApiFloat, LegacyApiNumber]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def average(self) -> "LegacyApiFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegacyApiFloat(PyLegendAverageExpression(nested_expr))  # type: ignore

    def max(self) -> "LegacyApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegacyApiNumber(PyLegendNumberMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegacyApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegacyApiNumber(PyLegendNumberMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "LegacyApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegacyApiNumber(PyLegendNumberSumExpression(nested_expr))  # type: ignore

    def std_dev_sample(self) -> "LegacyApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegacyApiNumber(PyLegendStdDevSampleExpression(nested_expr))  # type: ignore

    def std_dev(self) -> "LegacyApiNumber":
        return self.std_dev_sample()

    def std_dev_population(self) -> "LegacyApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegacyApiNumber(PyLegendStdDevPopulationExpression(nested_expr))  # type: ignore

    def variance_sample(self) -> "LegacyApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegacyApiNumber(PyLegendVarianceSampleExpression(nested_expr))  # type: ignore

    def variance(self) -> "LegacyApiNumber":
        return self.variance_sample()

    def variance_population(self) -> "LegacyApiNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float))
            else self.__nested.value()
        )
        return LegacyApiNumber(PyLegendVariancePopulationExpression(nested_expr))  # type: ignore


class LegacyApiIntegerCollection(LegacyApiNumberCollection):
    __nested: PyLegendUnion[int, LegacyApiInteger]

    def __init__(self, nested: PyLegendUnion[int, LegacyApiInteger]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "LegacyApiInteger":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return LegacyApiInteger(PyLegendIntegerMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegacyApiInteger":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return LegacyApiInteger(PyLegendIntegerMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "LegacyApiInteger":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return LegacyApiInteger(PyLegendIntegerSumExpression(nested_expr))  # type: ignore


class LegacyApiFloatCollection(LegacyApiNumberCollection):
    __nested: PyLegendUnion[float, LegacyApiFloat]

    def __init__(self, nested: PyLegendUnion[float, LegacyApiFloat]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "LegacyApiFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return LegacyApiFloat(PyLegendFloatMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegacyApiFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return LegacyApiFloat(PyLegendFloatMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "LegacyApiFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return LegacyApiFloat(PyLegendFloatSumExpression(nested_expr))  # type: ignore


class LegacyApiStringCollection(LegacyApiPrimitiveCollection):
    __nested: PyLegendUnion[str, LegacyApiString]

    def __init__(self, nested: PyLegendUnion[str, LegacyApiString]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "LegacyApiString":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        return LegacyApiString(PyLegendStringMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegacyApiString":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        return LegacyApiString(PyLegendStringMinExpression(nested_expr))  # type: ignore

    def join(self, separator: str) -> "LegacyApiString":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        separator_expr = convert_literal_to_literal_expression(separator)
        return LegacyApiString(PyLegendJoinStringsExpression(nested_expr, separator_expr))  # type: ignore


class LegacyApiBooleanCollection(LegacyApiPrimitiveCollection):
    __nested: PyLegendUnion[bool, LegacyApiBoolean]

    def __init__(self, nested: PyLegendUnion[bool, LegacyApiBoolean]) -> None:
        super().__init__(nested)
        self.__nested = nested


class LegacyApiDateCollection(LegacyApiPrimitiveCollection):
    __nested: PyLegendUnion[date, datetime, LegacyApiDate]

    def __init__(self, nested: PyLegendUnion[date, datetime, LegacyApiDate]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "LegacyApiDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (date, datetime))
            else self.__nested.value()
        )
        return LegacyApiDate(PyLegendDateMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegacyApiDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (date, datetime))
            else self.__nested.value()
        )
        return LegacyApiDate(PyLegendDateMinExpression(nested_expr))  # type: ignore


class LegacyApiDateTimeCollection(LegacyApiDateCollection):
    __nested: PyLegendUnion[datetime, LegacyApiDateTime]

    def __init__(self, nested: PyLegendUnion[datetime, LegacyApiDateTime]) -> None:
        super().__init__(nested)
        self.__nested = nested


class LegacyApiStrictDateCollection(LegacyApiDateCollection):
    __nested: PyLegendUnion[date, LegacyApiStrictDate]

    def __init__(self, nested: PyLegendUnion[date, LegacyApiStrictDate]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "LegacyApiStrictDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, date)
            else self.__nested.value()
        )
        return LegacyApiStrictDate(PyLegendStrictDateMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "LegacyApiStrictDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, date)
            else self.__nested.value()
        )
        return LegacyApiStrictDate(PyLegendStrictDateMinExpression(nested_expr))  # type: ignore


def create_primitive_collection(nested: LegacyApiPrimitiveOrPythonPrimitive) -> LegacyApiPrimitiveCollection:
    if isinstance(nested, (int, LegacyApiInteger)):
        return LegacyApiIntegerCollection(nested)

    if isinstance(nested, (float, LegacyApiFloat)):
        return LegacyApiFloatCollection(nested)

    if isinstance(nested, LegacyApiNumber):
        return LegacyApiNumberCollection(nested)

    if isinstance(nested, (str, LegacyApiString)):
        return LegacyApiStringCollection(nested)

    if isinstance(nested, (bool, LegacyApiBoolean)):
        return LegacyApiBooleanCollection(nested)

    if isinstance(nested, (datetime, LegacyApiDateTime)):
        return LegacyApiDateTimeCollection(nested)

    if isinstance(nested, (date, LegacyApiStrictDate)):
        return LegacyApiStrictDateCollection(nested)

    if isinstance(nested, LegacyApiDate):
        return LegacyApiDateCollection(nested)

    raise RuntimeError(f"Not supported type - {type(nested)}")  # pragma: no cover
