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
from decimal import Decimal as PythonDecimal
from datetime import date, datetime
from pylegend._typing import (
    PyLegendSequence,
    PyLegendUnion,
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.language import (
    PyLegendInteger,
    PyLegendFloat,
    PyLegendNumber,
    PyLegendString,
    PyLegendBoolean,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendStrictDate,
    PyLegendDecimal,
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
    PyLegendDecimalMaxExpression,
    PyLegendDecimalMinExpression,
    PyLegendDecimalSumExpression,
    PyLegendDecimalUniqueValueOnlyExpression,
    PyLegendStringMaxExpression,
    PyLegendStringMinExpression,
    PyLegendJoinStringsExpression,
    PyLegendStrictDateMaxExpression,
    PyLegendStrictDateMinExpression,
    PyLegendDateMaxExpression,
    PyLegendDateMinExpression,
    PyLegendIntegerUniqueValueOnlyExpression,
    PyLegendFloatUniqueValueOnlyExpression,
    PyLegendNumberUniqueValueOnlyExpression,
    PyLegendStringUniqueValueOnlyExpression,
    PyLegendStrictDateUniqueValueOnlyExpression,
    PyLegendDateUniqueValueOnlyExpression,
    PyLegendDateTimeUniqueValueOnlyExpression,
    PyLegendBooleanUniqueValueOnlyExpression,
    PyLegendCorrExpression,
    PyLegendCovarPopulationExpression,
    PyLegendCovarSampleExpression,
    PyLegendMedianExpression,
    PyLegendModeExpression,
    PyLegendPercentileContExpression,
    PyLegendPercentileDiscExpression,
)


__all__: PyLegendSequence[str] = [
    "PyLegendPrimitiveCollection",
    "PyLegendIntegerCollection",
    "PyLegendFloatCollection",
    "PyLegendNumberCollection",
    "PyLegendDecimalCollection",
    "PyLegendStringCollection",
    "PyLegendBooleanCollection",
    "PyLegendDateCollection",
    "PyLegendDateTimeCollection",
    "PyLegendStrictDateCollection",
    "PyLegendNumberPairCollection",
    "create_primitive_collection",
]


class PyLegendPrimitiveCollection(metaclass=ABCMeta):
    __nested: PyLegendPrimitiveOrPythonPrimitive

    def __init__(self, nested: PyLegendPrimitiveOrPythonPrimitive) -> None:
        self.__nested = nested

    def count(self) -> "PyLegendInteger":
        if isinstance(self.__nested, (bool, int, float, str, date, datetime, PythonDecimal)):
            nested_expr = convert_literal_to_literal_expression(self.__nested)
        else:
            nested_expr = self.__nested.value()
        return PyLegendInteger(PyLegendCountExpression(nested_expr))

    def distinct_count(self) -> "PyLegendInteger":
        if isinstance(self.__nested, (bool, int, float, str, date, datetime, PythonDecimal)):
            nested_expr = convert_literal_to_literal_expression(self.__nested)
        else:
            nested_expr = self.__nested.value()
        return PyLegendInteger(PyLegendDistinctCountExpression(nested_expr))


class PyLegendNumberCollection(PyLegendPrimitiveCollection):
    __nested: PyLegendUnion[int, float, PythonDecimal, PyLegendInteger, PyLegendFloat, PyLegendNumber, PyLegendDecimal]

    def __init__(
            self,
            nested: PyLegendUnion[int, float, PythonDecimal, PyLegendInteger, PyLegendFloat, PyLegendNumber, PyLegendDecimal]
    ) -> None:
        super().__init__(nested)
        self.__nested = nested

    def average(self) -> "PyLegendFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendFloat(PyLegendAverageExpression(nested_expr))  # type: ignore

    def mean(self) -> "PyLegendFloat":
        return self.average()  # pragma: no cover

    def max(self) -> "PyLegendNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendNumberMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendNumberMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "PyLegendNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendNumberSumExpression(nested_expr))  # type: ignore

    def std_dev_sample(self) -> "PyLegendNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendStdDevSampleExpression(nested_expr))  # type: ignore

    def std_dev(self) -> "PyLegendNumber":
        return self.std_dev_sample()

    def std_dev_population(self) -> "PyLegendNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendStdDevPopulationExpression(nested_expr))  # type: ignore

    def variance_sample(self) -> "PyLegendNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendVarianceSampleExpression(nested_expr))  # type: ignore

    def variance(self) -> "PyLegendNumber":
        return self.variance_sample()

    def variance_population(self) -> "PyLegendNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendVariancePopulationExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendNumberUniqueValueOnlyExpression(nested_expr))  # type: ignore

    def median(self) -> "PyLegendNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendMedianExpression(nested_expr))  # type: ignore

    def mode(self) -> "PyLegendNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        return PyLegendNumber(PyLegendModeExpression(nested_expr))  # type: ignore

    def percentile(
            self,
            percentile: float,
            ascending: bool = True,
            continuous: bool = True,
    ) -> "PyLegendNumber":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (int, float, PythonDecimal))
            else self.__nested.value()
        )
        if continuous:
            return PyLegendNumber(
                PyLegendPercentileContExpression(nested_expr, percentile, ascending)  # type: ignore
            )
        else:
            return PyLegendNumber(
                PyLegendPercentileDiscExpression(nested_expr, percentile, ascending)  # type: ignore
            )

    def row_mapper(
        self,
        other: PyLegendUnion[
            int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber, "PyLegendNumberCollection"
        ],
    ) -> "PyLegendNumberPairCollection":
        return PyLegendNumberPairCollection(self, other)


class PyLegendIntegerCollection(PyLegendNumberCollection):
    __nested: PyLegendUnion[int, PyLegendInteger]

    def __init__(self, nested: PyLegendUnion[int, PyLegendInteger]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendInteger":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return PyLegendInteger(PyLegendIntegerMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendInteger":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return PyLegendInteger(PyLegendIntegerMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "PyLegendInteger":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return PyLegendInteger(PyLegendIntegerSumExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendInteger":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, int)
            else self.__nested.value()
        )
        return PyLegendInteger(PyLegendIntegerUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendFloatCollection(PyLegendNumberCollection):
    __nested: PyLegendUnion[float, PyLegendFloat]

    def __init__(self, nested: PyLegendUnion[float, PyLegendFloat]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return PyLegendFloat(PyLegendFloatMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return PyLegendFloat(PyLegendFloatMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "PyLegendFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return PyLegendFloat(PyLegendFloatSumExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendFloat":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, float)
            else self.__nested.value()
        )
        return PyLegendFloat(PyLegendFloatUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendDecimalCollection(PyLegendNumberCollection):
    __nested: PyLegendUnion[PythonDecimal, PyLegendDecimal]

    def __init__(self, nested: PyLegendUnion[PythonDecimal, PyLegendDecimal]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendDecimal":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, PythonDecimal)
            else self.__nested.value()
        )
        return PyLegendDecimal(PyLegendDecimalMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendDecimal":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, PythonDecimal)
            else self.__nested.value()
        )
        return PyLegendDecimal(PyLegendDecimalMinExpression(nested_expr))  # type: ignore

    def sum(self) -> "PyLegendDecimal":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, PythonDecimal)
            else self.__nested.value()
        )
        return PyLegendDecimal(PyLegendDecimalSumExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendDecimal":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, PythonDecimal)
            else self.__nested.value()
        )
        return PyLegendDecimal(PyLegendDecimalUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendStringCollection(PyLegendPrimitiveCollection):
    __nested: PyLegendUnion[str, PyLegendString]

    def __init__(self, nested: PyLegendUnion[str, PyLegendString]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendString":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        return PyLegendString(PyLegendStringMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendString":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        return PyLegendString(PyLegendStringMinExpression(nested_expr))  # type: ignore

    def join(self, separator: str) -> "PyLegendString":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        separator_expr = convert_literal_to_literal_expression(separator)
        return PyLegendString(PyLegendJoinStringsExpression(nested_expr, separator_expr))  # type: ignore

    def join_strings(self, separator: str = ";") -> "PyLegendString":
        return self.join(separator=separator)

    def distinct_value(self) -> "PyLegendString":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, str)
            else self.__nested.value()
        )
        return PyLegendString(PyLegendStringUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendBooleanCollection(PyLegendPrimitiveCollection):
    __nested: PyLegendUnion[bool, PyLegendBoolean]

    def __init__(self, nested: PyLegendUnion[bool, PyLegendBoolean]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def distinct_value(self) -> "PyLegendBoolean":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, bool)
            else self.__nested.value()
        )
        return PyLegendBoolean(PyLegendBooleanUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendDateCollection(PyLegendPrimitiveCollection):
    __nested: PyLegendUnion[date, datetime, PyLegendDate]

    def __init__(self, nested: PyLegendUnion[date, datetime, PyLegendDate]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (date, datetime))
            else self.__nested.value()
        )
        return PyLegendDate(PyLegendDateMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (date, datetime))
            else self.__nested.value()
        )
        return PyLegendDate(PyLegendDateMinExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (date, datetime))
            else self.__nested.value()
        )
        return PyLegendDate(PyLegendDateUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendDateTimeCollection(PyLegendDateCollection):
    __nested: PyLegendUnion[datetime, PyLegendDateTime]

    def __init__(self, nested: PyLegendUnion[datetime, PyLegendDateTime]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def distinct_value(self) -> "PyLegendDateTime":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, datetime)
            else self.__nested.value()
        )
        return PyLegendDateTime(PyLegendDateTimeUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


class PyLegendStrictDateCollection(PyLegendDateCollection):
    __nested: PyLegendUnion[date, PyLegendStrictDate]

    def __init__(self, nested: PyLegendUnion[date, PyLegendStrictDate]) -> None:
        super().__init__(nested)
        self.__nested = nested

    def max(self) -> "PyLegendStrictDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, date)
            else self.__nested.value()
        )
        return PyLegendStrictDate(PyLegendStrictDateMaxExpression(nested_expr))  # type: ignore

    def min(self) -> "PyLegendStrictDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, date)
            else self.__nested.value()
        )
        return PyLegendStrictDate(PyLegendStrictDateMinExpression(nested_expr))  # type: ignore

    def distinct_value(self) -> "PyLegendStrictDate":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, date)
            else self.__nested.value()
        )
        return PyLegendStrictDate(PyLegendStrictDateUniqueValueOnlyExpression(nested_expr))  # type: ignore[arg-type]


def create_primitive_collection(nested: PyLegendPrimitiveOrPythonPrimitive) -> PyLegendPrimitiveCollection:
    if isinstance(nested, (int, PyLegendInteger)):
        return PyLegendIntegerCollection(nested)

    if isinstance(nested, (float, PyLegendFloat)):
        return PyLegendFloatCollection(nested)

    if isinstance(nested, (PythonDecimal, PyLegendDecimal)):
        return PyLegendDecimalCollection(nested)

    if isinstance(nested, PyLegendNumber):
        return PyLegendNumberCollection(nested)

    if isinstance(nested, (str, PyLegendString)):
        return PyLegendStringCollection(nested)

    if isinstance(nested, (bool, PyLegendBoolean)):
        return PyLegendBooleanCollection(nested)

    if isinstance(nested, (datetime, PyLegendDateTime)):
        return PyLegendDateTimeCollection(nested)

    if isinstance(nested, (date, PyLegendStrictDate)):
        return PyLegendStrictDateCollection(nested)

    if isinstance(nested, PyLegendDate):
        return PyLegendDateCollection(nested)

    raise RuntimeError(f"Not supported type - {type(nested)}")  # pragma: no cover


class PyLegendNumberPairCollection(PyLegendPrimitiveCollection):
    __nested_a: PyLegendUnion[int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber]
    __nested_b: PyLegendUnion[int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber]

    def __init__(
        self,
        nested_a: PyLegendUnion[
            int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber, "PyLegendNumberCollection"
        ],
        nested_b: PyLegendUnion[
            int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber, "PyLegendNumberCollection"
        ],
    ) -> None:
        resolved_a = self._resolve(nested_a)
        resolved_b = self._resolve(nested_b)
        super().__init__(resolved_a)
        self.__nested_a = resolved_a
        self.__nested_b = resolved_b

    @staticmethod
    def _resolve(
        val: PyLegendUnion[
            int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber, "PyLegendNumberCollection"
        ]
    ) -> PyLegendUnion[int, float, PyLegendInteger, PyLegendFloat, PyLegendDecimal, PyLegendNumber]:
        if isinstance(val, PyLegendPrimitiveCollection):
            return val._PyLegendPrimitiveCollection__nested  # type: ignore
        return val

    def corr(self) -> "PyLegendFloat":
        nested_expr_a = (
            convert_literal_to_literal_expression(self.__nested_a) if isinstance(self.__nested_a, (int, float, PythonDecimal))
            else self.__nested_a.value()
        )
        nested_expr_b = (
            convert_literal_to_literal_expression(self.__nested_b) if isinstance(self.__nested_b, (int, float, PythonDecimal))
            else self.__nested_b.value()
        )
        return PyLegendFloat(PyLegendCorrExpression(nested_expr_a, nested_expr_b))  # type: ignore

    def _get_nested_exprs(self):  # type: ignore
        nested_expr_a = (
            convert_literal_to_literal_expression(self.__nested_a) if isinstance(self.__nested_a, (int, float, PythonDecimal))
            else self.__nested_a.value()
        )
        nested_expr_b = (
            convert_literal_to_literal_expression(self.__nested_b) if isinstance(self.__nested_b, (int, float, PythonDecimal))
            else self.__nested_b.value()
        )
        return nested_expr_a, nested_expr_b

    def covar_population(self) -> "PyLegendFloat":
        nested_expr_a, nested_expr_b = self._get_nested_exprs()  # type: ignore
        return PyLegendFloat(PyLegendCovarPopulationExpression(nested_expr_a, nested_expr_b))

    def covar_sample(self) -> "PyLegendFloat":
        nested_expr_a, nested_expr_b = self._get_nested_exprs()  # type: ignore
        return PyLegendFloat(PyLegendCovarSampleExpression(nested_expr_a, nested_expr_b))
