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
from pylegend._typing import (
    PyLegendSequence,
)
from pylegend.core.language.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive
from pylegend.core.language import (
    PyLegendInteger,
    PyLegendFloat,
    PyLegendNumber,
    PyLegendString,
    PyLegendBoolean,
    convert_literal_to_literal_expression,
)
from pylegend.core.language.operations.collection_operation_expressions import PyLegendCountExpression


__all__: PyLegendSequence[str] = [
    "PyLegendPrimitiveCollection",
    "PyLegendIntegerCollection",
    "PyLegendFloatCollection",
    "PyLegendNumberCollection",
    "PyLegendStringCollection",
    "PyLegendBooleanCollection",
    "create_primitive_collection"
]


class PyLegendPrimitiveCollection(metaclass=ABCMeta):
    __nested: PyLegendPrimitiveOrPythonPrimitive

    def __init__(self, nested: PyLegendPrimitiveOrPythonPrimitive) -> None:
        self.__nested = nested

    def count(self) -> "PyLegendInteger":
        nested_expr = (
            convert_literal_to_literal_expression(self.__nested) if isinstance(self.__nested, (bool, int, float, str))
            else self.__nested.value()
        )
        return PyLegendInteger(PyLegendCountExpression(nested_expr))


class PyLegendIntegerCollection(PyLegendPrimitiveCollection):
    def __init__(self, nested: PyLegendPrimitiveOrPythonPrimitive) -> None:
        super().__init__(nested)


class PyLegendFloatCollection(PyLegendPrimitiveCollection):
    def __init__(self, nested: PyLegendPrimitiveOrPythonPrimitive) -> None:
        super().__init__(nested)


class PyLegendNumberCollection(PyLegendPrimitiveCollection):
    def __init__(self, nested: PyLegendPrimitiveOrPythonPrimitive) -> None:
        super().__init__(nested)


class PyLegendStringCollection(PyLegendPrimitiveCollection):
    def __init__(self, nested: PyLegendPrimitiveOrPythonPrimitive) -> None:
        super().__init__(nested)


class PyLegendBooleanCollection(PyLegendPrimitiveCollection):
    def __init__(self, nested: PyLegendPrimitiveOrPythonPrimitive) -> None:
        super().__init__(nested)


def create_primitive_collection(nested: PyLegendPrimitiveOrPythonPrimitive) -> PyLegendPrimitiveCollection:
    if isinstance(nested, (int, PyLegendInteger)):
        return PyLegendIntegerCollection(nested)

    if isinstance(nested, (float, PyLegendFloat)):
        return PyLegendFloatCollection(nested)

    if isinstance(nested, PyLegendNumber):
        return PyLegendNumberCollection(nested)

    if isinstance(nested, (str, PyLegendString)):
        return PyLegendStringCollection(nested)

    if isinstance(nested, (bool, PyLegendBoolean)):
        return PyLegendBooleanCollection(nested)

    raise RuntimeError(f"Not supported type - {type(nested)}")  # pragma: no cover
