# Copyright 2025 Goldman Sachs
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
    PyLegendSequence
)
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.language import (
    PyLegendPrimitiveOrPythonPrimitive,
    PyLegendBoolean,
    PyLegendString,
    PyLegendNumber,
    PyLegendInteger,
    PyLegendFloat,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendStrictDate,
)

__all__: PyLegendSequence[str] = [
    "tds_column_for_primitive",
]


def tds_column_for_primitive(name: str, result: PyLegendPrimitiveOrPythonPrimitive) -> TdsColumn:
    if isinstance(result, (bool, PyLegendBoolean)):
        return PrimitiveTdsColumn.boolean_column(name)
    elif isinstance(result, (str, PyLegendString)):
        return PrimitiveTdsColumn.string_column(name)
    elif isinstance(result, (int, PyLegendInteger)):
        return PrimitiveTdsColumn.integer_column(name)
    elif isinstance(result, (float, PyLegendFloat)):
        return PrimitiveTdsColumn.float_column(name)
    elif isinstance(result, PyLegendNumber):
        return PrimitiveTdsColumn.number_column(name)
    elif isinstance(result, PyLegendDateTime):
        return PrimitiveTdsColumn.datetime_column(name)
    elif isinstance(result, PyLegendStrictDate):
        return PrimitiveTdsColumn.strictdate_column(name)
    elif isinstance(result, PyLegendDate):
        return PrimitiveTdsColumn.date_column(name)
    else:
        raise RuntimeError("Unhandled type: " + str(type(result)))  # pragma: no cover
