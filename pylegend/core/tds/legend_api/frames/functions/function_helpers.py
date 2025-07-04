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
    PyLegendSequence
)
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.language import (
    LegendApiPrimitiveOrPythonPrimitive,
    LegendApiBoolean,
    LegendApiString,
    LegendApiNumber,
    LegendApiInteger,
    LegendApiFloat,
    LegendApiDate,
    LegendApiDateTime,
    LegendApiStrictDate,
)

__all__: PyLegendSequence[str] = [
    "tds_column_for_primitive",
]


def tds_column_for_primitive(name: str, result: LegendApiPrimitiveOrPythonPrimitive) -> TdsColumn:
    if isinstance(result, (bool, LegendApiBoolean)):
        return PrimitiveTdsColumn.boolean_column(name)
    elif isinstance(result, (str, LegendApiString)):
        return PrimitiveTdsColumn.string_column(name)
    elif isinstance(result, (int, LegendApiInteger)):
        return PrimitiveTdsColumn.integer_column(name)
    elif isinstance(result, (float, LegendApiFloat)):
        return PrimitiveTdsColumn.float_column(name)
    elif isinstance(result, LegendApiNumber):
        return PrimitiveTdsColumn.number_column(name)
    elif isinstance(result, LegendApiDateTime):
        return PrimitiveTdsColumn.datetime_column(name)
    elif isinstance(result, LegendApiStrictDate):
        return PrimitiveTdsColumn.strictdate_column(name)
    elif isinstance(result, LegendApiDate):
        return PrimitiveTdsColumn.date_column(name)
    else:
        raise RuntimeError("Unhandled type: " + str(type(result)))  # pragma: no cover
