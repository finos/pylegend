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

from datetime import datetime, date
from pylegend._typing import (
    PyLegendSequence,
    PyLegendUnion,
)
from pylegend.core.language import (
    convert_literal_to_literal_expression,
    PyLegendBooleanLiteralExpression,
    PyLegendStringLiteralExpression,
    PyLegendIntegerLiteralExpression,
    PyLegendFloatLiteralExpression,
    PyLegendStrictDateLiteralExpression,
    PyLegendDateTimeLiteralExpression,
    PyLegendFloat,
    PyLegendDateTime,
    PyLegendStrictDate,
    PyLegendString,
    PyLegendInteger,
    PyLegendBoolean,
    PyLegendPrimitive
)

__all__: PyLegendSequence[str] = [
    "c",
]


def c(constant: PyLegendUnion[int, float, bool, str, datetime, date]) -> PyLegendPrimitive:
    expr = convert_literal_to_literal_expression(constant)
    if isinstance(expr, PyLegendBooleanLiteralExpression):
        return PyLegendBoolean(expr)
    if isinstance(expr, PyLegendStringLiteralExpression):
        return PyLegendString(expr)
    if isinstance(expr, PyLegendIntegerLiteralExpression):
        return PyLegendInteger(expr)
    if isinstance(expr, PyLegendFloatLiteralExpression):
        return PyLegendFloat(expr)
    if isinstance(expr, PyLegendDateTimeLiteralExpression):
        return PyLegendDateTime(expr)
    if isinstance(expr, PyLegendStrictDateLiteralExpression):
        return PyLegendStrictDate(expr)
    raise RuntimeError(f"Unsupported constant type: {type(constant)}")
