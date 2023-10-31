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
)
from pylegend.core.language.primitives import (
    PyLegendPrimitive,
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
from pylegend.core.language.expression import (
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
)
from pylegend.core.language.literal_expressions import (
    PyLegendBooleanLiteralExpression,
    PyLegendStringLiteralExpression,
    PyLegendIntegerLiteralExpression,
    PyLegendFloatLiteralExpression,
    PyLegendDateTimeLiteralExpression,
    PyLegendStrictDateLiteralExpression,
    convert_literal_to_literal_expression,
)
from pylegend.core.language.column_expressions import (
    PyLegendColumnExpression,
    PyLegendBooleanColumnExpression,
    PyLegendStringColumnExpression,
    PyLegendNumberColumnExpression,
    PyLegendIntegerColumnExpression,
    PyLegendFloatColumnExpression,
    PyLegendDateColumnExpression,
    PyLegendDateTimeColumnExpression,
    PyLegendStrictDateColumnExpression,
)
from pylegend.core.language.tds_row import TdsRow
from pylegend.core.language.aggregate_specification import AggregateSpecification, agg
from pylegend.core.language.primitive_collection import (
    PyLegendPrimitiveCollection,
    PyLegendIntegerCollection,
    PyLegendFloatCollection,
    PyLegendNumberCollection,
    PyLegendStringCollection,
    PyLegendBooleanCollection,
    PyLegendDateCollection,
    PyLegendDateTimeCollection,
    PyLegendStrictDateCollection,
    create_primitive_collection,
)
from pylegend.core.language.functions import (
    today,
    now,
)

__all__: PyLegendSequence[str] = [
    "PyLegendPrimitive",
    "PyLegendPrimitiveOrPythonPrimitive",
    "PyLegendBoolean",
    "PyLegendString",
    "PyLegendNumber",
    "PyLegendInteger",
    "PyLegendFloat",
    "PyLegendDate",
    "PyLegendDateTime",
    "PyLegendStrictDate",

    "PyLegendExpressionBooleanReturn",
    "PyLegendExpressionStringReturn",
    "PyLegendExpressionNumberReturn",
    "PyLegendExpressionIntegerReturn",
    "PyLegendExpressionFloatReturn",
    "PyLegendExpressionDateReturn",
    "PyLegendExpressionDateTimeReturn",
    "PyLegendExpressionStrictDateReturn",

    "PyLegendColumnExpression",
    "PyLegendBooleanColumnExpression",
    "PyLegendStringColumnExpression",
    "PyLegendNumberColumnExpression",
    "PyLegendIntegerColumnExpression",
    "PyLegendFloatColumnExpression",
    "PyLegendDateColumnExpression",
    "PyLegendDateTimeColumnExpression",
    "PyLegendStrictDateColumnExpression",

    "PyLegendBooleanLiteralExpression",
    "PyLegendStringLiteralExpression",
    "PyLegendIntegerLiteralExpression",
    "PyLegendFloatLiteralExpression",
    "PyLegendDateTimeLiteralExpression",
    "PyLegendStrictDateLiteralExpression",
    "convert_literal_to_literal_expression",

    "TdsRow",
    "AggregateSpecification",
    "agg",

    "PyLegendPrimitiveCollection",
    "PyLegendIntegerCollection",
    "PyLegendFloatCollection",
    "PyLegendNumberCollection",
    "PyLegendStringCollection",
    "PyLegendBooleanCollection",
    "PyLegendDateCollection",
    "PyLegendDateTimeCollection",
    "PyLegendStrictDateCollection",
    "create_primitive_collection",

    "today",
    "now",
]
