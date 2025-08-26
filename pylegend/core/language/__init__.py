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
from pylegend.core.language.shared.primitives import (
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
from pylegend.core.language.shared.expression import (
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
)
from pylegend.core.language.shared.literal_expressions import (
    PyLegendBooleanLiteralExpression,
    PyLegendStringLiteralExpression,
    PyLegendIntegerLiteralExpression,
    PyLegendFloatLiteralExpression,
    PyLegendDateTimeLiteralExpression,
    PyLegendStrictDateLiteralExpression,
    convert_literal_to_literal_expression,
)
from pylegend.core.language.shared.column_expressions import (
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
from pylegend.core.language.legacy_api.legacy_api_tds_row import LegacyApiTdsRow
from pylegend.core.language.legacy_api.aggregate_specification import LegacyApiAggregateSpecification, agg
from pylegend.core.language.shared.primitive_collection import (
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
from pylegend.core.language.shared.functions import (
    today,
    now,
    current_user,
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

    "LegacyApiTdsRow",
    "LegacyApiAggregateSpecification",
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
    "current_user",
]
