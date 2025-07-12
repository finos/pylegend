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
from pylegend.core.language.legacy_api.primitives import (
    LegacyApiPrimitive,
    LegacyApiPrimitiveOrPythonPrimitive,
    LegacyApiBoolean,
    LegacyApiString,
    LegacyApiNumber,
    LegacyApiInteger,
    LegacyApiFloat,
    LegacyApiDate,
    LegacyApiDateTime,
    LegacyApiStrictDate,
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
from pylegend.core.language.legacy_api.column_expressions import (
    LegacyApiColumnExpression,
    LegacyApiBooleanColumnExpression,
    LegacyApiStringColumnExpression,
    LegacyApiNumberColumnExpression,
    LegacyApiIntegerColumnExpression,
    LegacyApiFloatColumnExpression,
    LegacyApiDateColumnExpression,
    LegacyApiDateTimeColumnExpression,
    LegacyApiStrictDateColumnExpression,
)
from pylegend.core.language.legacy_api.tds_row import LegacyApiTdsRow
from pylegend.core.language.legacy_api.aggregate_specification import LegacyApiAggregateSpecification, agg
from pylegend.core.language.legacy_api.primitive_collection import (
    LegacyApiPrimitiveCollection,
    LegacyApiIntegerCollection,
    LegacyApiFloatCollection,
    LegacyApiNumberCollection,
    LegacyApiStringCollection,
    LegacyApiBooleanCollection,
    LegacyApiDateCollection,
    LegacyApiDateTimeCollection,
    LegacyApiStrictDateCollection,
    create_primitive_collection,
)
from pylegend.core.language.legacy_api.functions import (
    today,
    now,
)

__all__: PyLegendSequence[str] = [
    "LegacyApiPrimitive",
    "LegacyApiPrimitiveOrPythonPrimitive",
    "LegacyApiBoolean",
    "LegacyApiString",
    "LegacyApiNumber",
    "LegacyApiInteger",
    "LegacyApiFloat",
    "LegacyApiDate",
    "LegacyApiDateTime",
    "LegacyApiStrictDate",

    "PyLegendExpressionBooleanReturn",
    "PyLegendExpressionStringReturn",
    "PyLegendExpressionNumberReturn",
    "PyLegendExpressionIntegerReturn",
    "PyLegendExpressionFloatReturn",
    "PyLegendExpressionDateReturn",
    "PyLegendExpressionDateTimeReturn",
    "PyLegendExpressionStrictDateReturn",

    "LegacyApiColumnExpression",
    "LegacyApiBooleanColumnExpression",
    "LegacyApiStringColumnExpression",
    "LegacyApiNumberColumnExpression",
    "LegacyApiIntegerColumnExpression",
    "LegacyApiFloatColumnExpression",
    "LegacyApiDateColumnExpression",
    "LegacyApiDateTimeColumnExpression",
    "LegacyApiStrictDateColumnExpression",

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

    "LegacyApiPrimitiveCollection",
    "LegacyApiIntegerCollection",
    "LegacyApiFloatCollection",
    "LegacyApiNumberCollection",
    "LegacyApiStringCollection",
    "LegacyApiBooleanCollection",
    "LegacyApiDateCollection",
    "LegacyApiDateTimeCollection",
    "LegacyApiStrictDateCollection",
    "create_primitive_collection",

    "today",
    "now",
]
