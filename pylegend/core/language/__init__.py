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
from pylegend.core.language.legend_api.primitives import (
    LegendApiPrimitive,
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
from pylegend.core.language.legend_api.column_expressions import (
    LegendApiColumnExpression,
    LegendApiBooleanColumnExpression,
    LegendApiStringColumnExpression,
    LegendApiNumberColumnExpression,
    LegendApiIntegerColumnExpression,
    LegendApiFloatColumnExpression,
    LegendApiDateColumnExpression,
    LegendApiDateTimeColumnExpression,
    LegendApiStrictDateColumnExpression,
)
from pylegend.core.language.legend_api.tds_row import LegendApiTdsRow
from pylegend.core.language.legend_api.aggregate_specification import LegendApiAggregateSpecification, agg
from pylegend.core.language.legend_api.primitive_collection import (
    LegendApiPrimitiveCollection,
    LegendApiIntegerCollection,
    LegendApiFloatCollection,
    LegendApiNumberCollection,
    LegendApiStringCollection,
    LegendApiBooleanCollection,
    LegendApiDateCollection,
    LegendApiDateTimeCollection,
    LegendApiStrictDateCollection,
    create_primitive_collection,
)
from pylegend.core.language.legend_api.functions import (
    today,
    now,
)

__all__: PyLegendSequence[str] = [
    "LegendApiPrimitive",
    "LegendApiPrimitiveOrPythonPrimitive",
    "LegendApiBoolean",
    "LegendApiString",
    "LegendApiNumber",
    "LegendApiInteger",
    "LegendApiFloat",
    "LegendApiDate",
    "LegendApiDateTime",
    "LegendApiStrictDate",

    "PyLegendExpressionBooleanReturn",
    "PyLegendExpressionStringReturn",
    "PyLegendExpressionNumberReturn",
    "PyLegendExpressionIntegerReturn",
    "PyLegendExpressionFloatReturn",
    "PyLegendExpressionDateReturn",
    "PyLegendExpressionDateTimeReturn",
    "PyLegendExpressionStrictDateReturn",

    "LegendApiColumnExpression",
    "LegendApiBooleanColumnExpression",
    "LegendApiStringColumnExpression",
    "LegendApiNumberColumnExpression",
    "LegendApiIntegerColumnExpression",
    "LegendApiFloatColumnExpression",
    "LegendApiDateColumnExpression",
    "LegendApiDateTimeColumnExpression",
    "LegendApiStrictDateColumnExpression",

    "PyLegendBooleanLiteralExpression",
    "PyLegendStringLiteralExpression",
    "PyLegendIntegerLiteralExpression",
    "PyLegendFloatLiteralExpression",
    "PyLegendDateTimeLiteralExpression",
    "PyLegendStrictDateLiteralExpression",
    "convert_literal_to_literal_expression",

    "LegendApiTdsRow",
    "LegendApiAggregateSpecification",
    "agg",

    "LegendApiPrimitiveCollection",
    "LegendApiIntegerCollection",
    "LegendApiFloatCollection",
    "LegendApiNumberCollection",
    "LegendApiStringCollection",
    "LegendApiBooleanCollection",
    "LegendApiDateCollection",
    "LegendApiDateTimeCollection",
    "LegendApiStrictDateCollection",
    "create_primitive_collection",

    "today",
    "now",
]
