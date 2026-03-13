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
    PyLegendDecimal,
    PyLegendDate,
    PyLegendDateTime,
    PyLegendStrictDate,
    PyLegendTinyInt,
    PyLegendUTinyInt,
    PyLegendSmallInt,
    PyLegendUSmallInt,
    PyLegendInt,
    PyLegendUInt,
    PyLegendBigInt,
    PyLegendUBigInt,
    PyLegendVarchar,
    PyLegendTimestamp,
    PyLegendFloat4,
    PyLegendDouble,
    PyLegendNumeric,
)
from pylegend.core.language.shared.expression import (
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDecimalReturn,
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
)
from pylegend.core.language.shared.literal_expressions import (
    PyLegendBooleanLiteralExpression,
    PyLegendStringLiteralExpression,
    PyLegendIntegerLiteralExpression,
    PyLegendFloatLiteralExpression,
    PyLegendDecimalLiteralExpression,
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
    PyLegendDecimalColumnExpression,
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
    PyLegendDecimalCollection,
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
from pylegend.core.language.shared.variable_expressions import (
    PyLegendVariableExpression,
    PyLegendBooleanVariableExpression,
    PyLegendStringVariableExpression,
    PyLegendNumberVariableExpression,
    PyLegendIntegerVariableExpression,
    PyLegendFloatVariableExpression,
    PyLegendDateVariableExpression,
    PyLegendStrictDateVariableExpression,
    PyLegendDateTimeVariableExpression,
)
from pylegend.core.language.legacy_api.legacy_api_custom_expressions import (
    LegacyApiOLAPGroupByOperation,
    LegacyApiOLAPAggregation,
    LegacyApiOLAPRank,
    olap_agg,
    olap_rank,
)
from pylegend.core.language.shared.operations.date_operation_expressions import DurationUnit

__all__: PyLegendSequence[str] = [
    "PyLegendPrimitive",
    "PyLegendPrimitiveOrPythonPrimitive",
    "PyLegendBoolean",
    "PyLegendString",
    "PyLegendNumber",
    "PyLegendInteger",
    "PyLegendFloat",
    "PyLegendDecimal",
    "PyLegendDate",
    "PyLegendDateTime",
    "PyLegendStrictDate",
    "PyLegendTinyInt",
    "PyLegendUTinyInt",
    "PyLegendSmallInt",
    "PyLegendUSmallInt",
    "PyLegendInt",
    "PyLegendUInt",
    "PyLegendBigInt",
    "PyLegendUBigInt",
    "PyLegendVarchar",
    "PyLegendTimestamp",
    "PyLegendFloat4",
    "PyLegendDouble",
    "PyLegendNumeric",

    "PyLegendExpressionBooleanReturn",
    "PyLegendExpressionStringReturn",
    "PyLegendExpressionNumberReturn",
    "PyLegendExpressionIntegerReturn",
    "PyLegendExpressionFloatReturn",
    "PyLegendExpressionDecimalReturn",
    "PyLegendExpressionDateReturn",
    "PyLegendExpressionDateTimeReturn",
    "PyLegendExpressionStrictDateReturn",

    "PyLegendColumnExpression",
    "PyLegendBooleanColumnExpression",
    "PyLegendStringColumnExpression",
    "PyLegendNumberColumnExpression",
    "PyLegendIntegerColumnExpression",
    "PyLegendFloatColumnExpression",
    "PyLegendDecimalColumnExpression",
    "PyLegendDateColumnExpression",
    "PyLegendDateTimeColumnExpression",
    "PyLegendStrictDateColumnExpression",
    "DurationUnit"

    "PyLegendBooleanLiteralExpression",
    "PyLegendStringLiteralExpression",
    "PyLegendIntegerLiteralExpression",
    "PyLegendFloatLiteralExpression",
    "PyLegendDecimalLiteralExpression",
    "PyLegendDateTimeLiteralExpression",
    "PyLegendStrictDateLiteralExpression",
    "convert_literal_to_literal_expression",

    "LegacyApiTdsRow",
    "LegacyApiAggregateSpecification",
    "agg",

    "LegacyApiOLAPGroupByOperation",
    "LegacyApiOLAPAggregation",
    "LegacyApiOLAPRank",
    "olap_agg",
    "olap_rank",

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
    "create_primitive_collection",

    "today",
    "now",
    "current_user",

    "PyLegendVariableExpression",
    "PyLegendBooleanVariableExpression",
    "PyLegendStringVariableExpression",
    "PyLegendNumberVariableExpression",
    "PyLegendIntegerVariableExpression",
    "PyLegendFloatVariableExpression",
    "PyLegendDateVariableExpression",
    "PyLegendDateTimeVariableExpression",
    "PyLegendStrictDateVariableExpression",
]
