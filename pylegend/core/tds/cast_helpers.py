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
    PyLegendDict,
    PyLegendList,
    PyLegendSet,
)
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn, PrimitiveType

__all__: PyLegendSequence[str] = [
    "NUMERIC_TYPES",
    "STRING_TYPES",
    "DATE_TYPES",
    "BOOLEAN_TYPES",
    "ALLOWED_CAST_TARGETS",
    "PRIMITIVE_TYPE_TO_SQL_TYPE",
    "type_family",
    "is_cast_allowed",
    "validate_and_build_cast_columns",
]

NUMERIC_TYPES: PyLegendSet[PrimitiveType] = {
    PrimitiveType.Number, PrimitiveType.Integer, PrimitiveType.Float,
    PrimitiveType.Decimal, PrimitiveType.TinyInt, PrimitiveType.UTinyInt,
    PrimitiveType.SmallInt, PrimitiveType.USmallInt, PrimitiveType.Int,
    PrimitiveType.UInt, PrimitiveType.BigInt, PrimitiveType.UBigInt,
    PrimitiveType.Float4, PrimitiveType.Double, PrimitiveType.Numeric,
}

STRING_TYPES: PyLegendSet[PrimitiveType] = {
    PrimitiveType.String, PrimitiveType.Varchar,
}

DATE_TYPES: PyLegendSet[PrimitiveType] = {
    PrimitiveType.Date, PrimitiveType.DateTime,
    PrimitiveType.StrictDate, PrimitiveType.Timestamp,
}

BOOLEAN_TYPES: PyLegendSet[PrimitiveType] = {
    PrimitiveType.Boolean,
}

# Allowed cast: source family -> set of target families it can cast into.
# Key = frozenset id of the source family, Value = list of allowed target family sets.
ALLOWED_CAST_TARGETS: PyLegendDict[str, PyLegendList[PyLegendSet[PrimitiveType]]] = {
    "numeric": [NUMERIC_TYPES, STRING_TYPES],   # numeric -> numeric or string
    "string":  [STRING_TYPES],                   # string -> string only
    "date":    [DATE_TYPES],                     # date -> date only
    "boolean": [BOOLEAN_TYPES],                  # boolean -> boolean only
}

# Mapping from PrimitiveType to the SQL type name used in CAST expressions.
PRIMITIVE_TYPE_TO_SQL_TYPE: PyLegendDict[PrimitiveType, str] = {
    PrimitiveType.Boolean:    "BOOLEAN",
    PrimitiveType.StrictDate: "DATE",
    PrimitiveType.Number:     "NUMERIC",
    PrimitiveType.String:     "TEXT",
    PrimitiveType.LatestDate: "TIMESTAMP",
    PrimitiveType.Float:      "DOUBLE PRECISION",
    PrimitiveType.DateTime:   "TIMESTAMP",
    PrimitiveType.Date:       "DATE",
    PrimitiveType.Integer:    "INTEGER",
    PrimitiveType.Decimal:    "DECIMAL",
    PrimitiveType.TinyInt:    "SMALLINT",
    PrimitiveType.UTinyInt:   "SMALLINT",
    PrimitiveType.SmallInt:   "SMALLINT",
    PrimitiveType.USmallInt:  "INTEGER",
    PrimitiveType.Int:        "INTEGER",
    PrimitiveType.UInt:       "BIGINT",
    PrimitiveType.BigInt:     "BIGINT",
    PrimitiveType.UBigInt:    "BIGINT",
    PrimitiveType.Varchar:    "VARCHAR",
    PrimitiveType.Timestamp:  "TIMESTAMP",
    PrimitiveType.Float4:     "REAL",
    PrimitiveType.Double:     "DOUBLE PRECISION",
    PrimitiveType.Numeric:    "NUMERIC",
}


def type_family(t: PrimitiveType) -> str:
    if t in NUMERIC_TYPES:
        return "numeric"
    if t in STRING_TYPES:
        return "string"
    if t in DATE_TYPES:
        return "date"
    if t in BOOLEAN_TYPES:
        return "boolean"
    return "unknown"  # pragma: no cover


def is_cast_allowed(source: PrimitiveType, target: PrimitiveType) -> bool:
    family = type_family(source)
    allowed_target_sets = ALLOWED_CAST_TARGETS.get(family, [])
    return any(target in s for s in allowed_target_sets)


def validate_and_build_cast_columns(
        current_columns: PyLegendSequence[TdsColumn],
        column_type_map: PyLegendDict[str, PrimitiveType],
) -> PyLegendList[TdsColumn]:
    """Validate the cast request and return the new column list.

    Raises ValueError / TypeError on invalid requests.
    """
    current_col_names = {c.get_name() for c in current_columns}

    unknown_cols = set(column_type_map.keys()) - current_col_names
    if unknown_cols:
        raise ValueError(
            f"Column(s) not found in frame: {sorted(unknown_cols)}. "
            f"Available columns: {sorted(current_col_names)}"
        )

    new_columns: PyLegendList[TdsColumn] = []
    for col in current_columns:
        if col.get_name() in column_type_map:
            if not isinstance(col, PrimitiveTdsColumn):
                raise TypeError(
                    f"Cannot cast non-primitive column '{col.get_name()}' "
                    f"(type: {col.get_type()}). Only PrimitiveTdsColumn can be cast."
                )
            source_type = PrimitiveType[col.get_type()]
            target_type = column_type_map[col.get_name()]
            if not is_cast_allowed(source_type, target_type):
                source_family = type_family(source_type)
                allowed_families = sorted({
                    type_family(t)
                    for sets in ALLOWED_CAST_TARGETS.get(source_family, [])
                    for t in sets
                })
                raise ValueError(
                    f"Cannot cast column '{col.get_name()}' from {source_type.name} to {target_type.name}. "
                    f"{source_family.capitalize()} types can only be cast to: {allowed_families}"
                )
            new_columns.append(
                PrimitiveTdsColumn(col.get_name(), target_type)
            )
        else:
            new_columns.append(col.copy())
    return new_columns
