# Copyright 2026 Goldman Sachs
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
    PyLegendUnion,
    PyLegendTuple,
    PyLegendType,
)
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn, PrimitiveType
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.string import PyLegendString
from pylegend.core.language.shared.primitives.number import PyLegendNumber
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.primitives.float import PyLegendFloat
from pylegend.core.language.shared.primitives.decimal import PyLegendDecimal
from pylegend.core.language.shared.primitives.date import PyLegendDate
from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate
from pylegend.core.language.shared.primitives.precise_primitives import (
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

# Type that users pass as a cast target value:
#   - PrimitiveType.Integer                   (simple)
#   - (PrimitiveType.Numeric, 10, 2)          (with precision, scale)
#   - (PrimitiveType.Varchar, 200)            (with max length)
CastTarget = PyLegendUnion[PrimitiveType, PyLegendTuple[PrimitiveType, ...]]

__all__: PyLegendSequence[str] = [
    "CastTarget",
    "PRIMITIVE_TYPE_TO_PYLEGEND_CLASS",
    "is_cast_allowed",
    "validate_and_build_cast_columns",
    "pure_type_spec",
]

# Map each PrimitiveType enum to its corresponding PyLegend class.
# Cast validity is determined by whether the source and target classes
# have a subclass relationship (one is a subclass of the other).
PRIMITIVE_TYPE_TO_PYLEGEND_CLASS: PyLegendDict[PrimitiveType, PyLegendType[PyLegendPrimitive]] = {
    PrimitiveType.Boolean:    PyLegendBoolean,
    PrimitiveType.String:     PyLegendString,
    PrimitiveType.Number:     PyLegendNumber,
    PrimitiveType.Integer:    PyLegendInteger,
    PrimitiveType.Float:      PyLegendFloat,
    PrimitiveType.Decimal:    PyLegendDecimal,
    PrimitiveType.Date:       PyLegendDate,
    PrimitiveType.DateTime:   PyLegendDateTime,
    PrimitiveType.StrictDate: PyLegendStrictDate,
    PrimitiveType.TinyInt:    PyLegendTinyInt,
    PrimitiveType.UTinyInt:   PyLegendUTinyInt,
    PrimitiveType.SmallInt:   PyLegendSmallInt,
    PrimitiveType.USmallInt:  PyLegendUSmallInt,
    PrimitiveType.Int:        PyLegendInt,
    PrimitiveType.UInt:       PyLegendUInt,
    PrimitiveType.BigInt:     PyLegendBigInt,
    PrimitiveType.UBigInt:    PyLegendUBigInt,
    PrimitiveType.Varchar:    PyLegendVarchar,
    PrimitiveType.Timestamp:  PyLegendTimestamp,
    PrimitiveType.Float4:     PyLegendFloat4,
    PrimitiveType.Double:     PyLegendDouble,
    PrimitiveType.Numeric:    PyLegendNumeric,
}

# Types that require parameters in Pure syntax.
_PARAMETERIZED_TYPES: PyLegendSet[PrimitiveType] = {
    PrimitiveType.Numeric,
    PrimitiveType.Varchar,
}


def _normalize_target(target: CastTarget) -> PyLegendTuple[PrimitiveType, PyLegendTuple[int, ...]]:
    """Extract (PrimitiveType, params) from a CastTarget."""
    if isinstance(target, PrimitiveType):
        return target, ()
    ptype = target[0]
    params = tuple(target[1:])
    return ptype, params  # type: ignore


def pure_type_spec(ptype: PrimitiveType, params: PyLegendTuple[int, ...] = ()) -> str:
    """Return the Pure type string, e.g. 'Integer', 'Numeric(10, 2)', 'Varchar(200)'."""
    if params:
        param_str = ", ".join(str(p) for p in params)
        return f"{ptype.name}({param_str})"
    return ptype.name


def is_cast_allowed(source: PrimitiveType, target: PrimitiveType) -> bool:
    """A cast is allowed if the source and target PyLegend classes have
    a subclass relationship (one is a subclass of the other)."""
    source_cls = PRIMITIVE_TYPE_TO_PYLEGEND_CLASS.get(source)
    target_cls = PRIMITIVE_TYPE_TO_PYLEGEND_CLASS.get(target)
    if source_cls is None or target_cls is None:
        return False  # pragma: no cover
    return issubclass(source_cls, target_cls) or issubclass(target_cls, source_cls)


def validate_and_build_cast_columns(
        current_columns: PyLegendSequence[TdsColumn],
        column_type_map: PyLegendDict[str, CastTarget],
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
            target_type, params = _normalize_target(column_type_map[col.get_name()])
            if target_type in _PARAMETERIZED_TYPES and not params:
                raise ValueError(
                    f"Cast to {target_type.name} requires parameters. "
                    f"Use a tuple, e.g. (PrimitiveType.{target_type.name}, ...) "
                    f"instead of PrimitiveType.{target_type.name}"
                )
            if not is_cast_allowed(source_type, target_type):
                source_cls = PRIMITIVE_TYPE_TO_PYLEGEND_CLASS.get(source_type)
                target_cls = PRIMITIVE_TYPE_TO_PYLEGEND_CLASS.get(target_type)
                raise ValueError(
                    f"Cannot cast column '{col.get_name()}' from {source_type.name} to {target_type.name}. "
                    f"{source_cls.__name__ if source_cls else source_type.name} and "
                    f"{target_cls.__name__ if target_cls else target_type.name} "
                    f"do not share a subclass relationship."
                )
            new_columns.append(
                PrimitiveTdsColumn(col.get_name(), target_type)
            )
        else:
            new_columns.append(col.copy())
    return new_columns
