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

"""Tests for the three new primitive types added by Issue #220:
Time, Variant, and Binary.
"""

from pylegend.core.tds.tds_column import PrimitiveType, PrimitiveTdsColumn, tds_columns_from_json
from pylegend.core.language.shared.primitives import (
    PyLegendTime,
    PyLegendVariant,
    PyLegendBinary,
)
from pylegend.core.language import type_factory


class TestPrimitiveTypeEnum:
    def test_time_in_enum(self) -> None:
        assert PrimitiveType.Time is not None
        assert PrimitiveType.Time.name == "Time"

    def test_variant_in_enum(self) -> None:
        assert PrimitiveType.Variant is not None
        assert PrimitiveType.Variant.name == "Variant"

    def test_binary_in_enum(self) -> None:
        assert PrimitiveType.Binary is not None
        assert PrimitiveType.Binary.name == "Binary"

    def test_no_enum_value_collision(self) -> None:
        values = [t.value for t in PrimitiveType]
        assert len(values) == len(set(values)), "Duplicate enum values detected"


class TestPrimitiveTdsColumnFactoryMethods:
    def test_time_column_factory(self) -> None:
        col = PrimitiveTdsColumn.time_column("arrival_time")
        assert col.get_name() == "arrival_time"
        assert col.get_type() == "Time"

    def test_variant_column_factory(self) -> None:
        col = PrimitiveTdsColumn.variant_column("metadata")
        assert col.get_name() == "metadata"
        assert col.get_type() == "Variant"

    def test_binary_column_factory(self) -> None:
        col = PrimitiveTdsColumn.binary_column("payload")
        assert col.get_name() == "payload"
        assert col.get_type() == "Binary"

    def test_time_column_copy(self) -> None:
        col = PrimitiveTdsColumn.time_column("ts")
        copy = col.copy()
        assert copy.get_type() == "Time"
        assert copy.get_name() == "ts"

    def test_time_column_copy_with_changed_name(self) -> None:
        col = PrimitiveTdsColumn.time_column("old")
        renamed = col.copy_with_changed_name("new")
        assert renamed.get_name() == "new"
        assert renamed.get_type() == "Time"


class TestTypeFactory:
    def test_time_cast_target(self) -> None:
        target = type_factory.time()
        assert target == PrimitiveType.Time

    def test_variant_cast_target(self) -> None:
        target = type_factory.variant()
        assert target == PrimitiveType.Variant

    def test_binary_cast_target(self) -> None:
        target = type_factory.binary()
        assert target == PrimitiveType.Binary


class TestTdsColumnsFromJson:
    """Verify that tds_columns_from_json correctly round-trips new types."""

    def test_time_roundtrip(self) -> None:
        schema_json = '{"columns": [{"_type": "primitiveSchemaColumn", "name": "ts", "type": "Time"}]}'
        cols = tds_columns_from_json(schema_json)
        assert len(cols) == 1
        assert cols[0].get_name() == "ts"
        assert cols[0].get_type() == "Time"

    def test_variant_roundtrip(self) -> None:
        schema_json = '{"columns": [{"_type": "primitiveSchemaColumn", "name": "meta", "type": "Variant"}]}'
        cols = tds_columns_from_json(schema_json)
        assert cols[0].get_type() == "Variant"

    def test_binary_roundtrip(self) -> None:
        schema_json = '{"columns": [{"_type": "primitiveSchemaColumn", "name": "data", "type": "Binary"}]}'
        cols = tds_columns_from_json(schema_json)
        assert cols[0].get_type() == "Binary"


class TestPyLegendVariantIsStringCompatible:
    """Variant inherits PyLegendString so string operations work."""

    def test_variant_is_subclass_of_string(self) -> None:
        from pylegend.core.language.shared.primitives.string import PyLegendString
        assert issubclass(PyLegendVariant, PyLegendString)

    def test_binary_is_subclass_of_string(self) -> None:
        from pylegend.core.language.shared.primitives.string import PyLegendString
        assert issubclass(PyLegendBinary, PyLegendString)


class TestPyLegendTimeType:
    """Time is a standalone primitive (not String/Number/Date)."""

    def test_time_is_subclass_of_primitive(self) -> None:
        from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
        assert issubclass(PyLegendTime, PyLegendPrimitive)

    def test_time_is_not_subclass_of_string(self) -> None:
        from pylegend.core.language.shared.primitives.string import PyLegendString
        assert not issubclass(PyLegendTime, PyLegendString)

    def test_time_is_not_subclass_of_date(self) -> None:
        from pylegend.core.language.shared.primitives.date import PyLegendDate
        assert not issubclass(PyLegendTime, PyLegendDate)
