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

# type: ignore

import pytest

from pylegend.core.database.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.request.legend_client import LegendClient
from pylegend._typing import PyLegendDict, PyLegendUnion
from pylegend.core.tds.tds_column import PrimitiveType
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
from pylegend.core.language import (
    PyLegendIntegerColumnExpression,
    PyLegendStringColumnExpression,
    PyLegendFloatColumnExpression,
    PyLegendDecimalColumnExpression,
    PyLegendDateTimeColumnExpression,
)
from tests.core.language.shared import TestTableSpecInputFrame, TestTdsRow


class TestPreciseIntegerTypes:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.tinyint_column, "TinyInt"),
        (PrimitiveTdsColumn.utinyint_column, "UTinyInt"),
        (PrimitiveTdsColumn.smallint_column, "SmallInt"),
        (PrimitiveTdsColumn.usmallint_column, "USmallInt"),
        (PrimitiveTdsColumn.int_column, "Int"),
        (PrimitiveTdsColumn.uint_column, "UInt"),
        (PrimitiveTdsColumn.bigint_column, "BigInt"),
        (PrimitiveTdsColumn.ubigint_column, "UBigInt"),
    ], ids=["TinyInt", "UTinyInt", "SmallInt", "USmallInt", "Int", "UInt", "BigInt", "UBigInt"])
    def test_col_access(self, col_factory, type_name) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        assert self.__sql(frame, row, base_query, lambda x: x.get_integer("col1")) == '"root".col1'
        assert self.__pure(frame, row, base_query, type_name, lambda x: x.get_integer("col1")) == '$t.col1'

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.tinyint_column, "TinyInt"),
        (PrimitiveTdsColumn.utinyint_column, "UTinyInt"),
        (PrimitiveTdsColumn.smallint_column, "SmallInt"),
        (PrimitiveTdsColumn.usmallint_column, "USmallInt"),
        (PrimitiveTdsColumn.int_column, "Int"),
        (PrimitiveTdsColumn.uint_column, "UInt"),
        (PrimitiveTdsColumn.bigint_column, "BigInt"),
        (PrimitiveTdsColumn.ubigint_column, "UBigInt"),
    ], ids=["TinyInt", "UTinyInt", "SmallInt", "USmallInt", "Int", "UInt", "BigInt", "UBigInt"])
    def test_add(self, col_factory, type_name) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        assert self.__sql(frame, row, base_query, lambda x: x.get_integer("col1") + x.get_integer("col2")) == \
               '("root".col1 + "root".col2)'
        assert self.__sql(frame, row, base_query, lambda x: x.get_integer("col1") + 5) == \
               '("root".col1 + 5)'
        assert self.__pure(frame, row, base_query, type_name,
                           lambda x: x.get_integer("col1") + x.get_integer("col2")) == \
               '(toOne($t.col1) + toOne($t.col2))'
        assert self.__pure(frame, row, base_query, type_name, lambda x: x.get_integer("col1") + 5) == \
               '(toOne($t.col1) + 5)'

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.tinyint_column, "TinyInt"),
        (PrimitiveTdsColumn.utinyint_column, "UTinyInt"),
        (PrimitiveTdsColumn.smallint_column, "SmallInt"),
        (PrimitiveTdsColumn.usmallint_column, "USmallInt"),
        (PrimitiveTdsColumn.int_column, "Int"),
        (PrimitiveTdsColumn.uint_column, "UInt"),
        (PrimitiveTdsColumn.bigint_column, "BigInt"),
        (PrimitiveTdsColumn.ubigint_column, "UBigInt"),
    ], ids=["TinyInt", "UTinyInt", "SmallInt", "USmallInt", "Int", "UInt", "BigInt", "UBigInt"])
    def test_subtract(self, col_factory, type_name) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        assert self.__sql(frame, row, base_query, lambda x: x.get_integer("col1") - 3) == \
               '("root".col1 - 3)'
        assert self.__pure(frame, row, base_query, type_name, lambda x: x.get_integer("col1") - 3) == \
               '(toOne($t.col1) - 3)'

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.tinyint_column, "TinyInt"),
        (PrimitiveTdsColumn.utinyint_column, "UTinyInt"),
        (PrimitiveTdsColumn.smallint_column, "SmallInt"),
        (PrimitiveTdsColumn.usmallint_column, "USmallInt"),
        (PrimitiveTdsColumn.int_column, "Int"),
        (PrimitiveTdsColumn.uint_column, "UInt"),
        (PrimitiveTdsColumn.bigint_column, "BigInt"),
        (PrimitiveTdsColumn.ubigint_column, "UBigInt"),
    ], ids=["TinyInt", "UTinyInt", "SmallInt", "USmallInt", "Int", "UInt", "BigInt", "UBigInt"])
    def test_multiply(self, col_factory, type_name) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        assert self.__sql(frame, row, base_query, lambda x: x.get_integer("col1") * 2) == \
               '("root".col1 * 2)'
        assert self.__pure(frame, row, base_query, type_name, lambda x: x.get_integer("col1") * 2) == \
               '(toOne($t.col1) * 2)'

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.tinyint_column, "TinyInt"),
        (PrimitiveTdsColumn.utinyint_column, "UTinyInt"),
        (PrimitiveTdsColumn.smallint_column, "SmallInt"),
        (PrimitiveTdsColumn.usmallint_column, "USmallInt"),
        (PrimitiveTdsColumn.int_column, "Int"),
        (PrimitiveTdsColumn.uint_column, "UInt"),
        (PrimitiveTdsColumn.bigint_column, "BigInt"),
        (PrimitiveTdsColumn.ubigint_column, "UBigInt"),
    ], ids=["TinyInt", "UTinyInt", "SmallInt", "USmallInt", "Int", "UInt", "BigInt", "UBigInt"])
    def test_lt(self, col_factory, type_name) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        assert self.__sql(frame, row, base_query, lambda x: x.get_integer("col1") < 10) == \
               '("root".col1 < 10)'
        assert self.__pure(frame, row, base_query, type_name, lambda x: x.get_integer("col1") < 10) == \
               '($t.col1 < 10)'

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.tinyint_column, "TinyInt"),
        (PrimitiveTdsColumn.utinyint_column, "UTinyInt"),
        (PrimitiveTdsColumn.smallint_column, "SmallInt"),
        (PrimitiveTdsColumn.usmallint_column, "USmallInt"),
        (PrimitiveTdsColumn.int_column, "Int"),
        (PrimitiveTdsColumn.uint_column, "UInt"),
        (PrimitiveTdsColumn.bigint_column, "BigInt"),
        (PrimitiveTdsColumn.ubigint_column, "UBigInt"),
    ], ids=["TinyInt", "UTinyInt", "SmallInt", "USmallInt", "Int", "UInt", "BigInt", "UBigInt"])
    @pytest.mark.parametrize("target_type,expected_sql_type", [
        (PrimitiveType.TinyInt, "SMALLINT"),
        (PrimitiveType.UTinyInt, "SMALLINT"),
        (PrimitiveType.SmallInt, "SMALLINT"),
        (PrimitiveType.USmallInt, "INTEGER"),
        (PrimitiveType.Int, "INTEGER"),
        (PrimitiveType.UInt, "BIGINT"),
        (PrimitiveType.BigInt, "BIGINT"),
        (PrimitiveType.UBigInt, "NUMERIC"),
        (PrimitiveType.Integer, "INTEGER"),
    ], ids=[
        "to_TinyInt", "to_UTinyInt", "to_SmallInt", "to_USmallInt",
        "to_Int", "to_UInt", "to_BigInt", "to_UBigInt", "to_Integer",
    ])
    def test_cast_sql(self, col_factory, type_name, target_type, expected_sql_type) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        result = self.__sql(frame, row, base_query,
                            lambda x: x.get_integer("col1").cast(target_type))
        assert result == f'CAST("root".col1 AS {expected_sql_type})'

    def test_cast_returns_precise_type(self) -> None:
        frame, row, base_query = self.__make_frame(PrimitiveTdsColumn.tinyint_column)
        assert isinstance(row.get_integer("col1").cast(PrimitiveType.BigInt), PyLegendBigInt)
        assert isinstance(row.get_integer("col1").cast(PrimitiveType.SmallInt), PyLegendSmallInt)
        assert isinstance(row.get_integer("col1").cast(PrimitiveType.Int), PyLegendInt)
        assert isinstance(row.get_integer("col1").cast(PrimitiveType.UInt), PyLegendUInt)

    def test_cast_invalid_target_raises(self) -> None:
        frame, row, base_query = self.__make_frame(PrimitiveTdsColumn.tinyint_column)
        with pytest.raises(TypeError, match="cast target must be a PrimitiveType"):
            row.get_integer("col1").cast("BigInt")  # type: ignore

    def __make_frame(self, col_factory):
        frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
            col_factory("col1"), col_factory("col2")
        ])
        row = TestTdsRow.from_tds_frame("t", frame)
        base_query = frame.to_sql_query_object(self.frame_to_sql_config)
        return frame, row, base_query

    def __sql(self, frame, row, base_query, f) -> str:
        return self.db_extension.process_expression(
            f(row).to_sql_expression({"t": base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )

    def __pure(self, frame, row, base_query, type_name, f) -> str:
        expr = str(f(row).to_pure_expression(self.frame_to_pure_config))
        model_code = (
            "function test::testFunc(): Any[*]\n"
            "{\n"
            "    []->toOne()->cast(\n"
            f"        @meta::pure::metamodel::relation::Relation<(col1: {type_name}[0..1], col2: {type_name}[0..1])>\n"
            "    )\n"
            "    ->extend(~new_col:t|<<expression>>)\n"
            "}"
        )
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr


class TestPreciseFloatTypes:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.float4_column, "Float4"),
        (PrimitiveTdsColumn.double_column, "Double"),
    ], ids=["Float4", "Double"])
    def test_col_access(self, col_factory, type_name) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        assert self.__sql(frame, row, base_query, lambda x: x.get_float("col1")) == '"root".col1'
        assert self.__pure(frame, row, base_query, type_name, lambda x: x.get_float("col1")) == '$t.col1'

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.float4_column, "Float4"),
        (PrimitiveTdsColumn.double_column, "Double"),
    ], ids=["Float4", "Double"])
    def test_add(self, col_factory, type_name) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        assert self.__sql(frame, row, base_query, lambda x: x.get_float("col1") + 0.5) == \
               '("root".col1 + 0.5)'
        assert self.__pure(frame, row, base_query, type_name, lambda x: x.get_float("col1") + 0.5) == \
               '(toOne($t.col1) + 0.5)'

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.float4_column, "Float4"),
        (PrimitiveTdsColumn.double_column, "Double"),
    ], ids=["Float4", "Double"])
    def test_subtract(self, col_factory, type_name) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        assert self.__sql(frame, row, base_query, lambda x: x.get_float("col1") - 1.5) == \
               '("root".col1 - 1.5)'
        assert self.__pure(frame, row, base_query, type_name, lambda x: x.get_float("col1") - 1.5) == \
               '(toOne($t.col1) - 1.5)'

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.float4_column, "Float4"),
        (PrimitiveTdsColumn.double_column, "Double"),
    ], ids=["Float4", "Double"])
    def test_multiply(self, col_factory, type_name) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        assert self.__sql(frame, row, base_query, lambda x: x.get_float("col1") * 2.0) == \
               '("root".col1 * 2.0)'
        assert self.__pure(frame, row, base_query, type_name, lambda x: x.get_float("col1") * 2.0) == \
               '(toOne($t.col1) * 2.0)'

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.float4_column, "Float4"),
        (PrimitiveTdsColumn.double_column, "Double"),
    ], ids=["Float4", "Double"])
    def test_gt(self, col_factory, type_name) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        assert self.__sql(frame, row, base_query, lambda x: x.get_float("col1") > 3.14) == \
               '("root".col1 > 3.14)'
        assert self.__pure(frame, row, base_query, type_name, lambda x: x.get_float("col1") > 3.14) == \
               '($t.col1 > 3.14)'

    @pytest.mark.parametrize("col_factory,type_name", [
        (PrimitiveTdsColumn.float4_column, "Float4"),
        (PrimitiveTdsColumn.double_column, "Double"),
    ], ids=["Float4", "Double"])
    @pytest.mark.parametrize("target_type,expected_sql_type", [
        (PrimitiveType.Float4, "REAL"),
        (PrimitiveType.Double, "DOUBLE PRECISION"),
        (PrimitiveType.Float, "DOUBLE PRECISION"),
    ], ids=["to_Float4", "to_Double", "to_Float"])
    def test_cast_sql(self, col_factory, type_name, target_type, expected_sql_type) -> None:
        frame, row, base_query = self.__make_frame(col_factory)
        result = self.__sql(frame, row, base_query,
                            lambda x: x.get_float("col1").cast(target_type))
        assert result == f'CAST("root".col1 AS {expected_sql_type})'

    def test_cast_returns_precise_type(self) -> None:
        frame, row, base_query = self.__make_frame(PrimitiveTdsColumn.float4_column)
        assert isinstance(row.get_float("col1").cast(PrimitiveType.Double), PyLegendDouble)
        assert isinstance(row.get_float("col1").cast(PrimitiveType.Float4), PyLegendFloat4)

    def __make_frame(self, col_factory):
        frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
            col_factory("col1"), col_factory("col2")
        ])
        row = TestTdsRow.from_tds_frame("t", frame)
        base_query = frame.to_sql_query_object(self.frame_to_sql_config)
        return frame, row, base_query

    def __sql(self, frame, row, base_query, f) -> str:
        return self.db_extension.process_expression(
            f(row).to_sql_expression({"t": base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )

    def __pure(self, frame, row, base_query, type_name, f) -> str:
        expr = str(f(row).to_pure_expression(self.frame_to_pure_config))
        model_code = (
            "function test::testFunc(): Any[*]\n"
            "{\n"
            "    []->toOne()->cast(\n"
            f"        @meta::pure::metamodel::relation::Relation<(col1: {type_name}[0..1], col2: {type_name}[0..1])>\n"
            "    )\n"
            "    ->extend(~new_col:t|<<expression>>)\n"
            "}"
        )
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr


class TestPreciseNumericType:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.numeric_column("col1"),
        PrimitiveTdsColumn.numeric_column("col2"),
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_numeric_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col1")) == '"root".col1'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col1")) == '$t.col1'

    def test_numeric_add(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col1") + x.get_decimal("col2")) == \
               '("root".col1 + "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_decimal("col1") + 10) == \
               '("root".col1 + 10)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col1") + x.get_decimal("col2")) == \
               '(toOne($t.col1) + toOne($t.col2))'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col1") + 10) == \
               '(toOne($t.col1) + 10)'

    def test_numeric_subtract(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col1") - 3) == \
               '("root".col1 - 3)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col1") - 3) == \
               '(toOne($t.col1) - 3)'

    def test_numeric_multiply(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col1") * 2) == \
               '("root".col1 * 2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col1") * 2) == \
               '(toOne($t.col1) * 2)'

    def test_numeric_lt(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col1") < 100) == \
               '("root".col1 < 100)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col1") < 100) == \
               '($t.col1 < 100)'

    @pytest.mark.parametrize("target_type,expected_sql_type", [
        (PrimitiveType.Decimal, "DECIMAL"),
        (PrimitiveType.Integer, "INTEGER"),
    ], ids=["to_Decimal", "to_Integer"])
    def test_cast_sql(self, target_type, expected_sql_type) -> None:
        result = self.db_extension.process_expression(
            self.tds_row.get_decimal("col1").cast(target_type).to_sql_expression(
                {"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert result == f'CAST("root".col1 AS {expected_sql_type})'

    def __generate_sql_string(self, f) -> str:
        return self.db_extension.process_expression(
            f(self.tds_row).to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )

    def __generate_pure_string(self, f) -> str:
        expr = str(f(self.tds_row).to_pure_expression(self.frame_to_pure_config))
        model_code = (
            "function test::testFunc(): Any[*]\n"
            "{\n"
            "    []->toOne()->cast(\n"
            "        @meta::pure::metamodel::relation::Relation<(col1: Numeric(20, 6)[0..1], col2: Numeric(20, 6)[0..1])>\n"
            "    )\n"
            "    ->extend(~new_col:t|<<expression>>)\n"
            "}"
        )
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr


class TestPreciseVarcharType:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.varchar_column("col1"),
        PrimitiveTdsColumn.varchar_column("col2"),
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_varchar_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col1")) == '"root".col1'
        assert self.__generate_pure_string(lambda x: x.get_string("col1")) == '$t.col1'

    def test_varchar_length(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col1").len()) == 'CHAR_LENGTH("root".col1)'
        assert self.__generate_pure_string(lambda x: x.get_string("col1").len()) == 'toOne($t.col1)->length()'

    def test_varchar_concat(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col1") + x.get_string("col2")) == \
               'CONCAT("root".col1, "root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_string("col1") + "abc") == \
               "CONCAT(\"root\".col1, 'abc')"
        assert self.__generate_pure_string(lambda x: x.get_string("col1") + x.get_string("col2")) == \
               '(toOne($t.col1) + toOne($t.col2))'
        assert self.__generate_pure_string(lambda x: x.get_string("col1") + "abc") == \
               "(toOne($t.col1) + 'abc')"

    def test_varchar_upper(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col1").upper()) == 'UPPER("root".col1)'
        assert self.__generate_pure_string(lambda x: x.get_string("col1").upper()) == 'toOne($t.col1)->toUpper()'

    def test_varchar_lower(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col1").lower()) == 'LOWER("root".col1)'
        assert self.__generate_pure_string(lambda x: x.get_string("col1").lower()) == 'toOne($t.col1)->toLower()'

    def test_varchar_lt(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col1") < x.get_string("col2")) == \
               '("root".col1 < "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col1") < x.get_string("col2")) == \
               '($t.col1 < $t.col2)'

    def test_cast_to_string_sql(self) -> None:
        result = self.db_extension.process_expression(
            self.tds_row.get_string("col1").cast(PrimitiveType.String).to_sql_expression(
                {"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert result == 'CAST("root".col1 AS TEXT)'

    def __generate_sql_string(self, f) -> str:
        return self.db_extension.process_expression(
            f(self.tds_row).to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )

    def __generate_pure_string(self, f) -> str:
        expr = str(f(self.tds_row).to_pure_expression(self.frame_to_pure_config))
        model_code = (
            "function test::testFunc(): Any[*]\n"
            "{\n"
            "    []->toOne()->cast(\n"
            "        @meta::pure::metamodel::relation::Relation<(col1: Varchar(200)[0..1], col2: Varchar(200)[0..1])>\n"
            "    )\n"
            "    ->extend(~new_col:t|<<expression>>)\n"
            "}"
        )
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr


class TestPreciseTimestampType:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.timestamp_column("col1"),
        PrimitiveTdsColumn.timestamp_column("col2"),
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_timestamp_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_datetime("col1")) == '"root".col1'
        assert self.__generate_pure_string(lambda x: x.get_datetime("col1")) == '$t.col1'

    def test_timestamp_lt(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_datetime("col1") < x.get_datetime("col2")) == \
               '("root".col1 < "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_datetime("col1") < x.get_datetime("col2")) == \
               '($t.col1 < $t.col2)'

    @pytest.mark.parametrize("target_type,expected_sql_type", [
        (PrimitiveType.Timestamp, "TIMESTAMP"),
        (PrimitiveType.DateTime, "TIMESTAMP"),
        (PrimitiveType.Date, "DATE"),
    ], ids=["to_Timestamp", "to_DateTime", "to_Date"])
    def test_cast_sql(self, target_type, expected_sql_type) -> None:
        result = self.db_extension.process_expression(
            self.tds_row.get_datetime("col1").cast(target_type).to_sql_expression(
                {"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert result == f'CAST("root".col1 AS {expected_sql_type})'

    def test_cast_returns_timestamp_type(self) -> None:
        assert isinstance(self.tds_row.get_datetime("col1").cast(PrimitiveType.Timestamp), PyLegendTimestamp)

    def __generate_sql_string(self, f) -> str:
        return self.db_extension.process_expression(
            f(self.tds_row).to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )

    def __generate_pure_string(self, f) -> str:
        expr = str(f(self.tds_row).to_pure_expression(self.frame_to_pure_config))
        model_code = (
            "function test::testFunc(): Any[*]\n"
            "{\n"
            "    []->toOne()->cast(\n"
            "        @meta::pure::metamodel::relation::Relation<(col1: Timestamp[0..1], col2: Timestamp[0..1])>\n"
            "    )\n"
            "    ->extend(~new_col:t|<<expression>>)\n"
            "}"
        )
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr


class TestPrecisePrimitiveDirectInstantiation:
    """Unit tests that directly instantiate precise primitives and call to_sql_expression."""

    frame_to_sql_config = FrameToSqlConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))

    @pytest.mark.parametrize("cls", [
        PyLegendTinyInt,
        PyLegendUTinyInt,
        PyLegendSmallInt,
        PyLegendUSmallInt,
        PyLegendInt,
        PyLegendUInt,
        PyLegendBigInt,
        PyLegendUBigInt,
    ], ids=[
        "TinyInt", "UTinyInt", "SmallInt", "USmallInt",
        "Int", "UInt", "BigInt", "UBigInt",
    ])
    def test_integer_precise_to_sql_with_column(self, cls) -> None:
        frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
            PrimitiveTdsColumn.integer_column("col1"),
        ])
        row = TestTdsRow.from_tds_frame("t", frame)
        base_query = frame.to_sql_query_object(self.frame_to_sql_config)
        col_expr = PyLegendIntegerColumnExpression(row, "col1")
        obj = cls(col_expr)
        result = self.db_extension.process_expression(
            obj.to_sql_expression({"t": base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert result == '"root".col1'

    def test_varchar_to_sql_with_column(self) -> None:
        frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
            PrimitiveTdsColumn.string_column("col1"),
        ])
        row = TestTdsRow.from_tds_frame("t", frame)
        base_query = frame.to_sql_query_object(self.frame_to_sql_config)
        col_expr = PyLegendStringColumnExpression(row, "col1")
        obj = PyLegendVarchar(col_expr, max_length=100)
        assert obj.max_length == 100
        result = self.db_extension.process_expression(
            obj.to_sql_expression({"t": base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert result == '"root".col1'

    def test_timestamp_to_sql_with_column(self) -> None:
        frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
            PrimitiveTdsColumn.datetime_column("col1"),
        ])
        row = TestTdsRow.from_tds_frame("t", frame)
        base_query = frame.to_sql_query_object(self.frame_to_sql_config)
        col_expr = PyLegendDateTimeColumnExpression(row, "col1")
        obj = PyLegendTimestamp(col_expr)
        result = self.db_extension.process_expression(
            obj.to_sql_expression({"t": base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert result == '"root".col1'

    @pytest.mark.parametrize("cls", [PyLegendFloat4, PyLegendDouble], ids=["Float4", "Double"])
    def test_float_precise_to_sql_with_column(self, cls) -> None:
        frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
            PrimitiveTdsColumn.float_column("col1"),
        ])
        row = TestTdsRow.from_tds_frame("t", frame)
        base_query = frame.to_sql_query_object(self.frame_to_sql_config)
        col_expr = PyLegendFloatColumnExpression(row, "col1")
        obj = cls(col_expr)
        result = self.db_extension.process_expression(
            obj.to_sql_expression({"t": base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert result == '"root".col1'

    def test_numeric_to_sql_with_column(self) -> None:
        frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
            PrimitiveTdsColumn.decimal_column("col1"),
        ])
        row = TestTdsRow.from_tds_frame("t", frame)
        base_query = frame.to_sql_query_object(self.frame_to_sql_config)
        col_expr = PyLegendDecimalColumnExpression(row, "col1")
        obj = PyLegendNumeric(col_expr, precision=20, scale=6)
        assert obj.precision == 20
        assert obj.scale == 6
        result = self.db_extension.process_expression(
            obj.to_sql_expression({"t": base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert result == '"root".col1'
