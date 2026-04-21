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


class TestPandasResultHandlerPreciseTypes:
    """Tests that the ToPandasDfResultHandler correctly creates Series for Decimal and precise primitive types."""

    def test_decimal_column_series(self) -> None:
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        col = PrimitiveTdsColumn.decimal_column("dec_col")
        series = ToPandasDfResultHandler._create_series([1.1, 2.2, 3.3], col, 0, 1)
        assert series.name == "dec_col"
        assert series.dtype.name == "Float64"
        assert list(series) == [1.1, 2.2, 3.3]

    def test_decimal_column_with_none(self) -> None:
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        import pandas as pd
        col = PrimitiveTdsColumn.decimal_column("dec_col")
        series = ToPandasDfResultHandler._create_series([1.5, None, 3.5], col, 0, 1)
        assert series.dtype.name == "Float64"
        assert pd.isna(series.iloc[1])

    def test_decimal_column_with_python_decimal(self) -> None:
        from decimal import Decimal as PythonDecimal
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        col = PrimitiveTdsColumn.decimal_column("dec_col")
        series = ToPandasDfResultHandler._create_series(
            [PythonDecimal("1.23"), PythonDecimal("4.56")], col, 0, 1
        )
        assert series.dtype.name == "Float64"
        assert abs(series.iloc[0] - 1.23) < 1e-10
        assert abs(series.iloc[1] - 4.56) < 1e-10

    @pytest.mark.parametrize("col_factory,expected_dtype", [
        (PrimitiveTdsColumn.tinyint_column, "Int8"),
        (PrimitiveTdsColumn.utinyint_column, "UInt8"),
        (PrimitiveTdsColumn.smallint_column, "Int16"),
        (PrimitiveTdsColumn.usmallint_column, "UInt16"),
        (PrimitiveTdsColumn.int_column, "Int32"),
        (PrimitiveTdsColumn.uint_column, "UInt32"),
        (PrimitiveTdsColumn.bigint_column, "Int64"),
        (PrimitiveTdsColumn.ubigint_column, "UInt64"),
    ], ids=["TinyInt", "UTinyInt", "SmallInt", "USmallInt", "Int", "UInt", "BigInt", "UBigInt"])
    def test_precise_integer_column_series(self, col_factory, expected_dtype) -> None:
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        col = col_factory("int_col")
        series = ToPandasDfResultHandler._create_series([10, 20, 30], col, 0, 1)
        assert series.name == "int_col"
        assert series.dtype.name == expected_dtype
        assert list(series) == [10, 20, 30]

    @pytest.mark.parametrize("col_factory,expected_dtype", [
        (PrimitiveTdsColumn.float4_column, "Float32"),
        (PrimitiveTdsColumn.double_column, "Float64"),
    ], ids=["Float4", "Double"])
    def test_precise_float_column_series(self, col_factory, expected_dtype) -> None:
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        col = col_factory("float_col")
        series = ToPandasDfResultHandler._create_series([1.1, 2.2, 3.3], col, 0, 1)
        assert series.name == "float_col"
        assert series.dtype.name == expected_dtype

    def test_numeric_column_series(self) -> None:
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        col = PrimitiveTdsColumn.numeric_column("num_col")
        series = ToPandasDfResultHandler._create_series([100.5, 200.5], col, 0, 1)
        assert series.name == "num_col"
        assert series.dtype.name == "Float64"

    def test_varchar_column_series(self) -> None:
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        col = PrimitiveTdsColumn.varchar_column("str_col")
        series = ToPandasDfResultHandler._create_series(["a", "b", "c"], col, 0, 1)
        assert series.name == "str_col"
        assert series.dtype == "object"
        assert list(series) == ["a", "b", "c"]

    def test_timestamp_column_series(self) -> None:
        import pandas as pd
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        col = PrimitiveTdsColumn.timestamp_column("ts_col")
        series = ToPandasDfResultHandler._create_series(
            ["2026-01-15T10:30:00.000000000", "2026-06-20T14:00:00.000000000"], col, 0, 1
        )
        assert series.name == "ts_col"
        assert pd.api.types.is_datetime64_any_dtype(series)


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


class TestCastToPreciseTypesPandasResultHandler:
    """Verify that casting columns to precise/decimal types produces column metadata
    that the ToPandasDfResultHandler can convert into pandas Series without errors."""

    @staticmethod
    def _cast_columns(original_columns, column_type_map):
        from pylegend.core.tds.cast_helpers import validate_and_build_cast_columns
        return validate_and_build_cast_columns(original_columns, column_type_map)

    @pytest.mark.parametrize("cast_factory,col_factory,sample_data,expected_dtype", [
        ("bigint", PrimitiveTdsColumn.integer_column, [1, 2, 3], "Int64"),
        ("tinyint", PrimitiveTdsColumn.integer_column, [1, 2, 3], "Int8"),
        ("utinyint", PrimitiveTdsColumn.integer_column, [1, 2, 3], "UInt8"),
        ("smallint", PrimitiveTdsColumn.integer_column, [10, 20, 30], "Int16"),
        ("usmallint", PrimitiveTdsColumn.integer_column, [10, 20, 30], "UInt16"),
        ("int", PrimitiveTdsColumn.integer_column, [10, 20, 30], "Int32"),
        ("uint", PrimitiveTdsColumn.integer_column, [10, 20, 30], "UInt32"),
        ("ubigint", PrimitiveTdsColumn.integer_column, [10, 20, 30], "UInt64"),
        ("float4", PrimitiveTdsColumn.float_column, [1.1, 2.2, 3.3], "Float32"),
        ("double", PrimitiveTdsColumn.float_column, [1.1, 2.2, 3.3], "Float64"),
    ], ids=[
        "BigInt", "TinyInt", "UTinyInt", "SmallInt", "USmallInt",
        "Int", "UInt", "UBigInt", "Float4", "Double",
    ])
    def test_cast_simple_precise_types(self, cast_factory, col_factory, sample_data, expected_dtype) -> None:
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        from pylegend.core.language import type_factory as tf

        original_columns = [col_factory("col1")]
        cast_target = getattr(tf, cast_factory)()
        casted_columns = self._cast_columns(original_columns, {"col1": cast_target})

        series = ToPandasDfResultHandler._create_series(sample_data, casted_columns[0], 0, 1)
        assert series.dtype.name == expected_dtype
        assert list(series) == sample_data

    def test_cast_to_decimal(self) -> None:
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        from pylegend.core.language import type_factory as tf

        original_columns = [PrimitiveTdsColumn.number_column("amount")]
        casted_columns = self._cast_columns(original_columns, {"amount": tf.decimal()})

        series = ToPandasDfResultHandler._create_series([10.5, 20.3, None], casted_columns[0], 0, 1)
        assert series.dtype.name == "Float64"
        assert series.iloc[0] == 10.5

    def test_cast_to_decimal_with_python_decimal(self) -> None:
        from decimal import Decimal as PythonDecimal
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        from pylegend.core.language import type_factory as tf

        original_columns = [PrimitiveTdsColumn.number_column("amount")]
        casted_columns = self._cast_columns(original_columns, {"amount": tf.decimal()})

        series = ToPandasDfResultHandler._create_series(
            [PythonDecimal("1.23"), PythonDecimal("4.56")], casted_columns[0], 0, 1
        )
        assert series.dtype.name == "Float64"
        assert abs(series.iloc[0] - 1.23) < 1e-10

    def test_cast_to_numeric(self) -> None:
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        from pylegend.core.language import type_factory as tf

        original_columns = [PrimitiveTdsColumn.decimal_column("amount")]
        casted_columns = self._cast_columns(original_columns, {"amount": tf.numeric(10, 2)})

        series = ToPandasDfResultHandler._create_series([99.99, 100.01], casted_columns[0], 0, 1)
        assert series.dtype.name == "Float64"

    def test_cast_to_varchar(self) -> None:
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        from pylegend.core.language import type_factory as tf

        original_columns = [PrimitiveTdsColumn.string_column("name")]
        casted_columns = self._cast_columns(original_columns, {"name": tf.varchar(200)})

        series = ToPandasDfResultHandler._create_series(["alice", "bob"], casted_columns[0], 0, 1)
        assert series.dtype == "object"
        assert list(series) == ["alice", "bob"]

    def test_cast_to_timestamp(self) -> None:
        import pandas as pd
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        from pylegend.core.language import type_factory as tf

        original_columns = [PrimitiveTdsColumn.datetime_column("ts")]
        casted_columns = self._cast_columns(original_columns, {"ts": tf.timestamp()})

        series = ToPandasDfResultHandler._create_series(
            ["2026-01-15T10:30:00.000000000"], casted_columns[0], 0, 1
        )
        assert pd.api.types.is_datetime64_any_dtype(series)

    def test_cast_multiple_columns_at_once(self) -> None:
        from pylegend.extensions.tds.result_handler.to_pandas_df_result_handler import ToPandasDfResultHandler
        from pylegend.core.language import type_factory as tf

        original_columns = [
            PrimitiveTdsColumn.integer_column("Order Id"),
            PrimitiveTdsColumn.string_column("Ship Name"),
        ]
        casted_columns = self._cast_columns(original_columns, {
            "Order Id": tf.bigint(),
            "Ship Name": tf.varchar(200),
        })

        assert casted_columns[0].get_type() == "BigInt"
        assert casted_columns[1].get_type() == "Varchar"

        # Interleaved data: [row0_col0, row0_col1, row1_col0, row1_col1, ...]
        all_values = [101, "Ship A", 102, "Ship B", 103, "Ship C"]
        s0 = ToPandasDfResultHandler._create_series(all_values, casted_columns[0], 0, 2)
        s1 = ToPandasDfResultHandler._create_series(all_values, casted_columns[1], 1, 2)

        assert s0.dtype.name == "Int64"
        assert list(s0) == [101, 102, 103]
        assert s1.dtype == "object"
        assert list(s1) == ["Ship A", "Ship B", "Ship C"]
