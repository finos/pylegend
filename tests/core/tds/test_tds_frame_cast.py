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

import json
from textwrap import dedent

import pytest

from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
    PyLegendSequence,
    PyLegendCallable,
)
from pylegend.core.tds.tds_column import (
    PrimitiveTdsColumn,
    PrimitiveType,
    TdsColumn,
    EnumTdsColumn,
)
from pylegend.core.tds.tds_frame import PyLegendTdsFrame, FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from pylegend.extensions.tds.legendql_api.frames.legendql_api_table_spec_input_frame import LegendQLApiTableSpecInputFrame
from pylegend.extensions.tds.legacy_api.frames.legacy_api_table_spec_input_frame import LegacyApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import (
    simple_person_service_frame_pandas_api,
)
from pylegend.core.request.legend_client import LegendClient


def _pandas_frame(columns: PyLegendSequence[TdsColumn]) -> PyLegendTdsFrame:
    return PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)


def _legendql_frame(columns: PyLegendSequence[TdsColumn]) -> PyLegendTdsFrame:
    return LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)


def _legacy_frame(columns: PyLegendSequence[TdsColumn]) -> PyLegendTdsFrame:
    return LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)


_ALL_FRAME_FACTORIES = pytest.mark.parametrize(
    "frame_factory",
    [_pandas_frame, _legendql_frame, _legacy_frame],
    ids=["pandas_api", "legendql_api", "legacy_api"],
)


class TestTdsFrameCastValidation:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    @_ALL_FRAME_FACTORIES
    def test_cast_unknown_column_raises_error(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("col1")])
        with pytest.raises(ValueError, match="Column.*not found.*nonexistent"):
            frame.cast({"nonexistent": PrimitiveType.Float})

    @_ALL_FRAME_FACTORIES
    def test_cast_unknown_column_error_shows_available(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")])
        with pytest.raises(ValueError, match="Available columns.*col1.*col2"):
            frame.cast({"bad_col": PrimitiveType.Float})

    @_ALL_FRAME_FACTORIES
    def test_cast_enum_column_raises_type_error(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([EnumTdsColumn("city", "my::CityType", ["Tier1", "Tier2"])])
        with pytest.raises(TypeError, match="Cannot cast non-primitive column 'city'"):
            frame.cast({"city": PrimitiveType.String})

    @_ALL_FRAME_FACTORIES
    def test_cast_string_to_integer_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.string_column("s")])
        with pytest.raises(ValueError, match="Cannot cast column 's' from String to Integer"):
            frame.cast({"s": PrimitiveType.Integer})

    @_ALL_FRAME_FACTORIES
    def test_cast_string_to_boolean_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.string_column("s")])
        with pytest.raises(ValueError, match="Cannot cast column 's' from String to Boolean"):
            frame.cast({"s": PrimitiveType.Boolean})

    @_ALL_FRAME_FACTORIES
    def test_cast_string_to_date_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.string_column("s")])
        with pytest.raises(ValueError, match="Cannot cast column 's' from String to DateTime"):
            frame.cast({"s": PrimitiveType.DateTime})

    @_ALL_FRAME_FACTORIES
    def test_cast_boolean_to_integer_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.boolean_column("b")])
        with pytest.raises(ValueError, match="Cannot cast column 'b' from Boolean to Integer"):
            frame.cast({"b": PrimitiveType.Integer})

    @_ALL_FRAME_FACTORIES
    def test_cast_boolean_to_string_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.boolean_column("b")])
        with pytest.raises(ValueError, match="Cannot cast column 'b' from Boolean to String"):
            frame.cast({"b": PrimitiveType.String})

    @_ALL_FRAME_FACTORIES
    def test_cast_date_to_integer_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.date_column("d")])
        with pytest.raises(ValueError, match="Cannot cast column 'd' from Date to Integer"):
            frame.cast({"d": PrimitiveType.Integer})

    @_ALL_FRAME_FACTORIES
    def test_cast_date_to_string_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.date_column("d")])
        with pytest.raises(ValueError, match="Cannot cast column 'd' from Date to String"):
            frame.cast({"d": PrimitiveType.String})

    @_ALL_FRAME_FACTORIES
    def test_cast_integer_to_boolean_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("n")])
        with pytest.raises(ValueError, match="Cannot cast column 'n' from Integer to Boolean"):
            frame.cast({"n": PrimitiveType.Boolean})

    @_ALL_FRAME_FACTORIES
    def test_cast_integer_to_date_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("n")])
        with pytest.raises(ValueError, match="Cannot cast column 'n' from Integer to DateTime"):
            frame.cast({"n": PrimitiveType.DateTime})

    @_ALL_FRAME_FACTORIES
    def test_cast_integer_to_float_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("n")])
        with pytest.raises(ValueError, match="Cannot cast column 'n' from Integer to Float"):
            frame.cast({"n": PrimitiveType.Float})

    @_ALL_FRAME_FACTORIES
    def test_cast_integer_to_decimal_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("n")])
        with pytest.raises(ValueError, match="Cannot cast column 'n' from Integer to Decimal"):
            frame.cast({"n": PrimitiveType.Decimal})

    @_ALL_FRAME_FACTORIES
    def test_cast_float_to_integer_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.float_column("n")])
        with pytest.raises(ValueError, match="Cannot cast column 'n' from Float to Integer"):
            frame.cast({"n": PrimitiveType.Integer})

    @_ALL_FRAME_FACTORIES
    def test_cast_integer_to_string_not_allowed(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("n")])
        with pytest.raises(ValueError, match="Cannot cast column 'n' from Integer to String"):
            frame.cast({"n": PrimitiveType.String})


class TestTdsFrameCastColumns:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    @_ALL_FRAME_FACTORIES
    def test_cast_integer_to_bigint(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")])
        result = frame.cast({"col1": PrimitiveType.BigInt})
        assert "TdsColumn(Name: col1, Type: BigInt)" == str(result.columns()[0])
        assert "TdsColumn(Name: col2, Type: String)" == str(result.columns()[1])

    @_ALL_FRAME_FACTORIES
    def test_cast_multiple_columns(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
        ])
        result = frame.cast({"col1": PrimitiveType.BigInt, "col3": PrimitiveType.Double})
        assert "TdsColumn(Name: col1, Type: BigInt)" == str(result.columns()[0])
        assert "TdsColumn(Name: col2, Type: String)" == str(result.columns()[1])
        assert "TdsColumn(Name: col3, Type: Double)" == str(result.columns()[2])

    @_ALL_FRAME_FACTORIES
    def test_cast_within_integer_branch(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("n")])
        assert "TdsColumn(Name: n, Type: TinyInt)" == str(frame.cast({"n": PrimitiveType.TinyInt}).columns()[0])
        assert "TdsColumn(Name: n, Type: BigInt)" == str(frame.cast({"n": PrimitiveType.BigInt}).columns()[0])
        assert "TdsColumn(Name: n, Type: Number)" == str(frame.cast({"n": PrimitiveType.Number}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_within_float_branch(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.float_column("n")])
        assert "TdsColumn(Name: n, Type: Float4)" == str(frame.cast({"n": PrimitiveType.Float4}).columns()[0])
        assert "TdsColumn(Name: n, Type: Double)" == str(frame.cast({"n": PrimitiveType.Double}).columns()[0])
        assert "TdsColumn(Name: n, Type: Number)" == str(frame.cast({"n": PrimitiveType.Number}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_within_decimal_branch(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.decimal_column("n")])
        assert "TdsColumn(Name: n, Type: Numeric)" == str(
            frame.cast({"n": (PrimitiveType.Numeric, 10, 2)}).columns()[0])
        assert "TdsColumn(Name: n, Type: Number)" == str(frame.cast({"n": PrimitiveType.Number}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_within_string_branch(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.string_column("s")])
        assert "TdsColumn(Name: s, Type: Varchar)" == str(
            frame.cast({"s": (PrimitiveType.Varchar, 200)}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_within_date_branch(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.date_column("d")])
        assert "TdsColumn(Name: d, Type: DateTime)" == str(frame.cast({"d": PrimitiveType.DateTime}).columns()[0])
        assert "TdsColumn(Name: d, Type: StrictDate)" == str(frame.cast({"d": PrimitiveType.StrictDate}).columns()[0])
        assert "TdsColumn(Name: d, Type: Timestamp)" == str(frame.cast({"d": PrimitiveType.Timestamp}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_boolean_to_boolean(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.boolean_column("b")])
        assert "TdsColumn(Name: b, Type: Boolean)" == str(frame.cast({"b": PrimitiveType.Boolean}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_same_type_is_noop(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("col1")])
        assert "TdsColumn(Name: col1, Type: Integer)" == str(
            frame.cast({"col1": PrimitiveType.Integer}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_empty_map_returns_copy(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")])
        result = frame.cast({})
        assert "TdsColumn(Name: col1, Type: Integer)" == str(result.columns()[0])
        assert "TdsColumn(Name: col2, Type: String)" == str(result.columns()[1])

    @_ALL_FRAME_FACTORIES
    def test_cast_preserves_column_order(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([
            PrimitiveTdsColumn.integer_column("a"),
            PrimitiveTdsColumn.string_column("b"),
            PrimitiveTdsColumn.float_column("c"),
            PrimitiveTdsColumn.boolean_column("d"),
        ])
        result = frame.cast({"c": PrimitiveType.Double, "a": PrimitiveType.BigInt})
        assert "TdsColumn(Name: a, Type: BigInt)" == str(result.columns()[0])
        assert "TdsColumn(Name: b, Type: String)" == str(result.columns()[1])
        assert "TdsColumn(Name: c, Type: Double)" == str(result.columns()[2])
        assert "TdsColumn(Name: d, Type: Boolean)" == str(result.columns()[3])

    @_ALL_FRAME_FACTORIES
    def test_cast_does_not_mutate_original_frame(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("col1")])
        assert "TdsColumn(Name: col1, Type: Integer)" == str(frame.columns()[0])
        frame.cast({"col1": PrimitiveType.BigInt})
        assert "TdsColumn(Name: col1, Type: Integer)" == str(frame.columns()[0])


class TestTdsFrameCastQueryGeneration:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    @_ALL_FRAME_FACTORIES
    def test_cast_sql_generation(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col 2"),
            PrimitiveTdsColumn.float_column("col3"),
        ]
        frame = frame_factory(columns)
        result = frame.cast({"col1": PrimitiveType.BigInt, "col3": PrimitiveType.Double})

        expected_sql = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col 2 AS "col 2",
                "root".col3 AS "col3"
            FROM
                test_schema.test_table AS "root"'''

        assert result.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    @_ALL_FRAME_FACTORIES
    def test_cast_pure_generation(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col 2"),
            PrimitiveTdsColumn.float_column("col3"),
        ]
        frame = frame_factory(columns)
        result = frame.cast({"col1": PrimitiveType.BigInt, "col3": PrimitiveType.Double})

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->cast(@meta::pure::metamodel::relation::Relation<(col1:BigInt, 'col 2':String, col3:Double)>)"
        )

        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == expected_pure

    @_ALL_FRAME_FACTORIES
    def test_cast_pure_generation_precise_integer_types(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("a"),
            PrimitiveTdsColumn.integer_column("b"),
            PrimitiveTdsColumn.integer_column("c"),
            PrimitiveTdsColumn.integer_column("d"),
            PrimitiveTdsColumn.integer_column("e"),
            PrimitiveTdsColumn.integer_column("f"),
            PrimitiveTdsColumn.integer_column("g"),
            PrimitiveTdsColumn.integer_column("h"),
            PrimitiveTdsColumn.integer_column("i"),
        ]
        frame = frame_factory(columns)
        result = frame.cast({
            "a": PrimitiveType.TinyInt, "b": PrimitiveType.UTinyInt,
            "c": PrimitiveType.SmallInt, "d": PrimitiveType.USmallInt,
            "e": PrimitiveType.Int, "f": PrimitiveType.UInt,
            "g": PrimitiveType.BigInt, "h": PrimitiveType.UBigInt,
            "i": PrimitiveType.Integer,
        })

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->cast(@meta::pure::metamodel::relation::Relation<("
            "a:TinyInt, b:UTinyInt, c:SmallInt, d:USmallInt, "
            "e:Int, f:UInt, g:BigInt, h:UBigInt, i:Integer)>)"
        )

        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == expected_pure

    @_ALL_FRAME_FACTORIES
    def test_cast_pure_generation_precise_float_types(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        columns = [
            PrimitiveTdsColumn.float_column("x"),
            PrimitiveTdsColumn.float_column("y"),
            PrimitiveTdsColumn.float_column("z"),
        ]
        frame = frame_factory(columns)
        result = frame.cast({"x": PrimitiveType.Float4, "y": PrimitiveType.Double, "z": PrimitiveType.Float})

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->cast(@meta::pure::metamodel::relation::Relation<(x:Float4, y:Double, z:Float)>)"
        )

        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == expected_pure

    @_ALL_FRAME_FACTORIES
    def test_cast_pure_generation_precise_decimal_types(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        columns = [PrimitiveTdsColumn.decimal_column("a"), PrimitiveTdsColumn.decimal_column("b")]
        frame = frame_factory(columns)
        result = frame.cast({"a": (PrimitiveType.Numeric, 10, 2), "b": PrimitiveType.Decimal})

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->cast(@meta::pure::metamodel::relation::Relation<(a:Numeric(10, 2), b:Decimal)>)"
        )

        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == expected_pure

    @_ALL_FRAME_FACTORIES
    def test_cast_pure_generation_precise_string_types(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        columns = [PrimitiveTdsColumn.string_column("a"), PrimitiveTdsColumn.string_column("b")]
        frame = frame_factory(columns)
        result = frame.cast({"a": (PrimitiveType.Varchar, 200), "b": PrimitiveType.String})

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->cast(@meta::pure::metamodel::relation::Relation<(a:Varchar(200), b:String)>)"
        )

        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == expected_pure

    @_ALL_FRAME_FACTORIES
    def test_cast_pure_generation_precise_date_types(
            self, frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        columns = [PrimitiveTdsColumn.datetime_column("a"), PrimitiveTdsColumn.datetime_column("b")]
        frame = frame_factory(columns)
        result = frame.cast({"a": PrimitiveType.Timestamp, "b": PrimitiveType.DateTime})

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->cast(@meta::pure::metamodel::relation::Relation<(a:Timestamp, b:DateTime)>)"
        )

        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == expected_pure


class TestTdsFrameCastE2E:

    _COLUMNS = ["First Name", "Last Name", "Age", "Firm/Legal Name"]

    _AGE_PLUS_1_ROWS = [
        {"values": ["Peter", "Smith", 24, "Firm X"]},
        {"values": ["John", "Johnson", 23, "Firm X"]},
        {"values": ["John", "Hill", 13, "Firm X"]},
        {"values": ["Anthony", "Allen", 23, "Firm X"]},
        {"values": ["Fabrice", "Roberts", 35, "Firm A"]},
        {"values": ["Oliver", "Hill", 33, "Firm B"]},
        {"values": ["David", "Harris", 36, "Firm C"]},
    ]

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def _cast_age_and_add_one(
            self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]], target_type: PrimitiveType
    ) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.cast({"Age": target_type})
        frame['Age'] = frame['Age'] + 1
        expected = {"columns": self._COLUMNS, "rows": self._AGE_PLUS_1_ROWS}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_cast_integer(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self._cast_age_and_add_one(legend_test_server, PrimitiveType.Integer)

    def test_e2e_cast_tinyint(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self._cast_age_and_add_one(legend_test_server, PrimitiveType.TinyInt)

    def test_e2e_cast_utinyint(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self._cast_age_and_add_one(legend_test_server, PrimitiveType.UTinyInt)

    def test_e2e_cast_smallint(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self._cast_age_and_add_one(legend_test_server, PrimitiveType.SmallInt)

    def test_e2e_cast_usmallint(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self._cast_age_and_add_one(legend_test_server, PrimitiveType.USmallInt)

    def test_e2e_cast_int(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self._cast_age_and_add_one(legend_test_server, PrimitiveType.Int)

    def test_e2e_cast_uint(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self._cast_age_and_add_one(legend_test_server, PrimitiveType.UInt)

    def test_e2e_cast_bigint(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self._cast_age_and_add_one(legend_test_server, PrimitiveType.BigInt)

    def test_e2e_cast_ubigint(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self._cast_age_and_add_one(legend_test_server, PrimitiveType.UBigInt)

    def test_e2e_cast_number(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self._cast_age_and_add_one(legend_test_server, PrimitiveType.Number)

    def test_e2e_cast_varchar(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.cast({"First Name": (PrimitiveType.Varchar, 200)})
        frame["First Name"] = frame["First Name"].len()
        expected = {
            "columns": self._COLUMNS,
            "rows": [
                {"values": [5, "Smith", 23, "Firm X"]},
                {"values": [4, "Johnson", 22, "Firm X"]},
                {"values": [4, "Hill", 12, "Firm X"]},
                {"values": [7, "Allen", 22, "Firm X"]},
                {"values": [7, "Roberts", 34, "Firm A"]},
                {"values": [6, "Hill", 32, "Firm B"]},
                {"values": [5, "Harris", 35, "Firm C"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_cast_decimal_groupby(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.cast({"Age": PrimitiveType.Number})
        frame = frame.cast({"Age": PrimitiveType.Decimal})
        frame = frame.groupby("Firm/Legal Name")["Age"].aggregate("sum")
        expected = {
            "columns": ["Firm/Legal Name", "Age"],
            "rows": [
                {"values": ["Firm A", 34]},
                {"values": ["Firm B", 32]},
                {"values": ["Firm C", 35]},
                {"values": ["Firm X", 79]},
            ],
        }
        assert json.loads(frame.execute_frame_to_string())["result"] == expected

