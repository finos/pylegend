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
from pylegend.core.tds.tds_frame import PyLegendTdsFrame, FrameToSqlConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from pylegend.extensions.tds.legendql_api.frames.legendql_api_table_spec_input_frame import LegendQLApiTableSpecInputFrame
from pylegend.extensions.tds.legacy_api.frames.legacy_api_table_spec_input_frame import LegacyApiTableSpecInputFrame
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
    """Validation tests that apply to all three API flavours."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    @_ALL_FRAME_FACTORIES
    def test_cast_unknown_column_raises_error(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("col1")])
        with pytest.raises(ValueError) as r:
            frame.cast({"nonexistent": PrimitiveType.Float})
        assert "Column(s) not found in frame: ['nonexistent']" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_unknown_column_error_shows_available(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ])
        with pytest.raises(ValueError) as r:
            frame.cast({"bad_col": PrimitiveType.Float})
        assert "Available columns: ['col1', 'col2']" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_enum_column_raises_type_error(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([
            EnumTdsColumn("city", "my::CityType", ["Tier1", "Tier2"]),
            PrimitiveTdsColumn.integer_column("pop"),
        ])
        with pytest.raises(TypeError) as r:
            frame.cast({"city": PrimitiveType.String})
        assert "Cannot cast non-primitive column 'city'" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_string_to_integer_not_allowed(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.string_column("s")])
        with pytest.raises(ValueError) as r:
            frame.cast({"s": PrimitiveType.Integer})
        assert "Cannot cast column 's' from String to Integer" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_string_to_tinyint_not_allowed(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.string_column("s")])
        with pytest.raises(ValueError) as r:
            frame.cast({"s": PrimitiveType.TinyInt})
        assert "Cannot cast column 's' from String to TinyInt" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_string_to_boolean_not_allowed(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.string_column("s")])
        with pytest.raises(ValueError) as r:
            frame.cast({"s": PrimitiveType.Boolean})
        assert "Cannot cast column 's' from String to Boolean" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_string_to_date_not_allowed(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.string_column("s")])
        with pytest.raises(ValueError) as r:
            frame.cast({"s": PrimitiveType.DateTime})
        assert "Cannot cast column 's' from String to DateTime" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_boolean_to_integer_not_allowed(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.boolean_column("b")])
        with pytest.raises(ValueError) as r:
            frame.cast({"b": PrimitiveType.Integer})
        assert "Cannot cast column 'b' from Boolean to Integer" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_boolean_to_string_not_allowed(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.boolean_column("b")])
        with pytest.raises(ValueError) as r:
            frame.cast({"b": PrimitiveType.String})
        assert "Cannot cast column 'b' from Boolean to String" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_date_to_integer_not_allowed(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.date_column("d")])
        with pytest.raises(ValueError) as r:
            frame.cast({"d": PrimitiveType.Integer})
        assert "Cannot cast column 'd' from Date to Integer" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_date_to_string_not_allowed(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.date_column("d")])
        with pytest.raises(ValueError) as r:
            frame.cast({"d": PrimitiveType.String})
        assert "Cannot cast column 'd' from Date to String" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_numeric_to_boolean_not_allowed(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("n")])
        with pytest.raises(ValueError) as r:
            frame.cast({"n": PrimitiveType.Boolean})
        assert "Cannot cast column 'n' from Integer to Boolean" in r.value.args[0]

    @_ALL_FRAME_FACTORIES
    def test_cast_numeric_to_date_not_allowed(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("n")])
        with pytest.raises(ValueError) as r:
            frame.cast({"n": PrimitiveType.DateTime})
        assert "Cannot cast column 'n' from Integer to DateTime" in r.value.args[0]


class TestTdsFrameCastColumns:
    """Column-type tests that apply to all three API flavours."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    @_ALL_FRAME_FACTORIES
    def test_cast_single_column_integer_to_float(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ])
        result = frame.cast({"col1": PrimitiveType.Float})
        assert "TdsColumn(Name: col1, Type: Float)" == str(result.columns()[0])
        assert "TdsColumn(Name: col2, Type: String)" == str(result.columns()[1])

    @_ALL_FRAME_FACTORIES
    def test_cast_multiple_columns(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
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
    def test_cast_numeric_to_numeric_within_family(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("n")])
        assert "TdsColumn(Name: n, Type: TinyInt)" == str(frame.cast({"n": PrimitiveType.TinyInt}).columns()[0])
        assert "TdsColumn(Name: n, Type: BigInt)" == str(frame.cast({"n": PrimitiveType.BigInt}).columns()[0])
        assert "TdsColumn(Name: n, Type: Double)" == str(frame.cast({"n": PrimitiveType.Double}).columns()[0])
        assert "TdsColumn(Name: n, Type: Decimal)" == str(frame.cast({"n": PrimitiveType.Decimal}).columns()[0])
        assert "TdsColumn(Name: n, Type: Numeric)" == str(frame.cast({"n": PrimitiveType.Numeric}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_numeric_to_string_allowed(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("n")])
        assert "TdsColumn(Name: n, Type: String)" == str(frame.cast({"n": PrimitiveType.String}).columns()[0])
        assert "TdsColumn(Name: n, Type: Varchar)" == str(frame.cast({"n": PrimitiveType.Varchar}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_string_to_string_within_family(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.string_column("s")])
        assert "TdsColumn(Name: s, Type: Varchar)" == str(frame.cast({"s": PrimitiveType.Varchar}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_date_within_family(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.date_column("d")])
        assert "TdsColumn(Name: d, Type: DateTime)" == str(frame.cast({"d": PrimitiveType.DateTime}).columns()[0])
        assert "TdsColumn(Name: d, Type: StrictDate)" == str(frame.cast({"d": PrimitiveType.StrictDate}).columns()[0])
        assert "TdsColumn(Name: d, Type: Timestamp)" == str(frame.cast({"d": PrimitiveType.Timestamp}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_boolean_to_boolean(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.boolean_column("b")])
        assert "TdsColumn(Name: b, Type: Boolean)" == str(frame.cast({"b": PrimitiveType.Boolean}).columns()[0])

    @_ALL_FRAME_FACTORIES
    def test_cast_same_type_is_noop(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("col1")])
        assert "TdsColumn(Name: col1, Type: Integer)" == str(
            frame.cast({"col1": PrimitiveType.Integer}).columns()[0]
        )

    @_ALL_FRAME_FACTORIES
    def test_cast_empty_map_returns_copy(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ])
        result = frame.cast({})
        assert "TdsColumn(Name: col1, Type: Integer)" == str(result.columns()[0])
        assert "TdsColumn(Name: col2, Type: String)" == str(result.columns()[1])

    @_ALL_FRAME_FACTORIES
    def test_cast_preserves_column_order(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
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
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("col1")])
        assert "TdsColumn(Name: col1, Type: Integer)" == str(frame.columns()[0])
        frame.cast({"col1": PrimitiveType.Float})
        assert "TdsColumn(Name: col1, Type: Integer)" == str(frame.columns()[0])


class TestTdsFrameCastSqlGeneration:
    """SQL generation tests that apply to all three API flavours."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    @_ALL_FRAME_FACTORIES
    def test_cast_single_column_sql(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ])
        result = frame.cast({"col1": PrimitiveType.BigInt})
        expected = dedent('''\
            SELECT
                CAST("root".col1 AS BIGINT) AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"''')
        assert result.to_sql_query(FrameToSqlConfig()) == expected

    @_ALL_FRAME_FACTORIES
    def test_cast_multiple_columns_sql(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
        ])
        result = frame.cast({"col1": PrimitiveType.Double, "col3": PrimitiveType.Varchar})
        expected = dedent('''\
            SELECT
                CAST("root".col1 AS DOUBLE PRECISION) AS "col1",
                "root".col2 AS "col2",
                CAST("root".col3 AS VARCHAR) AS "col3"
            FROM
                test_schema.test_table AS "root"''')
        assert result.to_sql_query(FrameToSqlConfig()) == expected

    @_ALL_FRAME_FACTORIES
    def test_cast_empty_map_sql_unchanged(
            self,
            frame_factory: PyLegendCallable[[PyLegendSequence[TdsColumn]], PyLegendTdsFrame]
    ) -> None:
        frame = frame_factory([PrimitiveTdsColumn.integer_column("col1")])
        result = frame.cast({})
        expected = dedent('''\
            SELECT
                "root".col1 AS "col1"
            FROM
                test_schema.test_table AS "root"''')
        assert result.to_sql_query(FrameToSqlConfig()) == expected


class TestTdsFrameCastE2E:
    """End-to-end tests (pandas API only, since it supports execute_frame_to_string)."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_e2e_cast(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # Cast Age from Integer to BigInt, then add 1
        frame = frame.cast({"Age": PrimitiveType.BigInt})
        frame['Age'] = frame['Age'] + 1  # type: ignore
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Smith", 24, "Firm X"]},
                {"values": ["John", "Johnson", 23, "Firm X"]},
                {"values": ["John", "Hill", 13, "Firm X"]},
                {"values": ["Anthony", "Allen", 23, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 35, "Firm A"]},
                {"values": ["Oliver", "Hill", 33, "Firm B"]},
                {"values": ["David", "Harris", 36, "Firm C"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
