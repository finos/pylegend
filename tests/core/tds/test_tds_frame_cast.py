# Copyright 2025 Goldman Sachs
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

import pytest

from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import (
    PrimitiveTdsColumn,
    PrimitiveType,
    EnumTdsColumn,
)
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend.core.request.legend_client import LegendClient


class TestTdsFrameCast:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_cast_unknown_column_raises_error(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"nonexistent": PrimitiveType.Float})
        assert "Column(s) not found in frame: ['nonexistent']" in r.value.args[0]

    def test_cast_unknown_column_error_shows_available(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"bad_col": PrimitiveType.Float})
        assert "Available columns: ['col1', 'col2']" in r.value.args[0]

    def test_cast_enum_column_raises_type_error(self) -> None:
        columns = [
            EnumTdsColumn("city", "my::CityType", ["Tier1", "Tier2"]),
            PrimitiveTdsColumn.integer_column("pop"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.cast({"city": PrimitiveType.String})
        assert "Cannot cast non-primitive column 'city'" in r.value.args[0]

    def test_cast_string_to_integer_not_allowed(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("s")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"s": PrimitiveType.Integer})
        assert "Cannot cast column 's' from String to Integer" in r.value.args[0]

    def test_cast_string_to_tinyint_not_allowed(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("s")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"s": PrimitiveType.TinyInt})
        assert "Cannot cast column 's' from String to TinyInt" in r.value.args[0]

    def test_cast_string_to_boolean_not_allowed(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("s")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"s": PrimitiveType.Boolean})
        assert "Cannot cast column 's' from String to Boolean" in r.value.args[0]

    def test_cast_string_to_date_not_allowed(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("s")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"s": PrimitiveType.DateTime})
        assert "Cannot cast column 's' from String to DateTime" in r.value.args[0]

    def test_cast_boolean_to_integer_not_allowed(self) -> None:
        columns = [PrimitiveTdsColumn.boolean_column("b")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"b": PrimitiveType.Integer})
        assert "Cannot cast column 'b' from Boolean to Integer" in r.value.args[0]

    def test_cast_boolean_to_string_not_allowed(self) -> None:
        columns = [PrimitiveTdsColumn.boolean_column("b")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"b": PrimitiveType.String})
        assert "Cannot cast column 'b' from Boolean to String" in r.value.args[0]

    def test_cast_date_to_integer_not_allowed(self) -> None:
        columns = [PrimitiveTdsColumn.date_column("d")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"d": PrimitiveType.Integer})
        assert "Cannot cast column 'd' from Date to Integer" in r.value.args[0]

    def test_cast_date_to_string_not_allowed(self) -> None:
        columns = [PrimitiveTdsColumn.date_column("d")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"d": PrimitiveType.String})
        assert "Cannot cast column 'd' from Date to String" in r.value.args[0]

    def test_cast_numeric_to_boolean_not_allowed(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("n")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"n": PrimitiveType.Boolean})
        assert "Cannot cast column 'n' from Integer to Boolean" in r.value.args[0]

    def test_cast_numeric_to_date_not_allowed(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("n")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.cast({"n": PrimitiveType.DateTime})
        assert "Cannot cast column 'n' from Integer to DateTime" in r.value.args[0]

    def test_cast_single_column_integer_to_float(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        result = frame.cast({"col1": PrimitiveType.Float})
        assert "TdsColumn(Name: col1, Type: Float)" == str(result[0])
        assert "TdsColumn(Name: col2, Type: String)" == str(result[1])

    def test_cast_multiple_columns(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        result = frame.cast({"col1": PrimitiveType.BigInt, "col3": PrimitiveType.Double})
        assert "TdsColumn(Name: col1, Type: BigInt)" == str(result[0])
        assert "TdsColumn(Name: col2, Type: String)" == str(result[1])
        assert "TdsColumn(Name: col3, Type: Double)" == str(result[2])

    def test_cast_numeric_to_numeric_within_family(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("n")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        assert "TdsColumn(Name: n, Type: TinyInt)" == str(frame.cast({"n": PrimitiveType.TinyInt})[0])
        assert "TdsColumn(Name: n, Type: BigInt)" == str(frame.cast({"n": PrimitiveType.BigInt})[0])
        assert "TdsColumn(Name: n, Type: Double)" == str(frame.cast({"n": PrimitiveType.Double})[0])
        assert "TdsColumn(Name: n, Type: Decimal)" == str(frame.cast({"n": PrimitiveType.Decimal})[0])
        assert "TdsColumn(Name: n, Type: Numeric)" == str(frame.cast({"n": PrimitiveType.Numeric})[0])

    def test_cast_numeric_to_string_allowed(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("n")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        assert "TdsColumn(Name: n, Type: String)" == str(frame.cast({"n": PrimitiveType.String})[0])
        assert "TdsColumn(Name: n, Type: Varchar)" == str(frame.cast({"n": PrimitiveType.Varchar})[0])

    def test_cast_string_to_string_within_family(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("s")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        assert "TdsColumn(Name: s, Type: Varchar)" == str(frame.cast({"s": PrimitiveType.Varchar})[0])

    def test_cast_date_within_family(self) -> None:
        columns = [PrimitiveTdsColumn.date_column("d")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        assert "TdsColumn(Name: d, Type: DateTime)" == str(frame.cast({"d": PrimitiveType.DateTime})[0])
        assert "TdsColumn(Name: d, Type: StrictDate)" == str(frame.cast({"d": PrimitiveType.StrictDate})[0])
        assert "TdsColumn(Name: d, Type: Timestamp)" == str(frame.cast({"d": PrimitiveType.Timestamp})[0])

    def test_cast_boolean_to_boolean(self) -> None:
        columns = [PrimitiveTdsColumn.boolean_column("b")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        assert "TdsColumn(Name: b, Type: Boolean)" == str(frame.cast({"b": PrimitiveType.Boolean})[0])

    def test_cast_same_type_is_noop(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        assert "TdsColumn(Name: col1, Type: Integer)" == str(frame.cast({"col1": PrimitiveType.Integer})[0])

    def test_cast_empty_map_returns_copy(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        result = frame.cast({})
        assert "TdsColumn(Name: col1, Type: Integer)" == str(result[0])
        assert "TdsColumn(Name: col2, Type: String)" == str(result[1])

    def test_cast_preserves_column_order(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("a"),
            PrimitiveTdsColumn.string_column("b"),
            PrimitiveTdsColumn.float_column("c"),
            PrimitiveTdsColumn.boolean_column("d"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        result = frame.cast({"c": PrimitiveType.Double, "a": PrimitiveType.BigInt})
        assert "TdsColumn(Name: a, Type: BigInt)" == str(result[0])
        assert "TdsColumn(Name: b, Type: String)" == str(result[1])
        assert "TdsColumn(Name: c, Type: Double)" == str(result[2])
        assert "TdsColumn(Name: d, Type: Boolean)" == str(result[3])

    def test_cast_does_not_mutate_original_frame(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        assert "TdsColumn(Name: col1, Type: Integer)" == str(frame.columns()[0])
        frame.cast({"col1": PrimitiveType.Float})
        assert "TdsColumn(Name: col1, Type: Integer)" == str(frame.columns()[0])

    def test_e2e_cast(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # Cast Age from Integer to BigInt, then add 1
        frame.cast({"Age": PrimitiveType.BigInt})
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
