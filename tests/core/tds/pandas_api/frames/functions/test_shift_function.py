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
from textwrap import dedent

import pytest
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_relation_person_service_frame_pandas_api


USE_LEGEND_ENGINE: bool = False
TEST_PURE: bool = False


class TestErrorsOnBaseFrame:
    def test_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.shift(axis=1)

        expected_msg = "The 'axis' argument of the shift function must be 0 or 'index', but got: axis=1"
        assert v.value.args[0] == expected_msg

    def test_frequency_not_none(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.shift(freq='D')

        expected_msg = "The 'freq' argument of the shift function is not supported, but got: freq='D'"
        assert v.value.args[0] == expected_msg

    def test_suffix_with_int_periods(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.shift(periods=-1, suffix='abcd')

        expected_msg = "Cannot specify the 'suffix' argument of the shift function if the 'periods' argument is an int."
        assert v.value.args[0] == expected_msg

    def test_fill_value_with_incompatible_types(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("int_col"),
                   PrimitiveTdsColumn.string_column("str_col"),
                   PrimitiveTdsColumn.boolean_column("bool_col"),
                   PrimitiveTdsColumn.date_column("date_col"),
                   PrimitiveTdsColumn.datetime_column("datetime_col"),
                   PrimitiveTdsColumn.strictdate_column("strictdate_col"),
                   PrimitiveTdsColumn.float_column("float_col"),
                   PrimitiveTdsColumn.number_column("num_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.shift(fill_value="default_fill")

        expected_msg = (
            "Invalid 'fill_value' argument for the shift function. fill_value argument: default_fill (type: str) "
            "cannot be applied to the following column(s): "
            "['TdsColumn(Name: int_col, Type: Integer)', 'TdsColumn(Name: bool_col, Type: Boolean)', 'TdsColumn(Name: date_col, Type: Date)', 'TdsColumn(Name: datetime_col, Type: DateTime)', 'TdsColumn(Name: strictdate_col, Type: StrictDate)', 'TdsColumn(Name: float_col, Type: Float)', 'TdsColumn(Name: num_col, Type: Number)']"
            " because of type mismatch.")  # noqa: E501
        assert v.value.args[0] == expected_msg

        with pytest.raises(NotImplementedError) as v:
            frame.shift(fill_value=-1)

        expected_msg = (
            "Invalid 'fill_value' argument for the shift function. fill_value argument: -1 (type: int) "
            "cannot be applied to the following column(s): "
            "['TdsColumn(Name: str_col, Type: String)', 'TdsColumn(Name: bool_col, Type: Boolean)', 'TdsColumn(Name: date_col, Type: Date)', 'TdsColumn(Name: datetime_col, Type: DateTime)', 'TdsColumn(Name: strictdate_col, Type: StrictDate)']"
            " because of type mismatch.")  # noqa: E501
        assert v.value.args[0] == expected_msg


class TestErrorsOnGroupbyFrame:
    def test_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("group_col").shift(axis=1)

        expected_msg = "The 'axis' argument of the shift function must be 0 or 'index', but got: axis=1"
        assert v.value.args[0] == expected_msg

    def test_frequency_not_none(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("group_col").shift(freq='D')

        expected_msg = "The 'freq' argument of the shift function is not supported, but got: freq='D'"
        assert v.value.args[0] == expected_msg

    def test_fill_value_with_incompatible_types(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("group_col")["val_col"].shift(fill_value="default_fill")

        expected_msg = (
            "Invalid 'fill_value' argument for the shift function. "
            "fill_value argument: 'default_fill' (type: str) cannot be applied to the column: 'val_col' (type: Integer)"
            " because of type mismatch")
        assert v.value.args[0] == expected_msg
    
    def test_fill_value_with_incompatible_types_without_column_selection(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("group_col").shift(fill_value="default_fill")

        expected_msg = (
            "Invalid 'fill_value' argument for the shift function. "
            "fill_value argument: 'default_fill' (type: str) cannot be applied to the columns: "
            "['val_col' (type: Integer), 'random_col' (type: Integer)]"
            " because of type mismatch")
        assert v.value.args[0] == expected_msg

    def test_fill_value_with_incompatible_types_with_selected_groupby(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("group_col")["group_col"].shift(fill_value="default_fill")

        expected_msg = (
            "Invalid 'fill_value' argument for the shift function. "
            "fill_value argument: 'default_fill' (type: str) cannot be applied to the column: 'group_col' (type: Integer)"
            " because of type mismatch")
        assert v.value.args[0] == expected_msg


class TestUsageOnBaseFrame:
    if USE_LEGEND_ENGINE:
        @pytest.fixture(autouse=True)
        def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
            self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_no_arguments(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift()

        expected = '''
            SELECT
                "root"."col1__internal_sql_column_name__" AS "col1"
            FROM
                (
                    SELECT
                        lag("root"."col1") OVER (ORDER BY 0) AS "col1__internal_sql_column_name__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__internal_pure_col_name__:{p,w,r | $p->rank($w, $r)})
              ->project(~[col1:p|$p.col1__internal_pure_col_name__])
        '''
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected


class TestEndToEndUsage:
    def test_simple_usage(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]):
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.shift()
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
