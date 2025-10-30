# Copyright 2025 gold sick
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
import re
from textwrap import dedent

import pytest

from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile

class TestFilterFunction:

    # @pytest.fixture(autouse=True)
    # def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
    #     self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_filter_function_error_on_mutual_exclusion(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("gold"),
            PrimitiveTdsColumn.float_column("sick")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # With axis
        with pytest.raises(TypeError) as v:
            frame = frame.filter(axis=1, like="sac", items=["col1", "col2"])
        assert v.value.args[0] == "Keyword arguments `items`, `like`, or `regex` are mutually exclusive"

        # Without axis
        with pytest.raises(TypeError) as v:
            frame = frame.filter(like="sac", items=["col1", "col2"])
        assert v.value.args[0] == "Keyword arguments `items`, `like`, or `regex` are mutually exclusive"

    def test_filter_function_error_on_no_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("gold"),
            PrimitiveTdsColumn.float_column("sick")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # With axis
        with pytest.raises(TypeError) as v:
            frame = frame.filter(axis=1)
        assert v.value.args[0] == "Must pass either `items`, `like`, or `regex`"

        # Without axis
        with pytest.raises(TypeError) as v:
            frame = frame.filter()
        assert v.value.args[0] == "Must pass either `items`, `like`, or `regex`"

    def test_filter_function_error_on_axis_paramter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("gold"),
            PrimitiveTdsColumn.float_column("sick")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Parameter Mismatch
        with pytest.raises(ValueError) as v:
            frame = frame.filter(items=["col1", "gold"], axis=0)
        assert v.value.args[0] == f"Invalid axis value: 0. Expected 1 or 'columns'"
        with pytest.raises(ValueError) as v:
            frame = frame.filter(items=["col1", "gold"], axis='index')
        assert v.value.args[0] == f"Invalid axis value: index. Expected 1 or 'columns'"

        # Type mismatch
        with pytest.raises(ValueError) as v:
            frame = frame.filter(items=["col1", "gold"], axis=2.5)
        assert v.value.args[0] == f"Invalid axis value: 2.5. Expected 1 or 'columns'"

    def test_filter_function_error_on_items_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("gold"),
            PrimitiveTdsColumn.float_column("sick")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Parameter mismatch
        with pytest.raises(ValueError) as v:
            frame.filter(items=["gs", "sick", "col1", "saints"])
        assert v.value.args[0] == "Columns ['gs', 'saints'] in `filter` items list do not exist. Available: ['col1', 'col2', 'col3', 'gold', 'sick']"

        # Type mismatch
        with pytest.raises(TypeError) as v:
            frame = frame.filter(items="pope")
        assert v.value.args[0] == "Index(...) must be called with a collection, got 'pope'"

    def test_filter_function_error_on_like_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("gold"),
            PrimitiveTdsColumn.float_column("sick")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Parameter mismatch
        with pytest.raises(ValueError) as v:
            frame = frame.filter(like="zz")
        assert v.value.args[0] == "No columns match the pattern 'zz'. Available: ['col1', 'col2', 'col3', 'gold', 'sick']"

        # Type mismatch
        with pytest.raises(TypeError) as v:
            t1 = ["21", "step", 99]
            frame = frame.filter(like=t1)
        assert v.value.args[0] == f"'like' must be a string, got {type(t1)}"

    def test_filter_function_error_on_regex_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("gold"),
            PrimitiveTdsColumn.float_column("sick")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Parameter Mismatch
        with pytest.raises(ValueError) as v:
            frame = frame.filter(regex="$z")
        assert v.value.args[0] == "No columns match the regex '$z'. Available: ['col1', 'col2', 'col3', 'gold', 'sick']"

        # Type mismatch
        with pytest.raises(TypeError) as v:
            t1 = ["21", "step", 99]
            frame = frame.filter(regex=t1)
        assert v.value.args[0] == f"'regex' must be a string, got {type(t1)}"

    def test_filter_function_on_items_parameter_match(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("gold"),
            PrimitiveTdsColumn.float_column("sick")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.filter(items=["col2", "gold", "col1"]).filter(items=['col1'])
        expected = '''\
            SELECT
                "root".col1 AS "col1"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col2, gold, col1])
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               '#Table(test_schema.test_table)#->select(~[col2, gold, col1])->select(~[col1])'

    def test_filter_function_on_like_parameter_match(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("gold"),
            PrimitiveTdsColumn.float_column("sick")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Middle match
        newframe = frame.filter(like="ol")
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                "root".col3 AS "col3",
                "root".gold AS "gold"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1, col2, col3, gold])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[col1, col2, col3, gold])')

        # Start Match
        newframe = frame.filter(like="co")
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                "root".col3 AS "col3"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1, col2, col3])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[col1, col2, col3])')

        # End Match
        newframe = frame.filter(like="hs")
        expected = '''\
            SELECT
                "root".sick AS "sick"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[sick])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[sick])')

    def test_filter_function_on_regex_parameter_match(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("gold"),
            PrimitiveTdsColumn.float_column("sick"),
            PrimitiveTdsColumn.float_column("saints"),
            PrimitiveTdsColumn.float_column("sao")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # string input
        newframe = frame.filter(regex="an$")
        expected = '''\
            SELECT
                "root".gold AS "gold"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[gold])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[gold])')

        newframe = frame.filter(regex="sa.*s")
        expected = '''\
            SELECT
                "root".sick AS "sick",
                "root".saints AS "saints"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[sick, saints])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[sick, saints])')

    def test_filter_function_on_axis_paramter_match(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("gold"),
            PrimitiveTdsColumn.float_column("sick")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Axis as int
        newframe = frame.filter(items=["col1", "gold"], axis=1)
        expected = '''\
                SELECT
                    "root".col1 AS "col1",
                    "root".gold AS "gold"
                FROM
                    test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1, gold])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[col1, gold])')

        # Axis as str
        newframe = frame.filter(like="old", axis='columns')
        expected = '''\
                SELECT
                    "root".gold AS "gold"
                FROM
                    test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[gold])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[gold])')

        newframe = frame.filter(regex="old", axis='columns')
        expected = '''\
                        SELECT
                            "root".gold AS "gold"
                        FROM
                            test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[gold])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[gold])')

    def test_filter_function_nested(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("gold"),
            PrimitiveTdsColumn.float_column("sick")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Single line nest
        newframe = frame.filter(items=["col1", "sick", "col2"]).filter(like="ol").filter(regex="1$")
        expected = '''\
            SELECT
                "root".col1 AS "col1"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1, sick, col2])
              ->select(~[col1, col2])
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
                ('#Table(test_schema.test_table)#->select(~[col1, sick, col2])->select(~[col1, col2])->select(~[col1])')

        # Multi line nest
        newframe = frame.filter(items=["col1", "sick", "col2"])
        newframe = newframe.filter(like="ol")
        newframe = newframe.filter(regex="1$")

        expected = '''\
            SELECT
                "root".col1 AS "col1"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1, sick, col2])
              ->select(~[col1, col2])
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[col1, sick, col2])->select(~[col1, col2])->select(~[col1])')

    def test_e2e_filter_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) \
            -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        # Items filter
        newframe = frame.filter(items=["First Name", "Age"])
        expected_items = {
            "columns": ["First Name", "Age"],
            "rows": [
                {"values": ["Peter", 23]},
                {"values": ["John", 22]},
                {"values": ["John", 12]},
                {"values": ["Anthony", 22]},
                {"values": ["Fabrice", 34]},
                {"values": ["Oliver", 32]},
                {"values": ["David", 35]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected_items

        # Like filter
        frame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        newframe = frame.filter(like="Name")
        expected_like = {
            "columns": ["First Name", "Last Name", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Smith", "Firm X"]},
                {"values": ["John", "Johnson", "Firm X"]},
                {"values": ["John", "Hill", "Firm X"]},
                {"values": ["Anthony", "Allen", "Firm X"]},
                {"values": ["Fabrice", "Roberts", "Firm A"]},
                {"values": ["Oliver", "Hill", "Firm B"]},
                {"values": ["David", "Harris", "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected_like

        # Regex filter
        frame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        newframe = frame.filter(regex="^F.*Name$")
        expected_regex = {
            "columns": ["First Name", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Firm X"]},
                {"values": ["John", "Firm X"]},
                {"values": ["John", "Firm X"]},
                {"values": ["Anthony", "Firm X"]},
                {"values": ["Fabrice", "Firm A"]},
                {"values": ["Oliver", "Firm B"]},
                {"values": ["David", "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected_regex

    def test_e2e_filter_function_nested(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        newframe = (
            frame
            .filter(items=["First Name", "Age", "Firm/Legal Name"])
            .filter(like="Name")
            .filter(regex="^F.*Name$")
        )
        expected_nested = {
            "columns": ["First Name", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Firm X"]},
                {"values": ["John", "Firm X"]},
                {"values": ["John", "Firm X"]},
                {"values": ["Anthony", "Firm X"]},
                {"values": ["Fabrice", "Firm A"]},
                {"values": ["Oliver", "Firm B"]},
                {"values": ["David", "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected_nested
