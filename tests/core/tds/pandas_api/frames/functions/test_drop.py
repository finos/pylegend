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
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend.core.request.legend_client import LegendClient


class TestDropFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_drop_function_error_on_mutual_exclusion(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # With axis
        with pytest.raises(ValueError) as v:
            frame = frame.drop(labels=["col1", "col2"], columns=["col1", "col2"], axis=1)
        assert v.value.args[0] == "Cannot specify both 'labels' and 'columns'"

        # Without axis
        with pytest.raises(ValueError) as v:
            frame = frame.drop(labels=["col1", "col2"], columns=["col1", "col2"])
        assert v.value.args[0] == "Cannot specify both 'labels' and 'columns'"

    def test_drop_function_error_on_no_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # With axis
        with pytest.raises(ValueError) as v:
            frame = frame.drop(axis=1)
        assert v.value.args[0] == "Need to specify at least one of 'labels' or 'columns'"

        # Without axis
        with pytest.raises(ValueError) as v:
            frame = frame.drop()
        assert v.value.args[0] == "Need to specify at least one of 'labels' or 'columns'"

    def test_drop_function_error_on_level_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(NotImplementedError) as v:
            frame = frame.drop(columns=["col1", "col2"], level=0)
        assert v.value.args[0] == "'level' parameter is not supported for 'drop' function in PandasApi"

    def test_drop_function_error_on_index_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(NotImplementedError) as v:
            frame = frame.drop(index=[0, 1])
        assert v.value.args[0] == "'index' parameter is not supported for 'drop' function in PandasApi"

    def test_drop_function_error_on_inplace_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(NotImplementedError) as v:
            frame = frame.drop(columns=["col1", "col2"], inplace=False)
        assert v.value.args[0] == "Only inplace=True is supported. Got inplace=False"

    def test_drop_function_error_on_axis_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Axis = 0
        with pytest.raises(NotImplementedError) as v:
            frame = frame.drop(columns=["col1", "col2"], axis=0)
        assert v.value.args[0] == "Axis 0 is not supported for 'drop' function in PandasApi"
        # Axis invalid value
        with pytest.raises(ValueError) as v:  # type: ignore
            frame = frame.drop(columns=["col1", "col2"], axis=2)
        assert v.value.args[0] == "No axis named 2 for object type Tds DataFrame"

    def test_drop_function_error_on_labels_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Type Error
        with pytest.raises(TypeError) as v:
            frame.drop(labels=lambda x: x)  # type: ignore
        assert v.value.args[0] == "Unsupported type for columns: <class 'function'>"
        # Key Error
        with pytest.raises(KeyError) as v:  # type: ignore
            frame.drop(labels=["col6", "col7"]).to_sql_query()
        assert v.value.args[0] == "['col6', 'col7'] not found in axis"
        # Key Error with Axis
        with pytest.raises(KeyError) as v:  # type: ignore
            frame.drop(labels=["col6", "col7"], axis=1).to_pure_query()
        assert v.value.args[0] == "['col6', 'col7'] not found in axis"

    def test_drop_function_error_on_columns_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Type Error
        with pytest.raises(TypeError) as v:
            frame.drop(columns=lambda x: x)  # type: ignore
        assert v.value.args[0] == "Unsupported type for columns: <class 'function'>"
        # Key Error
        with pytest.raises(KeyError) as v:  # type: ignore
            frame.drop(columns=["col6", "col7"]).to_sql_query()
        assert v.value.args[0] == "['col6', 'col7'] not found in axis"
        # Key Error with Axis
        with pytest.raises(KeyError) as v:  # type: ignore
            frame.drop(columns=["col6", "col7"], axis=1).to_pure_query()
        assert v.value.args[0] == "['col6', 'col7'] not found in axis"

    def test_drop_function_on_labels_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Only labels (single column)
        newframe = frame.drop(labels="col1")
        expected = '''\
                   SELECT
                       "root".col2 AS "col2",
                       "root".col3 AS "col3"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col2, col3])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col2, col3])"

        # With axis (multiple columns)
        newframe = frame.drop(labels=["col2", "col3"], axis=1)
        expected = '''\
                   SELECT
                       "root".col1 AS "col1"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col1])"

        # With inplace (multiple columns)
        newframe = frame.drop(labels=["col2", "col3"], axis=1, inplace=True)
        expected = '''\
                   SELECT
                       "root".col1 AS "col1"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col1])"

    def test_drop_function_on_columns_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Only columns (single column)
        newframe = frame.drop(columns="col1")
        expected = '''\
                   SELECT
                       "root".col2 AS "col2",
                       "root".col3 AS "col3"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col2, col3])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col2, col3])"

        # With axis (multiple columns)
        newframe = frame.drop(columns=["col2", "col3"], axis=1)
        expected = '''\
                   SELECT
                       "root".col1 AS "col1"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col1])"

        # With inplace (multiple columns)
        newframe = frame.drop(columns=["col2", "col3"], axis=1, inplace=True)
        expected = '''\
                   SELECT
                       "root".col1 AS "col1"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col1])"

    def test_drop_function_on_input_types(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Single string
        newframe = frame.drop(columns="col1")
        expected = '''\
                   SELECT
                       "root".col2 AS "col2",
                       "root".col3 AS "col3"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col2, col3])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col2, col3])"

        # List of strings
        newframe = frame.drop(columns=["col2", "col3"])
        expected = '''\
                   SELECT
                       "root".col1 AS "col1"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col1])"

        # Tuple of strings
        newframe = frame.drop(columns=("col2", "col3"))  # type: ignore
        expected = '''\
                   SELECT
                       "root".col1 AS "col1"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col1])"

        # Set of strings
        newframe = frame.drop(columns={"col2", "col3"})
        expected = '''\
                     SELECT
                         "root".col1 AS "col1"
                     FROM
                         test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col1])"

        # Dict keys
        newframe = frame.drop(columns={"col2": 1, "col3": 2})  # type: ignore
        expected = '''\
                   SELECT
                       "root".col1 AS "col1"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col1])"

    def test_drop_function_chained(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        # Single line chain
        newframe = (
            frame
            .drop(columns=["col1", "col3"])
            .drop(columns=["col4"])
        )
        expected = '''\
                   SELECT
                       "root".col2 AS "col2",
                       "root".col5 AS "col5"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col2, col4, col5])
              ->select(~[col2, col5])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col2, col4, col5])->select(~[col2, col5])"

        # Multiline chain
        newframe = frame.drop(columns=["col1", "col3"])
        newframe = newframe.drop(columns=["col4"])
        expected = '''\
                   SELECT
                       "root".col2 AS "col2",
                       "root".col5 AS "col5"
                   FROM
                       test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col2, col4, col5])
              ->select(~[col2, col5])'''
        )
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(pretty=False), self.legend_client) == \
               "#Table(test_schema.test_table)#->select(~[col2, col4, col5])->select(~[col2, col5])"

    def test_e2e_drop_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        # Drop single column
        newframe = frame.drop(columns=["Age"])
        expected = {
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
        assert json.loads(res)["result"] == expected

        # Drop multiple columns
        newframe = frame.drop(columns=["Last Name", "Firm/Legal Name"])
        expected_multi = {
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
        assert json.loads(res)["result"] == expected_multi

    def test_e2e_drop_function_nested(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        newframe = (
            frame
            .drop(columns=["Firm/Legal Name"])
            .drop(columns=["Age"])
        )
        expected_nested = {
            "columns": ["First Name", "Last Name"],
            "rows": [
                {"values": ["Peter", "Smith"]},
                {"values": ["John", "Johnson"]},
                {"values": ["John", "Hill"]},
                {"values": ["Anthony", "Allen"]},
                {"values": ["Fabrice", "Roberts"]},
                {"values": ["Oliver", "Hill"]},
                {"values": ["David", "Harris"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected_nested
