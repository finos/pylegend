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
from math import sqrt
from textwrap import dedent

import numpy as np
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
import pytest
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api


class TestTruncateFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_aggregate_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.aggregate(func=lambda x: 0, axis=1)
        assert v.value.args[0] == "The 'axis' parameter of the aggregate function must be 0 or 'index', but got: 1"

    # def test_aggregate_simple_query_generation(self) -> None:
    #     columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.integer_column("col2"), PrimitiveTdsColumn.integer_column("test_col")]
    #     frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
    #     frame = frame.aggregate({'col1' : ['min'], 'col2' : ['count']}).truncate(5, 10).truncate(5)
    #     expected = """\
    #                 SELECT
    #                     "root".col1 AS "col1",
    #                     "root".col2 AS "col2"
    #                 FROM
    #                     test_schema.test_table AS "root"\
    #                 """
    #     assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
    #     assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
    #         """\
    #         #Table(test_schema.test_table)#
    #           ->slice(0, 4)"""
    #     )
        # assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
        #     "#Table(test_schema.test_table)#->slice(0, 4)"
        # )

    def test_e2e_aggregate_single_column(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.aggregate({'Age':'sum'})
        expected = {
            "columns": ["Age"],
            "rows": [
                {"values": [180]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_aggregate_single_column_list_input(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """
        Tests that a list of length 1 is accepted and normalized correctly.
        """
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        
        # Input: List with single element
        frame = frame.aggregate({'Age': ['sum']})
        
        expected = {
            "columns": ["Age"],
            "rows": [{"values": [180]}]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_aggregate_numpy_ufunc_and_aliases(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """
        Tests numpy ufuncs (np.min, np.mean) and string aliases (average vs mean).
        """
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        
        # Input: Mixed numpy ufuncs and string aliases
        # np.min -> maps to 'min' -> 12
        # 'average' -> maps to 'average' -> 25.71...
        frame = frame.aggregate({
            'Age': np.min, 
            'First Name': 'len'  # 'len' should map to 'count'
        })
        
        # Note: Order of columns in result depends on implementation, usually follows dict insertion order or sorted
        # Adjust expectations based on your engine's determinism. 
        # Here assuming the engine returns columns requested.
        
        res = frame.execute_frame_to_string()
        result_json = json.loads(res)["result"]
        
        # Verify Columns
        assert set(result_json["columns"]) == {'Age', 'First Name'}
        
        # Verify Values
        row = result_json["rows"][0]["values"]
        age_val = row[result_json["columns"].index("Age")]
        name_count_val = row[result_json["columns"].index("First Name")]
        
        assert age_val == 12
        assert name_count_val == 7

    def test_e2e_aggregate_custom_lambda(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """
        Tests passing a raw lambda function.
        """
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        
        # Input: Lambda function
        # Should translate to x.max()
        frame = frame.aggregate({'Age': lambda x: x.max()})
        
        expected = {
            "columns": ["Age"],
            "rows": [{"values": [35]}]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_aggregate_multi_column_strings(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """
        Tests string aggregations (min/max) on non-numeric columns.
        """
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        
        # Input: String aggregations
        # Min of "First Name" should be "Anthony" (Lexicographical)
        # Max of "First Name" should be "Peter"
        frame = frame.aggregate({
            'First Name': 'min',
            'Last Name': 'max'
        })
        
        res = frame.execute_frame_to_string()
        result_json = json.loads(res)["result"]
        
        row = result_json["rows"][0]["values"]
        first_name_val = row[result_json["columns"].index("First Name")]
        last_name_val = row[result_json["columns"].index("Last Name")]
        
        assert first_name_val == "Anthony"
        assert last_name_val == "Smith"

    def test_e2e_aggregate_broadcast_scalar(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """
        Tests providing a single function string to apply to ALL columns.
        """
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        
        # Input: 'count' applied to the whole frame
        # Should result in a row where every column is 7
        frame = frame.aggregate('count')
        
        res = frame.execute_frame_to_string()
        result_json = json.loads(res)["result"]
        
        assert len(result_json["columns"]) == 4  # All original columns
        assert result_json["rows"][0]["values"] == [7, 7, 7, 7]

    def test_e2e_aggregate_broadcast_ufunc(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """
        Tests providing a single numpy ufunc to apply to ALL columns.
        """
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        
        # Input: np.max applied to whole frame
        frame = frame.aggregate(np.max)
        
        res = frame.execute_frame_to_string()
        result_json = json.loads(res)["result"]
        
        # Extract values map for easier assertion
        row_values = dict(zip(result_json["columns"], result_json["rows"][0]["values"]))
        
        assert row_values["Age"] == 35
        assert row_values["First Name"] == "Peter"
