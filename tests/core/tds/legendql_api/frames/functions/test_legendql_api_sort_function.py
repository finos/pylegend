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
from textwrap import dedent
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame import LegendQLApiTdsFrame
from pylegend.extensions.tds.legendql_api.frames.legendql_api_table_spec_input_frame import LegendQLApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_legendql_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile


class TestSortAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_sort_function_error_on_unknown_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.sort(["col3"])
        assert v.value.args[0] == "Column - 'col3' doesn't exist in the current frame. " \
                                  "Current frame columns: ['col1', 'col2']"

    def test_sort_function_error_on_unknown_col2(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as v:
            frame.sort(lambda r: r.col3)
        assert v.value.args[0] == (
            "'sort' function sort_infos argument lambda incompatible. Error occurred while evaluating. "
            "Message: Column - 'col3' doesn't exist in the current frame. Current frame columns: ['col1', 'col2']"
        )

    def test_sort_function_error_on_non_column_expr(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as v:
            frame.sort(lambda r: r.col1 + 1)  # type: ignore
        assert v.value.args[0] == (
            "'sort' function sort_infos argument lambda incompatible. Columns can be simple column expressions or "
            "sort infos. (E.g - lambda r: [r.column1, r['column with spaces'].descending(), r.column3.ascending()). "
            "Element at index 0 (0-indexed) is incompatible"
        )

    def test_query_gen_sort_function_no_top(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.sort(["col2", "col1"])
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            ORDER BY
                "root".col2,
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->sort([ascending(~col2), ascending(~col1)])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->sort([ascending(~col2), ascending(~col1)])')

    def test_query_gen_sort_function_existing_top(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.head(10)
        frame = frame.sort(lambda r: r.col2.descending())
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 10
                ) AS "root"
            ORDER BY
                "root"."col2" DESC'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->limit(10)
              ->sort([descending(~col2)])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->limit(10)->sort([descending(~col2)])')

    def test_e2e_sort_function_no_top(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.sort(lambda r: r["Firm/Legal Name"])
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Fabrice', 'Roberts', 34, 'Firm A']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B']},
                             {'values': ['David', 'Harris', 35, 'Firm C']},
                             {'values': ['Peter', 'Smith', 23, 'Firm X']},
                             {'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_sort_function_no_top_multi(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.sort(lambda r: [r["Firm/Legal Name"], r["First Name"].ascending()])
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Fabrice', 'Roberts', 34, 'Firm A']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B']},
                             {'values': ['David', 'Harris', 35, 'Firm C']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X']},
                             {'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Peter', 'Smith', 23, 'Firm X']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_sort_function_existing_top(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.head(5)
        frame = frame.sort(lambda r: [r["Firm/Legal Name"].descending()])
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X']},
                             {'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_sort_function_existing_top_multi(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) \
            -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.head(5)
        frame = frame.sort(lambda r: [r["Firm/Legal Name"].descending(), r["First Name"].ascending()])
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Anthony', 'Allen', 22, 'Firm X']},
                             {'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Peter', 'Smith', 23, 'Firm X']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
