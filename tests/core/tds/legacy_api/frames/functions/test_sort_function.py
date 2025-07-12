# Copyright 2023 Goldman Sachs
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
from pylegend.core.tds.legacy_api.frames.legacy_api_tds_frame import LegacyApiTdsFrame
from pylegend.extensions.tds.legacy_api.frames.legacy_api_table_spec_input_frame import LegacyApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)


class TestSortAppliedFunction:

    def test_sort_function_error_on_unknown_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.sort(["col3"])
        assert v.value.args[0] == "Column - 'col3' in sort columns list doesn't exist in the current frame. " \
                                  "Current frame columns: ['col1', 'col2']"

    def test_sort_function_error_on_unknown_direction(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.sort(["col1"], ["A"])
        assert v.value.args[0] == "Sort direction can be ASC/DESC (case insensitive). Passed unknown value: A"

    def test_sort_function_error_on_direction_column_mismatch(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.sort(["col1"], ["ASC", "DESC"])
        assert v.value.args[0] == "Sort directions (ASC/DESC) provided need to be in sync with columns or left empty " \
                                  "to choose defaults. Passed column list: ['col1'], directions: ['ASC', 'DESC']"

    def test_query_gen_sort_function_no_top(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
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
        assert frame.to_pure_query() == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->sort([ascending(~col2), ascending(~col1)])'''
        )
        assert frame.to_pure_query(FrameToPureConfig(pretty=False)) == \
               ('#Table(test_schema.test_table)#->sort([ascending(~col2), ascending(~col1)])')

    def test_query_gen_sort_function_existing_top(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.head(10)
        frame = frame.sort(["col2"], ["DESC"])
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
        assert frame.to_pure_query() == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->limit(10)
              ->sort([descending(~col2)])'''
        )
        assert frame.to_pure_query(FrameToPureConfig(pretty=False)) == \
               ('#Table(test_schema.test_table)#->limit(10)->sort([descending(~col2)])')

    def test_query_gen_sort_function_existing_top_multi_column(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.head(10)
        frame = frame.extend([lambda x: x["col1"]], ["col3 with ' quote"])
        frame = frame.sort(["col2", "col3 with ' quote"], ["DESC", "ASC"])
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col3 with ' quote" AS "col3 with ' quote"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col1 AS "col3 with ' quote"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 10
                ) AS "root"
            ORDER BY
                "root"."col2" DESC,
                "root"."col3 with ' quote"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert frame.to_pure_query() == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->limit(10)
              ->extend(~'col3 with \\' quote':{r | $r.col1})
              ->sort([descending(~col2), ascending(~'col3 with \\' quote')])'''
        )
        assert frame.to_pure_query(FrameToPureConfig(pretty=False)) == \
               ('#Table(test_schema.test_table)#->limit(10)->extend(~\'col3 with \\\' quote\':{r | $r.col1})'
               '->sort([descending(~col2), ascending(~\'col3 with \\\' quote\')])')

    def test_e2e_sort_function_no_top(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.sort(["Firm/Legal Name"])
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
        frame: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.sort(["Firm/Legal Name", "First Name"])
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
        frame: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.take(5)
        frame = frame.sort(["Firm/Legal Name"], ["DESC"])
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
        frame: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.take(5)
        frame = frame.sort(["Firm/Legal Name", "First Name"], ["DESC", "ASC"])
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Anthony', 'Allen', 22, 'Firm X']},
                             {'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Peter', 'Smith', 23, 'Firm X']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
