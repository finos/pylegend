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


class TestProjectAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_project_function_error_on_non_tuple(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.project('col1')  # type: ignore
        assert r.value.args[0] == (
            "'project' function project_columns argument should be a list of tuples with two elements - "
            "first element being a string (new column name), second element being a lambda function which takes "
            "one argument (LegendQLApiTdsRow) "
            "E.g - [('new col1', lambda r: r.c1 + 1), ('new col2', lambda r: r.c2)]. "
            "Element at index 0 (0-indexed) is incompatible"
        )

    def test_project_function_error_on_incompatible_lambda_func(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.project(("col4", lambda r: r.unknown))
        assert r.value.args[0] == (
            "'project' function project_columns argument incompatible. Error occurred while evaluating "
            "project lambda at index 0 (0-indexed). Message: Column - 'unknown' doesn't exist in the current frame. "
            "Current frame columns: ['col1', 'col2']"
        )

    def test_project_function_error_on_non_string_name(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.project((1, lambda r: r.col1 + 1))  # type: ignore
        assert r.value.args[0] == (
            "'project' function project_columns argument incompatible. First element in an project tuple should be "
            "a string (new column name). E.g - ('new col', lambda r: r.c1 + 1). "
            "Element at index 0 (0-indexed) is incompatible"
        )

    def test_project_function_error_on_incompatible_lambda_1(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.project(("col4", lambda r: {}))  # type: ignore
        assert r.value.args[0] == ("'project' function project_columns argument incompatible. "
                                   "Project lambda at index 0 (0-indexed) returns non-primitive - <class 'dict'>")

    def test_project_function_error_on_duplicate_column_names(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.project([("col4", lambda r: 1), ("col4", lambda r: 2)])
        assert r.value.args[0] == "Project column names list has duplicates: ['col4', 'col4']"

    def test_query_gen_project_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.project(("col3", lambda r: r.get_integer('col1') + 1))
        expected = '''\
            SELECT
                ("root"."col1" + 1) AS "col3"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->project(~[
                col3:{r | toOne($r.col1) + 1}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->project(~[col3:{r | toOne($r.col1) + 1}])')

    def test_query_gen_project_function_col_name_with_spaces(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.project(("col3 with spaces", lambda r: r.get_integer('col1') + 1))
        expected = '''\
            SELECT
                ("root"."col1" + 1) AS "col3 with spaces"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->project(~[
                'col3 with spaces':{r | toOne($r.col1) + 1}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->project(~[\'col3 with spaces\':{r | toOne($r.col1) + 1}])')

    def test_query_gen_project_function_multi(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.project(
            [("col1", lambda r: r.get_integer('col1') + 1), ("col2", lambda r: r.get_integer('col1') + 2)]
        )
        expected = '''\
            SELECT
                ("root"."col1" + 1) AS "col1",
                ("root"."col1" + 2) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->project(~[
                col1:{r | toOne($r.col1) + 1},
                col2:{r | toOne($r.col1) + 2}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->project(~[col1:{r | toOne($r.col1) + 1}, col2:{r | toOne($r.col1) + 2}])')

    def test_query_gen_project_function_literals(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.project([
            ("col3", lambda r: 1),
            ("col4", lambda r: 2.0),
            ("col5", lambda r: "Hello"),
            ("col6", lambda r: True)
        ])
        expected = '''\
            SELECT
                1 AS "col3",
                2.0 AS "col4",
                'Hello' AS "col5",
                true AS "col6"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->project(~[
                col3:{r | 1},
                col4:{r | 2.0},
                col5:{r | 'Hello'},
                col6:{r | true}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->project(~[col3:{r | 1}, col4:{r | 2.0}, '
                'col5:{r | \'Hello\'}, col6:{r | true}])')

    def test_e2e_project_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.project(("Upper", lambda r: r.get_string("First Name").upper()))
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: Upper, Type: String)]")
        expected = {'columns': ['Upper'],
                    'rows': [{'values': ['PETER']},
                             {'values': ['JOHN']},
                             {'values': ['JOHN']},
                             {'values': ['ANTHONY']},
                             {'values': ['FABRICE']},
                             {'values': ['OLIVER']},
                             {'values': ['DAVID']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_project_function_multi(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.project([
            ("Upper", lambda r: r.get_string("First Name").upper()),
            ("AgeCheck", lambda r: r.get_integer('Age') < 25)
        ])
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: Upper, Type: String), TdsColumn(Name: AgeCheck, Type: Boolean)]")
        expected = {'columns': ['Upper', 'AgeCheck'],
                    'rows': [{'values': ['PETER', True]},
                             {'values': ['JOHN', True]},
                             {'values': ['JOHN', True]},
                             {'values': ['ANTHONY', True]},
                             {'values': ['FABRICE', False]},
                             {'values': ['OLIVER', False]},
                             {'values': ['DAVID', False]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_project_function_literals(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.select(["Last Name"])
        frame = frame.project([
            ("Last Name", lambda r: r.get_string("Last Name")),
            ("col3", lambda r: 1),
            ("col4", lambda r: 2.0),
            ("col5", lambda r: "Hello"),
            ("col6", lambda r: True)
        ])
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]") == \
               ('[TdsColumn(Name: Last Name, Type: String), TdsColumn(Name: col3, Type: Integer), '
                'TdsColumn(Name: col4, Type: Float), TdsColumn(Name: col5, Type: String), '
                'TdsColumn(Name: col6, Type: Boolean)]')
        expected = {'columns': ['Last Name', 'col3', 'col4', 'col5', 'col6'],
                    'rows': [{'values': ['Smith', 1, 2.0, 'Hello', True]},
                             {'values': ['Johnson', 1, 2.0, 'Hello', True]},
                             {'values': ['Hill', 1, 2.0, 'Hello', True]},
                             {'values': ['Allen', 1, 2.0, 'Hello', True]},
                             {'values': ['Roberts', 1, 2.0, 'Hello', True]},
                             {'values': ['Hill', 1, 2.0, 'Hello', True]},
                             {'values': ['Harris', 1, 2.0, 'Hello', True]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
