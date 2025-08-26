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


class TestExtendAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_extend_function_error_on_non_tuple(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.extend('col1')  # type: ignore
        assert r.value.args[0] == (
            "'extend' function extend_columns argument should be a list of tuples with two/three elements - "
            "first element being a string (new column name), second element being a lambda function which takes "
            "one argument (LegendQLApiTdsRow) and third element being an optional aggregation lambda function "
            "which takes one argument "
            "E.g - [('new col1', lambda r: r.c1 + 1), ('new col2', lambda r: r.c2, lambda c: c.sum())]. "
            "Element at index 0 (0-indexed) is incompatible"
        )

    def test_extend_function_error_on_incompatible_lambda_func(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.extend(("col4", lambda r: r.unknown))
        assert r.value.args[0] == (
            "'extend' function extend_columns argument incompatible. Error occurred while evaluating "
            "extend lambda at index 0 (0-indexed). Message: Column - 'unknown' doesn't exist in the current frame. "
            "Current frame columns: ['col1', 'col2']"
        )

    def test_extend_function_error_on_non_string_name(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.extend((1, lambda r: r.col1 + 1))  # type: ignore
        assert r.value.args[0] == (
            "'extend' function extend_columns argument incompatible. First element in an extend tuple should be "
            "a string (new column name). E.g - ('new col', lambda r: r.c1 + 1). "
            "Element at index 0 (0-indexed) is incompatible"
        )

    def test_extend_function_error_on_incompatible_lambda_1(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.extend(("col4", lambda r: {}))  # type: ignore
        assert r.value.args[0] == ("'extend' function extend_columns argument incompatible. "
                                   "Extend lambda at index 0 (0-indexed) returns non-primitive - <class 'dict'>")

    def test_extend_function_error_on_duplicate_column_names(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.extend([("col4", lambda r: 1), ("col4", lambda r: 2)])
        assert r.value.args[0] == "Extend column names list has duplicates: ['col4', 'col4']"

    def test_extend_function_error_on_conflicting_column_names(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.extend([("col1", lambda r: 1), ("col4", lambda r: 2)])
        assert r.value.args[0] == "Extend column name - 'col1' already exists in base frame"

    def test_extend_function_error_on_incompatible_agg_func(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.extend(("col4", lambda r: r.col1, lambda: 1))  # type: ignore
        assert r.value.args[0] == (
            "'extend' function extend_columns argument incompatible. "
            "Third element in an extend tuple should be a lambda function which takes one argument (collection) "
            "E.g - ('new col', lambda r: r.c1, lambda c: c.sum()). "
            "Element at index 0 (0-indexed) is incompatible"
        )

    def test_extend_function_error_on_incompatible_agg_func1(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.extend(("col4", lambda r: r.col1, lambda c: c))  # type: ignore
        assert r.value.args[0] == (
            "'extend' function extend_columns argument incompatible. Aggregation lambda at index 0 (0-indexed) returns "
            "non-primitive - <class 'pylegend.core.language.shared.primitive_collection.PyLegendIntegerCollection'>"
        )

    def test_query_gen_extend_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.extend(("col3", lambda r: r.get_integer('col1') + 1))
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                ("root".col1 + 1) AS "col3"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(~col3:{r | toOne($r.col1) + 1})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(~col3:{r | toOne($r.col1) + 1})')

    def test_query_gen_extend_function_col_name_with_spaces(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.extend(("col3 with spaces", lambda r: r.get_integer('col1') + 1))
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                ("root".col1 + 1) AS "col3 with spaces"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(~'col3 with spaces':{r | toOne($r.col1) + 1})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(~\'col3 with spaces\':{r | toOne($r.col1) + 1})')

    def test_query_gen_extend_function_multi(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.extend(
            [("col3", lambda r: r.get_integer('col1') + 1), ("col4", lambda r: r.get_integer('col1') + 2)]
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                ("root".col1 + 1) AS "col3",
                ("root".col1 + 2) AS "col4"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(~[
                col3:{r | toOne($r.col1) + 1},
                col4:{r | toOne($r.col1) + 2}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(~[col3:{r | toOne($r.col1) + 1}, col4:{r | toOne($r.col1) + 2}])')

    def test_query_gen_extend_function_literals(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.extend([
            ("col3", lambda r: 1),
            ("col4", lambda r: 2.0),
            ("col5", lambda r: "Hello"),
            ("col6", lambda r: True)
        ])
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                1 AS "col3",
                2.0 AS "col4",
                'Hello' AS "col5",
                true AS "col6"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(~[
                col3:{r | 1},
                col4:{r | 2.0},
                col5:{r | 'Hello'},
                col6:{r | true}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(~[col3:{r | 1}, col4:{r | 2.0}, '
                'col5:{r | \'Hello\'}, col6:{r | true}])')

    def test_query_gen_extend_function_with_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.extend(("Sum col", lambda r: r.col1, lambda c: c.sum()))  # type: ignore
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                SUM("root".col1) OVER () AS "Sum col"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(~'Sum col':{r | $r.col1}:{c | $c->sum()})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(~\'Sum col\':{r | $r.col1}:{c | $c->sum()})')

    def test_query_gen_extend_function_with_multi_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.extend([
            ("Count col", lambda r: 1, lambda c: c.count()),
            ("Sum col", lambda r: r.col1, lambda c: c.sum())  # type: ignore
        ])
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                COUNT(1) OVER () AS "Count col",
                SUM("root".col1) OVER () AS "Sum col"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(~[
                'Count col':{r | 1}:{c | $c->count()},
                'Sum col':{r | $r.col1}:{c | $c->sum()}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(~[\'Count col\':{r | 1}:{c | $c->count()}, '
                '\'Sum col\':{r | $r.col1}:{c | $c->sum()}])')

    def test_query_gen_extend_function_mixed(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.extend([
            ('Simple col 1', lambda r: 1),
            ("Sum col", lambda r: r.col1, lambda c: c.sum()),  # type: ignore
            ('Simple col 2', lambda r: 2)
        ])
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                1 AS "Simple col 1",
                SUM("root".col1) OVER () AS "Sum col",
                2 AS "Simple col 2"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(~'Simple col 1':{r | 1})
              ->extend(~'Sum col':{r | $r.col1}:{c | $c->sum()})
              ->extend(~'Simple col 2':{r | 2})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(~\'Simple col 1\':{r | 1})'
                '->extend(~\'Sum col\':{r | $r.col1}:{c | $c->sum()})->extend(~\'Simple col 2\':{r | 2})')

    def test_e2e_extend_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.extend(("Upper", lambda r: r.get_string("First Name").upper()))
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Firm/Legal Name, Type: String), "
                "TdsColumn(Name: Upper, Type: String)]")
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name', 'Upper'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X', 'PETER']},
                             {'values': ['John', 'Johnson', 22, 'Firm X', 'JOHN']},
                             {'values': ['John', 'Hill', 12, 'Firm X', 'JOHN']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X', 'ANTHONY']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A', 'FABRICE']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B', 'OLIVER']},
                             {'values': ['David', 'Harris', 35, 'Firm C', 'DAVID']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_extend_function_multi(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.extend([
            ("Upper", lambda r: r.get_string("First Name").upper()),
            ("AgeCheck", lambda r: r.get_integer('Age') < 25)
        ])
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Firm/Legal Name, Type: String), "
                "TdsColumn(Name: Upper, Type: String), TdsColumn(Name: AgeCheck, Type: Boolean)]")
        expected = {'columns': ['First Name',
                                'Last Name',
                                'Age',
                                'Firm/Legal Name',
                                'Upper',
                                'AgeCheck'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X', 'PETER', True]},
                             {'values': ['John', 'Johnson', 22, 'Firm X', 'JOHN', True]},
                             {'values': ['John', 'Hill', 12, 'Firm X', 'JOHN', True]},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X', 'ANTHONY', True]},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A', 'FABRICE', False]},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B', 'OLIVER', False]},
                             {'values': ['David', 'Harris', 35, 'Firm C', 'DAVID', False]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_extend_function_literals(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.select(["Last Name"])
        frame = frame.extend([
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

    @pytest.mark.skip(reason="Server does not handle window functions of this form yet")
    def test_e2e_extend_function_with_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.extend([
            ("Count", lambda r: 1, lambda c: c.count())
        ])
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Firm/Legal Name, Type: String), "
                "TdsColumn(Name: Count, Type: Integer)]")
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name', 'Count'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X', 7]},
                             {'values': ['John', 'Johnson', 22, 'Firm X', 7]},
                             {'values': ['John', 'Hill', 12, 'Firm X', 7]},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X', 7]},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A', 7]},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B', 7]},
                             {'values': ['David', 'Harris', 35, 'Firm C', 7]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    @pytest.mark.skip(reason="Server does not handle window functions of this form yet")
    def test_e2e_extend_function_multi_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.extend([
            ("AgeSum", lambda r: r['Age'], lambda c: c.sum()),  # type: ignore
            ("DistinctCount", lambda r: r["Last Name"], lambda c: c.distinct_count())
        ])
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Firm/Legal Name, Type: String), "
                "TdsColumn(Name: AgeSum, Type: Integer), TdsColumn(Name: DistinctCount, Type: Integer)]")
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name', 'AgeSum', 'DistinctCount'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X', 'PETER']},
                             {'values': ['John', 'Johnson', 22, 'Firm X', 'JOHN']},
                             {'values': ['John', 'Hill', 12, 'Firm X', 'JOHN']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X', 'ANTHONY']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A', 'FABRICE']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B', 'OLIVER']},
                             {'values': ['David', 'Harris', 35, 'Firm C', 'DAVID']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    @pytest.mark.skip(reason="Server does not handle window functions of this form yet")
    def test_e2e_extend_function_mixed(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.extend([
            ("Upper", lambda r: r.get_string("First Name").upper()),
            ("Count", lambda r: 1, lambda c: c.count())
        ])
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Firm/Legal Name, Type: String), "
                "TdsColumn(Name: Upper, Type: String), TdsColumn(Name: Count, Type: Integer)]")
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name', 'Upper', 'Count'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X', 'PETER', 7]},
                             {'values': ['John', 'Johnson', 22, 'Firm X', 'JOHN', 7]},
                             {'values': ['John', 'Hill', 12, 'Firm X', 'JOHN', 7]},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X', 'ANTHONY', 7]},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A', 'FABRICE', 7]},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B', 'OLIVER', 7]},
                             {'values': ['David', 'Harris', 35, 'Firm C', 'DAVID', 7]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
