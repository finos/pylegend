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


class TestJoinAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_join_error_on_incompatible_lambda(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame1.join(frame2, lambda: True)  # type: ignore
        assert r.value.args[0] == ("Join condition function should be a lambda which takes two arguments "
                                   "(TDSRow, TDSRow)")
        with pytest.raises(TypeError) as r:
            frame1.join(frame2, lambda x: True)  # type: ignore
        assert r.value.args[0] == ("Join condition function should be a lambda which takes two arguments "
                                   "(TDSRow, TDSRow)")

    def test_join_error_on_duplicated_columns(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame1.join(frame2, lambda x, y: True)
        assert r.value.args[0] == (
            "Found duplicate columns in joined frames. Use rename function to ensure there are no duplicate columns in "
            "joined frames. Columns - Left Frame: ['col1', 'col2'], Right Frame: ['col1', 'col2']")

    def test_join_error_on_non_boolean_lambda(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame1.join(frame2, lambda x, y: 1)  # type: ignore
        assert r.value.args[0] == "Join condition function incompatible. Returns non boolean - <class 'int'>"

    def test_join_error_on_failing_lambda(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame1.join(frame2, lambda x, y: x['col3'] == y['col1'])
        assert r.value.args[0] == ("Join condition function incompatible. Error occurred while evaluating. "
                                   "Message: Column - 'col3' doesn't exist in the current frame. "
                                   "Current frame columns: ['col1', 'col2']")

    def test_join_error_on_join_type(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col3"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)
        with pytest.raises(ValueError) as r:
            frame1.join(frame2, lambda x, y: x['col1'] == y['col3'], "i")
        assert r.value.args[0] == "Unknown join type - i. Supported types are - INNER, LEFT_OUTER, RIGHT_OUTER"

    def test_query_gen_join(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col3"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)
        frame = frame1.join(frame2, lambda x, y: x['col2'] == y['col4'])

        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), "
            "TdsColumn(Name: col3, Type: Integer), TdsColumn(Name: col4, Type: String)]"
        )
        expected = '''\
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."col3" AS "col3",
                        "root"."col4" AS "col4"
                    FROM
                        (
                            SELECT
                                "left"."col1" AS "col1",
                                "left"."col2" AS "col2",
                                "right"."col3" AS "col3",
                                "right"."col4" AS "col4"
                            FROM
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".col2 AS "col2"
                                    FROM
                                        test_schema.test_table1 AS "root"
                                ) AS "left"
                                LEFT OUTER JOIN
                                    (
                                        SELECT
                                            "root".col3 AS "col3",
                                            "root".col4 AS "col4"
                                        FROM
                                            test_schema.test_table2 AS "root"
                                    ) AS "right"
                                    ON ("left"."col2" = "right"."col4")
                        ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->join(
                #Table(test_schema.test_table2)#,
                JoinKind.LEFT,
                {l, r | $l.col2 == $r.col4}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->join(#Table(test_schema.test_table2)#, '
                'JoinKind.LEFT, {l, r | $l.col2 == $r.col4})')

    def test_query_gen_join_inner(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col3"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)
        frame = frame1.join(frame2, lambda x, y: x['col2'] == y['col4'], 'INNER')

        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), "
            "TdsColumn(Name: col3, Type: Integer), TdsColumn(Name: col4, Type: String)]"
        )
        expected = '''\
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."col3" AS "col3",
                        "root"."col4" AS "col4"
                    FROM
                        (
                            SELECT
                                "left"."col1" AS "col1",
                                "left"."col2" AS "col2",
                                "right"."col3" AS "col3",
                                "right"."col4" AS "col4"
                            FROM
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".col2 AS "col2"
                                    FROM
                                        test_schema.test_table1 AS "root"
                                ) AS "left"
                                INNER JOIN
                                    (
                                        SELECT
                                            "root".col3 AS "col3",
                                            "root".col4 AS "col4"
                                        FROM
                                            test_schema.test_table2 AS "root"
                                    ) AS "right"
                                    ON ("left"."col2" = "right"."col4")
                        ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->join(
                #Table(test_schema.test_table2)#,
                JoinKind.INNER,
                {l, r | $l.col2 == $r.col4}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->join(#Table(test_schema.test_table2)#, '
                'JoinKind.INNER, {l, r | $l.col2 == $r.col4})')

    @pytest.mark.skip(reason="JoinKind.RIGHT not supported by server")
    def test_query_gen_join_right_outer(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col3"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)
        frame = frame1.join(frame2, lambda x, y: x['col2'] == y['col4'], 'RIGHT_OUTER')

        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), "
            "TdsColumn(Name: col3, Type: Integer), TdsColumn(Name: col4, Type: String)]"
        )
        expected = '''\
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."col3" AS "col3",
                        "root"."col4" AS "col4"
                    FROM
                        (
                            SELECT
                                "left"."col1" AS "col1",
                                "left"."col2" AS "col2",
                                "right"."col3" AS "col3",
                                "right"."col4" AS "col4"
                            FROM
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".col2 AS "col2"
                                    FROM
                                        test_schema.test_table1 AS "root"
                                ) AS "left"
                                RIGHT OUTER JOIN
                                    (
                                        SELECT
                                            "root".col3 AS "col3",
                                            "root".col4 AS "col4"
                                        FROM
                                            test_schema.test_table2 AS "root"
                                    ) AS "right"
                                    ON ("left"."col2" = "right"."col4")
                        ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->join(
                #Table(test_schema.test_table2)#,
                JoinKind.RIGHT,
                {l, r | $l.col2 == $r.col4}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->join(#Table(test_schema.test_table2)#, '
                'JoinKind.RIGHT, {l, r | $l.col2 == $r.col4})')

    def test_query_gen_join_complex_condition(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col3"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)
        frame = frame1.join(
            frame2,
            lambda x, y: (x['col2'] == y['col4']) &
                         ((x['col1'] > 10) | (y['col3'] > 10)) &  # type: ignore
                         (x['col1'] > y['col3'])   # type: ignore
        )

        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), "
            "TdsColumn(Name: col3, Type: Integer), TdsColumn(Name: col4, Type: String)]"
        )
        expected = '''\
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."col3" AS "col3",
                        "root"."col4" AS "col4"
                    FROM
                        (
                            SELECT
                                "left"."col1" AS "col1",
                                "left"."col2" AS "col2",
                                "right"."col3" AS "col3",
                                "right"."col4" AS "col4"
                            FROM
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".col2 AS "col2"
                                    FROM
                                        test_schema.test_table1 AS "root"
                                ) AS "left"
                                LEFT OUTER JOIN
                                    (
                                        SELECT
                                            "root".col3 AS "col3",
                                            "root".col4 AS "col4"
                                        FROM
                                            test_schema.test_table2 AS "root"
                                    ) AS "right"
                                    ON ((("left"."col2" = "right"."col4") AND \
(("left"."col1" > 10) OR ("right"."col3" > 10))) AND ("left"."col1" > "right"."col3"))
                        ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->join(
                #Table(test_schema.test_table2)#,
                JoinKind.LEFT,
                {l, r | (($l.col2 == $r.col4) && (($l.col1 > 10) || ($r.col3 > 10))) && ($l.col1 > $r.col3)}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->join(#Table(test_schema.test_table2)#, '
                'JoinKind.LEFT, '
                '{l, r | (($l.col2 == $r.col4) && (($l.col1 > 10) || ($r.col3 > 10))) && ($l.col1 > $r.col3)})')

    def test_e2e_join(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame1: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame1 = frame1.select(['Last Name', 'First Name'])
        frame1 = frame1.rename(lambda r: ('Last Name', 'Last Name 1'))
        frame2: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame2 = frame2.select(['Age', 'Last Name'])
        frame2 = frame2.rename(lambda r: ('Last Name', 'Last Name 2'))
        frame = frame1.join(frame2, lambda x, y: x['Last Name 1'] == y['Last Name 2'])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Last Name 1, Type: String), TdsColumn(Name: First Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Last Name 2, Type: String)]")
        expected = {'columns': ['Last Name 1', 'First Name', 'Age', 'Last Name 2'],
                    'rows': [{'values': ['Smith', 'Peter', 23, 'Smith']},
                             {'values': ['Johnson', 'John', 22, 'Johnson']},
                             {'values': ['Hill', 'John', 12, 'Hill']},
                             {'values': ['Hill', 'John', 32, 'Hill']},
                             {'values': ['Allen', 'Anthony', 22, 'Allen']},
                             {'values': ['Roberts', 'Fabrice', 34, 'Roberts']},
                             {'values': ['Hill', 'Oliver', 12, 'Hill']},
                             {'values': ['Hill', 'Oliver', 32, 'Hill']},
                             {'values': ['Harris', 'David', 35, 'Harris']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        assert json.loads(
            frame1.left_join(frame2, lambda x, y: x['Last Name 1'] == y['Last Name 2']).execute_frame_to_string()
        )["result"] == expected

    def test_e2e_join_inner(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame1: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame1 = frame1.select(['Last Name', 'First Name'])
        frame1 = frame1.rename(lambda r: ('Last Name', 'Last Name 1'))
        frame2: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame2 = frame2.filter(lambda x: x['First Name'] == 'John')
        frame2 = frame2.select(['Age', 'Last Name'])
        frame2 = frame2.rename(lambda r: ('Last Name', 'Last Name 2'))
        frame = frame1.join(frame2, lambda x, y: x['Last Name 1'] == y['Last Name 2'], 'INNER')
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Last Name 1, Type: String), TdsColumn(Name: First Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Last Name 2, Type: String)]")
        expected = {'columns': ['Last Name 1', 'First Name', 'Age', 'Last Name 2'],
                    'rows': [{'values': ['Johnson', 'John', 22, 'Johnson']},
                             {'values': ['Hill', 'John', 12, 'Hill']},
                             {'values': ['Hill', 'Oliver', 12, 'Hill']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        assert json.loads(
            frame1.inner_join(frame2, lambda x, y: x['Last Name 1'] == y['Last Name 2']).execute_frame_to_string()
        )["result"] == expected

    def test_e2e_join_right_outer(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame1: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame1 = frame1.filter(lambda x: x['First Name'] == 'John')
        frame1 = frame1.select(['Last Name', 'First Name'])
        frame1 = frame1.rename(lambda r: ('Last Name', 'Last Name 1'))
        frame2: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame2 = frame2.select(['Age', 'Last Name'])
        frame2 = frame2.rename(lambda r: ('Last Name', 'Last Name 2'))
        frame = frame1.join(frame2, lambda x, y: x['Last Name 1'] == y['Last Name 2'], 'RIGHT_OUTER')
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Last Name 1, Type: String), TdsColumn(Name: First Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Last Name 2, Type: String)]")
        expected = {'columns': ['Last Name 1', 'First Name', 'Age', 'Last Name 2'],
                    'rows': [{'values': [None, None, 23, 'Smith']},
                             {'values': ['Johnson', 'John', 22, 'Johnson']},
                             {'values': ['Hill', 'John', 12, 'Hill']},
                             {'values': [None, None, 22, 'Allen']},
                             {'values': [None, None, 34, 'Roberts']},
                             {'values': ['Hill', 'John', 32, 'Hill']},
                             {'values': [None, None, 35, 'Harris']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        assert json.loads(
            frame1.right_join(frame2, lambda x, y: x['Last Name 1'] == y['Last Name 2']).execute_frame_to_string()
        )["result"] == expected

    def test_e2e_join_true_literal(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame1: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame1 = frame1.head(2)
        frame1 = frame1.select(['Last Name', 'First Name'])
        frame1 = frame1.rename(lambda r: ('Last Name', 'Last Name 1'))
        frame2: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame2 = frame2.head(2)
        frame2 = frame2.select(['Age', 'Last Name'])
        frame2 = frame2.rename(lambda r: ('Last Name', 'Last Name 2'))
        frame = frame1.join(frame2, lambda x, y: True)
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Last Name 1, Type: String), TdsColumn(Name: First Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Last Name 2, Type: String)]")
        expected = {'columns': ['Last Name 1', 'First Name', 'Age', 'Last Name 2'],
                    'rows': [{'values': ['Smith', 'Peter', 23, 'Smith']},
                             {'values': ['Smith', 'Peter', 22, 'Johnson']},
                             {'values': ['Johnson', 'John', 23, 'Smith']},
                             {'values': ['Johnson', 'John', 22, 'Johnson']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_join_false_literal(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame1: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame1 = frame1.head(2)
        frame1 = frame1.select(['Last Name', 'First Name'])
        frame1 = frame1.rename(lambda r: ('Last Name', 'Last Name 1'))
        frame2: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame2 = frame2.head(2)
        frame2 = frame2.select(['Age', 'Last Name'])
        frame2 = frame2.rename(lambda r: ('Last Name', 'Last Name 2'))
        frame = frame1.join(frame2, lambda x, y: 1 == 2)  # type: ignore
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Last Name 1, Type: String), TdsColumn(Name: First Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Last Name 2, Type: String)]")
        expected = {'columns': ['Last Name 1', 'First Name', 'Age', 'Last Name 2'],
                    'rows': [{'values': ['Smith', 'Peter', None, None]},
                             {'values': ['Johnson', 'John', None, None]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_join_by_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame1: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame1 = frame1.select(['Last Name', 'First Name'])
        frame1 = frame1.rename(lambda r: ('Last Name', 'Last Name 1'))
        frame2: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame2 = frame2.select(['Age', 'Last Name'])
        frame2 = frame2.rename(lambda r: ('Last Name', 'Last Name 2'))
        frame = frame1.join(frame2, lambda x, y: x['Last Name 1'] == y['Last Name 2'])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Last Name 1, Type: String), TdsColumn(Name: First Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Last Name 2, Type: String)]")
        expected = {'columns': ['Last Name 1', 'First Name', 'Age', 'Last Name 2'],
                    'rows': [{'values': ['Smith', 'Peter', 23, 'Smith']},
                             {'values': ['Johnson', 'John', 22, 'Johnson']},
                             {'values': ['Hill', 'John', 12, 'Hill']},
                             {'values': ['Hill', 'John', 32, 'Hill']},
                             {'values': ['Allen', 'Anthony', 22, 'Allen']},
                             {'values': ['Roberts', 'Fabrice', 34, 'Roberts']},
                             {'values': ['Hill', 'Oliver', 12, 'Hill']},
                             {'values': ['Hill', 'Oliver', 32, 'Hill']},
                             {'values': ['Harris', 'David', 35, 'Harris']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
