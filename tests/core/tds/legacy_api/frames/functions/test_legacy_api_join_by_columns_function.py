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
from pylegend.core.request.legend_client import LegendClient
from tests.core.tds.legacy_api import generate_pure_query_and_compile


class TestJoinByColumnsAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_join_by_columns_error_on_unknown_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame1.join_by_columns(frame2, ["col1", "col3"], ["col1", "col2"])
        assert r.value.args[0] == ("Column - 'col3' in join columns list doesn't exist in the left frame being joined. "
                                   "Current left frame columns: ['col1', 'col2']")
        with pytest.raises(ValueError) as r:
            frame1.join_by_columns(frame2, ["col1", "col2"], ["col1", "col3"])
        assert r.value.args[0] == ("Column - 'col3' in join columns list doesn't exist in the right frame being joined."
                                   " Current right frame columns: ['col1', 'col2']")

    def test_join_by_columns_error_on_diff_size_col_list(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame1.join_by_columns(frame2, ["col1"], ["col1", "col2"])
        assert r.value.args[0] == ("For join_by_columns function, column lists should be of same size. "
                                   "Passed column list sizes -  Left: 1, Right: 2")

    def test_join_by_columns_error_on_empty_col_list(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame1.join_by_columns(frame2, [], [])
        assert r.value.args[0] == "For join_by_columns function, column lists should not be empty"

    def test_join_by_columns_error_on_non_match_col_type(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        cols2 = [
            PrimitiveTdsColumn.string_column("col3"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], cols1)
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], cols2)
        with pytest.raises(ValueError) as r:
            frame1.join_by_columns(frame2, ["col1"], ["col3"])
        assert r.value.args[0] == ("Trying to join on columns with different types -  "
                                   "Left Col: TdsColumn(Name: col1, Type: Integer), "
                                   "Right Col: TdsColumn(Name: col3, Type: String)")

    def test_join_by_columns_error_on_duplicated_columns_not_being_joined(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        cols2 = [
            PrimitiveTdsColumn.string_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], cols1)
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], cols2)
        with pytest.raises(ValueError) as r:
            frame1.join_by_columns(frame2, ["col2"], ["col2"])
        assert r.value.args[0] == ("Found duplicate columns in joined frames (which are not join keys). "
                                   "Columns -  Left Frame: ['col1', 'col2'], Right Frame: ['col1', 'col2'], "
                                   "Common Join Keys: ['col2']")

    def test_join_by_columns_error_on_unknown_join_type(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame1.join_by_columns(frame2, ["col1", "col2"], ["col1", "col2"], "i")
        assert r.value.args[0] == "Unknown join type - i. Supported types are - INNER, LEFT_OUTER, RIGHT_OUTER"

    def test_query_gen_join_by_columns_function(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col3"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        frame = frame1.join_by_columns(frame2, ["col1"], ["col3"])
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
                            ON ("left"."col1" = "right"."col3")
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->join(
                #Table(test_schema.test_table2)#,
                JoinKind.LEFT,
                {l, r | $l.col1 == $r.col3}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->join(#Table(test_schema.test_table2)#, '
                'JoinKind.LEFT, '
                '{l, r | $l.col1 == $r.col3})')

    def test_query_gen_join_by_columns_function_multi_key(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col3"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        frame = frame1.join_by_columns(frame2, ["col1", "col2"], ["col3", "col4"])
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
                            ON (("left"."col1" = "right"."col3") AND ("left"."col2" = "right"."col4"))
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->join(
                #Table(test_schema.test_table2)#,
                JoinKind.LEFT,
                {l, r | ($l.col1 == $r.col3) && ($l.col2 == $r.col4)}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->join(#Table(test_schema.test_table2)#, '
                'JoinKind.LEFT, '
                '{l, r | ($l.col1 == $r.col3) && ($l.col2 == $r.col4)})')

    def test_query_gen_join_by_columns_function_shared_key(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        frame = frame1.join_by_columns(frame2, ["col1"], ["col1"])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: col1, Type: Integer), "
            "TdsColumn(Name: col4, Type: String)]"
        )
        expected = '''\
            SELECT
                "root"."col2" AS "col2",
                "root"."col1" AS "col1",
                "root"."col4" AS "col4"
            FROM
                (
                    SELECT
                        "left"."col2" AS "col2",
                        "left"."col1" AS "col1",
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
                                    "root".col1 AS "col1",
                                    "root".col4 AS "col4"
                                FROM
                                    test_schema.test_table2 AS "root"
                            ) AS "right"
                            ON ("left"."col1" = "right"."col1")
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->join(
                #Table(test_schema.test_table2)#
                  ->rename(~col1, ~col1_gen_r),
                JoinKind.LEFT,
                {l, r | $l.col1 == $r.col1_gen_r}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->join(#Table(test_schema.test_table2)#->rename(~col1, ~col1_gen_r), '
                'JoinKind.LEFT, '
                '{l, r | $l.col1 == $r.col1_gen_r})')

    def test_query_gen_join_by_columns_function_shared_multi_key(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        frame = frame1.join_by_columns(frame2, ["col2", "col1"], ["col2", "col1"])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col3, Type: String), TdsColumn(Name: col1, Type: Integer), "
            "TdsColumn(Name: col2, Type: String), TdsColumn(Name: col4, Type: String)]"
        )
        expected = '''\
            SELECT
                "root"."col3" AS "col3",
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col4" AS "col4"
            FROM
                (
                    SELECT
                        "left"."col3" AS "col3",
                        "left"."col1" AS "col1",
                        "left"."col2" AS "col2",
                        "right"."col4" AS "col4"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3"
                            FROM
                                test_schema.test_table1 AS "root"
                        ) AS "left"
                        LEFT OUTER JOIN
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".col2 AS "col2",
                                    "root".col4 AS "col4"
                                FROM
                                    test_schema.test_table2 AS "root"
                            ) AS "right"
                            ON (("left"."col2" = "right"."col2") AND ("left"."col1" = "right"."col1"))
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->join(
                #Table(test_schema.test_table2)#
                  ->rename(~col2, ~col2_gen_r)
                  ->rename(~col1, ~col1_gen_r),
                JoinKind.LEFT,
                {l, r | ($l.col2 == $r.col2_gen_r) && ($l.col1 == $r.col1_gen_r)}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->join('
                '#Table(test_schema.test_table2)#->rename(~col2, ~col2_gen_r)->rename(~col1, ~col1_gen_r), '
                'JoinKind.LEFT, '
                '{l, r | ($l.col2 == $r.col2_gen_r) && ($l.col1 == $r.col1_gen_r)})')

    def test_query_gen_join_by_columns_function_shared_multi_key_inner(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        frame = frame1.join_by_columns(frame2, ["col2", "col1"], ["col2", "col1"], 'INNER')
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col3, Type: String), TdsColumn(Name: col1, Type: Integer), "
            "TdsColumn(Name: col2, Type: String), TdsColumn(Name: col4, Type: String)]"
        )
        expected = '''\
            SELECT
                "root"."col3" AS "col3",
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col4" AS "col4"
            FROM
                (
                    SELECT
                        "left"."col3" AS "col3",
                        "left"."col1" AS "col1",
                        "left"."col2" AS "col2",
                        "right"."col4" AS "col4"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3"
                            FROM
                                test_schema.test_table1 AS "root"
                        ) AS "left"
                        INNER JOIN
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".col2 AS "col2",
                                    "root".col4 AS "col4"
                                FROM
                                    test_schema.test_table2 AS "root"
                            ) AS "right"
                            ON (("left"."col2" = "right"."col2") AND ("left"."col1" = "right"."col1"))
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->join(
                #Table(test_schema.test_table2)#
                  ->rename(~col2, ~col2_gen_r)
                  ->rename(~col1, ~col1_gen_r),
                JoinKind.INNER,
                {l, r | ($l.col2 == $r.col2_gen_r) && ($l.col1 == $r.col1_gen_r)}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->join('
                '#Table(test_schema.test_table2)#->rename(~col2, ~col2_gen_r)->rename(~col1, ~col1_gen_r), '
                'JoinKind.INNER, '
                '{l, r | ($l.col2 == $r.col2_gen_r) && ($l.col1 == $r.col1_gen_r)})')

    @pytest.mark.skip(reason="JoinKind.RIGHT not supported by server")
    def test_query_gen_join_by_columns_function_shared_multi_key_right_outer(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        frame = frame1.join_by_columns(frame2, ["col2", "col1"], ["col2", "col1"], 'RIGHT_OUTER')
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col3, Type: String), TdsColumn(Name: col1, Type: Integer), "
            "TdsColumn(Name: col2, Type: String), TdsColumn(Name: col4, Type: String)]"
        )
        expected = '''\
            SELECT
                "root"."col3" AS "col3",
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col4" AS "col4"
            FROM
                (
                    SELECT
                        "left"."col3" AS "col3",
                        "right"."col1" AS "col1",
                        "right"."col2" AS "col2",
                        "right"."col4" AS "col4"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3"
                            FROM
                                test_schema.test_table1 AS "root"
                        ) AS "left"
                        RIGHT OUTER JOIN
                            (
                                SELECT
                                    "root".col1 AS "col1",
                                    "root".col2 AS "col2",
                                    "root".col4 AS "col4"
                                FROM
                                    test_schema.test_table2 AS "root"
                            ) AS "right"
                            ON (("left"."col2" = "right"."col2") AND ("left"."col1" = "right"."col1"))
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->join(
                #Table(test_schema.test_table2)#
                  ->rename(~col2, ~col2_gen_r)
                  ->rename(~col1, ~col1_gen_r),
                JoinKind.RIGHT,
                {l, r | ($l.col2 == $r.col2_gen_r) && ($l.col1 == $r.col1_gen_r)}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->join('
                '#Table(test_schema.test_table2)#->rename(~col2, ~col2_gen_r)->rename(~col1, ~col1_gen_r), '
                'JoinKind.RIGHT, '
                '{l, r | ($l.col2 == $r.col2_gen_r) && ($l.col1 == $r.col1_gen_r)})')

    def test_e2e_join_by_columns_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame1: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame1 = frame1.restrict(['Last Name', 'First Name'])
        frame2: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame2 = frame2.restrict(['Age', 'Last Name'])
        frame = frame1.join_by_columns(frame2, ['Last Name'], ['Last Name'])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer)]")
        expected = {'columns': ['First Name', 'Last Name', 'Age'],
                    'rows': [{'values': ['Peter', 'Smith', 23]},
                             {'values': ['John', 'Johnson', 22]},
                             {'values': ['John', 'Hill', 12]},
                             {'values': ['John', 'Hill', 32]},
                             {'values': ['Anthony', 'Allen', 22]},
                             {'values': ['Fabrice', 'Roberts', 34]},
                             {'values': ['Oliver', 'Hill', 12]},
                             {'values': ['Oliver', 'Hill', 32]},
                             {'values': ['David', 'Harris', 35]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_join_by_columns_function_multi_key(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]])\
            -> None:
        frame1: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame1 = frame1.restrict(['First Name', 'Last Name', 'Age'])
        frame2: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame2 = frame2.restrict(['First Name', 'Last Name', 'Firm/Legal Name'])
        frame = frame1.join_by_columns(frame2, ['First Name', 'Last Name'], ['First Name', 'Last Name'])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: First Name, Type: String), "
                "TdsColumn(Name: Last Name, Type: String), TdsColumn(Name: Firm/Legal Name, Type: String)]")
        expected = {'columns': ['Age', 'First Name', 'Last Name', 'Firm/Legal Name'],
                    'rows': [{'values': [23, 'Peter', 'Smith', 'Firm X']},
                             {'values': [22, 'John', 'Johnson', 'Firm X']},
                             {'values': [12, 'John', 'Hill', 'Firm X']},
                             {'values': [22, 'Anthony', 'Allen', 'Firm X']},
                             {'values': [34, 'Fabrice', 'Roberts', 'Firm A']},
                             {'values': [32, 'Oliver', 'Hill', 'Firm B']},
                             {'values': [35, 'David', 'Harris', 'Firm C']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_join_by_columns_function_inner_join(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]])\
            -> None:
        frame1: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame1 = frame1.restrict(['First Name', 'Last Name', 'Age'])
        frame2: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame2 = frame2.filter(lambda r: r['First Name'] == 'John')
        frame2 = frame2.restrict(['First Name', 'Last Name', 'Firm/Legal Name'])
        frame = frame1.join_by_columns(frame2, ['First Name', 'Last Name'], ['First Name', 'Last Name'], 'INNER')
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: First Name, Type: String), "
                "TdsColumn(Name: Last Name, Type: String), TdsColumn(Name: Firm/Legal Name, Type: String)]")
        expected = {'columns': ['Age', 'First Name', 'Last Name', 'Firm/Legal Name'],
                    'rows': [{'values': [22, 'John', 'Johnson', 'Firm X']},
                             {'values': [12, 'John', 'Hill', 'Firm X']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_join_by_columns_function_right_join(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]])\
            -> None:
        frame1: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame1 = frame1.filter(lambda r: r['First Name'] == 'John')
        frame1 = frame1.restrict(['First Name', 'Last Name', 'Age'])
        frame2: LegacyApiTdsFrame = simple_person_service_frame(legend_test_server['engine_port'])
        frame2 = frame2.restrict(['First Name', 'Last Name', 'Firm/Legal Name'])
        frame = frame1.join_by_columns(frame2, ['First Name', 'Last Name'], ['First Name', 'Last Name'], 'RIGHT_OUTER')
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: First Name, Type: String), "
                "TdsColumn(Name: Last Name, Type: String), TdsColumn(Name: Firm/Legal Name, Type: String)]")
        expected = {'columns': ['Age', 'First Name', 'Last Name', 'Firm/Legal Name'],
                    'rows': [{'values': [None, 'Peter', 'Smith', 'Firm X']},
                             {'values': [22, 'John', 'Johnson', 'Firm X']},
                             {'values': [12, 'John', 'Hill', 'Firm X']},
                             {'values': [None, 'Anthony', 'Allen', 'Firm X']},
                             {'values': [None, 'Fabrice', 'Roberts', 'Firm A']},
                             {'values': [None, 'Oliver', 'Hill', 'Firm B']},
                             {'values': [None, 'David', 'Harris', 'Firm C']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
