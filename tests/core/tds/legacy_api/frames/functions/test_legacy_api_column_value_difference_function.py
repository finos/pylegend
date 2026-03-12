# Copyright 2026 Goldman Sachs
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

import pytest
from textwrap import dedent
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legacy_api.frames.legacy_api_tds_frame import LegacyApiTdsFrame
from pylegend.extensions.tds.legacy_api.frames.legacy_api_table_spec_input_frame import LegacyApiTableSpecInputFrame
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile


class TestColumnValueDifferenceFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_column_value_difference_result_columns(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)

        cols2 = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        result = frame1.column_value_difference(frame2, ["id"], ["id"], ["val"])
        result_col_names = [c.get_name() for c in result.columns()]
        assert result_col_names == ["id", "val_1", "val_2", "val_valueDifference"]

    def test_column_value_difference_result_columns_multiple_check_cols(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)

        cols2 = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        result = frame1.column_value_difference(frame2, ["id"], ["id"], ["valA", "valB"])
        result_col_names = [c.get_name() for c in result.columns()]
        assert result_col_names == [
            "id",
            "valA_1", "valA_2", "valA_valueDifference",
            "valB_1", "valB_2", "valB_valueDifference",
        ]

    def test_column_value_difference_result_columns_different_join_cols(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("key1"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)

        cols2 = [
            PrimitiveTdsColumn.integer_column("key2"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        result = frame1.column_value_difference(frame2, ["key1"], ["key2"], ["val"])
        result_col_names = [c.get_name() for c in result.columns()]
        assert result_col_names == ["key1", "key2", "val_1", "val_2", "val_valueDifference"]

    def test_column_value_difference_sql_gen(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)

        cols2 = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        result = frame1.column_value_difference(frame2, ["id"], ["id"], ["val"])
        expected = '''\
            SELECT
                "root"."id" AS "id",
                "root"."val_1" AS "val_1",
                "root"."val_2" AS "val_2",
                "root"."val_valueDifference" AS "val_valueDifference"
            FROM
                (
                    SELECT
                        "left"."id" AS "id",
                        "left"."val_1" AS "val_1",
                        "left"."val_2" AS "val_2",
                        "left"."val_valueDifference" AS "val_valueDifference"
                    FROM
                        (
                            SELECT
                                "root"."id" AS "id",
                                "root"."val_1" AS "val_1",
                                "root"."val_2" AS "val_2",
                                ("root"."val_1" - "root"."val_2") AS "val_valueDifference"
                            FROM
                                (
                                    SELECT
                                        "left"."val_1" AS "val_1",
                                        "left"."id" AS "id",
                                        "right"."val_2" AS "val_2"
                                    FROM
                                        (
                                            SELECT
                                                "root".id AS "id",
                                                "root".val AS "val_1"
                                            FROM
                                                test_schema.test_table1 AS "root"
                                        ) AS "left"
                                        LEFT OUTER JOIN
                                            (
                                                SELECT
                                                    "root".id AS "id",
                                                    "root".val AS "val_2"
                                                FROM
                                                    test_schema.test_table2 AS "root"
                                            ) AS "right"
                                            ON ("left"."id" = "right"."id")
                                ) AS "root"
                            WHERE
                                ("root"."val_1" IS NOT NULL)
                        ) AS "left"
                    UNION ALL
                    SELECT
                        "right"."id" AS "id",
                        "right"."val_1" AS "val_1",
                        "right"."val_2" AS "val_2",
                        "right"."val_valueDifference" AS "val_valueDifference"
                    FROM
                        (
                            SELECT
                                "root"."id" AS "id",
                                "root"."val_1" AS "val_1",
                                "root"."val_2" AS "val_2",
                                ("root"."val_1" - "root"."val_2") AS "val_valueDifference"
                            FROM
                                (
                                    SELECT
                                        "left"."val_1" AS "val_1",
                                        "right"."id" AS "id",
                                        "right"."val_2" AS "val_2"
                                    FROM
                                        (
                                            SELECT
                                                "root".id AS "id",
                                                "root".val AS "val_1"
                                            FROM
                                                test_schema.test_table1 AS "root"
                                        ) AS "left"
                                        RIGHT OUTER JOIN
                                            (
                                                SELECT
                                                    "root".id AS "id",
                                                    "root".val AS "val_2"
                                                FROM
                                                    test_schema.test_table2 AS "root"
                                            ) AS "right"
                                            ON ("left"."id" = "right"."id")
                                ) AS "root"
                            WHERE
                                ("root"."val_1" IS NULL)
                        ) AS "right"
                ) AS "root"'''
        assert result.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_column_value_difference_pure_gen(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)

        cols2 = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        result = frame1.column_value_difference(frame2, ["id"], ["id"], ["val"])
        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->select(~[id, val])
              ->rename(~val, ~val_1)
              ->join(
                #Table(test_schema.test_table2)#
                  ->select(~[id, val])
                  ->rename(~val, ~val_2)
                  ->rename(~id, ~id_gen_r),
                JoinKind.LEFT,
                {l, r  $l.id == $r.id_gen_r}
              )
              ->filter({r  $r.val_1->isNotEmpty()})
              ->extend(~val_valueDifference:{r  toOne($r.val_1) - toOne($r.val_2)})
              ->select(~[id, val_1, val_2, val_valueDifference])
              ->concatenate(
                #Table(test_schema.test_table1)#
                  ->select(~[id, val])
                  ->rename(~val, ~val_1)
                  ->join(
                    #Table(test_schema.test_table2)#
                      ->select(~[id, val])
                      ->rename(~val, ~val_2)
                      ->rename(~id, ~id_gen_r),
                    JoinKind.RIGHT,
                    {l, r  $l.id == $r.id_gen_r}
                  )
                  ->filter({r  $r.val_1->isEmpty()})
                  ->extend(~val_valueDifference:{r  toOne($r.val_1) - toOne($r.val_2)})
                  ->select(~[id, val_1, val_2, val_valueDifference])
              )'''
        )
        assert generate_pure_query_and_compile(result, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table1)#'
            '->select(~[id, val])->rename(~val, ~val_1)'
            '->join(#Table(test_schema.test_table2)#->select(~[id, val])'
            '->rename(~val, ~val_2)->rename(~id, ~id_gen_r), '
            'JoinKind.LEFT, {l, r  $l.id == $r.id_gen_r})'
            '->filter({r  $r.val_1->isNotEmpty()})'
            '->extend(~val_valueDifference:{r  toOne($r.val_1) - toOne($r.val_2)})'
            '->select(~[id, val_1, val_2, val_valueDifference])'
            '->concatenate(#Table(test_schema.test_table1)#'
            '->select(~[id, val])->rename(~val, ~val_1)'
            '->join(#Table(test_schema.test_table2)#->select(~[id, val])'
            '->rename(~val, ~val_2)->rename(~id, ~id_gen_r), '
            'JoinKind.RIGHT, {l, r  $l.id == $r.id_gen_r})'
            '->filter({r  $r.val_1->isEmpty()})'
            '->extend(~val_valueDifference:{r  toOne($r.val_1) - toOne($r.val_2)})'
            '->select(~[id, val_1, val_2, val_valueDifference]))'
        )

    def test_column_value_difference_different_join_cols_sql_gen(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("key1"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)

        cols2 = [
            PrimitiveTdsColumn.integer_column("key2"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        result = frame1.column_value_difference(frame2, ["key1"], ["key2"], ["val"])
        expected = '''\
            SELECT
                "root"."key1" AS "key1",
                "root"."key2" AS "key2",
                "root"."val_1" AS "val_1",
                "root"."val_2" AS "val_2",
                "root"."val_valueDifference" AS "val_valueDifference"
            FROM
                (
                    SELECT
                        "left"."key1" AS "key1",
                        "left"."key2" AS "key2",
                        "left"."val_1" AS "val_1",
                        "left"."val_2" AS "val_2",
                        "left"."val_valueDifference" AS "val_valueDifference"
                    FROM
                        (
                            SELECT
                                "root"."key1" AS "key1",
                                "root"."key2" AS "key2",
                                "root"."val_1" AS "val_1",
                                "root"."val_2" AS "val_2",
                                ("root"."val_1" - "root"."val_2") AS "val_valueDifference"
                            FROM
                                (
                                    SELECT
                                        "left"."key1" AS "key1",
                                        "left"."val_1" AS "val_1",
                                        "right"."key2" AS "key2",
                                        "right"."val_2" AS "val_2"
                                    FROM
                                        (
                                            SELECT
                                                "root".key1 AS "key1",
                                                "root".val AS "val_1"
                                            FROM
                                                test_schema.test_table1 AS "root"
                                        ) AS "left"
                                        LEFT OUTER JOIN
                                            (
                                                SELECT
                                                    "root".key2 AS "key2",
                                                    "root".val AS "val_2"
                                                FROM
                                                    test_schema.test_table2 AS "root"
                                            ) AS "right"
                                            ON ("left"."key1" = "right"."key2")
                                ) AS "root"
                            WHERE
                                ("root"."val_1" IS NOT NULL)
                        ) AS "left"
                    UNION ALL
                    SELECT
                        "right"."key1" AS "key1",
                        "right"."key2" AS "key2",
                        "right"."val_1" AS "val_1",
                        "right"."val_2" AS "val_2",
                        "right"."val_valueDifference" AS "val_valueDifference"
                    FROM
                        (
                            SELECT
                                "root"."key1" AS "key1",
                                "root"."key2" AS "key2",
                                "root"."val_1" AS "val_1",
                                "root"."val_2" AS "val_2",
                                ("root"."val_1" - "root"."val_2") AS "val_valueDifference"
                            FROM
                                (
                                    SELECT
                                        "left"."key1" AS "key1",
                                        "left"."val_1" AS "val_1",
                                        "right"."key2" AS "key2",
                                        "right"."val_2" AS "val_2"
                                    FROM
                                        (
                                            SELECT
                                                "root".key1 AS "key1",
                                                "root".val AS "val_1"
                                            FROM
                                                test_schema.test_table1 AS "root"
                                        ) AS "left"
                                        RIGHT OUTER JOIN
                                            (
                                                SELECT
                                                    "root".key2 AS "key2",
                                                    "root".val AS "val_2"
                                                FROM
                                                    test_schema.test_table2 AS "root"
                                            ) AS "right"
                                            ON ("left"."key1" = "right"."key2")
                                ) AS "root"
                            WHERE
                                ("root"."val_1" IS NULL)
                        ) AS "right"
                ) AS "root"'''
        assert result.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_column_value_difference_multiple_join_columns(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("key1"),
            PrimitiveTdsColumn.string_column("key2"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)

        cols2 = [
            PrimitiveTdsColumn.integer_column("key1"),
            PrimitiveTdsColumn.string_column("key2"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        result = frame1.column_value_difference(frame2, ["key1", "key2"], ["key1", "key2"], ["val"])
        result_col_names = [c.get_name() for c in result.columns()]
        assert result_col_names == ["key1", "key2", "val_1", "val_2", "val_valueDifference"]

        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->select(~[key1, key2, val])
              ->rename(~val, ~val_1)
              ->join(
                #Table(test_schema.test_table2)#
                  ->select(~[key1, key2, val])
                  ->rename(~val, ~val_2)
                  ->rename(~key1, ~key1_gen_r)
                  ->rename(~key2, ~key2_gen_r),
                JoinKind.LEFT,
                {l, r  ($l.key1 == $r.key1_gen_r) && ($l.key2 == $r.key2_gen_r)}
              )
              ->filter({r  $r.val_1->isNotEmpty()})
              ->extend(~val_valueDifference:{r  toOne($r.val_1) - toOne($r.val_2)})
              ->select(~[key1, key2, val_1, val_2, val_valueDifference])
              ->concatenate(
                #Table(test_schema.test_table1)#
                  ->select(~[key1, key2, val])
                  ->rename(~val, ~val_1)
                  ->join(
                    #Table(test_schema.test_table2)#
                      ->select(~[key1, key2, val])
                      ->rename(~val, ~val_2)
                      ->rename(~key1, ~key1_gen_r)
                      ->rename(~key2, ~key2_gen_r),
                    JoinKind.RIGHT,
                    {l, r  ($l.key1 == $r.key1_gen_r) && ($l.key2 == $r.key2_gen_r)}
                  )
                  ->filter({r  $r.val_1->isEmpty()})
                  ->extend(~val_valueDifference:{r  toOne($r.val_1) - toOne($r.val_2)})
                  ->select(~[key1, key2, val_1, val_2, val_valueDifference])
              )'''
        )

    def test_column_value_difference_multiple_check_cols_sql_and_pure_gen(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame1: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)

        cols2 = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.integer_column("valA"),
            PrimitiveTdsColumn.integer_column("valB"),
        ]
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)

        result = frame1.column_value_difference(frame2, ["id"], ["id"], ["valA", "valB"])
        expected = '''\
            SELECT
                "root"."id" AS "id",
                "root"."valA_1" AS "valA_1",
                "root"."valA_2" AS "valA_2",
                "root"."valA_valueDifference" AS "valA_valueDifference",
                "root"."valB_1" AS "valB_1",
                "root"."valB_2" AS "valB_2",
                "root"."valB_valueDifference" AS "valB_valueDifference"
            FROM
                (
                    SELECT
                        "left"."id" AS "id",
                        "left"."valA_1" AS "valA_1",
                        "left"."valA_2" AS "valA_2",
                        "left"."valA_valueDifference" AS "valA_valueDifference",
                        "left"."valB_1" AS "valB_1",
                        "left"."valB_2" AS "valB_2",
                        "left"."valB_valueDifference" AS "valB_valueDifference"
                    FROM
                        (
                            SELECT
                                "root"."id" AS "id",
                                "root"."valA_1" AS "valA_1",
                                "root"."valA_2" AS "valA_2",
                                ("root"."valA_1" - "root"."valA_2") AS "valA_valueDifference",
                                "root"."valB_1" AS "valB_1",
                                "root"."valB_2" AS "valB_2",
                                ("root"."valB_1" - "root"."valB_2") AS "valB_valueDifference"
                            FROM
                                (
                                    SELECT
                                        "left"."valA_1" AS "valA_1",
                                        "left"."valB_1" AS "valB_1",
                                        "left"."id" AS "id",
                                        "right"."valA_2" AS "valA_2",
                                        "right"."valB_2" AS "valB_2"
                                    FROM
                                        (
                                            SELECT
                                                "root".id AS "id",
                                                "root".valA AS "valA_1",
                                                "root".valB AS "valB_1"
                                            FROM
                                                test_schema.test_table1 AS "root"
                                        ) AS "left"
                                        LEFT OUTER JOIN
                                            (
                                                SELECT
                                                    "root".id AS "id",
                                                    "root".valA AS "valA_2",
                                                    "root".valB AS "valB_2"
                                                FROM
                                                    test_schema.test_table2 AS "root"
                                            ) AS "right"
                                            ON ("left"."id" = "right"."id")
                                ) AS "root"
                            WHERE
                                (("root"."valA_1" IS NOT NULL) AND ("root"."valB_1" IS NOT NULL))
                        ) AS "left"
                    UNION ALL
                    SELECT
                        "right"."id" AS "id",
                        "right"."valA_1" AS "valA_1",
                        "right"."valA_2" AS "valA_2",
                        "right"."valA_valueDifference" AS "valA_valueDifference",
                        "right"."valB_1" AS "valB_1",
                        "right"."valB_2" AS "valB_2",
                        "right"."valB_valueDifference" AS "valB_valueDifference"
                    FROM
                        (
                            SELECT
                                "root"."id" AS "id",
                                "root"."valA_1" AS "valA_1",
                                "root"."valA_2" AS "valA_2",
                                ("root"."valA_1" - "root"."valA_2") AS "valA_valueDifference",
                                "root"."valB_1" AS "valB_1",
                                "root"."valB_2" AS "valB_2",
                                ("root"."valB_1" - "root"."valB_2") AS "valB_valueDifference"
                            FROM
                                (
                                    SELECT
                                        "left"."valA_1" AS "valA_1",
                                        "left"."valB_1" AS "valB_1",
                                        "right"."id" AS "id",
                                        "right"."valA_2" AS "valA_2",
                                        "right"."valB_2" AS "valB_2"
                                    FROM
                                        (
                                            SELECT
                                                "root".id AS "id",
                                                "root".valA AS "valA_1",
                                                "root".valB AS "valB_1"
                                            FROM
                                                test_schema.test_table1 AS "root"
                                        ) AS "left"
                                        RIGHT OUTER JOIN
                                            (
                                                SELECT
                                                    "root".id AS "id",
                                                    "root".valA AS "valA_2",
                                                    "root".valB AS "valB_2"
                                                FROM
                                                    test_schema.test_table2 AS "root"
                                            ) AS "right"
                                            ON ("left"."id" = "right"."id")
                                ) AS "root"
                            WHERE
                                (("root"."valA_1" IS NULL) AND ("root"."valB_1" IS NULL))
                        ) AS "right"
                ) AS "root"'''
        assert result.to_sql_query(FrameToSqlConfig()) == dedent(expected)

        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->select(~[id, valA, valB])
              ->rename(~valA, ~valA_1)
              ->rename(~valB, ~valB_1)
              ->join(
                #Table(test_schema.test_table2)#
                  ->select(~[id, valA, valB])
                  ->rename(~valA, ~valA_2)
                  ->rename(~valB, ~valB_2)
                  ->rename(~id, ~id_gen_r),
                JoinKind.LEFT,
                {l, r  $l.id == $r.id_gen_r}
              )
              ->filter({r  $r.valA_1->isNotEmpty() && $r.valB_1->isNotEmpty()})
              ->extend(~[
                valA_valueDifference:{r  toOne($r.valA_1) - toOne($r.valA_2)},
                valB_valueDifference:{r  toOne($r.valB_1) - toOne($r.valB_2)}
              ])
              ->select(~[id, valA_1, valA_2, valA_valueDifference, valB_1, valB_2, valB_valueDifference])
              ->concatenate(
                #Table(test_schema.test_table1)#
                  ->select(~[id, valA, valB])
                  ->rename(~valA, ~valA_1)
                  ->rename(~valB, ~valB_1)
                  ->join(
                    #Table(test_schema.test_table2)#
                      ->select(~[id, valA, valB])
                      ->rename(~valA, ~valA_2)
                      ->rename(~valB, ~valB_2)
                      ->rename(~id, ~id_gen_r),
                    JoinKind.RIGHT,
                    {l, r  $l.id == $r.id_gen_r}
                  )
                  ->filter({r  $r.valA_1->isEmpty() && $r.valB_1->isEmpty()})
                  ->extend(~[
                    valA_valueDifference:{r  toOne($r.valA_1) - toOne($r.valA_2)},
                    valB_valueDifference:{r  toOne($r.valB_1) - toOne($r.valB_2)}
                  ])
                  ->select(~[id, valA_1, valA_2, valA_valueDifference, valB_1, valB_2, valB_valueDifference])
              )'''
        )
