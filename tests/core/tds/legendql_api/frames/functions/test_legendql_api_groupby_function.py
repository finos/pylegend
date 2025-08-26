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
from tests.test_helpers.test_legend_service_frames import (
    simple_person_service_frame_legendql_api,
    simple_trade_service_frame_legendql_api,
)
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile


class TestGroupByAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_group_by_error_on_incompatible_columns_func(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.group_by(lambda: ["col1"], [])  # type: ignore
        assert r.value.args[0] == (
            "'group_by' function grouping columns argument can either be a list of strings (column names) "
            "or a lambda function which takes one argument (LegendQLApiTdsRow)"
        )

    def test_group_by_error_on_unknown_column(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.group_by(lambda x: x["col3"], [])
        assert r.value.args[0] == (
            "'group_by' function grouping columns argument lambda incompatible. "
            "Error occurred while evaluating. Message: Column - 'col3' doesn't exist in the current frame. "
            "Current frame columns: ['col1', 'col2']"
        )

    def test_group_by_error_on_unknown_column2(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.group_by(lambda x: [x.col3], [])
        assert r.value.args[0] == (
            "'group_by' function grouping columns argument lambda incompatible. "
            "Error occurred while evaluating. Message: Column - 'col3' doesn't exist in the current frame. "
            "Current frame columns: ['col1', 'col2']"
        )

    def test_group_by_error_on_incompatible_column(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.group_by(lambda x: x.col1 + 1, [])  # type: ignore
        assert r.value.args[0] == (
            "'group_by' function grouping columns argument lambda incompatible. "
            "Columns can be simple column expressions (E.g - lambda r: [r.column1, r.column2, r['column with spaces']). "
            "Element at index 0 (0-indexed) is incompatible")

    def test_group_by_error_on_empty_cols(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.group_by(lambda x: [], [])
        assert r.value.args[0] == ("At-least one grouping column or aggregate specification must be provided "
                                   "when using group_by function")

    def test_group_by_error_on_duplicate_cols(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.group_by(
                "col1", ("col1", lambda x: x.col1, lambda y: y.sum())  # type: ignore
            )
        assert r.value.args[0] == ("Found duplicate column names in grouping columns and aggregation columns. "
                                   "Grouping columns - ['col1'], Aggregation columns - ['col1']")

    def test_group_by_error_on_incompatible_map_fn(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.group_by(
                "col1", ("col3", lambda: 1, lambda y: y.sum())   # type: ignore
            )
        assert r.value.args[0] == (
            "'group_by' function aggregate specifications incompatible. Each aggregate specification should be a "
            "triplet with first element being the aggregation column name, second element being a mapper function "
            "(single argument lambda) and third element being the aggregation function (single argument lambda). "
            "E.g - ('count_col', lambda r: r['col1'], lambda c: c.count()). "
            "Element at index 0 (0-indexed) is incompatible"
        )

    def test_group_by_error_on_map_fn_evaluation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.group_by(
                "col1", ("col1", lambda x: x.col3, lambda y: y.sum())  # type: ignore
            )
        assert r.value.args[0] == (
            "'group_by' function aggregate specifications incompatible. Error occurred while evaluating mapper lambda "
            "in the aggregate specification at index 0 (0-indexed). "
            "Message: Column - 'col3' doesn't exist in the current frame. Current frame columns: ['col1', 'col2']"
        )

    def test_group_by_error_on_map_fn_non_primitive(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.group_by(
                "col1", ("col1", lambda x: x, lambda y: y.sum())  # type: ignore
            )
        assert r.value.args[0] == (
            "'group_by' function aggregate specifications incompatible. Mapper lambda in the aggregate specification "
            "at index 0 (0-indexed) returns non-primitive - <"
            "class 'pylegend.core.language.legendql_api.legendql_api_tds_row.LegendQLApiTdsRow'>")

    def test_group_by_error_on_incompatible_agg_fn(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.group_by(
                "col1", ("col3", lambda x: x.col2, lambda: 3)  # type: ignore
            )
        assert r.value.args[0] == (
            "'group_by' function aggregate specifications incompatible. Each aggregate specification should be a "
            "triplet with first element being the aggregation column name, second element being a mapper function "
            "(single argument lambda) and third element being the aggregation function (single argument lambda). "
            "E.g - ('count_col', lambda r: r['col1'], lambda c: c.count()). "
            "Element at index 0 (0-indexed) is incompatible"
        )

    def test_group_by_error_on_agg_fn_evaluation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.group_by(
                "col1", ("col3", lambda x: x.col2, lambda y: y.unknown())  # type: ignore
            )
        assert r.value.args[0] == (
            "'group_by' function aggregate specifications incompatible. Error occurred while evaluating aggregation "
            "lambda in the aggregate specification at index 0 (0-indexed). "
            "Message: 'PyLegendStringCollection' object has no attribute 'unknown'"
        )

    def test_group_by_error_on_agg_fn_non_primitive(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.group_by(
                "col1", ("col3", lambda x: x.col2, lambda y: y)  # type: ignore
            )
        assert r.value.args[0] == (
            "'group_by' function aggregate specifications incompatible. Aggregation lambda in the aggregate "
            "specification at index 0 (0-indexed) returns non-primitive - "
            "<class 'pylegend.core.language.shared.primitive_collection.PyLegendStringCollection'>"
        )

    def test_query_gen_group_by(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            lambda r: r.col1,
            ("Count", lambda r: r.col2, lambda col: col.count())
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: Count, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                COUNT("root".col2) AS "Count"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[Count:{r | $r.col2}:{c | $c->count()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col1], ~[Count:{r | $r.col2}:{c | $c->count()}])')

    def test_query_gen_group_by_on_literal(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            lambda r: r.col1,
            ("Count", lambda r: 1, lambda col: col.count())
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: Count, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                COUNT(1) AS "Count"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[Count:{r | 1}:{c | $c->count()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col1], ~[Count:{r | 1}:{c | $c->count()}])')

    def test_query_gen_group_by_with_distinct(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.distinct()
        frame = frame.group_by(
            lambda r: r.col1,
            ("Count", lambda r: r.col2, lambda col: col.count())
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: Count, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                COUNT("root"."col2") AS "Count"
            FROM
                (
                    SELECT DISTINCT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
            GROUP BY
                "root"."col1"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->distinct()
              ->groupBy(
                ~[col1],
                ~[Count:{r | $r.col2}:{c | $c->count()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->distinct()'
                '->groupBy(~[col1], ~[Count:{r | $r.col2}:{c | $c->count()}])')

    def test_query_gen_group_by_with_limit(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.head(10)
        frame = frame.group_by(
            lambda r: r.col1,
            ("Count", lambda r: r.col2, lambda col: col.count())
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: Count, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                COUNT("root"."col2") AS "Count"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 10
                ) AS "root"
            GROUP BY
                "root"."col1"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->limit(10)
              ->groupBy(
                ~[col1],
                ~[Count:{r | $r.col2}:{c | $c->count()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->limit(10)'
                '->groupBy(~[col1], ~[Count:{r | $r.col2}:{c | $c->count()}])')

    def test_query_gen_multi_group_by(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.head(10)
        frame = frame.group_by(
            lambda r: r.col1,
            ("Count1", lambda r: r.col2, lambda col: col.count())
        )
        frame = frame.group_by(
            lambda r: r.col1,
            ("Count2", lambda r: r.Count1, lambda col: col.count())
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: Count2, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                COUNT("root"."Count1") AS "Count2"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        COUNT("root"."col2") AS "Count1"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2"
                            FROM
                                test_schema.test_table AS "root"
                            LIMIT 10
                        ) AS "root"
                    GROUP BY
                        "root"."col1"
                ) AS "root"
            GROUP BY
                "root"."col1"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->limit(10)
              ->groupBy(
                ~[col1],
                ~[Count1:{r | $r.col2}:{c | $c->count()}]
              )
              ->groupBy(
                ~[col1],
                ~[Count2:{r | $r.Count1}:{c | $c->count()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->limit(10)'
                '->groupBy(~[col1], ~[Count1:{r | $r.col2}:{c | $c->count()}])'
                '->groupBy(~[col1], ~[Count2:{r | $r.Count1}:{c | $c->count()}])')

    def test_query_gen_group_by_multi_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            lambda r: r.col1,
            [
                ("Count1", lambda r: r.col2, lambda c: c.count()),
                ("Count2", lambda r: r.col2, lambda c: c.count())
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: Count1, Type: Integer), "
            "TdsColumn(Name: Count2, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                COUNT("root".col2) AS "Count1",
                COUNT("root".col2) AS "Count2"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[Count1:{r | $r.col2}:{c | $c->count()}, Count2:{r | $r.col2}:{c | $c->count()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->groupBy(~[col1], ~[Count1:{r | $r.col2}:{c | $c->count()}, Count2:{r | $r.col2}:{c | $c->count()}])')

    def test_query_gen_group_by_distinct_count_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Cnt", lambda r: r.col1, lambda c: c.distinct_count()),
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Cnt, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                COUNT(DISTINCT "root".col1) AS "Cnt"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Cnt:{r | $r.col1}:{c | $c->distinct()->count()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Cnt:{r | $r.col1}:{c | $c->distinct()->count()}])')

    def test_query_gen_group_by_average_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Average", lambda r: r.col1, lambda c: c.average()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Average, Type: Float)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                AVG("root".col1) AS "Average"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Average:{r | $r.col1}:{c | $c->average()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->groupBy(~[col2], ~[Average:{r | $r.col1}:{c | $c->average()}])')

    def test_query_gen_group_by_average_agg_pre_op(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Average", lambda r: r.col1 + 20, lambda c: c.average()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Average, Type: Float)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                AVG(("root".col1 + 20)) AS "Average"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Average:{r | toOne($r.col1) + 20}:{c | $c->average()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Average:{r | toOne($r.col1) + 20}:{c | $c->average()}])')

    def test_query_gen_group_by_average_agg_post_op(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Average", lambda r: r.col1, lambda c: c.average() + 2),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Average, Type: Number)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                (AVG("root".col1) + 2) AS "Average"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Average:{r | $r.col1}:{c | toOne($c->average()) + 2}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Average:{r | $r.col1}:{c | toOne($c->average()) + 2}])')

    def test_query_gen_group_by_integer_max_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Maximum", lambda r: r.col1, lambda c: c.max()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Maximum, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                MAX("root".col1) AS "Maximum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Maximum:{r | $r.col1}:{c | $c->max()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Maximum:{r | $r.col1}:{c | $c->max()}])')

    def test_query_gen_group_by_integer_min_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Minimum", lambda r: r.col1, lambda c: c.min()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Minimum, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                MIN("root".col1) AS "Minimum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Minimum:{r | $r.col1}:{c | $c->min()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Minimum:{r | $r.col1}:{c | $c->min()}])')

    def test_query_gen_group_by_integer_sum_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Sum", lambda r: r.col1, lambda c: c.sum()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Sum, Type: Integer)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                SUM("root".col1) AS "Sum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Sum:{r | $r.col1}:{c | $c->sum()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Sum:{r | $r.col1}:{c | $c->sum()}])')

    def test_query_gen_group_by_float_max_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Maximum", lambda r: r.col1, lambda c: c.max()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Maximum, Type: Float)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                MAX("root".col1) AS "Maximum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Maximum:{r | $r.col1}:{c | $c->max()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Maximum:{r | $r.col1}:{c | $c->max()}])')

    def test_query_gen_group_by_float_min_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Minimum", lambda r: r.col1, lambda c: c.min()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Minimum, Type: Float)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                MIN("root".col1) AS "Minimum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Minimum:{r | $r.col1}:{c | $c->min()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Minimum:{r | $r.col1}:{c | $c->min()}])')

    def test_query_gen_group_by_float_sum_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.float_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Sum", lambda r: r.col1, lambda c: c.sum()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Sum, Type: Float)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                SUM("root".col1) AS "Sum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Sum:{r | $r.col1}:{c | $c->sum()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Sum:{r | $r.col1}:{c | $c->sum()}])')

    def test_query_gen_group_by_number_max_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Maximum", lambda r: r.col1, lambda c: c.max()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Maximum, Type: Number)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                MAX("root".col1) AS "Maximum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Maximum:{r | $r.col1}:{c | $c->max()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Maximum:{r | $r.col1}:{c | $c->max()}])')

    def test_query_gen_group_by_number_min_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Minimum", lambda r: r.col1, lambda c: c.min()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Minimum, Type: Number)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                MIN("root".col1) AS "Minimum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Minimum:{r | $r.col1}:{c | $c->min()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Minimum:{r | $r.col1}:{c | $c->min()}])')

    def test_query_gen_group_by_number_sum_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Sum", lambda r: r.col1, lambda c: c.sum()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Sum, Type: Number)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                SUM("root".col1) AS "Sum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Sum:{r | $r.col1}:{c | $c->sum()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col2], ~[Sum:{r | $r.col1}:{c | $c->sum()}])')

    def test_query_gen_group_by_std_dev_sample_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Std Dev Sample", lambda r: r.col1, lambda c: c.std_dev_sample()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Std Dev Sample, Type: Number)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                STDDEV_SAMP("root".col1) AS "Std Dev Sample"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~['Std Dev Sample':{r | $r.col1}:{c | $c->stdDevSample()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->groupBy(~[col2], ~[\'Std Dev Sample\':{r | $r.col1}:{c | $c->stdDevSample()}])')

    def test_query_gen_group_by_std_dev_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Std Dev", lambda r: r.col1, lambda c: c.std_dev()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Std Dev, Type: Number)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                STDDEV_SAMP("root".col1) AS "Std Dev"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~['Std Dev':{r | $r.col1}:{c | $c->stdDevSample()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->groupBy(~[col2], ~[\'Std Dev\':{r | $r.col1}:{c | $c->stdDevSample()}])')

    def test_query_gen_group_by_std_dev_population_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Std Dev Population", lambda r: r.col1, lambda c: c.std_dev_population()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Std Dev Population, Type: Number)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                STDDEV_POP("root".col1) AS "Std Dev Population"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~['Std Dev Population':{r | $r.col1}:{c | $c->stdDevPopulation()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->groupBy(~[col2], ~[\'Std Dev Population\':{r | $r.col1}:{c | $c->stdDevPopulation()}])')

    def test_query_gen_group_by_variance_sample_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Variance Sample", lambda r: r.col1, lambda c: c.variance_sample()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Variance Sample, Type: Number)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                VAR_SAMP("root".col1) AS "Variance Sample"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~['Variance Sample':{r | $r.col1}:{c | $c->varianceSample()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->groupBy(~[col2], ~[\'Variance Sample\':{r | $r.col1}:{c | $c->varianceSample()}])')

    def test_query_gen_group_by_variance_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Variance", lambda r: r.col1, lambda c: c.variance()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Variance, Type: Number)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                VAR_SAMP("root".col1) AS "Variance"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~[Variance:{r | $r.col1}:{c | $c->varianceSample()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->groupBy(~[col2], ~[Variance:{r | $r.col1}:{c | $c->varianceSample()}])')

    def test_query_gen_group_by_variance_population_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col2"],
            ("Variance Population", lambda r: r.col1, lambda c: c.variance_population()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: Variance Population, Type: Number)]"
        )
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                VAR_POP("root".col1) AS "Variance Population"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col2],
                ~['Variance Population':{r | $r.col1}:{c | $c->variancePopulation()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->groupBy(~[col2], ~[\'Variance Population\':{r | $r.col1}:{c | $c->variancePopulation()}])')

    def test_query_gen_group_by_string_max_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col1"],
            ("Maximum", lambda r: r.col2, lambda c: c.max()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Number), TdsColumn(Name: Maximum, Type: String)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                MAX("root".col2) AS "Maximum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[Maximum:{r | $r.col2}:{c | $c->max()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col1], ~[Maximum:{r | $r.col2}:{c | $c->max()}])')

    def test_query_gen_group_by_string_min_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col1"],
            ("Minimum", lambda r: r.col2, lambda c: c.min()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Number), TdsColumn(Name: Minimum, Type: String)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                MIN("root".col2) AS "Minimum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[Minimum:{r | $r.col2}:{c | $c->min()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col1], ~[Minimum:{r | $r.col2}:{c | $c->min()}])')

    def test_query_gen_group_by_join_strings_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col1"],
            ("Joined", lambda r: r.col2, lambda c: c.join(' ')),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Number), TdsColumn(Name: Joined, Type: String)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                STRING_AGG("root".col2, ' ') AS "Joined"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[Joined:{r | $r.col2}:{c | $c->joinStrings(' ')}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col1], ~[Joined:{r | $r.col2}:{c | $c->joinStrings(\' \')}])')

    def test_query_gen_group_by_strictdate_max_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.strictdate_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col1"],
            ("Maximum", lambda r: r.col2, lambda c: c.max()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Number), TdsColumn(Name: Maximum, Type: StrictDate)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                MAX("root".col2) AS "Maximum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[Maximum:{r | $r.col2}:{c | $c->max()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col1], ~[Maximum:{r | $r.col2}:{c | $c->max()}])')

    def test_query_gen_group_by_strictdate_min_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.strictdate_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col1"],
            ("Minimum", lambda r: r.col2, lambda c: c.min()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Number), TdsColumn(Name: Minimum, Type: StrictDate)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                MIN("root".col2) AS "Minimum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[Minimum:{r | $r.col2}:{c | $c->min()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col1], ~[Minimum:{r | $r.col2}:{c | $c->min()}])')

    def test_query_gen_group_by_date_max_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.date_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col1"],
            ("Maximum", lambda r: r.col2, lambda c: c.max()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Number), TdsColumn(Name: Maximum, Type: Date)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                MAX("root".col2) AS "Maximum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[Maximum:{r | $r.col2}:{c | $c->max()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col1], ~[Maximum:{r | $r.col2}:{c | $c->max()}])')

    def test_query_gen_group_by_date_min_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.number_column("col1"),
            PrimitiveTdsColumn.date_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.group_by(
            ["col1"],
            ("Minimum", lambda r: r.col2, lambda c: c.min()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Number), TdsColumn(Name: Minimum, Type: Date)]"
        )
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                MIN("root".col2) AS "Minimum"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[Minimum:{r | $r.col2}:{c | $c->min()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->groupBy(~[col1], ~[Minimum:{r | $r.col2}:{c | $c->min()}])')

    def test_e2e_group_by(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Firm/Legal Name"],
            ("Employee Count", lambda r: r["First Name"], lambda c: c.count()),
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: Employee Count, Type: Integer)]"
        expected = {'columns': ['Firm/Legal Name', 'Employee Count'],
                    'rows': [{'values': ['Firm A', 1]},
                             {'values': ['Firm B', 1]},
                             {'values': ['Firm C', 1]},
                             {'values': ['Firm X', 4]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_with_limit(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.head(5)
        frame = frame.group_by(
            ["Firm/Legal Name"],
            ("Employee Count", lambda r: r["First Name"], lambda c: c.count()),
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: Employee Count, Type: Integer)]"
        expected = {'columns': ['Firm/Legal Name', 'Employee Count'],
                    'rows': [{'values': ['Firm A', 1]},
                             {'values': ['Firm X', 4]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_on_literal(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            lambda r: [],
            ("Total Employees", lambda r: 1, lambda c: c.count()),
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Total Employees, Type: Integer)]"
        expected = {'columns': ['Total Employees'], 'rows': [{'values': [7]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_multi_grouping_cols(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) \
            -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Firm/Legal Name", "First Name"],
            ("Employee Count", lambda r: r["Last Name"], lambda c: c.count()),
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: First Name, Type: String), "
                "TdsColumn(Name: Employee Count, Type: Integer)]")
        expected = {'columns': ['Firm/Legal Name', 'First Name', 'Employee Count'],
                    'rows': [{'values': ['Firm A', 'Fabrice', 1]},
                             {'values': ['Firm B', 'Oliver', 1]},
                             {'values': ['Firm C', 'David', 1]},
                             {'values': ['Firm X', 'Anthony', 1]},
                             {'values': ['Firm X', 'John', 2]},
                             {'values': ['Firm X', 'Peter', 1]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    @pytest.mark.skip(reason="Legend server doesn't execute this SQL as group by clause has derivation")
    def test_e2e_group_by_on_extended_col(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.extend(lambda x: ("Last Name Gen", x['Last Name'] + '_Gen'))  # type: ignore
        frame = frame.group_by(
            ["Firm/Legal Name", "Last Name Gen"],
            ("Employee Count", lambda r: r["First Name"], lambda c: c.count()),
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: Last Name Gen, Type: String), "
                "TdsColumn(Name: Employee Count, Type: Integer)]")
        expected = {'columns': ['Firm/Legal Name', 'Employee Count'],
                    'rows': [{'values': ['Firm A', 1]},
                             {'values': ['Firm B', 1]},
                             {'values': ['Firm C', 1]},
                             {'values': ['Firm X', 4]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_multi_aggregations(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Firm/Legal Name"],
            [
                ("Employee Count1", lambda r: r["First Name"], lambda c: c.count()),
                ("Employee Count2", lambda r: r["First Name"], lambda c: c.count())
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: Employee Count1, Type: Integer), "
                "TdsColumn(Name: Employee Count2, Type: Integer)]")
        expected = {'columns': ['Firm/Legal Name', 'Employee Count1', 'Employee Count2'],
                    'rows': [{'values': ['Firm A', 1, 1]},
                             {'values': ['Firm B', 1, 1]},
                             {'values': ['Firm C', 1, 1]},
                             {'values': ['Firm X', 4, 4]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_distinct_count_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.head(5)
        frame = frame.group_by(
            ["Firm/Legal Name"],
            ("Employee Count", lambda r: r["First Name"], lambda c: c.distinct_count()),
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: Employee Count, Type: Integer)]"
        expected = {'columns': ['Firm/Legal Name', 'Employee Count'],
                    'rows': [{'values': ['Firm A', 1]},
                             {'values': ['Firm X', 3]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_average_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_trade_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            ("Average Qty", lambda r: r["Quantity"], lambda c: c.average()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Average Qty, Type: Float)]"
        expected = {'columns': ['Product/Name', 'Average Qty'],
                    'rows': [{'values': [None, 5]},
                             {'values': ['Firm A', 22]},
                             {'values': ['Firm C', 35.2]},
                             {'values': ['Firm X', 172.5]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_average_agg_pre_op(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_trade_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            ("Average Qty", lambda r: r["Quantity"] + 20, lambda c: c.average()),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Average Qty, Type: Float)]"
        expected = {'columns': ['Product/Name', 'Average Qty'],
                    'rows': [{'values': [None, 25]},
                             {'values': ['Firm A', 42]},
                             {'values': ['Firm C', 55.2]},
                             {'values': ['Firm X', 192.5]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_average_agg_post_op(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) \
            -> None:
        frame: LegendQLApiTdsFrame = simple_trade_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            ("Average Qty", lambda r: r["Quantity"], lambda c: c.average() + 2),  # type: ignore
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Average Qty, Type: Number)]"
        expected = {'columns': ['Product/Name', 'Average Qty'],
                    'rows': [{'values': [None, 7]},
                             {'values': ['Firm A', 24]},
                             {'values': ['Firm C', 37.2]},
                             {'values': ['Firm X', 174.5]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_integer_max_min_sum_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_trade_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            [
                ("Max Trade Id", lambda r: r["Id"], lambda c: c.max()),  # type: ignore
                ("Min Trade Id", lambda r: r["Id"], lambda c: c.min()),  # type: ignore
                ("Sum Trade Id", lambda r: r["Id"], lambda c: c.sum()),  # type: ignore
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Max Trade Id, Type: Integer), "
                "TdsColumn(Name: Min Trade Id, Type: Integer), TdsColumn(Name: Sum Trade Id, Type: Integer)]")
        expected = {'columns': ['Product/Name', 'Max Trade Id', 'Min Trade Id', 'Sum Trade Id'],
                    'rows': [{'values': [None, 11, 11, 11]},
                             {'values': ['Firm A', 5, 3, 12]},
                             {'values': ['Firm C', 10, 6, 40]},
                             {'values': ['Firm X', 2, 1, 3]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_float_max_min_sum_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_trade_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            [
                ("Max Qty", lambda r: r["Quantity"], lambda c: c.max()),  # type: ignore
                ("Min Qty", lambda r: r["Quantity"], lambda c: c.min()),  # type: ignore
                ("Sum Qty", lambda r: r["Quantity"], lambda c: c.sum()),  # type: ignore
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Max Qty, Type: Float), "
                "TdsColumn(Name: Min Qty, Type: Float), TdsColumn(Name: Sum Qty, Type: Float)]")
        expected = {'columns': ['Product/Name', 'Max Qty', 'Min Qty', 'Sum Qty'],
                    'rows': [{'values': [None, 5.0, 5.0, 5.0]},
                             {'values': ['Firm A', 32.0, 11.0, 66.0]},
                             {'values': ['Firm C', 45.0, 22.0, 176.0]},
                             {'values': ['Firm X', 320.0, 25.0, 345.0]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_number_max_min_sum_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_trade_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            [
                ("Max Qty", lambda r: (r["Quantity"] + 2) * 1.0 + 0, lambda c: c.max()),  # type: ignore
                ("Min Qty", lambda r: (r["Quantity"] + 2) * 1.0 + 0, lambda c: c.min()),  # type: ignore
                ("Sum Qty", lambda r: (r["Quantity"] + 2) * 1.0 + 0, lambda c: c.sum()),  # type: ignore
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Max Qty, Type: Number), "
                "TdsColumn(Name: Min Qty, Type: Number), TdsColumn(Name: Sum Qty, Type: Number)]")
        expected = {'columns': ['Product/Name', 'Max Qty', 'Min Qty', 'Sum Qty'],
                    'rows': [{'values': [None, 7.0, 7.0, 7.0]},
                             {'values': ['Firm A', 34.0, 13.0, 72.0]},
                             {'values': ['Firm C', 47.0, 24.0, 186.0]},
                             {'values': ['Firm X', 322.0, 27.0, 349.0]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_std_dev_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) \
            -> None:
        frame: LegendQLApiTdsFrame = simple_trade_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            [
                ("Std Dev", lambda r: r["Quantity"], lambda c: c.std_dev()),  # type: ignore
                ("Std Dev Sample", lambda r: r["Quantity"], lambda c: c.std_dev_sample()),  # type: ignore
                ("Std Dev Population", lambda r: r["Quantity"], lambda c: c.std_dev_population()),  # type: ignore
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Std Dev, Type: Number), "
                "TdsColumn(Name: Std Dev Sample, Type: Number), TdsColumn(Name: Std Dev Population, Type: Number)]")
        expected = {'columns': ['Product/Name', 'Std Dev', 'Std Dev Sample', 'Std Dev Population'],
                    'rows': [{'values': [None, None, None, 0.0]},
                             {'values': ['Firm A', 10.535653752852738, 10.535653752852738, 8.602325267042627]},
                             {'values': ['Firm C', 10.2810505299799, 10.2810505299799, 9.19565114605812]},
                             {'values': ['Firm X', 208.59650045003153, 208.59650045003153, 147.5]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_variance_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) \
            -> None:
        frame: LegendQLApiTdsFrame = simple_trade_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            [
                ("Variance", lambda r: r["Quantity"], lambda c: c.variance()),  # type: ignore
                ("Variance Sample", lambda r: r["Quantity"], lambda c: c.variance_sample()),  # type: ignore
                ("Variance Population", lambda r: r["Quantity"], lambda c: c.variance_population()),  # type: ignore
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Variance, Type: Number), "
                "TdsColumn(Name: Variance Sample, Type: Number), TdsColumn(Name: Variance Population, Type: Number)]")
        expected = {'columns': ['Product/Name', 'Variance', 'Variance Sample', 'Variance Population'],
                    'rows': [{'values': [None, None, None, 0.0]},
                             {'values': ['Firm A', 111.0, 111.0, 74.0]},
                             {'values': ['Firm C', 105.7, 105.7, 84.56]},
                             {'values': ['Firm X', 43512.5, 43512.5, 21756.25]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_string_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Firm/Legal Name"],
            [
                ("Max Str", lambda r: r["First Name"], lambda c: c.max()),  # type: ignore
                ("Min Str", lambda r: r["First Name"], lambda c: c.min()),  # type: ignore
                ("Joined", lambda r: r["First Name"], lambda c: c.join('|')),  # type: ignore
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: Max Str, Type: String), "
                "TdsColumn(Name: Min Str, Type: String), TdsColumn(Name: Joined, Type: String)]")
        expected = {'columns': ['Firm/Legal Name', 'Max Str', 'Min Str', 'Joined'],
                    'rows': [{'values': ['Firm A', 'Fabrice', 'Fabrice', 'Fabrice']},
                             {'values': ['Firm B', 'Oliver', 'Oliver', 'Oliver']},
                             {'values': ['Firm C', 'David', 'David', 'David']},
                             {'values': ['Firm X', 'Peter', 'Anthony', 'Peter|John|John|Anthony']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_strictdate_max_min_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_trade_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            [
                ("Max Date", lambda r: r["Date"], lambda c: c.max()),  # type: ignore
                ("Min Date", lambda r: r["Date"], lambda c: c.min()),  # type: ignore
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Max Date, Type: StrictDate), "
                "TdsColumn(Name: Min Date, Type: StrictDate)]")
        expected = {'columns': ['Product/Name', 'Max Date', 'Min Date'],
                    'rows': [{'values': [None, '2014-12-05', '2014-12-05']},
                             {'values': ['Firm A', '2014-12-02', '2014-12-01']},
                             {'values': ['Firm C', '2014-12-04', '2014-12-03']},
                             {'values': ['Firm X', '2014-12-01', '2014-12-01']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_group_by_date_max_min_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_trade_service_frame_legendql_api(legend_test_server['engine_port'])
        frame = frame.group_by(
            ["Product/Name"],
            [
                ("Max Date Time", lambda r: r["Settlement Date Time"], lambda c: c.max()),  # type: ignore
                ("Min Date Time", lambda r: r["Settlement Date Time"], lambda c: c.min()),  # type: ignore
            ]
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               ("[TdsColumn(Name: Product/Name, Type: String), TdsColumn(Name: Max Date Time, Type: Date), "
                "TdsColumn(Name: Min Date Time, Type: Date)]")
        expected = {'columns': ['Product/Name', 'Max Date Time', 'Min Date Time'],
                    'rows': [{'values': [None, None, None]},
                             {'values': [
                                 'Firm A', '2014-12-03T21:00:00.000000000+0000', '2014-12-02T21:00:00.000000000+0000'
                             ]},
                             {'values': [
                                 'Firm C', '2014-12-05T21:00:00.000000000+0000', '2014-12-04T15:22:23.123456789+0000'
                             ]},
                             {'values': [
                                 'Firm X', '2014-12-02T21:00:00.000000000+0000', '2014-12-02T21:00:00.000000000+0000'
                             ]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
