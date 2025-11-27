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
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_relation_trade_service_frame_legendql_api


class TestWindowExtendAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_window_extend_function_error_on_non_tuple(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.window_extend(frame.window(), 'col1')  # type: ignore
        assert r.value.args[0] == (
            "'window_extend' function extend_columns argument should be a list of tuples with two/three elements - "
            "first element being a string (new column name), second element being a lambda function which takes three "
            "arguments (LegendQLApiPartialFrame, LegendQLApiWindowReference, LegendQLApiTdsRow) and third element being an "
            "optional aggregation lambda function which takes one argument "
            "E.g - [('new col1', lambda p,w,r: r.c1 + 1), ('new col2', lambda p,w,r: r.c2, lambda c: c.sum())]. "
            "Element at index 0 (0-indexed) is incompatible"
        )

    def test_window_extend_function_error_on_incompatible_lambda_func(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame.window_extend(frame.window(), ("col4", lambda p, w, r: r.unknown))
        assert r.value.args[0] == (
            "'window_extend' function extend_columns argument incompatible. Error occurred while evaluating "
            "window_extend lambda at index 0 (0-indexed). Message: Column - 'unknown' doesn't exist in the current frame. "
            "Current frame columns: ['col1', 'col2']"
        )

    def test_window_extend_function_error_on_non_string_name(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.window_extend(frame.window(), (4, lambda p, w, r: r.col1 + 1))  # type: ignore
        assert r.value.args[0] == (
            "'window_extend' function extend_columns argument incompatible. First element in an window_extend tuple "
            "should be a string (new column name). E.g - ('new col', lambda p,w,r: r.c1 + 1). "
            "Element at index 0 (0-indexed) is incompatible"
        )

    def test_window_extend_function_error_on_incompatible_lambda_1(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.window_extend(frame.window(), ("col4", lambda p, w, r: {}))  # type: ignore
        assert r.value.args[0] == ("'window_extend' function extend_columns argument incompatible. "
                                   "window_extend lambda at index 0 (0-indexed) returns non-primitive - <class 'dict'>")

    def test_window_extend_function_error_on_duplicate_column_names(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.window_extend(
                frame.window(),
                [
                    ("col4", lambda p, w, r: 1),
                    ("col4", lambda p, w, r: 2),
                ]
            )
        assert r.value.args[0] == "Extend column names list has duplicates: ['col4', 'col4']"

    def test_window_extend_function_error_on_conflicting_column_names(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.window_extend(
                frame.window(),
                [
                    ("col1", lambda p, w, r: 1),
                    ("col4", lambda p, w, r: 2),
                ]
            )
        assert r.value.args[0] == "Extend column name - 'col1' already exists in base frame"

    def test_window_extend_function_error_on_incompatible_agg_func(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.window_extend(frame.window(), ("col4", lambda p, w, r: r.col1, lambda: 1))  # type: ignore
        assert r.value.args[0] == (
            "'window_extend' function extend_columns argument incompatible. "
            "Third element in an window_extend tuple should be a lambda function which takes one argument (collection) "
            "E.g - ('new col', lambda p,w,r: r.c1, lambda c: c.sum()). "
            "Element at index 0 (0-indexed) is incompatible"
        )

    def test_window_extend_function_error_on_incompatible_agg_func1(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame.window_extend(frame.window(), ("col4", lambda p, w, r: r.col1, lambda c: c))  # type: ignore
        assert r.value.args[0] == (
            "'window_extend' function extend_columns argument incompatible. Aggregation lambda at index 0 (0-indexed) returns "
            "non-primitive - <class 'pylegend.core.language.shared.primitive_collection.PyLegendIntegerCollection'>"
        )

    def test_query_gen_window_extend_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.window_extend(
            frame.window(partition_by="col2"),
            ("col4", lambda p, w, r: r.get_integer('col1') + 1)
        )
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col3" AS "col3",
                ("root"."col1" + 1) OVER (PARTITION BY "root"."col2") AS "col4"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], []), ~col4:{p,w,r | toOne($r.col1) + 1})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->extend(over(~[col2], []), ~col4:{p,w,r | toOne($r.col1) + 1})')

    def test_query_gen_window_extend_function_col_name_with_spaces(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.window_extend(
            frame.window(order_by="col3"),
            ("col4 with spaces", lambda p, w, r: r.get_integer('col1') + 1)
        )
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col3" AS "col3",
                ("root"."col1" + 1) OVER (ORDER BY "root"."col3") AS "col4 with spaces"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over([], [ascending(~col3)]), ~'col4 with spaces':{p,w,r | toOne($r.col1) + 1})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->extend(over([], [ascending(~col3)]), ~\'col4 with spaces\':{p,w,r | toOne($r.col1) + 1})')

    def test_query_gen_window_extend_function_multi(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.window_extend(
            frame.window(partition_by="col2", order_by="col3"),
            [
                ("col4", lambda p, w, r: r.get_integer('col1') + 1),
                ("col5", lambda p, w, r: r.get_integer('col1') + 2),
            ]
        )
        expected = '''\
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."col3" AS "col3",
                        ("root"."col1" + 1) OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col4",
                        ("root"."col1" + 2) OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col5"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [ascending(~col3)]), ~[
                col4:{p,w,r | toOne($r.col1) + 1},
                col5:{p,w,r | toOne($r.col1) + 2}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(over(~[col2], [ascending(~col3)]), '
                '~[col4:{p,w,r | toOne($r.col1) + 1}, col5:{p,w,r | toOne($r.col1) + 2}])')

    def test_query_gen_window_extend_function_complex_window(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
            PrimitiveTdsColumn.string_column("col4"),
            PrimitiveTdsColumn.string_column("col5"),
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.window_extend(
            frame.window(partition_by=lambda r: [r.col2, r.col3], order_by=lambda r: [r.col4.descending(), r.col5]),
            ("col6", lambda p, w, r: r.get_integer('col1') + 1)
        )
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col3" AS "col3",
                "root"."col4" AS "col4",
                "root"."col5" AS "col5",
                ("root"."col1" + 1) OVER (PARTITION BY "root"."col2", "root"."col3" ORDER BY "root"."col4" DESC, "root"."col5") AS "col6"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3",
                        "root".col4 AS "col4",
                        "root".col5 AS "col5"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''   # noqa: E501
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2, col3], [descending(~col4), ascending(~col5)]), ~col6:{p,w,r | toOne($r.col1) + 1})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->extend(over(~[col2, col3], [descending(~col4), ascending(~col5)]), ~col6:{p,w,r | toOne($r.col1) + 1})')

    def test_query_gen_window_extend_function_with_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.window_extend(
            frame.window(partition_by="col2", order_by="col3"),
            ("col4", lambda p, w, r: r['col1'], lambda c: c.sum())  # type: ignore
        )
        expected = '''\
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."col3" AS "col3",
                        SUM("root"."col1") OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col4"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [ascending(~col3)]), ~col4:{p,w,r | $r.col1}:{c | $c->sum()})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->extend(over(~[col2], [ascending(~col3)]), ~col4:{p,w,r | $r.col1}:{c | $c->sum()})')

    def test_query_gen_window_extend_function_with_multi_agg(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.window_extend(
            frame.window(partition_by="col2", order_by="col3"),
            [
                ("col4", lambda p, w, r: r['col1'], lambda c: c.sum()),  # type: ignore
                ("col5", lambda p, w, r: 1, lambda c: c.count()),
            ]
        )
        expected = '''\
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        "root"."col3" AS "col3",
                        SUM("root"."col1") OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col4",
                        COUNT(1) OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col5"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [ascending(~col3)]), ~[
                col4:{p,w,r | $r.col1}:{c | $c->sum()},
                col5:{p,w,r | 1}:{c | $c->count()}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(over(~[col2], [ascending(~col3)]), '
                '~[col4:{p,w,r | $r.col1}:{c | $c->sum()}, col5:{p,w,r | 1}:{c | $c->count()}])')

    def test_query_gen_window_extend_function_mixed(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.window_extend(
            frame.window(partition_by="col2", order_by="col3"),
            [
                ("col4", lambda p, w, r: r.get_integer('col1') + 1),
                ("col5", lambda p, w, r: 1, lambda c: c.count()),
                ("col6", lambda p, w, r: r.get_integer('col1') + 2),
            ]
        )
        expected = '''\
                            SELECT
                                "root"."col1" AS "col1",
                                "root"."col2" AS "col2",
                                "root"."col3" AS "col3",
                                ("root"."col1" + 1) OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col4",
                                COUNT(1) OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col5",
                                ("root"."col1" + 2) OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col6"
                            FROM
                                (
                                    SELECT
                                        "root".col1 AS "col1",
                                        "root".col2 AS "col2",
                                        "root".col3 AS "col3"
                                    FROM
                                        test_schema.test_table AS "root"
                                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [ascending(~col3)]), ~col4:{p,w,r | toOne($r.col1) + 1})
              ->extend(over(~[col2], [ascending(~col3)]), ~col5:{p,w,r | 1}:{c | $c->count()})
              ->extend(over(~[col2], [ascending(~col3)]), ~col6:{p,w,r | toOne($r.col1) + 2})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->extend(over(~[col2], [ascending(~col3)]), ~col4:{p,w,r | toOne($r.col1) + 1})'
                '->extend(over(~[col2], [ascending(~col3)]), ~col5:{p,w,r | 1}:{c | $c->count()})'
                '->extend(over(~[col2], [ascending(~col3)]), ~col6:{p,w,r | toOne($r.col1) + 2})')

    def test_query_gen_window_extend_function_window_functions(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3"),
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.window_extend(
            frame.window(partition_by="col2", order_by="col3"),
            [
                ("col4", lambda p, w, r: p.row_number(r)),
                ("col5", lambda p, w, r: p.rank(w, r)),
                ("col6", lambda p, w, r: p.dense_rank(w, r) + 1),
                ("col7", lambda p, w, r: p.percent_rank(w, r)),
                ("col8", lambda p, w, r: round(p.cume_dist(w, r), 2)),
                ("col9", lambda p, w, r: p.ntile(r, 10)),
                ("col10", lambda p, w, r: p.lead(r).col1),
                ("col11", lambda p, w, r: p.lag(r).col1 + 1),
                ("col12", lambda p, w, r: p.first(w, r).col1),
                ("col13", lambda p, w, r: p.last(w, r).col1),
                ("col14", lambda p, w, r: p.nth(w, r, 10).col1),
            ]
        )
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."col3" AS "col3",
                row_number() OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col4",
                rank() OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col5",
                (dense_rank() + 1) OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col6",
                percent_rank() OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col7",
                ROUND(cume_dist(), 2) OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col8",
                ntile(10) OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col9",
                lead("root"."col1") OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col10",
                (lag("root"."col1") + 1) OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col11",
                first_value("root"."col1") OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col12",
                last_value("root"."col1") OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col13",
                nth_value("root"."col1", 10) OVER (PARTITION BY "root"."col2" ORDER BY "root"."col3") AS "col14"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [ascending(~col3)]), ~[
                col4:{p,w,r | $p->rowNumber($r)},
                col5:{p,w,r | $p->rank($w, $r)},
                col6:{p,w,r | toOne($p->denseRank($w, $r)) + 1},
                col7:{p,w,r | $p->percentRank($w, $r)},
                col8:{p,w,r | cast(toOne($p->cumulativeDistribution($w, $r)), @Float)->round(2)},
                col9:{p,w,r | $p->ntile($r, 10)},
                col10:{p,w,r | $p->lead($r).col1},
                col11:{p,w,r | toOne($p->lag($r).col1) + 1},
                col12:{p,w,r | $p->first($w, $r).col1},
                col13:{p,w,r | $p->last($w, $r).col1},
                col14:{p,w,r | $p->nth($w, $r, 10).col1}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(over(~[col2], [ascending(~col3)]), '
                '~[col4:{p,w,r | $p->rowNumber($r)}, col5:{p,w,r | $p->rank($w, $r)}, '
                'col6:{p,w,r | toOne($p->denseRank($w, $r)) + 1}, col7:{p,w,r | $p->percentRank($w, $r)}, '
                'col8:{p,w,r | cast(toOne($p->cumulativeDistribution($w, $r)), @Float)->round(2)}, '
                'col9:{p,w,r | $p->ntile($r, 10)}, col10:{p,w,r | $p->lead($r).col1}, '
                'col11:{p,w,r | toOne($p->lag($r).col1) + 1}, col12:{p,w,r | $p->first($w, $r).col1}, '
                'col13:{p,w,r | $p->last($w, $r).col1}, col14:{p,w,r | $p->nth($w, $r, 10).col1}])')

    def test_e2e_window_extend_function_agg(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_relation_trade_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.select([
            "Id",
            "Date",
            "Quantity",
            "Product/Name",
            "Account/Name",
        ])
        frame = frame.window_extend(
            frame.window(partition_by='Date'),
            [
                ("Cnt", lambda p, w, r: r['Id'], lambda c: c.count()),
                ("Min", lambda p, w, r: r['Id'], lambda c: c.min()),  # type: ignore
                ("Max", lambda p, w, r: r['Id'], lambda c: c.max()),  # type: ignore
                ("Sum", lambda p, w, r: r['Quantity'], lambda c: c.sum()),  # type: ignore
            ]
        )
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: Id, Type: Integer), TdsColumn(Name: Date, Type: StrictDate), "
                "TdsColumn(Name: Quantity, Type: Float), TdsColumn(Name: Product/Name, Type: String), "
                "TdsColumn(Name: Account/Name, Type: String), TdsColumn(Name: Cnt, Type: Integer), "
                "TdsColumn(Name: Min, Type: Integer), TdsColumn(Name: Max, Type: Integer), "
                "TdsColumn(Name: Sum, Type: Float)]")
        expected = {'columns': ['Id', 'Date', 'Quantity', 'Product/Name', 'Account/Name', 'Cnt', 'Min', 'Max', 'Sum'],
                    'rows': [{'values': [1, '2014-12-01', 25.0, 'Firm X', 'Account 1', 3, 1, 3, 356.0]},
                             {'values': [2, '2014-12-01', 320.0, 'Firm X', 'Account 2', 3, 1, 3, 356.0]},
                             {'values': [3, '2014-12-01', 11.0, 'Firm A', 'Account 1', 3, 1, 3, 356.0]},
                             {'values': [4, '2014-12-02', 23.0, 'Firm A', 'Account 2', 2, 4, 5, 55.0]},
                             {'values': [5, '2014-12-02', 32.0, 'Firm A', 'Account 1', 2, 4, 5, 55.0]},
                             {'values': [6, '2014-12-03', 27.0, 'Firm C', 'Account 1', 2, 6, 7, 71.0]},
                             {'values': [7, '2014-12-03', 44.0, 'Firm C', 'Account 1', 2, 6, 7, 71.0]},
                             {'values': [8, '2014-12-04', 22.0, 'Firm C', 'Account 2', 3, 8, 10, 105.0]},
                             {'values': [9, '2014-12-04', 45.0, 'Firm C', 'Account 2', 3, 8, 10, 105.0]},
                             {'values': [10, '2014-12-04', 38.0, 'Firm C', 'Account 2', 3, 8, 10, 105.0]},
                             {'values': [11, '2014-12-05', 5.0, None, None, 1, 11, 11, 5.0]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_window_extend_function_rank(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_relation_trade_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.select([
            "Id",
            "Date",
            "Quantity",
        ])
        frame = frame.window_extend(
            frame.window(partition_by=lambda r: [r.Date], order_by=lambda r: [r.Quantity.descending()]),
            [
                ("RN", lambda p, w, r: p.row_number(r)),
            ]
        )
        # Workaround engine issue of needing a partition column
        frame = frame.extend(("FirstDayOfYear", lambda r: r.Date.first_day_of_year()))
        frame = frame.window_extend(
            frame.window(partition_by=["FirstDayOfYear"], order_by=["Date"]),
            [
                ("R", lambda p, w, r: p.rank(w, r)),
                ("DR", lambda p, w, r: p.dense_rank(w, r)),
                ("PR", lambda p, w, r: p.percent_rank(w, r)),
                ("CD", lambda p, w, r: round(p.cume_dist(w, r), 2)),
                ("T", lambda p, w, r: p.ntile(r, 3)),
            ]
        )
        frame = frame.select(["Id", "Date", "Quantity", "RN", "R", "DR", "PR", "CD", "T"])
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: Id, Type: Integer), TdsColumn(Name: Date, Type: StrictDate), "
                "TdsColumn(Name: Quantity, Type: Float), TdsColumn(Name: RN, Type: Integer), "
                "TdsColumn(Name: R, Type: Integer), TdsColumn(Name: DR, Type: Integer), "
                "TdsColumn(Name: PR, Type: Float), TdsColumn(Name: CD, Type: Number), "
                "TdsColumn(Name: T, Type: Integer)]")
        expected = {'columns': ['Id', 'Date', 'Quantity', 'RN', 'R', 'DR', 'PR', 'CD', 'T'],
                    'rows': [{'values': [1, '2014-12-01', 25.0, 2, 1, 1, 0.0, 0.27, 1]},
                             {'values': [2, '2014-12-01', 320.0, 1, 1, 1, 0.0, 0.27, 1]},
                             {'values': [3, '2014-12-01', 11.0, 3, 1, 1, 0.0, 0.27, 1]},
                             {'values': [4, '2014-12-02', 23.0, 2, 4, 2, 0.3, 0.45, 1]},
                             {'values': [5, '2014-12-02', 32.0, 1, 4, 2, 0.3, 0.45, 2]},
                             {'values': [6, '2014-12-03', 27.0, 2, 6, 3, 0.5, 0.64, 2]},
                             {'values': [7, '2014-12-03', 44.0, 1, 6, 3, 0.5, 0.64, 2]},
                             {'values': [8, '2014-12-04', 22.0, 3, 8, 4, 0.7, 0.91, 2]},
                             {'values': [9, '2014-12-04', 45.0, 1, 8, 4, 0.7, 0.91, 3]},
                             {'values': [10, '2014-12-04', 38.0, 2, 8, 4, 0.7, 0.91, 3]},
                             {'values': [11, '2014-12-05', 5.0, 1, 11, 5, 1.0, 1.0, 3]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_window_extend_function_rows(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_relation_trade_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.select([
            "Id",
            "Date",
        ])
        # Workaround engine issue of needing a partition column
        frame = frame.extend(("FirstDayOfYear", lambda r: r.Date.first_day_of_year()))
        frame = frame.window_extend(
            frame.window(partition_by=["FirstDayOfYear"], order_by=["Id"]),
            [
                ("Lead", lambda p, w, r: p.lead(r).Id),
                ("Lag", lambda p, w, r: p.lag(r).Id),
                ("First", lambda p, w, r: p.first(w, r).Id),
                ("Last", lambda p, w, r: p.last(w, r).Id),
                # ("Nth", lambda p, w, r: p.nth(w, r, 3).Id), # Nth value not supported in engine
            ]
        )
        frame = frame.select(["Id", "Date", "Lead", "Lag", "First", "Last"])
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: Id, Type: Integer), TdsColumn(Name: Date, Type: StrictDate), "
                "TdsColumn(Name: Lead, Type: Integer), TdsColumn(Name: Lag, Type: Integer), "
                "TdsColumn(Name: First, Type: Integer), TdsColumn(Name: Last, Type: Integer)]")
        expected = {'columns': ['Id', 'Date', 'Lead', 'Lag', 'First', 'Last'],
                    'rows': [{'values': [1, '2014-12-01', 2, None, 1, 1]},
                             {'values': [2, '2014-12-01', 3, 1, 1, 2]},
                             {'values': [3, '2014-12-01', 4, 2, 1, 3]},
                             {'values': [4, '2014-12-02', 5, 3, 1, 4]},
                             {'values': [5, '2014-12-02', 6, 4, 1, 5]},
                             {'values': [6, '2014-12-03', 7, 5, 1, 6]},
                             {'values': [7, '2014-12-03', 8, 6, 1, 7]},
                             {'values': [8, '2014-12-04', 9, 7, 1, 8]},
                             {'values': [9, '2014-12-04', 10, 8, 1, 9]},
                             {'values': [10, '2014-12-04', 11, 9, 1, 10]},
                             {'values': [11, '2014-12-05', None, 10, 1, 11]}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
