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
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                "root".col3 AS "col3",
                ("root".col1 + 1) OVER (PARTITION BY "root".col2) AS "col4"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], []), ~col4:{p,w,r | $r.col1 + 1})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->extend(over(~[col2], []), ~col4:{p,w,r | $r.col1 + 1})')

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
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                "root".col3 AS "col3",
                ("root".col1 + 1) OVER (ORDER BY "root".col3) AS "col4 with spaces"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over([], [ascending(~col3)]), ~'col4 with spaces':{p,w,r | $r.col1 + 1})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->extend(over([], [ascending(~col3)]), ~\'col4 with spaces\':{p,w,r | $r.col1 + 1})')

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
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3",
                        ("root".col1 + 1) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col4",
                        ("root".col1 + 2) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col5"
                    FROM
                        test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [ascending(~col3)]), ~[
                col4:{p,w,r | $r.col1 + 1},
                col5:{p,w,r | $r.col1 + 2}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(over(~[col2], [ascending(~col3)]), '
                '~[col4:{p,w,r | $r.col1 + 1}, col5:{p,w,r | $r.col1 + 2}])')

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
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                "root".col3 AS "col3",
                "root".col4 AS "col4",
                "root".col5 AS "col5",
                ("root".col1 + 1) OVER (PARTITION BY "root".col2, "root".col3 ORDER BY "root".col4 DESC, "root".col5) AS "col6"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2, col3], [descending(~col4), ascending(~col5)]), ~col6:{p,w,r | $r.col1 + 1})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->extend(over(~[col2, col3], [descending(~col4), ascending(~col5)]), ~col6:{p,w,r | $r.col1 + 1})')

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
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3",
                        SUM("root".col1) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col4"
                    FROM
                        test_schema.test_table AS "root"'''
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
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3",
                        SUM("root".col1) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col4",
                        COUNT(1) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col5"
                    FROM
                        test_schema.test_table AS "root"'''
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
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3",
                                ("root".col1 + 1) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col4",
                                COUNT(1) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col5",
                                ("root".col1 + 2) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col6"
                            FROM
                                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [ascending(~col3)]), ~col4:{p,w,r | $r.col1 + 1})
              ->extend(over(~[col2], [ascending(~col3)]), ~col5:{p,w,r | 1}:{c | $c->count()})
              ->extend(over(~[col2], [ascending(~col3)]), ~col6:{p,w,r | $r.col1 + 2})'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->extend(over(~[col2], [ascending(~col3)]), ~col4:{p,w,r | $r.col1 + 1})'
                '->extend(over(~[col2], [ascending(~col3)]), ~col5:{p,w,r | 1}:{c | $c->count()})'
                '->extend(over(~[col2], [ascending(~col3)]), ~col6:{p,w,r | $r.col1 + 2})')

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
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                "root".col3 AS "col3",
                row_number() OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col4",
                rank() OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col5",
                (dense_rank() + 1) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col6",
                percent_rank() OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col7",
                ROUND(cume_dist(), 2) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col8",
                ntile(
                    10
                ) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col9",
                lead(
                    "root".col1
                ) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col10",
                (lag(
                    "root".col1
                ) + 1) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col11",
                first_value(
                    "root".col1
                ) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col12",
                last_value(
                    "root".col1
                ) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col13",
                nth_value(
                    "root".col1,
                    10
                ) OVER (PARTITION BY "root".col2 ORDER BY "root".col3) AS "col14"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [ascending(~col3)]), ~[
                col4:{p,w,r | $p->rowNumber($r)},
                col5:{p,w,r | $p->rank($w, $r)},
                col6:{p,w,r | $p->denseRank($w, $r) + 1},
                col7:{p,w,r | $p->percentRank($w, $r)},
                col8:{p,w,r | cast($p->cumulativeDistribution($w, $r), @Float)->round(2)},
                col9:{p,w,r | $p->ntile($r, 10)},
                col10:{p,w,r | $p->lead($r).col1},
                col11:{p,w,r | $p->lag($r).col1 + 1},
                col12:{p,w,r | $p->first($w, $r).col1},
                col13:{p,w,r | $p->last($w, $r).col1},
                col14:{p,w,r | $p->nth($w, $r, 10).col1}
              ])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->extend(over(~[col2], [ascending(~col3)]), '
                '~[col4:{p,w,r | $p->rowNumber($r)}, col5:{p,w,r | $p->rank($w, $r)}, '
                'col6:{p,w,r | $p->denseRank($w, $r) + 1}, col7:{p,w,r | $p->percentRank($w, $r)}, '
                'col8:{p,w,r | cast($p->cumulativeDistribution($w, $r), @Float)->round(2)}, '
                'col9:{p,w,r | $p->ntile($r, 10)}, col10:{p,w,r | $p->lead($r).col1}, '
                'col11:{p,w,r | $p->lag($r).col1 + 1}, col12:{p,w,r | $p->first($w, $r).col1}, '
                'col13:{p,w,r | $p->last($w, $r).col1}, col14:{p,w,r | $p->nth($w, $r, 10).col1}])')

    @pytest.mark.skip(reason="Server does not handle window functions of this form yet")
    def test_e2e_window_extend_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.window_extend(
            frame.window(partition_by='Firm/Legal Name'),
            [
                ("Upper", lambda p, w, r: r.get_string("First Name").upper()),
                ("Count", lambda p, w, r: r.get_string("First Name"), lambda c: c.count()),
                ("Lower", lambda p, w, r: r.get_string("First Name").lower())
            ]
        )
        assert ("[" + ", ".join([str(c) for c in frame.columns()]) + "]" ==
                "[TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), "
                "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Firm/Legal Name, Type: String), "
                "TdsColumn(Name: Upper, Type: String), TdsColumn(Name: Count, Type: Integer), "
                "TdsColumn(Name: Lower, Type: String)]")
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
