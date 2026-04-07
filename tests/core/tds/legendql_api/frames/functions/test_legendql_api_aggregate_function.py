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
from pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame import LegendQLApiTdsFrame
from pylegend.extensions.tds.legendql_api.frames.legendql_api_table_spec_input_frame import LegendQLApiTableSpecInputFrame
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile


class TestAggregateAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_aggregate_error_cases(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(ValueError) as v:
            frame.aggregate([])
        assert v.value.args[0] == ("At-least one aggregate specification must be provided "
                                   "when using aggregate function")

        with pytest.raises(ValueError) as v:
            frame.aggregate([
                ("Count", lambda x: x.col1, lambda c: c.count()),
                ("Count", lambda x: x.col2, lambda c: c.count()),
            ])
        assert v.value.args[0] == ("Found duplicate column names in aggregation columns. "
                                   "Aggregation columns - ['Count', 'Count']")

        with pytest.raises(TypeError) as t:
            frame.aggregate(("Count", lambda: 1, lambda y: y.sum()))   # type: ignore
        assert "Element at index 0 (0-indexed) is incompatible" in t.value.args[0]

        with pytest.raises(RuntimeError) as r:
            frame.aggregate(("Count", lambda x: x.col3, lambda y: y.sum()))  # type: ignore
        assert "Error occurred while evaluating mapper lambda" in r.value.args[0]

        with pytest.raises(TypeError) as t:
            frame.aggregate(("Count", lambda x: x, lambda y: y.sum()))  # type: ignore
        assert "returns non-primitive" in t.value.args[0]

        with pytest.raises(TypeError) as t:
            frame.aggregate(("Count", lambda x: x.col2, lambda: 3))  # type: ignore
        assert "Element at index 0 (0-indexed) is incompatible" in t.value.args[0]

        with pytest.raises(RuntimeError) as r:
            frame.aggregate(("Count", lambda x: x.col2, lambda y: y.unknown()))  # type: ignore
        assert "Error occurred while evaluating aggregation lambda" in r.value.args[0]

        with pytest.raises(TypeError) as t:
            frame.aggregate(("Count", lambda x: x.col2, lambda y: y))  # type: ignore
        assert "returns non-primitive" in t.value.args[0]

    def test_query_gen_aggregate(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
            PrimitiveTdsColumn.string_column("col3")
        ]
        frame_base: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame = frame_base.aggregate(
            ("Count", lambda r: r.col3, lambda col: col.count())
        )
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: Count, Type: Integer)]"
        )
        expected = '''\
            SELECT
                COUNT("root".col3) AS "Count"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->aggregate(
                ~[Count:{r | $r.col3}:{c | $c->count()}]
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->aggregate(~[Count:{r | $r.col3}:{c | $c->count()}])')

        frame = frame_base.aggregate(
            ("Count", lambda r: 1, lambda col: col.count())
        )
        expected = '''\
            SELECT
                COUNT(1) AS "Count"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->aggregate(~[Count:{r | 1}:{c | $c->count()}])')

        frame = frame_base.aggregate([
            ("Count", lambda r: r.col3, lambda c: c.count()),
            ("Total", lambda r: r.col1, lambda c: c.sum()),  # type: ignore[attr-defined]
            ("Avg", lambda r: r.col2, lambda c: c.average()),  # type: ignore[attr-defined]
            ("Max", lambda r: r.col1, lambda c: c.max()),  # type: ignore[attr-defined]
            ("Min", lambda r: r.col1, lambda c: c.min()),  # type: ignore[attr-defined]
            ("Cnt", lambda r: r.col1, lambda c: c.distinct_count()),
            ("StdDev", lambda r: r.col2, lambda c: c.std_dev_sample()),  # type: ignore[attr-defined]
            ("Var", lambda r: r.col2, lambda c: c.variance_sample()),  # type: ignore[attr-defined]
        ])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: Count, Type: Integer), TdsColumn(Name: Total, Type: Integer), "
            "TdsColumn(Name: Avg, Type: Float), TdsColumn(Name: Max, Type: Integer), "
            "TdsColumn(Name: Min, Type: Integer), TdsColumn(Name: Cnt, Type: Integer), "
            "TdsColumn(Name: StdDev, Type: Number), TdsColumn(Name: Var, Type: Number)]"
        )
        expected = '''\
            SELECT
                COUNT("root".col3) AS "Count",
                SUM("root".col1) AS "Total",
                AVG("root".col2) AS "Avg",
                MAX("root".col1) AS "Max",
                MIN("root".col1) AS "Min",
                COUNT(DISTINCT "root".col1) AS "Cnt",
                STDDEV_SAMP("root".col2) AS "StdDev",
                VAR_SAMP("root".col2) AS "Var"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            '#Table(test_schema.test_table)#->aggregate(~['
            'Count:{r | $r.col3}:{c | $c->count()}, '
            'Total:{r | $r.col1}:{c | $c->sum()}, '
            'Avg:{r | $r.col2}:{c | $c->average()}, '
            'Max:{r | $r.col1}:{c | $c->max()}, '
            'Min:{r | $r.col1}:{c | $c->min()}, '
            'Cnt:{r | $r.col1}:{c | $c->distinct()->count()}, '
            'StdDev:{r | $r.col2}:{c | $c->stdDevSample()->cast(@Float)}, '
            'Var:{r | $r.col2}:{c | $c->varianceSample()->cast(@Float)}])'
        )

        frame = frame_base.distinct().aggregate(
            ("Count", lambda r: r.col3, lambda col: col.count())
        )
        expected = '''\
            SELECT
                COUNT("root"."col3") AS "Count"
            FROM
                (
                    SELECT DISTINCT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->distinct()'
                '->aggregate(~[Count:{r | $r.col3}:{c | $c->count()}])')

        frame = frame_base.head(10).aggregate(
            ("Count", lambda r: r.col3, lambda col: col.count())
        )
        expected = '''\
            SELECT
                COUNT("root"."col3") AS "Count"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 10
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->limit(10)'
                '->aggregate(~[Count:{r | $r.col3}:{c | $c->count()}])')
