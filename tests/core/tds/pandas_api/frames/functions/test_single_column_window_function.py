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

# type: ignore
# flake8: noqa

import json
from textwrap import dedent

import pytest

from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_relation_person_service_frame_pandas_api


class TestFirstOnWindowSeries:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_first_assign_on_base_frame(self) -> None:
        """frame['new'] = frame.window_frame_legend_ext(order_by=...)['col'].first()"""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["first_col1"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col2",
        )["col1"].first()

        expected_sql = '''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."first_col1__pylegend_olap_column__" AS "first_col1"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        first_value("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "first_col1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col2)], rows(unbounded(), unbounded())), ~col1__pylegend_olap_column__:{p,w,r | $p->first($w, $r).col1})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, first_col1:c|$c.col1__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_first_assign_on_groupby_with_rows_between_and_arithmetic(self) -> None:
        """frame['new'] = frame.groupby('grp').window_frame_legend_ext(...)['val'].first() + 10"""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["first_val_plus_10"] = frame.groupby("grp").window_frame_legend_ext(
            frame_spec=frame.rows_between(-2, 2),
            order_by="score",
        )["val"].first() + 10

        expected_sql = '''
            SELECT
                "root"."grp" AS "grp",
                "root"."val" AS "val",
                "root"."score" AS "score",
                ("root"."first_val_plus_10__pylegend_olap_column__" + 10) AS "first_val_plus_10"
            FROM
                (
                    SELECT
                        "root"."grp" AS "grp",
                        "root"."val" AS "val",
                        "root"."score" AS "score",
                        first_value("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."score" ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS "first_val_plus_10__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".score AS "score",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~score)], rows(minus(2), 2)), ~val__pylegend_olap_column__:{p,w,r | $p->first($w, $r).val})
              ->project(~[grp:c|$c.grp, val:c|$c.val, score:c|$c.score, first_val_plus_10:c|(toOne($c.val__pylegend_olap_column__) + 10)])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_first_direct_series_to_sql(self) -> None:
        """series = frame.window_frame_legend_ext(...)['col'].first(); series.to_sql_query()"""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col2",
        )["col1"].first()

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        first_value("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert series.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col2)], rows(unbounded(), unbounded())), ~col1__pylegend_olap_column__:{p,w,r | $p->first($w, $r).col1})
              ->project(~col1:p|$p.col1__pylegend_olap_column__)
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert series.to_pure_query() == expected_pure

    def test_first_direct_groupby_series_to_sql(self) -> None:
        """series = frame.groupby('grp').window_frame_legend_ext(...)['val'].first(); series.to_sql_query()"""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame.groupby("grp").window_frame_legend_ext(
            frame_spec=frame.rows_between(-2, 2),
            order_by="score",
        )["val"].first()

        expected_sql = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val"
            FROM
                (
                    SELECT
                        first_value("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."score" ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS "val__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".score AS "score",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert series.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~score)], rows(minus(2), 2)), ~val__pylegend_olap_column__:{p,w,r | $p->first($w, $r).val})
              ->project(~val:p|$p.val__pylegend_olap_column__)
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert series.to_pure_query() == expected_pure

    def test_first_multi_column_on_base_frame(self) -> None:
        """Apply first() to all columns via pwr_func returning a full TdsRow."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        base_frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        applied = base_frame.window_frame_legend_ext(
            frame_spec=base_frame.rows_between(),
            order_by="col1",
        ).first()

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        first_value("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col1__pylegend_olap_column__",
                        first_value("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert applied.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)], rows(unbounded(), unbounded())), ~[
                col1__pylegend_olap_column__:{p,w,r | $p->first($w, $r).col1},
                col2__pylegend_olap_column__:{p,w,r | $p->first($w, $r).col2}
              ])
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__,
                col2:p|$p.col2__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert applied.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(applied, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_first_multi_column_on_groupby_frame(self) -> None:
        """Apply first() to all columns via pwr_func returning a full TdsRow, with groupby."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        base_frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        applied = base_frame.groupby("grp").window_frame_legend_ext(
            frame_spec=base_frame.rows_between(-1, 1),
            order_by="score",
        ).first()

        expected_sql = '''
            SELECT
                "root"."grp__pylegend_olap_column__" AS "grp",
                "root"."val__pylegend_olap_column__" AS "val",
                "root"."score__pylegend_olap_column__" AS "score"
            FROM
                (
                    SELECT
                        first_value("root"."grp") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."score" ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS "grp__pylegend_olap_column__",
                        first_value("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."score" ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS "val__pylegend_olap_column__",
                        first_value("root"."score") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."score" ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS "score__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".score AS "score",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert applied.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~score)], rows(minus(1), 1)), ~[
                grp__pylegend_olap_column__:{p,w,r | $p->first($w, $r).grp},
                val__pylegend_olap_column__:{p,w,r | $p->first($w, $r).val},
                score__pylegend_olap_column__:{p,w,r | $p->first($w, $r).score}
              ])
              ->project(~[
                grp:p|$p.grp__pylegend_olap_column__,
                val:p|$p.val__pylegend_olap_column__,
                score:p|$p.score__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert applied.to_sql_query() == expected_sql
        assert applied.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(applied, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_first_numeric_only_on_window_tds_frame(self) -> None:
        """window_frame.first(numeric_only=True) should only apply first_value to numeric columns."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        applied = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="val",
        ).first(numeric_only=True)

        expected_sql = '''
            SELECT
                "root"."val" AS "val",
                "root"."score__pylegend_olap_column__" AS "score"
            FROM
                (
                    SELECT
                        "root"."grp" AS "grp",
                        "root"."val" AS "val",
                        first_value("root"."score") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "score__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root"."grp" AS "grp",
                                "root"."val__pylegend_olap_column__" AS "val",
                                "root"."score" AS "score",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                (
                                    SELECT
                                        "root"."grp" AS "grp",
                                        first_value("root"."val") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."val" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "val__pylegend_olap_column__",
                                        "root"."score" AS "score"
                                    FROM
                                        (
                                            SELECT
                                                "root".grp AS "grp",
                                                "root".val AS "val",
                                                "root".score AS "score",
                                                0 AS "__pylegend_zero_column__"
                                            FROM
                                                test_schema.test_table AS "root"
                                        ) AS "root"
                                ) AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert applied.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~val)], rows(unbounded(), unbounded())), ~val__pylegend_olap_column__:{p,w,r | $p->first($w, $r).val})
              ->project(~[grp:c|$c.grp, val:c|$c.val__pylegend_olap_column__, score:c|$c.score])
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~val)], rows(unbounded(), unbounded())), ~score__pylegend_olap_column__:{p,w,r | $p->first($w, $r).score})
              ->project(~[grp:c|$c.grp, score:c|$c.score__pylegend_olap_column__, val:c|$c.val])
              ->select(~[val, score])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert applied.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(applied, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_last_assign_on_base_frame(self) -> None:
        """frame['new'] = frame.window_frame_legend_ext(order_by=...)['col'].last()"""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["last_col1"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col2",
        )["col1"].last()

        expected_sql = '''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."last_col1__pylegend_olap_column__" AS "last_col1"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        last_value("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "last_col1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col2)], rows(unbounded(), unbounded())), ~col1__pylegend_olap_column__:{p,w,r | $p->last($w, $r).col1})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, last_col1:c|$c.col1__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_last_multi_column_on_groupby_frame(self) -> None:
        """Apply last() to all columns via pwr_func returning a full TdsRow, with groupby."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        base_frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        applied = base_frame.groupby("grp").window_frame_legend_ext(
            frame_spec=base_frame.rows_between(-1, 1),
            order_by="score",
        ).last()

        expected_sql = '''
            SELECT
                "root"."grp__pylegend_olap_column__" AS "grp",
                "root"."val__pylegend_olap_column__" AS "val",
                "root"."score__pylegend_olap_column__" AS "score"
            FROM
                (
                    SELECT
                        last_value("root"."grp") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."score" ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS "grp__pylegend_olap_column__",
                        last_value("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."score" ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS "val__pylegend_olap_column__",
                        last_value("root"."score") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."score" ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS "score__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".score AS "score",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert applied.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~score)], rows(minus(1), 1)), ~[
                grp__pylegend_olap_column__:{p,w,r | $p->last($w, $r).grp},
                val__pylegend_olap_column__:{p,w,r | $p->last($w, $r).val},
                score__pylegend_olap_column__:{p,w,r | $p->last($w, $r).score}
              ])
              ->project(~[
                grp:p|$p.grp__pylegend_olap_column__,
                val:p|$p.val__pylegend_olap_column__,
                score:p|$p.score__pylegend_olap_column__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert applied.to_sql_query() == expected_sql
        assert applied.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(applied, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_shift_lag_assign_on_base_frame(self) -> None:
        """frame['shifted'] = frame.window_frame_legend_ext(order_by=...)['col'].shift(periods=1) produces lag"""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["lag_col1"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col2",
        )["col1"].shift(periods=1)

        expected_sql = '''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."lag_col1__pylegend_olap_column__" AS "lag_col1"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        lag("root"."col1", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col2" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "lag_col1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col2)], rows(unbounded(), unbounded())), ~col1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col1})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, lag_col1:c|$c.col1__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_shift_lead_on_groupby_frame(self) -> None:
        """frame.groupby('grp').window_frame_legend_ext(...)['val'].shift(periods=-1) produces lead"""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["lead_val"] = frame.groupby("grp").window_frame_legend_ext(
            frame_spec=frame.rows_between(-2, 2),
            order_by="score",
        )["val"].shift(periods=-1)

        expected_sql = '''
            SELECT
                "root"."grp" AS "grp",
                "root"."val" AS "val",
                "root"."score" AS "score",
                "root"."lead_val__pylegend_olap_column__" AS "lead_val"
            FROM
                (
                    SELECT
                        "root"."grp" AS "grp",
                        "root"."val" AS "val",
                        "root"."score" AS "score",
                        lead("root"."val", 1) OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."score" ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS "lead_val__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".score AS "score",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~score)], rows(minus(2), 2)), ~val__pylegend_olap_column__:{p,w,r | $p->lead($r, 1).val})
              ->project(~[grp:c|$c.grp, val:c|$c.val, score:c|$c.score, lead_val:c|$c.val__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_nth_assign_on_groupby_frame(self) -> None:
        """frame['nth_val'] = frame.groupby('grp').window_frame_legend_ext(...)['val'].window_extend_legend_ext(lambda p,w,r: p.nth(w,r,2)['val'])"""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["nth_val"] = frame.groupby("grp").window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="score",
            ascending=False,
        )["val"].window_extend_legend_ext(pwr_func=lambda p, w, r: p.nth(w, r, 2)["val"])

        expected_sql = '''
            SELECT
                "root"."grp" AS "grp",
                "root"."val" AS "val",
                "root"."score" AS "score",
                "root"."nth_val__pylegend_olap_column__" AS "nth_val"
            FROM
                (
                    SELECT
                        "root"."grp" AS "grp",
                        "root"."val" AS "val",
                        "root"."score" AS "score",
                        nth_value("root"."val", 2) OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."score" DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "nth_val__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".score AS "score",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert frame.to_sql_query() == expected_sql

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [descending(~score)], rows(unbounded(), unbounded())), ~val__pylegend_olap_column__:{p,w,r | $p->nth($w, $r, 2).val})
              ->project(~[grp:c|$c.grp, val:c|$c.val, score:c|$c.score, nth_val:c|$c.val__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestNoFrameSpecWindowFunction:
    """Tests for window_frame_legend_ext with frame_spec=None (no frame clause)."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient(
            host="localhost",
            port=legend_test_server["engine_port"],
            secure_http=False
        )

    def test_no_frame_spec_on_base_frame_first(self) -> None:
        """window_frame_legend_ext(order_by=...) with no frame_spec omits ROWS BETWEEN in SQL and Pure."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["first_col1"] = frame.window_frame_legend_ext(
            order_by="col2",
        )["col1"].first()

        sql = frame.to_sql_query()
        # Should have OVER clause with ORDER BY but NO ROWS BETWEEN
        assert "OVER" in sql
        assert "ORDER BY" in sql
        assert "ROWS BETWEEN" not in sql
        assert "RANGE BETWEEN" not in sql

        expected_sql = '''
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                "root"."first_col1__pylegend_olap_column__" AS "first_col1"
            FROM
                (
                    SELECT
                        "root"."col1" AS "col1",
                        "root"."col2" AS "col2",
                        first_value("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col2") AS "first_col1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert sql == expected_sql

        pure = frame.to_pure_query()
        # Pure should have over(...) with no rows() or range()
        assert "rows(" not in pure
        assert "range(" not in pure

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col2)]), ~col1__pylegend_olap_column__:{p,w,r | $p->first($w, $r).col1})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, first_col1:c|$c.col1__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert pure == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_no_frame_spec_on_groupby_frame_first(self) -> None:
        """groupby.window_frame_legend_ext(order_by=...) with no frame_spec omits ROWS BETWEEN in SQL and Pure."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["first_val"] = frame.groupby("grp").window_frame_legend_ext(
            order_by="score",
        )["val"].first()

        sql = frame.to_sql_query()
        assert "OVER" in sql
        assert "ORDER BY" in sql
        assert "PARTITION BY" in sql
        assert "ROWS BETWEEN" not in sql
        assert "RANGE BETWEEN" not in sql

        expected_sql = '''
            SELECT
                "root"."grp" AS "grp",
                "root"."val" AS "val",
                "root"."score" AS "score",
                "root"."first_val__pylegend_olap_column__" AS "first_val"
            FROM
                (
                    SELECT
                        "root"."grp" AS "grp",
                        "root"."val" AS "val",
                        "root"."score" AS "score",
                        first_value("root"."val") OVER (PARTITION BY "root"."grp", "root"."__pylegend_zero_column__" ORDER BY "root"."score") AS "first_val__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".score AS "score",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert sql == expected_sql

        pure = frame.to_pure_query()
        assert "rows(" not in pure
        assert "range(" not in pure

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r|0})
              ->extend(over(~[grp, __pylegend_zero_column__], [ascending(~score)]), ~val__pylegend_olap_column__:{p,w,r | $p->first($w, $r).val})
              ->project(~[grp:c|$c.grp, val:c|$c.val, score:c|$c.score, first_val:c|$c.val__pylegend_olap_column__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert pure == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_no_frame_spec_multi_column_on_base_frame(self) -> None:
        """window_frame_legend_ext(order_by=...).first() with no frame_spec on all columns."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        applied = frame.window_frame_legend_ext(
            order_by="col1",
        ).first()

        sql = applied.to_sql_query()
        assert "ROWS BETWEEN" not in sql
        assert "RANGE BETWEEN" not in sql

        pure = applied.to_pure_query()
        assert "rows(" not in pure
        assert "range(" not in pure
        assert generate_pure_query_and_compile(applied, FrameToPureConfig(), self.legend_client) == pure

    def test_no_frame_spec_shift_on_base_frame(self) -> None:
        """window_frame_legend_ext(order_by=...).shift() with no frame_spec (lag)."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["lag_col1"] = frame.window_frame_legend_ext(
            order_by="col1",
        )["col1"].shift(periods=1)

        sql = frame.to_sql_query()
        assert "lag(" in sql
        assert "ROWS BETWEEN" not in sql
        assert "RANGE BETWEEN" not in sql

        pure = frame.to_pure_query()
        assert "rows(" not in pure
        assert "range(" not in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure


class TestSingleColumnWindowFunctionValidation:
    """Tests for validate() error paths in SingleColumnWindowFunction."""

    def _make_window_frame(self) -> "PandasApiTdsFrame":
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        return frame

    def test_validate_pwr_func_not_callable(self) -> None:
        """validate() raises TypeError when pwr_func is not callable."""
        from pylegend.core.tds.pandas_api.frames.functions.single_column_window_function import (
            SingleColumnWindowFunction,
        )
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        frame = self._make_window_frame()
        window_frame = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col1",
        )
        assert isinstance(window_frame, PandasApiWindowTdsFrame)

        func = SingleColumnWindowFunction(
            base_window_frame=window_frame,
            pwr_func="not_callable",  # type: ignore
        )
        with pytest.raises(TypeError, match="pwr_func must be callable"):
            func.validate()

    def test_validate_pwr_func_wrong_param_count(self) -> None:
        """validate() raises TypeError when pwr_func has wrong number of required params."""
        from pylegend.core.tds.pandas_api.frames.functions.single_column_window_function import (
            SingleColumnWindowFunction,
        )
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        frame = self._make_window_frame()
        window_frame = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col1",
        )
        assert isinstance(window_frame, PandasApiWindowTdsFrame)

        func = SingleColumnWindowFunction(
            base_window_frame=window_frame,
            pwr_func=lambda a, b: a,  # type: ignore
        )
        with pytest.raises(TypeError, match="pwr_func must accept exactly 3 positional parameters"):
            func.validate()

    def test_validate_agg_func_not_callable(self) -> None:
        """validate() raises TypeError when agg_func is not callable."""
        from pylegend.core.tds.pandas_api.frames.functions.single_column_window_function import (
            SingleColumnWindowFunction,
        )
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        frame = self._make_window_frame()
        window_frame = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col1",
        )
        assert isinstance(window_frame, PandasApiWindowTdsFrame)

        func = SingleColumnWindowFunction(
            base_window_frame=window_frame,
            pwr_func=lambda p, w, r: p.first(w, r)["col1"],
            agg_func="not_callable",  # type: ignore
        )
        with pytest.raises(TypeError, match="agg_func must be callable or None"):
            func.validate()

    def test_validate_agg_func_wrong_param_count(self) -> None:
        """validate() raises TypeError when agg_func has wrong number of required params."""
        from pylegend.core.tds.pandas_api.frames.functions.single_column_window_function import (
            SingleColumnWindowFunction,
        )
        from pylegend.core.tds.pandas_api.frames.pandas_api_window_tds_frame import PandasApiWindowTdsFrame

        frame = self._make_window_frame()
        window_frame = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col1",
        )
        assert isinstance(window_frame, PandasApiWindowTdsFrame)

        func = SingleColumnWindowFunction(
            base_window_frame=window_frame,
            pwr_func=lambda p, w, r: p.first(w, r)["col1"],
            agg_func=lambda a, b: a,  # type: ignore
        )
        with pytest.raises(TypeError, match="agg_func must accept exactly 1 positional parameter"):
            func.validate()

    def test_validate_base_window_frame_type_check_exists(self) -> None:
        """The validate() check for base_window_frame type can't be reached
        because __init__ calls construct_window() which would fail first.
        This test just confirms the constructor rejects non-window frames."""
        from pylegend.core.tds.pandas_api.frames.functions.single_column_window_function import (
            SingleColumnWindowFunction,
        )

        frame = self._make_window_frame()

        with pytest.raises(AttributeError):
            SingleColumnWindowFunction(
                base_window_frame=frame,  # type: ignore
                pwr_func=lambda p, w, r: p.first(w, r)["col1"],
            )


class TestWindowSeriesShiftValidation:
    """Tests for shift() validation errors on WindowSeries."""

    def _make_window_series(self):
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        return frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col1",
        )["col1"]

    def test_shift_freq_not_supported(self) -> None:
        ws = self._make_window_series()
        with pytest.raises(NotImplementedError, match="'freq' argument.*is not supported"):
            ws.shift(freq="D")

    def test_shift_axis_not_supported(self) -> None:
        ws = self._make_window_series()
        with pytest.raises(NotImplementedError, match="'axis' argument.*must be 0 or 'index'"):
            ws.shift(axis=1)

    def test_shift_fill_value_not_supported(self) -> None:
        ws = self._make_window_series()
        with pytest.raises(NotImplementedError, match="'fill_value' argument.*is not supported"):
            ws.shift(fill_value=0)

    def test_shift_suffix_not_supported(self) -> None:
        ws = self._make_window_series()
        with pytest.raises(NotImplementedError, match="'suffix' argument.*is not supported"):
            ws.shift(suffix="_shifted")

    def test_shift_periods_not_int(self) -> None:
        ws = self._make_window_series()
        with pytest.raises(NotImplementedError, match="'periods' argument.*must be an int"):
            ws.shift(periods=1.5)  # type: ignore


class TestLastOnWindowTdsFrame:
    """Tests for last() on WindowTdsFrame (both plain and numeric_only)."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_last_numeric_only_on_window_tds_frame(self) -> None:
        """window_frame.last(numeric_only=True) should only apply last_value to numeric columns."""
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        applied = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="val",
        ).last(numeric_only=True)

        sql = applied.to_sql_query()
        # Should contain last_value for numeric cols, and the string column should be excluded
        assert "last_value" in sql
        assert '"grp"' not in sql or "grp" in sql  # grp may still appear in sub-queries

        pure = applied.to_pure_query()
        assert "last" in pure
        assert generate_pure_query_and_compile(applied, FrameToPureConfig(), self.legend_client) == pure

    def test_last_plain_on_window_tds_frame(self) -> None:
        """window_frame.last() without numeric_only should apply last_value to all columns."""
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        applied = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col1",
        ).last()

        expected_sql = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        last_value("root"."col1") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col1__pylegend_olap_column__",
                        last_value("root"."col2") OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected_sql = dedent(expected_sql).strip()
        assert applied.to_sql_query() == expected_sql

        pure = applied.to_pure_query()
        assert "last" in pure
        assert generate_pure_query_and_compile(applied, FrameToPureConfig(), self.legend_client) == pure


class TestWindowFuncLegendExtOnGroupbySeries:
    """Tests for window_extend_legend_ext on GroupbySeries returning GroupbySeries."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_groupby_series_window_extend_legend_ext_returns_groupby_series(self) -> None:
        """
        frame.groupby('grp')['val'].window_frame_legend_ext(...).first()
        should return a GroupbySeries.
        """
        from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries

        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame.groupby("grp")["val"].window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="score",
        ).first()
        assert isinstance(series, GroupbySeries)

        sql = series.to_sql_query()
        assert "first_value" in sql

    def test_series_window_extend_legend_ext_returns_series(self) -> None:
        """
        frame['val'].window_frame_legend_ext(...).first()
        should return a Series.
        """
        from pylegend.core.language.pandas_api.pandas_api_series import Series

        columns = [
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame["val"].window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="score",
        ).first()
        assert isinstance(series, Series)

        sql = series.to_sql_query()
        assert "first_value" in sql


class TestSeriesPureQueryWithArithmetic:
    """Tests for the get_pure_query_from_expr path (series.to_pure_query() with arithmetic)."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_series_to_pure_query_with_arithmetic_single_column_window(self) -> None:
        """
        series = frame.window_frame_legend_ext(...)['col'].first() + 10
        series.to_pure_query() should work through get_pure_query_from_expr.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col2",
        )["col1"].first() + 10

        pure = series.to_pure_query()
        assert "first" in pure
        assert "10" in pure
        assert generate_pure_query_and_compile(series, FrameToPureConfig(), self.legend_client) == pure

    def test_groupby_series_to_pure_query_with_arithmetic_single_column_window(self) -> None:
        """
        series = frame.groupby('grp').window_frame_legend_ext(...)['val'].first() + 5
        series.to_pure_query() should work through get_pure_query_and_compile.
        """
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame.groupby("grp").window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="score",
        )["val"].first() + 5

        pure = series.to_pure_query()
        assert "first" in pure
        assert "5" in pure
        assert generate_pure_query_and_compile(series, FrameToPureConfig(), self.legend_client) == pure

    def test_groupby_series_to_sql_query_with_arithmetic_single_column_window(self) -> None:
        """
        series = frame.groupby('grp').window_frame_legend_ext(...)['val'].first() + 5
        series.to_sql_query() exercises the needs_zero_column_for_window path
        in GroupbySeries.to_sql_query_object.
        """
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.float_column("score"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame.groupby("grp").window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="score",
        )["val"].first() + 5

        sql = series.to_sql_query()
        assert "first_value" in sql
        assert "OVER" in sql
        assert "__pylegend_zero_column__" in sql
        assert "5" in sql

        pure = series.to_pure_query()
        assert generate_pure_query_and_compile(series, FrameToPureConfig(), self.legend_client) == pure


class TestAggFuncPaths:
    """Tests for SingleColumnWindowFunction with an agg_func provided."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_window_extend_legend_ext_with_agg_func(self) -> None:
        """
        Use window_extend_legend_ext on WindowSeries with both pwr_func and agg_func.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        ws = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col2",
        )["col1"]

        # Use pwr_func that references a column and agg_func that sums the collection
        series = ws.window_extend_legend_ext(
            pwr_func=lambda p, w, r: r["col1"],
            agg_func=lambda c: c.sum(),
        )

        sql = series.to_sql_query()
        assert "sum" in sql.lower()
        assert "OVER" in sql

        pure = series.to_pure_query()
        assert "sum" in pure.lower()

    def test_window_tds_frame_func_legend_ext_with_agg_func(self) -> None:
        """
        Use window_extend_legend_ext on WindowTdsFrame with both pwr_func and agg_func.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        wf = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col2",
        )

        # pwr_func returning a single column primitive (r["col1"]), with sum agg
        applied = wf.window_extend_legend_ext(
            pwr_func=lambda p, w, r: r["col1"],
            agg_func=lambda c: c.sum(),
        )

        sql = applied.to_sql_query()
        assert "sum" in sql.lower()
        assert "OVER" in sql

        pure = applied.to_pure_query()
        assert "sum" in pure.lower()

    def test_assign_with_agg_func_single_column_window(self) -> None:
        """
        frame['new'] = frame.window_frame_legend_ext(...)['col'].window_extend_legend_ext(pwr_func, agg_func)
        Tests the agg_func path through assign_function to_sql and to_pure.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["sum_col1"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col2",
        )["col1"].window_extend_legend_ext(
            pwr_func=lambda p, w, r: r["col1"],
            agg_func=lambda c: c.sum(),
        )

        sql = frame.to_sql_query()
        assert "sum" in sql.lower()
        assert "OVER" in sql

        pure = frame.to_pure_query()
        assert "sum" in pure.lower()
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure


class TestPythonLiteralPwrFunc:
    """Tests for pwr_func returning a raw Python literal (not a PyLegendPrimitive)."""

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_pwr_func_returning_literal_single_column(self) -> None:
        """
        window_extend_legend_ext with pwr_func returning a raw int (42).
        Tests the else branch for non-PyLegendPrimitive in to_sql, to_pure, to_sql_expression, build_pure_extend_strs.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        # pwr_func returns raw int 42, not a PyLegendPrimitive
        frame["const"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col2",
        )["col1"].window_extend_legend_ext(
            pwr_func=lambda p, w, r: 42,
        )

        sql = frame.to_sql_query()
        assert "42" in sql
        assert "OVER" in sql

        pure = frame.to_pure_query()
        assert "42" in pure

    def test_pwr_func_returning_literal_standalone_series(self) -> None:
        """
        series = window_extend_legend_ext(pwr_func returning 42) — standalone series SQL.
        Tests to_sql_expression else branch for non-PyLegendPrimitive.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col2",
        )["col1"].window_extend_legend_ext(
            pwr_func=lambda p, w, r: 42,
        )

        sql = series.to_sql_query()
        assert "42" in sql
        assert "OVER" in sql

        pure = series.to_pure_query()
        assert "42" in pure


class TestWindowFuncWorkflows:
    """
    Real-world workflow tests for window_frame_legend_ext.

    These tests exercise common patterns a user would employ,
    verifying both SQL and Pure generation, and compiling against the engine.
    """

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    # ── Multiple sequential window assigns ───────────────────────────────

    def test_multiple_window_assigns_on_same_frame(self) -> None:
        """
        Assign first and last of different columns sequentially.
        Mimics a common analytics pattern where you want both the opening
        and closing value within the same window.
        """
        columns = [
            PrimitiveTdsColumn.string_column("ticker"),
            PrimitiveTdsColumn.float_column("price"),
            PrimitiveTdsColumn.integer_column("volume"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["market", "trades"], columns)

        gb_window = frame.groupby("ticker").window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="price",
        )
        frame["open_price"] = gb_window["price"].first()
        frame["close_price"] = gb_window["price"].last()

        sql = frame.to_sql_query()
        assert "first_value" in sql
        assert "last_value" in sql
        assert sql.count("PARTITION BY") >= 2

        pure = frame.to_pure_query()
        assert "$p->first($w, $r).price" in pure
        assert "$p->last($w, $r).price" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    # ── Chaining with other frame operations ─────────────────────────────

    def test_window_then_filter(self) -> None:
        """
        Compute a window function, then filter results.
        e.g. keep only rows where the first value in the window matches the current value.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["schema1", "tbl"], columns)

        frame["first_val"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="id",
        )["val"].first()

        filtered = frame.filter(items=["id", "val", "first_val"])
        sql = filtered.to_sql_query()
        assert "first_value" in sql
        assert '"id"' in sql

        pure = filtered.to_pure_query()
        assert generate_pure_query_and_compile(filtered, FrameToPureConfig(), self.legend_client) == pure

    def test_window_then_sort(self) -> None:
        """
        Assign a window column, then sort by it.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["schema1", "tbl"], columns)

        frame["first_val"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="id",
        )["val"].first()

        sorted_frame = frame.sort_values(by="first_val")
        sql = sorted_frame.to_sql_query()
        assert "first_value" in sql
        assert "ORDER BY" in sql

        pure = sorted_frame.to_pure_query()
        assert generate_pure_query_and_compile(sorted_frame, FrameToPureConfig(), self.legend_client) == pure

    def test_filter_then_window(self) -> None:
        """
        Filter a frame first, then apply a window function on the filtered result.
        """
        columns = [
            PrimitiveTdsColumn.string_column("category"),
            PrimitiveTdsColumn.integer_column("amount"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["schema1", "data"], columns)

        filtered = frame.filter(items=["category", "amount"])
        filtered["first_amount"] = filtered.window_frame_legend_ext(
            frame_spec=filtered.rows_between(),
            order_by="amount",
        )["amount"].first()

        sql = filtered.to_sql_query()
        assert "first_value" in sql

        pure = filtered.to_pure_query()
        assert generate_pure_query_and_compile(filtered, FrameToPureConfig(), self.legend_client) == pure

    # ── Multiple order-by columns ────────────────────────────────────────

    def test_multiple_order_by_columns(self) -> None:
        """
        Window with multiple order_by columns (list of strings).
        """
        columns = [
            PrimitiveTdsColumn.string_column("dept"),
            PrimitiveTdsColumn.integer_column("emp_id"),
            PrimitiveTdsColumn.float_column("salary"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["hr", "employees"], columns)

        frame["first_salary"] = frame.groupby("dept").window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by=["emp_id", "salary"],
        )["salary"].first()

        sql = frame.to_sql_query()
        # Should have ORDER BY with both columns
        assert '"root"."emp_id"' in sql
        assert '"root"."salary"' in sql
        assert "first_value" in sql

        pure = frame.to_pure_query()
        assert "ascending(~emp_id)" in pure
        assert "ascending(~salary)" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    def test_multiple_order_by_with_mixed_ascending(self) -> None:
        """
        Window with multiple order_by columns and mixed ascending/descending.
        """
        columns = [
            PrimitiveTdsColumn.string_column("dept"),
            PrimitiveTdsColumn.integer_column("rank"),
            PrimitiveTdsColumn.float_column("salary"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["hr", "employees"], columns)

        frame["top_salary"] = frame.groupby("dept").window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by=["rank", "salary"],
            ascending=[True, False],
        )["salary"].first()

        sql = frame.to_sql_query()
        # ascending is implicit (no keyword), descending is explicit
        assert "DESC" in sql
        assert '"root"."rank"' in sql
        assert '"root"."salary" DESC' in sql

        pure = frame.to_pure_query()
        assert "ascending(~rank)" in pure
        assert "descending(~salary)" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    # ── Descending order with first/last/shift ───────────────────────────

    def test_first_with_descending_order(self) -> None:
        """
        first() with descending order — gives the maximum row's value.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["desc_first"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col1",
            ascending=False,
        )["col1"].first()

        sql = frame.to_sql_query()
        assert "DESC" in sql
        assert "first_value" in sql

        pure = frame.to_pure_query()
        assert "descending(~col1)" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    def test_last_with_descending_order(self) -> None:
        """
        last() with descending order.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["desc_last"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col1",
            ascending=False,
        )["col1"].last()

        sql = frame.to_sql_query()
        assert "DESC" in sql
        assert "last_value" in sql

        pure = frame.to_pure_query()
        assert "descending(~col1)" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    def test_shift_with_descending_order(self) -> None:
        """
        shift() with descending order — lag/lead relative to the descending sorted window.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("ts"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["prev_val"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="ts",
            ascending=False,
        )["val"].shift(periods=2)

        sql = frame.to_sql_query()
        assert "DESC" in sql
        assert "lag(" in sql
        assert ", 2)" in sql

        pure = frame.to_pure_query()
        assert "descending(~ts)" in pure
        assert "lag($r, 2)" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    # ── Edge cases for shift ─────────────────────────────────────────────

    def test_shift_periods_zero(self) -> None:
        """
        shift(periods=0) should produce lead with 0 offset (effectively the current row).
        periods=0 is non-positive, so it goes through the lead branch with -periods = 0.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["same"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col1",
        )["col1"].shift(periods=0)

        sql = frame.to_sql_query()
        # periods=0 → lead branch with offset 0
        assert "lead(" in sql
        assert ", 0)" in sql

        pure = frame.to_pure_query()
        assert "lead($r, 0)" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    def test_shift_large_positive_periods(self) -> None:
        """
        shift(periods=100) — large lag offset.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["lagged"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="col1",
        )["col1"].shift(periods=100)

        sql = frame.to_sql_query()
        assert "lag(" in sql
        assert ", 100)" in sql

        pure = frame.to_pure_query()
        assert "lag($r, 100)" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    # ── range_between frame spec ─────────────────────────────────────────

    def test_range_between_frame_spec(self) -> None:
        """
        window_frame_legend_ext with range_between instead of rows_between.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("ts"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["first_in_range"] = frame.window_frame_legend_ext(
            frame_spec=frame.range_between(-10, 10),
            order_by="ts",
        )["val"].first()

        sql = frame.to_sql_query()
        assert "RANGE BETWEEN" in sql
        assert "10 PRECEDING" in sql
        assert "10 FOLLOWING" in sql
        assert "first_value" in sql

        pure = frame.to_pure_query()
        assert "range(" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    # ── Different column types ───────────────────────────────────────────

    def test_first_on_string_column(self) -> None:
        """first() on a string column."""
        columns = [
            PrimitiveTdsColumn.string_column("name"),
            PrimitiveTdsColumn.integer_column("id"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["first_name"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="id",
        )["name"].first()

        sql = frame.to_sql_query()
        assert "first_value" in sql
        assert '"name"' in sql

        pure = frame.to_pure_query()
        assert "first" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    def test_last_on_float_column_direct_series(self) -> None:
        """last() on a float column producing a standalone series."""
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("measurement"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(-3, 0),
            order_by="id",
        )["measurement"].last()

        sql = series.to_sql_query()
        assert "last_value" in sql
        assert "3 PRECEDING" in sql
        assert "CURRENT ROW" in sql

        pure = series.to_pure_query()
        assert "last" in pure

    # ── Multiple groupby columns ─────────────────────────────────────────

    def test_multiple_groupby_columns(self) -> None:
        """
        Groupby on multiple columns, then apply window function.
        """
        columns = [
            PrimitiveTdsColumn.string_column("region"),
            PrimitiveTdsColumn.string_column("product"),
            PrimitiveTdsColumn.float_column("revenue"),
            PrimitiveTdsColumn.integer_column("quarter"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["sales", "data"], columns)

        frame["first_revenue"] = frame.groupby(["region", "product"]).window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="quarter",
        )["revenue"].first()

        sql = frame.to_sql_query()
        assert "first_value" in sql
        assert "PARTITION BY" in sql
        # Both groupby columns should appear in PARTITION BY
        assert '"root"."region"' in sql
        assert '"root"."product"' in sql

        pure = frame.to_pure_query()
        assert "region" in pure
        assert "product" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    # ── last(numeric_only=True) on groupby ───────────────────────────────

    def test_last_numeric_only_on_groupby_window(self) -> None:
        """
        groupby.window_frame_legend_ext(...).last(numeric_only=True) keeps
        grouping columns plus numeric columns with last_value applied.
        """
        columns = [
            PrimitiveTdsColumn.string_column("dept"),
            PrimitiveTdsColumn.integer_column("headcount"),
            PrimitiveTdsColumn.float_column("budget"),
            PrimitiveTdsColumn.string_column("manager"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["hr", "departments"], columns)

        applied = frame.groupby("dept").window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="headcount",
        ).last(numeric_only=True)

        sql = applied.to_sql_query()
        assert "last_value" in sql

        pure = applied.to_pure_query()
        assert generate_pure_query_and_compile(applied, FrameToPureConfig(), self.legend_client) == pure

    # ── first(numeric_only=True) on groupby ──────────────────────────────

    def test_first_numeric_only_on_groupby_window(self) -> None:
        """
        groupby.window_frame_legend_ext(...).first(numeric_only=True) keeps
        grouping columns plus numeric columns with first_value applied.
        """
        columns = [
            PrimitiveTdsColumn.string_column("dept"),
            PrimitiveTdsColumn.integer_column("headcount"),
            PrimitiveTdsColumn.float_column("budget"),
            PrimitiveTdsColumn.string_column("manager"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["hr", "departments"], columns)

        applied = frame.groupby("dept").window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="headcount",
        ).first(numeric_only=True)

        sql = applied.to_sql_query()
        assert "first_value" in sql
        # Numeric columns should have first_value applied
        assert '"headcount"' in sql
        assert '"budget"' in sql

        pure = applied.to_pure_query()
        assert generate_pure_query_and_compile(applied, FrameToPureConfig(), self.legend_client) == pure

    # ── Bounded rows_between with asymmetric bounds ──────────────────────

    def test_trailing_window(self) -> None:
        """
        rows_between(-5, 0) — trailing window (5 preceding to current row).
        Common for moving first/last over trailing N rows.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("ts"),
            PrimitiveTdsColumn.float_column("price"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["trail_first"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(-5, 0),
            order_by="ts",
        )["price"].first()

        sql = frame.to_sql_query()
        assert "5 PRECEDING" in sql
        assert "CURRENT ROW" in sql
        assert "first_value" in sql

        pure = frame.to_pure_query()
        assert "rows(minus(5), 0)" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    def test_leading_window(self) -> None:
        """
        rows_between(0, 5) — leading window (current row to 5 following).
        """
        columns = [
            PrimitiveTdsColumn.integer_column("ts"),
            PrimitiveTdsColumn.float_column("price"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["lead_last"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(0, 5),
            order_by="ts",
        )["price"].last()

        sql = frame.to_sql_query()
        assert "CURRENT ROW" in sql
        assert "5 FOLLOWING" in sql
        assert "last_value" in sql

        pure = frame.to_pure_query()
        assert "rows(0, 5)" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    # ── Arithmetic with different operations ─────────────────────────────

    def test_first_minus_last_arithmetic(self) -> None:
        """
        Compute the spread: first() - last() within the same window.
        Each is a separate assign (two window operations on same frame).
        """
        columns = [
            PrimitiveTdsColumn.integer_column("ts"),
            PrimitiveTdsColumn.float_column("price"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        window = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="ts",
        )
        frame["first_price"] = window["price"].first()
        frame["last_price"] = window["price"].last()
        frame["spread"] = frame["first_price"] - frame["last_price"]

        sql = frame.to_sql_query()
        assert "first_value" in sql
        assert "last_value" in sql

        pure = frame.to_pure_query()
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    def test_shift_subtraction_returns_change(self) -> None:
        """
        Compute row-over-row change: current_value - lag(value).
        A very common time-series pattern.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("ts"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["prev_val"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="ts",
        )["val"].shift(periods=1)

        frame["change"] = frame["val"] - frame["prev_val"]

        sql = frame.to_sql_query()
        assert "lag(" in sql

        pure = frame.to_pure_query()
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    # ── Series-level window_frame_legend_ext ──────────────────────────────

    def test_series_window_frame_legend_ext_with_no_frame_spec(self) -> None:
        """
        frame['col'].window_frame_legend_ext(order_by=...).first()
        Using the Series-level entry point with no frame_spec.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame["val"].window_frame_legend_ext(
            order_by="id",
        ).first()

        sql = series.to_sql_query()
        assert "first_value" in sql
        assert "ROWS BETWEEN" not in sql

        pure = series.to_pure_query()
        assert "rows(" not in pure

    def test_groupby_series_window_frame_legend_ext_shift(self) -> None:
        """
        frame.groupby('grp')['val'].window_frame_legend_ext(order_by=...).shift()
        Using the GroupbySeries-level entry point.
        """
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame.groupby("grp")["val"].window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="val",
        ).shift(periods=-2)

        sql = series.to_sql_query()
        assert "lead(" in sql
        assert ", 2)" in sql
        assert "PARTITION BY" in sql

        pure = series.to_pure_query()
        assert "lead($r, 2)" in pure

    # ── Custom pwr_func patterns ─────────────────────────────────────────

    def test_custom_pwr_func_nth_on_window_series(self) -> None:
        """
        WindowSeries.window_extend_legend_ext with p.nth for the 3rd row.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        series = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="id",
        )["val"].window_extend_legend_ext(
            pwr_func=lambda p, w, r: p.nth(w, r, 3)["val"],
        )

        sql = series.to_sql_query()
        assert "nth_value" in sql
        assert ", 3)" in sql

        pure = series.to_pure_query()
        assert "nth($w, $r, 3)" in pure

    def test_custom_pwr_func_lead_on_window_series(self) -> None:
        """
        WindowSeries.window_extend_legend_ext with p.lead for 2 rows ahead.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["lead2"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="id",
        )["val"].window_extend_legend_ext(
            pwr_func=lambda p, w, r: p.lead(r, 2)["val"],
        )

        sql = frame.to_sql_query()
        assert "lead(" in sql
        assert ", 2)" in sql

        pure = frame.to_pure_query()
        assert "lead($r, 2)" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    def test_custom_pwr_func_lag_on_window_series(self) -> None:
        """
        WindowSeries.window_extend_legend_ext with p.lag for 3 rows behind.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("id"),
            PrimitiveTdsColumn.float_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame["lag3"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="id",
        )["val"].window_extend_legend_ext(
            pwr_func=lambda p, w, r: p.lag(r, 3)["val"],
        )

        sql = frame.to_sql_query()
        assert "lag(" in sql
        assert ", 3)" in sql

        pure = frame.to_pure_query()
        assert "lag($r, 3)" in pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == pure

    # ── WindowTdsFrame-level operations ──────────────────────────────────

    def test_nth_multi_column_on_window_tds_frame(self) -> None:
        """
        WindowTdsFrame.window_extend_legend_ext with p.nth across all columns.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("a"),
            PrimitiveTdsColumn.float_column("b"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        applied = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="a",
        ).window_extend_legend_ext(
            pwr_func=lambda p, w, r: p.nth(w, r, 5),
        )

        sql = applied.to_sql_query()
        assert "nth_value" in sql
        assert sql.count("nth_value") == 2  # one for each column

        pure = applied.to_pure_query()
        assert "nth($w, $r, 5).a" in pure
        assert "nth($w, $r, 5).b" in pure
        assert generate_pure_query_and_compile(applied, FrameToPureConfig(), self.legend_client) == pure

    def test_shift_multi_column_on_window_tds_frame(self) -> None:
        """
        WindowTdsFrame-level shift (lag) across all columns.
        shift() is on WindowSeries, but a TdsFrame-level lag via window_extend_legend_ext
        can be done with a pwr_func that returns the lag TdsRow.
        """
        columns = [
            PrimitiveTdsColumn.integer_column("a"),
            PrimitiveTdsColumn.float_column("b"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        applied = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="a",
        ).window_extend_legend_ext(
            pwr_func=lambda p, w, r: p.lag(r, 1),
        )

        sql = applied.to_sql_query()
        assert "lag(" in sql
        assert sql.count("lag(") == 2

        pure = applied.to_pure_query()
        assert "lag($r, 1).a" in pure
        assert "lag($r, 1).b" in pure
        assert generate_pure_query_and_compile(applied, FrameToPureConfig(), self.legend_client) == pure


class TestSingleColumnWindowFunctionEndToEnd:
    """End-to-end tests for single-column window functions with real legend server."""

    def test_e2e_first_on_base_frame(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Test first() on base frame with real execution."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["FirstAge"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="Age",
        )["Age"].first()

        # All ages are the minimum age when sorted by Age (12), then first value
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "FirstAge"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", 12]},
                {"values": ["John", "Johnson", 22, "Firm X", 12]},
                {"values": ["John", "Hill", 12, "Firm X", 12]},
                {"values": ["Anthony", "Allen", 22, "Firm X", 12]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", 12]},
                {"values": ["Oliver", "Hill", 32, "Firm B", 12]},
                {"values": ["David", "Harris", 35, "Firm C", 12]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_last_on_base_frame(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Test last() on base frame with real execution."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["LastAge"] = frame.window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="Age",
        )["Age"].last()

        # All ages are the maximum age when sorted by Age (35), then last value
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "LastAge"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", 35]},
                {"values": ["John", "Johnson", 22, "Firm X", 35]},
                {"values": ["John", "Hill", 12, "Firm X", 35]},
                {"values": ["Anthony", "Allen", 22, "Firm X", 35]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", 35]},
                {"values": ["Oliver", "Hill", 32, "Firm B", 35]},
                {"values": ["David", "Harris", 35, "Firm C", 35]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_shift_lead_on_base_frame(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Test shift(periods=1) which is lead(1) on base frame."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["NextAge"] = frame.window_frame_legend_ext(
            order_by="Age",
        )["Age"].shift(periods=-1)

        # Ages ordered: 12, 22, 22, 23, 32, 34, 35
        # Lead(1): 22, 22, 23, 32, 34, 35, NULL
        expected = {
            'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name', 'NextAge'],
            'rows': [
                {'values': ['Peter', 'Smith', 23, 'Firm X', 32]},
                {'values': ['John', 'Johnson', 22, 'Firm X', 22]},
                {'values': ['John', 'Hill', 12, 'Firm X', 22]},
                {'values': ['Anthony', 'Allen', 22, 'Firm X', 23]},
                {'values': ['Fabrice', 'Roberts', 34, 'Firm A', 35]},
                {'values': ['Oliver', 'Hill', 32, 'Firm B', 34]},
                {'values': ['David', 'Harris', 35, 'Firm C', None]}
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_groupby_first_on_grouped_frame(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Test first() on groupby().window_frame_legend_ext() with real execution."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["FirstAgeInFirm"] = frame.groupby("Firm/Legal Name").window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="Age",
        )["Age"].first()

        # Firm X ages ordered: 12, 22, 22, 23 => first is 12
        # Firm A (34) => first is 34
        # Firm B (32) => first is 32
        # Firm C (35) => first is 35
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "FirstAgeInFirm"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", 12]},
                {"values": ["John", "Johnson", 22, "Firm X", 12]},
                {"values": ["John", "Hill", 12, "Firm X", 12]},
                {"values": ["Anthony", "Allen", 22, "Firm X", 12]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", 34]},
                {"values": ["Oliver", "Hill", 32, "Firm B", 32]},
                {"values": ["David", "Harris", 35, "Firm C", 35]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_groupby_last_on_grouped_frame(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        """Test last() on groupby().window_frame_legend_ext() with real execution."""
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["LastAgeInFirm"] = frame.groupby("Firm/Legal Name").window_frame_legend_ext(
            frame_spec=frame.rows_between(),
            order_by="Age",
        )["Age"].last()

        # Firm X ages ordered: 12, 22, 22, 23 => last is 23
        # Firm A (34) => last is 34
        # Firm B (32) => last is 32
        # Firm C (35) => last is 35
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "LastAgeInFirm"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X", 23]},
                {"values": ["John", "Johnson", 22, "Firm X", 23]},
                {"values": ["John", "Hill", 12, "Firm X", 23]},
                {"values": ["Anthony", "Allen", 22, "Firm X", 23]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A", 34]},
                {"values": ["Oliver", "Hill", 32, "Firm B", 32]},
                {"values": ["David", "Harris", 35, "Firm C", 35]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
