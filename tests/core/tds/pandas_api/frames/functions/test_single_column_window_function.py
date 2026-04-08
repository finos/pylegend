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
        """frame['nth_val'] = frame.groupby('grp').window_frame_legend_ext(...)['val'].window_func_legend_ext(lambda p,w,r: p.nth(w,r,2)['val'])"""
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
        )["val"].window_func_legend_ext(pwr_func=lambda p, w, r: p.nth(w, r, 2)["val"])

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
