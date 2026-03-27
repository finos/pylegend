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
from pylegend.core.language.legacy_api.legacy_api_custom_expressions import (
    LegacyApiOLAPAggregation,
    LegacyApiOLAPGroupByOperation,
    LegacyApiOLAPRank,
    LegacyApiSortInfo,
    LegacyApiPartialFrame,
    olap_agg,
    olap_rank,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.legacy_api.frames.functions.legacy_api_olap_group_by_function import LegacyApiOlapGroupByFunction
from tests.test_helpers import generate_pure_query_and_compile


class TestOlapGroupByAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_olap_operation_constructor_errors(self) -> None:
        # OLAPAggregation errors
        with pytest.raises(TypeError) as r:
            LegacyApiOLAPAggregation("col1", 1)  # type: ignore
        assert "Function should be a lambda" in r.value.args[0]

        with pytest.raises(TypeError) as r:
            LegacyApiOLAPAggregation("col1", lambda x, y: x)  # type: ignore
        assert "Function should be a lambda" in r.value.args[0]

        # OLAPRank errors
        with pytest.raises(TypeError) as r:
            LegacyApiOLAPRank(1)  # type: ignore
        assert "Rank function should be a lambda" in r.value.args[0]

        with pytest.raises(TypeError) as r:
            LegacyApiOLAPRank(lambda x, y: x)  # type: ignore
        assert "Rank function should be a lambda" in r.value.args[0]

        # Helper functions return correct types
        agg = olap_agg("col1", lambda c: c.count())
        assert isinstance(agg, LegacyApiOLAPAggregation)
        assert agg.column_name == "col1"

        rk = olap_rank(lambda p: p.rank())
        assert isinstance(rk, LegacyApiOLAPRank)

        # OLAPGroupByOperation with non-string name
        with pytest.raises(TypeError) as r:
            LegacyApiOLAPGroupByOperation(_type="bad", name=123)  # type: ignore
        assert '"name" should be a string' in r.value.args[0]

        # SortInfo with invalid direction
        with pytest.raises(ValueError) as v:
            LegacyApiSortInfo(column="col1", direction="INVALID")
        assert "Sort direction must be 'ASC' or 'DESC'" in v.value.args[0]

    def test_sort_info_and_partial_frame_accessors(self) -> None:
        # SortInfo get_direction
        sort_info = LegacyApiSortInfo(column="col1", direction="desc")
        assert sort_info.get_direction() == "DESC"

        # PartialFrame get_base_frame
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        base_frame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        partial = LegacyApiPartialFrame(base_frame=base_frame, var_name="p")
        assert partial.get_base_frame() is base_frame

    def test_olap_group_by_input_validation_errors(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Empty operations
        with pytest.raises(ValueError) as r:
            frame.olap_group_by([], [], [])
        assert r.value.args[0] == "At least one operation must be provided for olap_group_by"

        # Unknown partition column
        with pytest.raises(ValueError) as r:
            frame.olap_group_by(["unknown_col"], [olap_agg("col1", lambda c: c.count())], [])
        assert r.value.args[0] == (
            "Column - 'unknown_col' in partition columns list doesn't exist in the current frame. "
            "Current frame columns: ['col1', 'col2']"
        )

        # Unknown sort column
        with pytest.raises(ValueError) as r:
            frame.olap_group_by([], [olap_agg("col1", lambda c: c.count())], ["unknown_col"])
        assert r.value.args[0] == (
            "Column - 'unknown_col' in sort columns list doesn't exist "
            "in the current frame. Current frame columns: ['col1', 'col2']"
        )

        # sort_direction_list length mismatch with sort_column_list
        with pytest.raises(ValueError) as r:
            frame.olap_group_by(
                [],
                [olap_agg("col1", lambda c: c.count())],
                ["col1", "col2"],
                ["ASC"]
            )
        assert r.value.args[0] == (
            "Length of sort_direction_list (1) must match length of sort_column_list (2)"
        )

        # Duplicate new column names
        with pytest.raises(ValueError) as r:
            frame.olap_group_by(
                [], [olap_agg("col1", lambda c: c.count()), olap_agg("col1", lambda c: c.count())], []
            )
        assert "OLAP group by column names list has duplicates" in r.value.args[0]

        # Conflicting column name with existing frame
        frame2: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(
            ['test_schema', 'test_table'],
            [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.integer_column("col1 Count")]
        )
        with pytest.raises(ValueError) as r:
            frame2.olap_group_by([], [olap_agg("col1", lambda c: c.count())], [])
        assert r.value.args[0] == "OLAP group by column name - 'col1 Count' already exists in base frame"

        # Unrecognized operation type
        bad_op = LegacyApiOLAPGroupByOperation(_type="bad", name=None)
        with pytest.raises(TypeError) as t:
            frame.olap_group_by([], [bad_op], [])
        assert "'olap_group_by' function operations_list argument incompatible" in t.value.args[0]
        assert "is not a recognized" in t.value.args[0]

    def test_olap_group_by_aggregation_operation_errors(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Unknown aggregation column
        with pytest.raises(RuntimeError) as r:
            frame.olap_group_by([], [olap_agg("col_nonexistent", lambda c: c.count())], [])
        assert "'olap_group_by' function operations_list argument incompatible" in r.value.args[0]

        # Aggregation function evaluation error
        with pytest.raises(RuntimeError) as r:
            frame.olap_group_by([], [olap_agg("col1", lambda c: c.unknown())], [])  # type: ignore
        assert "'olap_group_by' function operations_list argument incompatible" in r.value.args[0]

        # Aggregation function returns non-primitive
        with pytest.raises(TypeError) as t:
            frame.olap_group_by([], [olap_agg("col1", lambda c: c)], [])  # type: ignore
        assert "'olap_group_by' function operations_list argument incompatible" in t.value.args[0]

    def test_olap_group_by_rank_operation_errors(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Rank function evaluation error
        with pytest.raises(RuntimeError) as r:
            frame.olap_group_by([], [olap_rank(lambda p: p.unknown())], [])  # type: ignore
        assert "'olap_group_by' function operations_list argument incompatible" in r.value.args[0]

        # Rank function returns non-primitive
        with pytest.raises(TypeError) as t:
            frame.olap_group_by([], [olap_rank(lambda p: {})], [])  # type: ignore
        assert "'olap_group_by' function operations_list argument incompatible" in t.value.args[0]

        # Rank function returns raw Python literal
        with pytest.raises(TypeError) as t:
            frame.olap_group_by([], [olap_rank(lambda p: 1)], [])
        assert "'olap_group_by' function operations_list argument incompatible" in t.value.args[0]

    def test_olap_group_by_result_columns(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Aggregation appends new column
        agg_frame = frame.olap_group_by(["col1"], [olap_agg("col1", lambda c: c.count())], [])
        assert "[" + ", ".join([str(c) for c in agg_frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), "
            "TdsColumn(Name: col1 Count, Type: Integer)]"
        )

        # Rank appends new column
        rank_frame = frame.olap_group_by([], [olap_rank(lambda p: p.rank())], ["col1"])
        assert "[" + ", ".join([str(c) for c in rank_frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), "
            "TdsColumn(Name: Rank, Type: Integer)]"
        )

        # Dense rank appends new column
        dense_rank_frame = frame.olap_group_by([], [olap_rank(lambda p: p.dense_rank())], ["col1"])
        assert "[" + ", ".join([str(c) for c in dense_rank_frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), "
            "TdsColumn(Name: DenseRank, Type: Integer)]"
        )

    def test_sql_gen_olap_group_by_aggregation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Count with partition
        count_frame = frame.olap_group_by(["col2"], [olap_agg("col1", lambda c: c.count())], [])
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                COUNT("root"."col1") OVER (PARTITION BY "root"."col2") AS "col1 Count"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert count_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

        # Multiple operations
        multi_frame = frame.olap_group_by(
            ["col2"],
            [olap_agg("col1", lambda c: c.count()), olap_agg("col1", lambda c: c.sum())],  # type: ignore
            [],
        )
        assert "[" + ", ".join([str(c) for c in multi_frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), "
            "TdsColumn(Name: col1 Count, Type: Integer), TdsColumn(Name: col1 Sum, Type: Integer)]"
        )
        multi_expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                COUNT("root"."col1") OVER (PARTITION BY "root"."col2") AS "col1 Count",
                SUM("root"."col1") OVER (PARTITION BY "root"."col2") AS "col1 Sum"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert multi_frame.to_sql_query(FrameToSqlConfig()) == dedent(multi_expected)

    def test_sql_gen_olap_group_by_rank_and_dense_rank(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Rank with partition and sort ASC
        rank_frame = frame.olap_group_by(["col2"], [olap_rank(lambda p: p.rank())], ["col1"])
        rank_expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                rank() OVER (PARTITION BY "root"."col2" ORDER BY "root"."col1") AS "Rank"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert rank_frame.to_sql_query(FrameToSqlConfig()) == dedent(rank_expected)

        # Dense rank
        dense_frame = frame.olap_group_by(["col2"], [olap_rank(lambda p: p.dense_rank())], ["col1"])
        dense_expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                dense_rank() OVER (PARTITION BY "root"."col2" ORDER BY "root"."col1") AS "DenseRank"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert dense_frame.to_sql_query(FrameToSqlConfig()) == dedent(dense_expected)

        # Sort DESC
        desc_frame = frame.olap_group_by(["col2"], [olap_rank(lambda p: p.rank())], ["col1"], ["DESC"])
        desc_expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                rank() OVER (PARTITION BY "root"."col2" ORDER BY "root"."col1" DESC) AS "Rank"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert desc_frame.to_sql_query(FrameToSqlConfig()) == dedent(desc_expected)

        # No partition
        no_part_frame = frame.olap_group_by([], [olap_rank(lambda p: p.rank())], ["col1"])
        no_part_expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                rank() OVER (ORDER BY "root"."col1") AS "Rank"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert no_part_frame.to_sql_query(FrameToSqlConfig()) == dedent(no_part_expected)

    def test_sql_gen_olap_group_by_with_preceding_operations(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # With distinct
        distinct_frame = frame.distinct().olap_group_by(
            ["col2"], [olap_agg("col1", lambda c: c.count())], []
        )
        distinct_expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                COUNT("root"."col1") OVER (PARTITION BY "root"."col2") AS "col1 Count"
            FROM
                (
                    SELECT DISTINCT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert distinct_frame.to_sql_query(FrameToSqlConfig()) == dedent(distinct_expected)

        # With limit
        limit_frame = frame.take(5).olap_group_by(
            ["col2"], [olap_agg("col1", lambda c: c.count())], []
        )
        limit_expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2",
                COUNT("root"."col1") OVER (PARTITION BY "root"."col2") AS "col1 Count"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 5
                ) AS "root"'''
        assert limit_frame.to_sql_query(FrameToSqlConfig()) == dedent(limit_expected)

    def test_sql_gen_olap_group_by_chained_with_filter_and_restrict(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        olap_frame = frame.olap_group_by(["col2"], [olap_agg("col1", lambda c: c.count())], [])

        # Chained with filter
        filtered = olap_frame.filter(lambda r: r["col1 Count"] > 1)  # type: ignore
        assert "col1 Count" in filtered.to_sql_query(FrameToSqlConfig())

        # Chained with restrict
        restricted = olap_frame.restrict(["col2", "col1 Count"])
        assert "col1 Count" in restricted.to_sql_query(FrameToSqlConfig())
        assert "[" + ", ".join([str(c) for c in restricted.columns()]) + "]" == (
            "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: col1 Count, Type: Integer)]"
        )

    def test_pure_gen_olap_group_by_aggregation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.extend([lambda x: x["col1"]], ["col3"])

        # Count with partition (pretty and non-pretty)
        count_frame = frame.olap_group_by(["col2"], [olap_agg("col1", lambda c: c.count())], [])
        assert generate_pure_query_and_compile(count_frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(~col3:{r | $r.col1})
              ->extend(over(~[col2], []), ~'col1 Count':{r | $r.col1}:{c | $c->count()})'''
        )
        assert generate_pure_query_and_compile(count_frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->extend(~col3:{r | $r.col1})"
            "->extend(over(~[col2], []), ~'col1 Count':{r | $r.col1}:{c | $c->count()})"
        )

        # Multiple operations
        multi_frame = frame.olap_group_by(
            ["col2"],
            [olap_agg("col1", lambda c: c.count()), olap_agg("col1", lambda c: c.sum())],  # type: ignore
            [],
        )
        assert generate_pure_query_and_compile(multi_frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(~col3:{r | $r.col1})
              ->extend(over(~[col2], []), ~[
                'col1 Count':{r | $r.col1}:{c | $c->count()},
                'col1 Sum':{r | $r.col1}:{c | $c->sum()}
              ])'''
        )

    def test_pure_gen_olap_group_by_rank_variants(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Rank with partition and sort ASC (pretty and non-pretty)
        rank_frame = frame.olap_group_by(["col2"], [olap_rank(lambda p: p.rank())], ["col1"])
        assert generate_pure_query_and_compile(rank_frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [ascending(~col1)]), ~Rank:{p | $p->rank()})'''
        )
        assert generate_pure_query_and_compile(rank_frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#->extend(over(~[col2], [ascending(~col1)]), ~Rank:{p | $p->rank()})"
        )

        # Dense rank
        dense_frame = frame.olap_group_by(["col2"], [olap_rank(lambda p: p.dense_rank())], ["col1"])
        assert generate_pure_query_and_compile(dense_frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [ascending(~col1)]), ~DenseRank:{p | $p->denseRank()})'''
        )

        # Desc sort
        desc_frame = frame.olap_group_by(["col2"], [olap_rank(lambda p: p.rank())], ["col1"], ["DESC"])
        assert generate_pure_query_and_compile(desc_frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [descending(~col1)]), ~Rank:{p | $p->rank()})'''
        )

    def test_olap_group_by_function_name(self) -> None:
        assert LegacyApiOlapGroupByFunction.name() == "olap_group_by"

    def test_pure_gen_olap_group_by_mixed_rank_and_aggregation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Mixed rank + aggregation operations (covers the else branch in to_pure)
        mixed_frame = frame.olap_group_by(
            ["col2"],
            [olap_rank(lambda p: p.rank()), olap_agg("col1", lambda c: c.count())],
            ["col1"],
        )
        assert generate_pure_query_and_compile(mixed_frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->extend(over(~[col2], [ascending(~col1)]), ~Rank:{p | $p->rank()})
              ->extend(over(~[col2], [ascending(~col1)]), ~'col1 Count':{r | $r.col1}:{c | $c->count()})'''
        )
        assert generate_pure_query_and_compile(mixed_frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->extend(over(~[col2], [ascending(~col1)]), ~Rank:{p | $p->rank()})"
            "->extend(over(~[col2], [ascending(~col1)]), ~'col1 Count':{r | $r.col1}:{c | $c->count()})"
        )
