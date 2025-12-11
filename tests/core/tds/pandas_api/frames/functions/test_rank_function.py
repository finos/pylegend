import pytest
from textwrap import dedent
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_frame import FrameToSqlConfig


class TestRankFunction:

    def test_rank_method_min_default(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.number_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        
        frame = frame.rank(method='min')

        expected = '''\
            SELECT
                CASE
                    WHEN
                        ("root"."col1" IS NULL)
                    THEN
                        null
                    ELSE
                        RANK() OVER (ORDER BY "root"."col1")
                END AS "col1",
                CASE
                    WHEN
                        ("root"."col2" IS NULL)
                    THEN
                        null
                    ELSE
                        RANK() OVER (ORDER BY "root"."col2")
                END AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_rank_method_dense_descending(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='dense', ascending=False)

        expected = '''\
            SELECT
                CASE
                    WHEN
                        ("root"."col1" IS NULL)
                    THEN
                        null
                    ELSE
                        DENSE_RANK() OVER (ORDER BY "root"."col1" DESC)
                END AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''

        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_rank_method_first(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='first')

        expected = '''\
            SELECT
                CASE
                    WHEN
                        ("root"."col1" IS NULL)
                    THEN
                        null
                    ELSE
                        ROW_NUMBER() OVER (ORDER BY "root"."col1")
                END AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_groupby_rank_min(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col").rank(method='min')

        expected = '''\
            SELECT
                CASE
                    WHEN
                        ("root"."val_col" IS NULL)
                    THEN
                        null
                    ELSE
                        RANK() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col")
                END AS "val_col",
                CASE
                    WHEN
                        ("root"."random_col" IS NULL)
                    THEN
                        null
                    ELSE
                        RANK() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col")
                END AS "random_col"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_groupby_rank_min_subset(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")["val_col"].rank(method='min')

        expected = '''\
            SELECT
                CASE
                    WHEN
                        ("root"."val_col" IS NULL)
                    THEN
                        null
                    ELSE
                        RANK() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col")
                END AS "val_col"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_rank_pct_true(self) -> None:
        # Scenario: Percentage rank
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(pct=True)

        expected = '''\
            SELECT
                CASE
                    WHEN
                        ("root"."col1" IS NULL)
                    THEN
                        null
                    ELSE
                        PERCENT_RANK() OVER (ORDER BY "root"."col1")
                END AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_rank_na_option_keep_default(self) -> None:
        # Scenario: Explicitly passing na_option='keep'
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='min', na_option='keep')

        expected = '''\
            SELECT
                CASE
                    WHEN
                        ("root"."col1" IS NULL)
                    THEN
                        null
                    ELSE
                        RANK() OVER (ORDER BY "root"."col1")
                END AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
