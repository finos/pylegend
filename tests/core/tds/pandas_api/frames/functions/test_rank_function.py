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
        # Scenario: Dense rank, descending order
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        
        # method='dense' maps to SQL DENSE_RANK()
        # ascending=False maps to ORDER BY ... DESC
        frame = frame.rank(method='dense', ascending=False)

        expected = '''\
            SELECT
                DENSE_RANK() OVER (ORDER BY "root".col1 DESC) AS "col1"
            FROM
                test_schema.test_table AS "root"'''
        
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_rank_method_first(self) -> None:
        # Scenario: Rank using 'first' (Row Number)
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        
        # method='first' maps to SQL ROW_NUMBER()
        frame = frame.rank(method='first')

        expected = '''\
            SELECT
                ROW_NUMBER() OVER (ORDER BY "root".col1 ASC) AS "col1"
            FROM
                test_schema.test_table AS "root"'''
        
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_groupby_rank_min(self) -> None:
        # Scenario: GroupBy rank. Should partition by the grouper.
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        
        # Group by 'group_col', rank 'val_col'
        # Note: The grouping column itself is NOT ranked, only the selection.
        frame = frame.groupby("group_col")["val_col"].rank(method='min')

        # Logic: 
        # 1. Selection 'val_col' means we select "val_col"
        # 2. GroupBy 'group_col' means PARTITION BY "group_col"
        # 3. Output is just the ranked column (consistent with Pandas SeriesGroupBy behavior)
        expected = '''\
            SELECT
                RANK() OVER (PARTITION BY "root".group_col ORDER BY "root".val_col ASC) AS "val_col"
            FROM
                test_schema.test_table AS "root"'''
        
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_rank_pct_true(self) -> None:
        # Scenario: Percentage rank
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        
        # pct=True maps to SQL PERCENT_RANK()
        frame = frame.rank(pct=True)

        expected = '''\
            SELECT
                PERCENT_RANK() OVER (ORDER BY "root".col1 ASC) AS "col1"
            FROM
                test_schema.test_table AS "root"'''
        
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_rank_na_option_keep_default(self) -> None:
        # Scenario: Default na_option='keep' (Assign NaN rank to NaNs)
        # This implies checking IS NOT NULL before ranking.
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        
        frame = frame.rank(method='min', na_option='keep')

        expected = '''\
            SELECT
                CASE
                    WHEN "root".col1 IS NULL THEN NULL
                    ELSE RANK() OVER (ORDER BY "root".col1 ASC)
                END AS "col1"
            FROM
                test_schema.test_table AS "root"'''
        
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)