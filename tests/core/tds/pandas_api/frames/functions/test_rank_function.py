from textwrap import dedent
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_frame import FrameToSqlConfig


class TestRankFunctionOnBaseFrame:
    def test_rank_method_simple_min(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='min')

        expected = '''
            SELECT
                CASE
                    WHEN
                        ("root"."col1" IS NULL)
                    THEN
                        null
                    ELSE
                        rank() OVER (ORDER BY "root"."col1")
                END AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test_rank_method_multiple(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.number_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='min')

        expected = '''
            SELECT
                CASE
                    WHEN
                        ("root"."col1" IS NULL)
                    THEN
                        null
                    ELSE
                        rank() OVER (ORDER BY "root"."col1", "root"."col2")
                END AS "col1",
                CASE
                    WHEN
                        ("root"."col2" IS NULL)
                    THEN
                        null
                    ELSE
                        rank() OVER (ORDER BY "root"."col1", "root"."col2")
                END AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test_rank_method_dense_descending(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='dense', ascending=False)

        expected = '''
            SELECT
                CASE
                    WHEN
                        ("root"."col1" IS NULL)
                    THEN
                        null
                    ELSE
                        dense_rank() OVER (ORDER BY "root"."col1" DESC)
                END AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test_rank_method_first(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='first', na_option='top')

        expected = '''
            SELECT
                row_number() OVER (ORDER BY "root"."col1") AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test_rank_pct_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(pct=True)

        expected = '''
            SELECT
                CASE
                    WHEN
                        ("root"."col1" IS NULL)
                    THEN
                        null
                    ELSE
                        percent_rank() OVER (ORDER BY "root"."col1")
                END AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test_rank_na_option_keep_default(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='min', na_option='keep')

        expected = '''
            SELECT
                CASE
                    WHEN
                        ("root"."col1" IS NULL)
                    THEN
                        null
                    ELSE
                        rank() OVER (ORDER BY "root"."col1")
                END AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected


class TestRankFunctionOnGroupbyFrame:
    
    def test_groupby_rank_min(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col").rank(method='min')

        expected = '''
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
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test_groupby_rank_min_subset(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")["val_col"].rank(method='min')

        expected = '''
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
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col").rank(method='min', pct=True)

    def test(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col").rank(method='dense')

    def test(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")['val_col', 'random_col'].rank(method='first')

    def test_groupby_rank_pct_descending_na_top(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col").rank(method='min', ascending=False, na_option='top', pct=True)

        expected = '''
            SELECT
                PERCENT_RANK() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col" DESC NULLS FIRST) AS "val_col",
                PERCENT_RANK() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col" DESC NULLS FIRST) AS "random_col"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected
