from textwrap import dedent

import pytest
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from tests.test_helpers import generate_pure_query_and_compile


class TestRankFunctionErrors:
    def test_rank_error_invaild_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rank(axis=1)

        expected_msg = "The 'axis' parameter of the rank function must be 0 or 'index', but got: axis=1"
        assert v.value.args[0] == expected_msg

    def test_rank_error_invalid_method(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        # 'average' or 'max' are standard pandas methods but not in your valid_methods set
        with pytest.raises(NotImplementedError) as v:
            frame.rank(method="average")

        expected_msg = f"The 'method' parameter of the rank function must be one of ['dense', 'first', 'min'], but got: method='average'"
        assert v.value.args[0] == expected_msg

    # 3. Test for PCT=True with unsupported method
    def test_rank_error_pct_with_invalid_method(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        # pct=True is only supported with method='min' in your code
        with pytest.raises(NotImplementedError) as v:
            frame.rank(pct=True, method='dense')

        expected_msg = "The 'pct=True' parameter of the rank function is only supported with method='min', but got: method='dense'."
        assert v.value.args[0] == expected_msg

    # 4. Test for Invalid NA Option
    def test_rank_error_invalid_na_option(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        invalid_na = "top"
        with pytest.raises(NotImplementedError) as v:
            frame.rank(na_option=invalid_na)

        valid_na_options = {'keep', 'bottom'}
        expected_msg = f"The 'na_option' parameter of the rank function must be one of {valid_na_options!r}, but got: na_option={invalid_na!r}"
        assert v.value.args[0] == expected_msg

class TestRankFunctionOnBaseFrame:

    # @pytest.fixture(autouse=True)
    # def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
    #     self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

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

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__internal_pure_col_name__:{p,w,r | if($r.col1->isEmpty(), | [], | $p->rank($w, $r))})
              ->project(~[col1:p|$p.col1__internal_pure_col_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        # assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

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
                        rank() OVER (ORDER BY "root"."col1")
                END AS "col1",
                CASE
                    WHEN
                        ("root"."col2" IS NULL)
                    THEN
                        null
                    ELSE
                        rank() OVER (ORDER BY "root"."col2")
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
        
        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__internal_pure_col_name__:{p,w,r | if($r.col1->isEmpty(), | [], | $p->rank($w, $r))})
              ->extend(over([ascending(~col2)]), ~col2__internal_pure_col_name__:{p,w,r | if($r.col2->isEmpty(), | [], | $p->rank($w, $r))})
              ->project(~[col1:p|$p.col1__internal_pure_col_name__, col2:p|$p.col2__internal_pure_col_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected

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
        
        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([descending(~col1)]), ~col1__internal_pure_col_name__:{p,w,r | if($r.col1->isEmpty(), | [], | $p->denseRank($w, $r))})
              ->project(~[col1:p|$p.col1__internal_pure_col_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected

    def test_rank_method_first(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='first', na_option='bottom')

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

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__internal_pure_col_name__:{p,w,r | $p->rowNumber($r)})
              ->project(~[col1:p|$p.col1__internal_pure_col_name__])
        '''
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected

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

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__internal_pure_col_name__:{p,w,r | if($r.col1->isEmpty(), | [], | $p->percentRank($w, $r))})
              ->project(~[col1:p|$p.col1__internal_pure_col_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected

    def test_rank_na_option_keep_default(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("int_col"),
                   PrimitiveTdsColumn.string_column("str_col"),
                   PrimitiveTdsColumn.date_column("date_col"),
                   PrimitiveTdsColumn.float_column("float_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='min', numeric_only=True)

        expected = '''
            SELECT
                CASE
                    WHEN
                        ("root"."int_col" IS NULL)
                    THEN
                        null
                    ELSE
                        rank() OVER (ORDER BY "root"."int_col")
                END AS "int_col",
                CASE
                    WHEN
                        ("root"."float_col" IS NULL)
                    THEN
                        null
                    ELSE
                        rank() OVER (ORDER BY "root"."float_col")
                END AS "float_col"
            FROM
                (
                    SELECT
                        "root".int_col AS "int_col",
                        "root".str_col AS "str_col",
                        "root".date_col AS "date_col",
                        "root".float_col AS "float_col"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~int_col)]), ~int_col__internal_pure_col_name__:{p,w,r | if($r.int_col->isEmpty(), | [], | $p->rank($w, $r))})
              ->extend(over([ascending(~float_col)]), ~float_col__internal_pure_col_name__:{p,w,r | if($r.float_col->isEmpty(), | [], | $p->rank($w, $r))})
              ->project(~[int_col:p|$p.int_col__internal_pure_col_name__, float_col:p|$p.float_col__internal_pure_col_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected


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
                        rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col")
                END AS "val_col",
                CASE
                    WHEN
                        ("root"."random_col" IS NULL)
                    THEN
                        null
                    ELSE
                        rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col")
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

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__internal_pure_col_name__:{p,w,r | if($r.val_col->isEmpty(), | [], | $p->rank($w, $r))})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__internal_pure_col_name__:{p,w,r | if($r.random_col->isEmpty(), | [], | $p->rank($w, $r))})
              ->project(~[val_col:p|$p.val_col__internal_pure_col_name__, random_col:p|$p.random_col__internal_pure_col_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected

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
                        rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col")
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

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__internal_pure_col_name__:{p,w,r | if($r.val_col->isEmpty(), | [], | $p->rank($w, $r))})
              ->project(~[val_col:p|$p.val_col__internal_pure_col_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected

    def test_groupby_rank_pct(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")[["val_col", "random_col"]].rank(method='min', pct=True)

        expected = '''
            SELECT
                CASE
                    WHEN
                        ("root"."val_col" IS NULL)
                    THEN
                        null
                    ELSE
                        percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col")
                END AS "val_col",
                CASE
                    WHEN
                        ("root"."random_col" IS NULL)
                    THEN
                        null
                    ELSE
                        percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col")
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

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__internal_pure_col_name__:{p,w,r | if($r.val_col->isEmpty(), | [], | $p->percentRank($w, $r))})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__internal_pure_col_name__:{p,w,r | if($r.random_col->isEmpty(), | [], | $p->percentRank($w, $r))})
              ->project(~[val_col:p|$p.val_col__internal_pure_col_name__, random_col:p|$p.random_col__internal_pure_col_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected

    def test_groupby_rank_dense(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col").rank(method='dense')

        expected = '''
            SELECT
                CASE
                    WHEN
                        ("root"."val_col" IS NULL)
                    THEN
                        null
                    ELSE
                        dense_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col")
                END AS "val_col",
                CASE
                    WHEN
                        ("root"."random_col" IS NULL)
                    THEN
                        null
                    ELSE
                        dense_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col")
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

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__internal_pure_col_name__:{p,w,r | if($r.val_col->isEmpty(), | [], | $p->denseRank($w, $r))})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__internal_pure_col_name__:{p,w,r | if($r.random_col->isEmpty(), | [], | $p->denseRank($w, $r))})
              ->project(~[val_col:p|$p.val_col__internal_pure_col_name__, random_col:p|$p.random_col__internal_pure_col_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
    
    def test_groupby_rank_first_subset(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")[['val_col', 'random_col']].rank(method='first')

        expected = '''
            SELECT
                CASE
                    WHEN
                        ("root"."val_col" IS NULL)
                    THEN
                        null
                    ELSE
                        row_number() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col")
                END AS "val_col",
                CASE
                    WHEN
                        ("root"."random_col" IS NULL)
                    THEN
                        null
                    ELSE
                        row_number() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col")
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

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__internal_pure_col_name__:{p,w,r | if($r.val_col->isEmpty(), | [], | $p->rowNumber($r))})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__internal_pure_col_name__:{p,w,r | if($r.random_col->isEmpty(), | [], | $p->rowNumber($r))})
              ->project(~[val_col:p|$p.val_col__internal_pure_col_name__, random_col:p|$p.random_col__internal_pure_col_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected

    def test_groupby_rank_pct_descending_na_bottom(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col").rank(method='min', ascending=False, na_option='bottom', pct=True)

        expected = '''
            SELECT
                percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col" DESC) AS "val_col",
                percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col" DESC) AS "random_col"
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

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [descending(~val_col)]), ~val_col__internal_pure_col_name__:{p,w,r | $p->percentRank($w, $r)})
              ->extend(over(~[group_col], [descending(~random_col)]), ~random_col__internal_pure_col_name__:{p,w,r | $p->percentRank($w, $r)})
              ->project(~[val_col:p|$p.val_col__internal_pure_col_name__, random_col:p|$p.random_col__internal_pure_col_name__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
