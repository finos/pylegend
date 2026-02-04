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

import json
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
from tests.test_helpers.test_legend_service_frames import simple_relation_person_service_frame_pandas_api


class TestRankFunctionErrors:
    def test_rank_error_invaild_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(
            ["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rank(axis=1)

        expected_msg = "The 'axis' parameter of the rank function must be 0 or 'index', but got: axis=1"
        assert v.value.args[0] == expected_msg

    def test_rank_error_invalid_method(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame = PandasApiTableSpecInputFrame(
            ["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rank(method="average")

        expected_msg = "The 'method' parameter of the rank function must be one of ['dense', 'first', 'min'], but got: method='average'"  # noqa: E501
        assert v.value.args[0] == expected_msg

    def test_rank_error_pct_with_invalid_method(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame = PandasApiTableSpecInputFrame(
            ["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rank(pct=True, method='dense')

        expected_msg = "The 'pct=True' parameter of the rank function is only supported with method='min', but got: method='dense'."  # noqa: E501
        assert v.value.args[0] == expected_msg

    def test_rank_error_invalid_na_option(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame = PandasApiTableSpecInputFrame(
            ["test_schema", "test_table"], columns)

        invalid_na = "top"
        with pytest.raises(NotImplementedError) as v:
            frame.rank(na_option=invalid_na)

        expected_msg = "The 'na_option' parameter of the rank function must be one of ['bottom'], but got: na_option='top'"
        assert v.value.args[0] == expected_msg


class TestRankFunctionOnBaseFrame:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_rank_method_simple_min(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='min')

        expected = '''
            SELECT
                "root"."col1__internal_pylegend_column__" AS "col1"
            FROM
                (
                    SELECT
                        rank() OVER (ORDER BY "root"."col1") AS "col1__internal_pylegend_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                col1:p|$p.col1__internal_pylegend_column__
              ])
        '''
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_rank_method_multiple(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.number_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='min')

        expected = '''
            SELECT
                "root"."col1__internal_pylegend_column__" AS "col1",
                "root"."col2__internal_pylegend_column__" AS "col2"
            FROM
                (
                    SELECT
                        rank() OVER (ORDER BY "root"."col1") AS "col1__internal_pylegend_column__",
                        rank() OVER (ORDER BY "root"."col2") AS "col2__internal_pylegend_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->extend(over([ascending(~col2)]), ~col2__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                col1:p|$p.col1__internal_pylegend_column__,
                col2:p|$p.col2__internal_pylegend_column__
              ])
        '''
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_rank_method_dense_descending(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='dense', ascending=False)

        expected = '''
            SELECT
                "root"."col1__internal_pylegend_column__" AS "col1"
            FROM
                (
                    SELECT
                        dense_rank() OVER (ORDER BY "root"."col1" DESC) AS "col1__internal_pylegend_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([descending(~col1)]), ~col1__internal_pylegend_column__:{p,w,r | $p->denseRank($w, $r)})
              ->project(~[
                col1:p|$p.col1__internal_pylegend_column__
              ])
        '''
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_rank_method_first(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='first', na_option='bottom')

        expected = '''
            SELECT
                "root"."col1__internal_pylegend_column__" AS "col1"
            FROM
                (
                    SELECT
                        row_number() OVER (ORDER BY "root"."col1") AS "col1__internal_pylegend_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__internal_pylegend_column__:{p,w,r | $p->rowNumber($r)})
              ->project(~[
                col1:p|$p.col1__internal_pylegend_column__
              ])
        '''
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_rank_pct_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(pct=True)

        expected = '''
            SELECT
                "root"."col1__internal_pylegend_column__" AS "col1"
            FROM
                (
                    SELECT
                        percent_rank() OVER (ORDER BY "root"."col1") AS "col1__internal_pylegend_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__internal_pylegend_column__:{p,w,r | $p->percentRank($w, $r)})
              ->project(~[
                col1:p|$p.col1__internal_pylegend_column__
              ])
        '''
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_rank_na_option_keep_default(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("int_col"),
                   PrimitiveTdsColumn.string_column("str_col"),
                   PrimitiveTdsColumn.date_column("date_col"),
                   PrimitiveTdsColumn.float_column("float_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.rank(method='min', numeric_only=True)

        expected = '''
            SELECT
                "root"."int_col__internal_pylegend_column__" AS "int_col",
                "root"."float_col__internal_pylegend_column__" AS "float_col"
            FROM
                (
                    SELECT
                        rank() OVER (ORDER BY "root"."int_col") AS "int_col__internal_pylegend_column__",
                        rank() OVER (ORDER BY "root"."float_col") AS "float_col__internal_pylegend_column__"
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
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~int_col)]), ~int_col__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->extend(over([ascending(~float_col)]), ~float_col__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                int_col:p|$p.int_col__internal_pylegend_column__,
                float_col:p|$p.float_col__internal_pylegend_column__
              ])
        '''
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_appending_ranked_column(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("name"),
            PrimitiveTdsColumn.integer_column("age"),
            PrimitiveTdsColumn.float_column("height"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame["age_rank"] = frame["age"].rank()  # type: ignore[assignment]
        frame['age_rank_plus_2'] = frame["age"].rank() + 4  # type: ignore[operator]
        frame['age_rank_plus_2'] = frame["age"].rank() + 2  # type: ignore[operator]
        frame["name_age_combined"] = \
            frame["age_rank_plus_2"] + frame["height"].rank()/5 + frame["name"].rank()  # type: ignore[operator]

        expected = '''
            SELECT
                "root".name AS "name",
                "root".age AS "age",
                "root".height AS "height",
                rank() OVER (ORDER BY "root".age) AS "age_rank",
                (rank() OVER (ORDER BY "root".age) + 2) AS "age_rank_plus_2",
                (((rank() OVER (ORDER BY "root".age) + 2) + ((1.0 * rank() OVER (ORDER BY "root".height)) / 5)) + rank() OVER (ORDER BY "root".name)) AS "name_age_combined"
            FROM
                test_schema.test_table AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~age)]), ~age__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[name:c|$c.name, age:c|$c.age, height:c|$c.height, age_rank:c|$c.age__internal_pylegend_column__])
              ->extend(over([ascending(~age)]), ~age__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[name:c|$c.name, age:c|$c.age, height:c|$c.height, age_rank:c|$c.age_rank, age_rank_plus_2:c|(toOne($c.age__internal_pylegend_column__) + 4)])
              ->extend(over([ascending(~age)]), ~age__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[name:c|$c.name, age:c|$c.age, height:c|$c.height, age_rank:c|$c.age_rank, age_rank_plus_2:c|(toOne($c.age__internal_pylegend_column__) + 2)])
              ->extend(over([ascending(~height)]), ~height__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->extend(over([ascending(~name)]), ~name__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[name:c|$c.name, age:c|$c.age, height:c|$c.height, age_rank:c|$c.age_rank, age_rank_plus_2:c|$c.age_rank_plus_2, name_age_combined:c|((toOne($c.age_rank_plus_2) + (toOne($c.height__internal_pylegend_column__) / 5)) + toOne($c.name__internal_pylegend_column__))])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_spaces(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("name"),
            PrimitiveTdsColumn.integer_column("present age"),
            PrimitiveTdsColumn.float_column("present height"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame["ranked height"] = frame["present height"].rank()  # type: ignore[assignment]

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~'present height')]), ~'present height__internal_pylegend_column__':{p,w,r | $p->rank($w, $r)})
              ->project(~[name:c|$c.name, 'present age':c|$c.'present age', 'present height':c|$c.'present height', 'ranked height':c|$c.'present height__internal_pylegend_column__'])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected


class TestRankFunctionOnGroupbyFrame:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

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
                "root"."val_col__internal_pylegend_column__" AS "val_col",
                "root"."random_col__internal_pylegend_column__" AS "random_col"
            FROM
                (
                    SELECT
                        rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__internal_pylegend_column__",
                        rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col__internal_pylegend_column__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                val_col:p|$p.val_col__internal_pylegend_column__,
                random_col:p|$p.random_col__internal_pylegend_column__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_groupby_rank_min_subset(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")["val_col"].rank(method='min')  # type: ignore[assignment]

        expected = '''
            SELECT
                "root"."val_col__internal_pylegend_column__" AS "val_col"
            FROM
                (
                    SELECT
                        rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__internal_pylegend_column__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__internal_pylegend_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                val_col:p|$p.val_col__internal_pylegend_column__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

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
                "root"."val_col__internal_pylegend_column__" AS "val_col",
                "root"."random_col__internal_pylegend_column__" AS "random_col"
            FROM
                (
                    SELECT
                        percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__internal_pylegend_column__",
                        percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col__internal_pylegend_column__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__internal_pylegend_column__:{p,w,r | $p->percentRank($w, $r)})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__internal_pylegend_column__:{p,w,r | $p->percentRank($w, $r)})
              ->project(~[
                val_col:p|$p.val_col__internal_pylegend_column__,
                random_col:p|$p.random_col__internal_pylegend_column__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

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
                "root"."val_col__internal_pylegend_column__" AS "val_col",
                "root"."random_col__internal_pylegend_column__" AS "random_col"
            FROM
                (
                    SELECT
                        dense_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__internal_pylegend_column__",
                        dense_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col__internal_pylegend_column__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__internal_pylegend_column__:{p,w,r | $p->denseRank($w, $r)})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__internal_pylegend_column__:{p,w,r | $p->denseRank($w, $r)})
              ->project(~[
                val_col:p|$p.val_col__internal_pylegend_column__,
                random_col:p|$p.random_col__internal_pylegend_column__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

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
                "root"."val_col__internal_pylegend_column__" AS "val_col",
                "root"."random_col__internal_pylegend_column__" AS "random_col"
            FROM
                (
                    SELECT
                        row_number() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__internal_pylegend_column__",
                        row_number() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col__internal_pylegend_column__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__internal_pylegend_column__:{p,w,r | $p->rowNumber($r)})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__internal_pylegend_column__:{p,w,r | $p->rowNumber($r)})
              ->project(~[
                val_col:p|$p.val_col__internal_pylegend_column__,
                random_col:p|$p.random_col__internal_pylegend_column__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

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
                "root"."val_col__internal_pylegend_column__" AS "val_col",
                "root"."random_col__internal_pylegend_column__" AS "random_col"
            FROM
                (
                    SELECT
                        percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col" DESC) AS "val_col__internal_pylegend_column__",
                        percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col" DESC) AS "random_col__internal_pylegend_column__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [descending(~val_col)]), ~val_col__internal_pylegend_column__:{p,w,r | $p->percentRank($w, $r)})
              ->extend(over(~[group_col], [descending(~random_col)]), ~random_col__internal_pylegend_column__:{p,w,r | $p->percentRank($w, $r)})
              ->project(~[
                val_col:p|$p.val_col__internal_pylegend_column__,
                random_col:p|$p.random_col__internal_pylegend_column__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_groupby_rank_with_assign(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame["val_col_rank"] = \
            frame.groupby("group_col")["val_col"].rank(method='min', ascending=False, na_option='bottom', pct=True)

        expected = '''
            SELECT
                "root".group_col AS "group_col",
                "root".val_col AS "val_col",
                "root".random_col AS "random_col",
                percent_rank() OVER (PARTITION BY "root".group_col ORDER BY "root".val_col DESC) AS "val_col_rank"
            FROM
                test_schema.test_table AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [descending(~val_col)]), ~val_col__internal_pylegend_column__:{p,w,r | $p->percentRank($w, $r)})
              ->project(~[group_col:c|$c.group_col, val_col:c|$c.val_col, random_col:c|$c.random_col, val_col_rank:c|$c.val_col__internal_pylegend_column__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected


class TestRankFunctionEndtoEnd:
    def test_e2e_rank_no_arguments(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["First Name Rank"] = frame["First Name"].rank(na_option='bottom')  # type: ignore[assignment]
        frame["Last Name Rank"] = frame["Last Name"].rank(na_option='bottom')  # type: ignore[assignment]
        frame["Age Rank"] = frame["Age"].rank(na_option='bottom')  # type: ignore[assignment]
        frame["Firm/Legal Name Rank"] = frame["Firm/Legal Name"].rank(na_option='bottom')  # type: ignore[assignment]

        expected = {
            "columns": [
                "First Name", "Last Name", "Age", "Firm/Legal Name",
                "First Name Rank", "Last Name Rank", "Age Rank", "Firm/Legal Name Rank"
            ],
            "rows": [
                {"values": ['Peter', 'Smith', 23, 'Firm X', 7, 7, 4, 4]},
                {"values": ['John', 'Johnson', 22, 'Firm X', 4, 5, 2, 4]},
                {"values": ['John', 'Hill', 12, 'Firm X', 4, 3, 1, 4]},
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 1, 1, 2, 4]},
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 3, 6, 6, 1]},
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 6, 3, 5, 2]},
                {"values": ['David', 'Harris', 35, 'Firm C', 2, 2, 7, 3]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_dense_rank_without_appending(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.rank(method='dense', na_option='bottom')

        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": [6, 6, 3, 4]},  # Peter, Smith, 23, Firm X
                {"values": [4, 4, 2, 4]},  # John, Johnson, 22, Firm X
                {"values": [4, 3, 1, 4]},  # John, Hill, 12, Firm X
                {"values": [1, 1, 2, 4]},  # Anthony, Allen, 22, Firm X
                {"values": [3, 5, 5, 1]},  # Fabrice, Roberts, 34, Firm A
                {"values": [5, 3, 4, 2]},  # Oliver, Hill, 32, Firm B
                {"values": [2, 2, 6, 3]},  # David, Harris, 35, Firm C
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_pct_rank(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["First Name Rank"] = \
            frame["First Name"].rank(pct=True, ascending=False, na_option='bottom')  # type: ignore[assignment]
        frame["Last Name Rank"] = \
            frame["Last Name"].rank(pct=True, ascending=False, na_option='bottom')  # type: ignore[assignment]
        frame["Age Rank"] = \
            frame["Age"].rank(pct=True, ascending=False, na_option='bottom')  # type: ignore[assignment]
        frame["Firm/Legal Name Rank"] = \
            frame["Firm/Legal Name"].rank(pct=True, ascending=False, na_option='bottom')  # type: ignore[assignment]

        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name",
                        "First Name Rank", "Last Name Rank", "Age Rank", "Firm/Legal Name Rank"],
            "rows": [
                # Peter (0.0), Smith (0.0), 23 (3/6=0.5), Firm X (0.0)
                {"values": ['Peter', 'Smith', 23, 'Firm X', 0.0, 0.0, 0.5, 0.0]},
                # John (2/6=0.33..), Johnson (2/6=0.33..), 22 (4/6=0.66..), Firm X (0.0)
                {"values": ['John', 'Johnson', 22, 'Firm X', 0.3333333333333333, 0.3333333333333333, 0.6666666666666666, 0.0]},
                # John (0.33..), Hill (3/6=0.5), 12 (6/6=1.0), Firm X (0.0)
                {"values": ['John', 'Hill', 12, 'Firm X', 0.3333333333333333, 0.5, 1.0, 0.0]},
                # Anthony (6/6=1.0), Allen (6/6=1.0), 22 (4/6=0.66..), Firm X (0.0)
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 1.0, 1.0, 0.6666666666666666, 0.0]},
                # Fabrice (4/6=0.66..), Roberts (1/6=0.16..), 34 (1/6=0.16..), Firm A (6/6=1.0)
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 0.6666666666666666, 0.16666666666666666, 0.16666666666666666, 1.0]},  # noqa: E501
                # Oliver (1/6=0.16..), Hill (3/6=0.5), 32 (2/6=0.33..), Firm B (5/6=0.83..)
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 0.16666666666666666, 0.5, 0.3333333333333333, 0.8333333333333334]},
                # David (5/6=0.83..), Harris (5/6=0.83..), 35 (0.0), Firm C (4/6=0.66..)
                {"values": ['David', 'Harris', 35, 'Firm C', 0.8333333333333334, 0.8333333333333334, 0.0, 0.6666666666666666]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_groupby_no_selection(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["First Name Rank"] = frame.groupby("Firm/Legal Name")["First Name"].rank(na_option='bottom')
        frame["Last Name Rank"] = frame.groupby("Firm/Legal Name")["Last Name"].rank(na_option='bottom')
        frame["Age Rank"] = frame.groupby("Firm/Legal Name")["Age"].rank(na_option='bottom')

        frame = frame[[
            "Firm/Legal Name",
            "First Name", "First Name Rank",
            "Last Name", "Last Name Rank",
            "Age", "Age Rank"
        ]]  # type: ignore[assignment]

        expected = {
            'columns': ['Firm/Legal Name', 'First Name', 'First Name Rank', 'Last Name', 'Last Name Rank', 'Age', 'Age Rank'],
            'rows': [
                {'values': ['Firm X', 'Peter', 4, 'Smith', 4, 23, 4]},
                {'values': ['Firm X', 'John', 2, 'Johnson', 3, 22, 2]},
                {'values': ['Firm X', 'John', 2, 'Hill', 2, 12, 1]},
                {'values': ['Firm X', 'Anthony', 1, 'Allen', 1, 22, 2]},
                {'values': ['Firm A', 'Fabrice', 1, 'Roberts', 1, 34, 1]},
                {'values': ['Firm B', 'Oliver', 1, 'Hill', 1, 32, 1]},
                {'values': ['Firm C', 'David', 1, 'Harris', 1, 35, 1]}
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
