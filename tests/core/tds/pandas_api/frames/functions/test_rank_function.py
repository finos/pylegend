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
from pylegend.core.language.shared.functions import pi
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

        expected_msg = "The 'method' parameter of the rank function must be one of ['cume_dist', 'dense', 'first', 'min', 'ntile'], but got: method='average'"  
        assert v.value.args[0] == expected_msg

    def test_rank_error_pct_with_invalid_method(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame = PandasApiTableSpecInputFrame(
            ["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.rank(pct=True, method='dense')

        expected_msg = "The 'pct=True' parameter of the rank function is only supported with method='min', but got: method='dense'."  
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

    def test_rank_on_computed_series(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(RuntimeError) as v:
            (frame.groupby("col1")["col2"] + 5).rank() 

        expected_msg = '''
            Applying rank function to a computed series expression is not supported yet.
            For example,
                not supported: (frame.groupby('grp')['col'] + 5).rank()
                supported: frame.groupby('grp')['col'].rank() + 5
        '''
        expected_msg = dedent(expected_msg).strip()
        assert v.value.args[0] == expected_msg

    def test_multiple_window_functions(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame["col2"].rank() + frame["col1"].rank()  

        expected_msg = '''
            Only expressions with maximum one Series/GroupbySeries function call (such as .rank()) is supported.
            If multiple Series/GroupbySeries need function calls, please compute them in separate steps.
            For example,
                unsupported:
                    frame['new_col'] = frame['col1'].rank() + 2 + frame['col2'].rank()
                supported:
                    frame['new_col'] = frame['col1'].rank() + 2
                    frame['new_col'] += frame['col2'].rank()
        '''
        expected_msg = dedent(expected_msg).strip()
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
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        rank() OVER (ORDER BY "root"."col1") AS "col1__pylegend_olap_column__"
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
              ->extend(over([ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__
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
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        rank() OVER (ORDER BY "root"."col1") AS "col1__pylegend_olap_column__",
                        rank() OVER (ORDER BY "root"."col2") AS "col2__pylegend_olap_column__"
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
              ->extend(over([ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->extend(over([ascending(~col2)]), ~col2__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__,
                col2:p|$p.col2__pylegend_olap_column__
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
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        dense_rank() OVER (ORDER BY "root"."col1" DESC) AS "col1__pylegend_olap_column__"
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
              ->extend(over([descending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->denseRank($w, $r)})
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__
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
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        row_number() OVER (ORDER BY "root"."col1") AS "col1__pylegend_olap_column__"
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
              ->extend(over([ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->rowNumber($r)})
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__
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
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        percent_rank() OVER (ORDER BY "root"."col1") AS "col1__pylegend_olap_column__"
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
              ->extend(over([ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->percentRank($w, $r)})
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__
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
                "root"."int_col__pylegend_olap_column__" AS "int_col",
                "root"."float_col__pylegend_olap_column__" AS "float_col"
            FROM
                (
                    SELECT
                        rank() OVER (ORDER BY "root"."int_col") AS "int_col__pylegend_olap_column__",
                        rank() OVER (ORDER BY "root"."float_col") AS "float_col__pylegend_olap_column__"
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
              ->extend(over([ascending(~int_col)]), ~int_col__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->extend(over([ascending(~float_col)]), ~float_col__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                int_col:p|$p.int_col__pylegend_olap_column__,
                float_col:p|$p.float_col__pylegend_olap_column__
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
        frame["new_col"] = frame["age"].rank() + 2 + 5  
        frame["new_col"] = frame["new_col"] + frame["name"].rank(pct=True)  

        expected = '''
            SELECT
                "root"."name" AS "name",
                "root"."age" AS "age",
                "root"."height" AS "height",
                "root"."new_col__pylegend_olap_column__" AS "new_col"
            FROM
                (
                    SELECT
                        "root"."name" AS "name",
                        "root"."age" AS "age",
                        "root"."height" AS "height",
                        ("root"."new_col__pylegend_olap_column__" + percent_rank() OVER (ORDER BY "root"."name")) AS "new_col__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".name AS "name",
                                "root".age AS "age",
                                "root".height AS "height",
                                ((rank() OVER (ORDER BY "root".age) + 2) + 5) AS "new_col__pylegend_olap_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~age)]), ~age__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[name:c|$c.name, age:c|$c.age, height:c|$c.height, new_col:c|((toOne($c.age__pylegend_olap_column__) + 2) + 5)])
              ->extend(over([ascending(~name)]), ~name__pylegend_olap_column__:{p,w,r | $p->percentRank($w, $r)})
              ->project(~[name:c|$c.name, age:c|$c.age, height:c|$c.height, new_col:c|(toOne($c.new_col) + toOne($c.name__pylegend_olap_column__))])
        '''  
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
        frame["ranked height"] = frame["present height"].rank()  

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~'present height')]), ~present height__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[name:c|$c.name, present age:c|$c.present age, present height:c|$c.present height, ranked height:c|$c.present height__pylegend_olap_column__])
        '''  
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_series_full_query(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("name"),
            PrimitiveTdsColumn.integer_column("age"),
            PrimitiveTdsColumn.float_column("height"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        series = frame["height"].rank()

        expected = '''
            SELECT
                "root"."height__pylegend_olap_column__" AS "height"
            FROM
                (
                    SELECT
                        rank() OVER (ORDER BY "root"."height") AS "height__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".height AS "height"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert series.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->select(~[height])
              ->extend(over([ascending(~height)]), ~height__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                height:p|$p.height__pylegend_olap_column__
              ])
        '''  
        expected = dedent(expected).strip()
        assert series.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(series, FrameToPureConfig(), self.legend_client) == expected

        series += 5  
        expected = '''
            SELECT
                ("root"."height__pylegend_olap_column__" + 5) AS "height"
            FROM
                (
                    SELECT
                        rank() OVER (ORDER BY "root".height) AS "height__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert series.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~height)]), ~height__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[height:c|(toOne($c.height__pylegend_olap_column__) + 5)])
        '''  
        expected = dedent(expected).strip()
        assert series.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(series, FrameToPureConfig(), self.legend_client) == expected

    def test_series_rank_with_literals(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("first_name"),
            PrimitiveTdsColumn.string_column("last_name"),
            PrimitiveTdsColumn.integer_column("age"),
            PrimitiveTdsColumn.float_column("height"),
            PrimitiveTdsColumn.datetime_column("date"),
            PrimitiveTdsColumn.boolean_column("is_active"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame["name"] = "Honorable" + frame["first_name"].replace("mr", "Mr.") + frame["last_name"] 
        frame["is_fit"] = frame["is_active"] | (frame["age"] < 10) | False  
        frame["circumference"] = pi() * frame["height"]  

        expected = '''
            SELECT
                "root".first_name AS "first_name",
                "root".last_name AS "last_name",
                "root".age AS "age",
                "root".height AS "height",
                "root"."date" AS "date",
                "root".is_active AS "is_active",
                CONCAT(CONCAT('Honorable', REPLACE("root".first_name, 'mr', 'Mr.')), "root".last_name) AS "name",
                (("root".is_active OR ("root".age < 10)) OR false) AS "is_fit",
                (PI() * "root".height) AS "circumference"
            FROM
                test_schema.test_table AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected


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
                "root"."val_col__pylegend_olap_column__" AS "val_col",
                "root"."random_col__pylegend_olap_column__" AS "random_col"
            FROM
                (
                    SELECT
                        rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__pylegend_olap_column__",
                        rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col__pylegend_olap_column__"
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
        '''  
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                val_col:p|$p.val_col__pylegend_olap_column__,
                random_col:p|$p.random_col__pylegend_olap_column__
              ])
        '''  
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
                "root"."val_col__pylegend_olap_column__" AS "val_col",
                "root"."random_col__pylegend_olap_column__" AS "random_col"
            FROM
                (
                    SELECT
                        percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__pylegend_olap_column__",
                        percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col__pylegend_olap_column__"
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
        '''  
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__pylegend_olap_column__:{p,w,r | $p->percentRank($w, $r)})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__pylegend_olap_column__:{p,w,r | $p->percentRank($w, $r)})
              ->project(~[
                val_col:p|$p.val_col__pylegend_olap_column__,
                random_col:p|$p.random_col__pylegend_olap_column__
              ])
        '''  
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
                "root"."val_col__pylegend_olap_column__" AS "val_col",
                "root"."random_col__pylegend_olap_column__" AS "random_col"
            FROM
                (
                    SELECT
                        dense_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__pylegend_olap_column__",
                        dense_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col__pylegend_olap_column__"
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
        '''  
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__pylegend_olap_column__:{p,w,r | $p->denseRank($w, $r)})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__pylegend_olap_column__:{p,w,r | $p->denseRank($w, $r)})
              ->project(~[
                val_col:p|$p.val_col__pylegend_olap_column__,
                random_col:p|$p.random_col__pylegend_olap_column__
              ])
        '''  
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
                "root"."val_col__pylegend_olap_column__" AS "val_col",
                "root"."random_col__pylegend_olap_column__" AS "random_col"
            FROM
                (
                    SELECT
                        row_number() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__pylegend_olap_column__",
                        row_number() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col__pylegend_olap_column__"
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
        '''  
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__pylegend_olap_column__:{p,w,r | $p->rowNumber($r)})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__pylegend_olap_column__:{p,w,r | $p->rowNumber($r)})
              ->project(~[
                val_col:p|$p.val_col__pylegend_olap_column__,
                random_col:p|$p.random_col__pylegend_olap_column__
              ])
        '''  
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
                "root"."val_col__pylegend_olap_column__" AS "val_col",
                "root"."random_col__pylegend_olap_column__" AS "random_col"
            FROM
                (
                    SELECT
                        percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col" DESC) AS "val_col__pylegend_olap_column__",
                        percent_rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col" DESC) AS "random_col__pylegend_olap_column__"
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
        '''  
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [descending(~val_col)]), ~val_col__pylegend_olap_column__:{p,w,r | $p->percentRank($w, $r)})
              ->extend(over(~[group_col], [descending(~random_col)]), ~random_col__pylegend_olap_column__:{p,w,r | $p->percentRank($w, $r)})
              ->project(~[
                val_col:p|$p.val_col__pylegend_olap_column__,
                random_col:p|$p.random_col__pylegend_olap_column__
              ])
        '''  
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_groupby_series_full_query_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        series = frame.groupby("group_col")["val_col"].rank()

        expected = '''
            SELECT
                "root"."val_col__pylegend_olap_column__" AS "val_col"
            FROM
                (
                    SELECT
                        rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__pylegend_olap_column__"
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
        '''
        expected = dedent(expected).strip()
        assert series.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[
                val_col:p|$p.val_col__pylegend_olap_column__
              ])
        '''
        expected = dedent(expected).strip()
        assert series.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(series, FrameToPureConfig(), self.legend_client) == expected

        series += 5
        expected = '''
            SELECT
                ("root"."val_col__pylegend_olap_column__" + 5) AS "val_col"
            FROM
                (
                    SELECT
                        rank() OVER (PARTITION BY "root".group_col ORDER BY "root".val_col) AS "val_col__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert series.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[val_col:c|(toOne($c.val_col__pylegend_olap_column__) + 5)])
        '''
        expected = dedent(expected).strip()
        assert series.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(series, FrameToPureConfig(), self.legend_client) == expected

    def test_groupby_rank_with_assign(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame["val_col_rank"] = \
            frame.groupby("group_col")["val_col"].rank(pct=True, ascending=False) + 5  

        expected = '''
            SELECT
                "root"."group_col" AS "group_col",
                "root"."val_col" AS "val_col",
                "root"."random_col" AS "random_col",
                "root"."val_col_rank__pylegend_olap_column__" AS "val_col_rank"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col",
                        (percent_rank() OVER (PARTITION BY "root".group_col ORDER BY "root".val_col DESC) + 5) AS "val_col_rank__pylegend_olap_column__"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"
        '''  
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [descending(~val_col)]), ~val_col__pylegend_olap_column__:{p,w,r | $p->percentRank($w, $r)})
              ->project(~[group_col:c|$c.group_col, val_col:c|$c.val_col, random_col:c|$c.random_col, val_col_rank:c|(toOne($c.val_col__pylegend_olap_column__) + 5)])
        '''  
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

        frame["random_col"] = frame.groupby("group_col")["random_col"].rank().rem(2)  
        expected = '''
            SELECT
                "root"."group_col" AS "group_col",
                "root"."val_col" AS "val_col",
                "root"."random_col__pylegend_olap_column__" AS "random_col",
                "root"."val_col_rank" AS "val_col_rank"
            FROM
                (
                    SELECT
                        "root"."group_col" AS "group_col",
                        "root"."val_col" AS "val_col",
                        MOD(rank() OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col"), 2) AS "random_col__pylegend_olap_column__",
                        "root"."val_col_rank__pylegend_olap_column__" AS "val_col_rank"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
                                (percent_rank() OVER (PARTITION BY "root".group_col ORDER BY "root".val_col DESC) + 5) AS "val_col_rank__pylegend_olap_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [descending(~val_col)]), ~val_col__pylegend_olap_column__:{p,w,r | $p->percentRank($w, $r)})
              ->project(~[group_col:c|$c.group_col, val_col:c|$c.val_col, random_col:c|$c.random_col, val_col_rank:c|(toOne($c.val_col__pylegend_olap_column__) + 5)])
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col__pylegend_olap_column__:{p,w,r | $p->rank($w, $r)})
              ->project(~[group_col:c|$c.group_col, val_col:c|$c.val_col, random_col:c|toOne($c.random_col__pylegend_olap_column__)->rem(2), val_col_rank:c|$c.val_col_rank])
        '''  
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected


class TestRankFunctionEndtoEnd:
    def test_e2e_rank_no_arguments(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["First Name Rank"] = frame["First Name"].rank(na_option='bottom')  
        frame["Last Name Rank"] = frame["Last Name"].rank(na_option='bottom')  
        frame["Age Rank"] = frame["Age"].rank(na_option='bottom')  
        frame["Firm/Legal Name Rank"] = frame["Firm/Legal Name"].rank(na_option='bottom')  

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
            frame["First Name"].rank(pct=True, ascending=False, na_option='bottom')  
        frame["Last Name Rank"] = \
            frame["Last Name"].rank(pct=True, ascending=False, na_option='bottom')  
        frame["Age Rank"] = \
            frame["Age"].rank(pct=True, ascending=False, na_option='bottom')  
        frame["Firm/Legal Name Rank"] = \
            frame["Firm/Legal Name"].rank(pct=True, ascending=False, na_option='bottom')  

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
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 0.6666666666666666, 0.16666666666666666, 0.16666666666666666, 1.0]},  
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
        ]]  

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

    def test_e2e_series_full_query(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        series = frame.groupby("Firm/Legal Name")["First Name"].rank()

        expected = {
            'columns': ['First Name'],
            'rows': [
                {'values': [4]},  # Firm X, Peter
                {'values': [2]},  # Firm X, John
                {'values': [2]},  # Firm X, John
                {'values': [1]},  # Firm X, Anthony
                {'values': [1]},  # Firm A, Fabrice
                {'values': [1]},  # Firm B, Oliver
                {'values': [1]},  # Firm C, David
            ]
        }
        res = series.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        series = frame["First Name"].rank()  
        expected = {
            'columns': ['First Name'],
            'rows': [
                {'values': [7]},  # Peter
                {'values': [4]},  # John
                {'values': [4]},  # John
                {'values': [1]},  # Anthony
                {'values': [3]},  # Fabrice
                {'values': [6]},  # Oliver
                {'values': [2]},  # David
            ]
        }
        res = series.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_series_assign(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame["First Name"] = frame.groupby("Firm/Legal Name")["First Name"].rank()
        expected = {
            'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
            'rows': [
                {'values': [4, 'Smith', 23, 'Firm X']},    # Firm X, Peter
                {'values': [2, 'Johnson', 22, 'Firm X']},  # Firm X, John
                {'values': [2, 'Hill', 12, 'Firm X']},     # Firm X, John
                {'values': [1, 'Allen', 22, 'Firm X']},    # Firm X, Anthony
                {'values': [1, 'Roberts', 34, 'Firm A']},  # Firm A, Fabrice
                {'values': [1, 'Hill', 32, 'Firm B']},     # Firm B, Oliver
                {'values': [1, 'Harris', 35, 'Firm C']}    # Firm C, David
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        frame["Rank Last Name"] = frame["Last Name"].rank()  
        expected = {
            'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name', 'Rank Last Name'],
            'rows': [
                {'values': [4, 'Smith', 23, 'Firm X', 7]},
                {'values': [2, 'Johnson', 22, 'Firm X', 5]},
                {'values': [2, 'Hill', 12, 'Firm X', 3]},
                {'values': [1, 'Allen', 22, 'Firm X', 1]},
                {'values': [1, 'Roberts', 34, 'Firm A', 6]},
                {'values': [1, 'Hill', 32, 'Firm B', 3]},
                {'values': [1, 'Harris', 35, 'Firm C', 2]}
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    @pytest.mark.skip(reason="window functions not currently supported within function call")
    def test_e2e_arithmetic_with_series(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:  # pragma: no cover
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        series = frame["First Name"].rank() - 1  
        expected = {
            'columns': ['First Name'],
            'rows': [
                {'values': [6]},  # Peter
                {'values': [3]},  # John
                {'values': [3]},  # John
                {'values': [0]},  # Anthony
                {'values': [2]},  # Fabrice
                {'values': [5]},  # Oliver
                {'values': [1]},  # David
            ]
        }
        res = series.execute_frame_to_string()  
        assert json.loads(res)["result"] == expected

        frame["First Name"] = frame.groupby("Firm/Legal Name")["First Name"].rank() - 1  
        expected = {
            'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
            'rows': [
                {'values': [3, 'Smith', 23, 'Firm X']},    # Firm X, Peter
                {'values': [1, 'Johnson', 22, 'Firm X']},  # Firm X, John
                {'values': [1, 'Hill', 12, 'Firm X']},     # Firm X, John
                {'values': [0, 'Allen', 22, 'Firm X']},    # Firm X, Anthony
                {'values': [0, 'Roberts', 34, 'Firm A']},  # Firm A, Fabrice
                {'values': [0, 'Hill', 32, 'Firm B']},     # Firm B, Oliver
                {'values': [0, 'Harris', 35, 'Firm C']}    # Firm C, David
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected


# ═══════════════════════════════════════════════════════════════════════
# CUME_DIST e2e tests
# ═══════════════════════════════════════════════════════════════════════


class TestCumeDistEndToEnd:
    def test_e2e_cume_dist_on_frame(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.cume_dist_legend_ext()

        # Data (7 rows): First Name, Last Name, Age, Firm/Legal Name
        # Rows: Peter/Smith/23/FirmX, John/Johnson/22/FirmX, John/Hill/12/FirmX,
        #        Anthony/Allen/22/FirmX, Fabrice/Roberts/34/FirmA, Oliver/Hill/32/FirmB,
        #        David/Harris/35/FirmC
        #
        # cume_dist = (rows with value <= current) / total_rows
        #
        # Age ascending: 12→1/7, 22→3/7, 23→4/7, 32→5/7, 34→6/7, 35→7/7
        # First Name ascending: Anthony→1/7, David→2/7, Fabrice→3/7, John→5/7, Oliver→6/7, Peter→7/7
        # Last Name ascending: Allen→1/7, Harris→2/7, Hill→4/7, Johnson→5/7, Roberts→6/7, Smith→7/7
        # Firm ascending: FirmA→1/7, FirmB→2/7, FirmC→3/7, FirmX→7/7
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": [1.0, 1.0, 4.0 / 7, 1.0]},                     # Peter, Smith, 23, Firm X
                {"values": [5.0 / 7, 5.0 / 7, 3.0 / 7, 1.0]},            # John, Johnson, 22, Firm X
                {"values": [5.0 / 7, 4.0 / 7, 1.0 / 7, 1.0]},            # John, Hill, 12, Firm X
                {"values": [1.0 / 7, 1.0 / 7, 3.0 / 7, 1.0]},            # Anthony, Allen, 22, Firm X
                {"values": [3.0 / 7, 6.0 / 7, 6.0 / 7, 1.0 / 7]},        # Fabrice, Roberts, 34, Firm A
                {"values": [6.0 / 7, 4.0 / 7, 5.0 / 7, 2.0 / 7]},        # Oliver, Hill, 32, Firm B
                {"values": [2.0 / 7, 2.0 / 7, 1.0, 3.0 / 7]},            # David, Harris, 35, Firm C
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_cume_dist_series_assign(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age CumeDist"] = frame["Age"].cume_dist_legend_ext()  

        # Age ascending: 12→1/7, 22→3/7, 23→4/7, 32→5/7, 34→6/7, 35→7/7
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age CumeDist"],
            "rows": [
                {"values": ['Peter', 'Smith', 23, 'Firm X', 4.0 / 7]},
                {"values": ['John', 'Johnson', 22, 'Firm X', 3.0 / 7]},
                {"values": ['John', 'Hill', 12, 'Firm X', 1.0 / 7]},
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 3.0 / 7]},
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 6.0 / 7]},
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 5.0 / 7]},
                {"values": ['David', 'Harris', 35, 'Firm C', 1.0]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_groupby_cume_dist(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age CumeDist"] = frame.groupby("Firm/Legal Name")["Age"].cume_dist_legend_ext()

        # Firm X ages: 12, 22, 22, 23  (4 rows)
        #   12 → 1/4=0.25, 22 → 3/4=0.75, 23 → 4/4=1.0
        # Firm A ages: 34 (1 row) → 1.0
        # Firm B ages: 32 (1 row) → 1.0
        # Firm C ages: 35 (1 row) → 1.0
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age CumeDist"],
            "rows": [
                {"values": ['Peter', 'Smith', 23, 'Firm X', 1.0]},
                {"values": ['John', 'Johnson', 22, 'Firm X', 0.75]},
                {"values": ['John', 'Hill', 12, 'Firm X', 0.25]},
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 0.75]},
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 1.0]},
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 1.0]},
                {"values": ['David', 'Harris', 35, 'Firm C', 1.0]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected


# ═══════════════════════════════════════════════════════════════════════
# NTILE e2e tests
# ═══════════════════════════════════════════════════════════════════════


class TestNtileEndToEnd:
    def test_e2e_ntile_on_frame(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age Ntile"] = frame["Age"].ntile_legend_ext(num_buckets=2)  

        # Age ascending (7 rows, 2 buckets):
        # Sorted: 12, 22, 22, 23, 32, 34, 35
        # Bucket 1 (4 rows): 12, 22, 22, 23   → ntile = 1
        # Bucket 2 (3 rows): 32, 34, 35        → ntile = 2
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age Ntile"],
            "rows": [
                {"values": ['Peter', 'Smith', 23, 'Firm X', 1]},       # Age 23 → bucket 1
                {"values": ['John', 'Johnson', 22, 'Firm X', 1]},     # Age 22 → bucket 1
                {"values": ['John', 'Hill', 12, 'Firm X', 1]},        # Age 12 → bucket 1
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 1]},    # Age 22 → bucket 1
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 2]},  # Age 34 → bucket 2
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 2]},      # Age 32 → bucket 2
                {"values": ['David', 'Harris', 35, 'Firm C', 2]},     # Age 35 → bucket 2
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_ntile_three_buckets(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age Ntile"] = frame["Age"].ntile_legend_ext(num_buckets=3)  

        # Age ascending (7 rows, 3 buckets):
        # Sorted: 12, 22, 22, 23, 32, 34, 35
        # Bucket 1 (3 rows): 12, 22, 22  → ntile = 1
        # Bucket 2 (2 rows): 23, 32      → ntile = 2
        # Bucket 3 (2 rows): 34, 35      → ntile = 3
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age Ntile"],
            "rows": [
                {"values": ['Peter', 'Smith', 23, 'Firm X', 2]},       # Age 23 → bucket 2
                {"values": ['John', 'Johnson', 22, 'Firm X', 1]},     # Age 22 → bucket 1
                {"values": ['John', 'Hill', 12, 'Firm X', 1]},        # Age 12 → bucket 1
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 1]},    # Age 22 → bucket 1
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 3]},  # Age 34 → bucket 3
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 2]},      # Age 32 → bucket 2
                {"values": ['David', 'Harris', 35, 'Firm C', 3]},     # Age 35 → bucket 3
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_groupby_ntile(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age Ntile"] = frame.groupby("Firm/Legal Name")["Age"].ntile_legend_ext(num_buckets=2)

        # Firm X ages ascending (4 rows, 2 buckets):
        #   Sorted: 12, 22, 22, 23
        #   Bucket 1 (2 rows): 12, 22       → ntile = 1
        #   Bucket 2 (2 rows): 22, 23       → ntile = 2
        #   (NTILE assigns by row position; with ties the later row gets a higher bucket)
        # Firm A/B/C (1 row each, 2 buckets) → always bucket 1
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age Ntile"],
            "rows": [
                {"values": ['Peter', 'Smith', 23, 'Firm X', 2]},       # Firm X, Age 23 → bucket 2
                {"values": ['John', 'Johnson', 22, 'Firm X', 1]},     # Firm X, Age 22 → bucket 1
                {"values": ['John', 'Hill', 12, 'Firm X', 1]},        # Firm X, Age 12 → bucket 1
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 2]},    # Firm X, Age 22 → bucket 2
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 1]},  # Firm A, Age 34 → bucket 1
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 1]},      # Firm B, Age 32 → bucket 1
                {"values": ['David', 'Harris', 35, 'Firm C', 1]},     # Firm C, Age 35 → bucket 1
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_ntile_descending(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame["Age Ntile"] = frame["Age"].ntile_legend_ext(num_buckets=2, ascending=False)  

        # Age descending (7 rows, 2 buckets):
        # Sorted DESC: 35, 34, 32, 23, 22, 22, 12
        # Bucket 1 (4 rows): 35, 34, 32, 23   → ntile = 1
        # Bucket 2 (3 rows): 22, 22, 12        → ntile = 2
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "Age Ntile"],
            "rows": [
                {"values": ['Peter', 'Smith', 23, 'Firm X', 1]},       # Age 23 → bucket 1
                {"values": ['John', 'Johnson', 22, 'Firm X', 2]},     # Age 22 → bucket 2
                {"values": ['John', 'Hill', 12, 'Firm X', 2]},        # Age 12 → bucket 2
                {"values": ['Anthony', 'Allen', 22, 'Firm X', 2]},    # Age 22 → bucket 2
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A', 1]},  # Age 34 → bucket 1
                {"values": ['Oliver', 'Hill', 32, 'Firm B', 1]},      # Age 32 → bucket 1
                {"values": ['David', 'Harris', 35, 'Firm C', 1]},     # Age 35 → bucket 1
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected


# ═══════════════════════════════════════════════════════════════════════
# CUME_DIST tests
# ═══════════════════════════════════════════════════════════════════════


class TestCumeDistOnBaseFrame:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_cume_dist_simple_sql(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.cume_dist_legend_ext()

        expected = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        cume_dist() OVER (ORDER BY "root"."col1") AS "col1__pylegend_olap_column__"
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

    def test_cume_dist_simple_pure(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.cume_dist_legend_ext()

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->cumulativeDistribution($w, $r)})
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__
              ])
        '''  
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_cume_dist_descending_sql(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.cume_dist_legend_ext(ascending=False)

        expected = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        cume_dist() OVER (ORDER BY "root"."col1" DESC) AS "col1__pylegend_olap_column__"
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

    def test_cume_dist_multiple_cols(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.cume_dist_legend_ext()

        expected = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        cume_dist() OVER (ORDER BY "root"."col1") AS "col1__pylegend_olap_column__",
                        cume_dist() OVER (ORDER BY "root"."col2") AS "col2__pylegend_olap_column__"
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
              ->extend(over([ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->cumulativeDistribution($w, $r)})
              ->extend(over([ascending(~col2)]), ~col2__pylegend_olap_column__:{p,w,r | $p->cumulativeDistribution($w, $r)})
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__,
                col2:p|$p.col2__pylegend_olap_column__
              ])
        '''  
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected


class TestCumeDistOnGroupbyFrame:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_groupby_cume_dist_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("grp").cume_dist_legend_ext()

        expected = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val"
            FROM
                (
                    SELECT
                        cume_dist() OVER (PARTITION BY "root"."grp" ORDER BY "root"."val") AS "val__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test_groupby_cume_dist_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("grp").cume_dist_legend_ext()

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[grp], [ascending(~val)]), ~val__pylegend_olap_column__:{p,w,r | $p->cumulativeDistribution($w, $r)})
              ->project(~[
                val:p|$p.val__pylegend_olap_column__
              ])
        '''  
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_groupby_series_cume_dist(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.integer_column("other"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        result = frame.groupby("grp")["val"].cume_dist_legend_ext()

        expected = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val"
            FROM
                (
                    SELECT
                        cume_dist() OVER (PARTITION BY "root"."grp" ORDER BY "root"."val") AS "val__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".other AS "other"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert result.to_sql_query(FrameToSqlConfig()) == expected


# ═══════════════════════════════════════════════════════════════════════
# NTILE tests
# ═══════════════════════════════════════════════════════════════════════


class TestNtileErrors:

    def test_ntile_invalid_num_buckets_zero(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(ValueError, match="num_buckets"):
            frame.ntile_legend_ext(num_buckets=0)

    def test_ntile_invalid_num_buckets_negative(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(ValueError, match="num_buckets"):
            frame.ntile_legend_ext(num_buckets=-1)


class TestNtileOnBaseFrame:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_ntile_simple_sql(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.ntile_legend_ext(num_buckets=4)

        expected = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        ntile(4) OVER (ORDER BY "root"."col1") AS "col1__pylegend_olap_column__"
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

    def test_ntile_simple_pure(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.ntile_legend_ext(num_buckets=4)

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->ntile($r, 4)})
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__
              ])
        '''
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_ntile_descending_sql(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.ntile_legend_ext(num_buckets=2, ascending=False)

        expected = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        ntile(2) OVER (ORDER BY "root"."col1" DESC) AS "col1__pylegend_olap_column__"
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

    def test_ntile_multiple_cols(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.ntile_legend_ext(num_buckets=3)

        expected = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        ntile(3) OVER (ORDER BY "root"."col1") AS "col1__pylegend_olap_column__",
                        ntile(3) OVER (ORDER BY "root"."col2") AS "col2__pylegend_olap_column__"
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
              ->extend(over([ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->ntile($r, 3)})
              ->extend(over([ascending(~col2)]), ~col2__pylegend_olap_column__:{p,w,r | $p->ntile($r, 3)})
              ->project(~[
                col1:p|$p.col1__pylegend_olap_column__,
                col2:p|$p.col2__pylegend_olap_column__
              ])
        '''
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected


class TestNtileOnGroupbyFrame:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_groupby_ntile_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("grp").ntile_legend_ext(num_buckets=2)

        expected = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val"
            FROM
                (
                    SELECT
                        ntile(2) OVER (PARTITION BY "root"."grp" ORDER BY "root"."val") AS "val__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test_groupby_ntile_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("grp").ntile_legend_ext(num_buckets=2)

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[grp], [ascending(~val)]), ~val__pylegend_olap_column__:{p,w,r | $p->ntile($r, 2)})
              ->project(~[
                val:p|$p.val__pylegend_olap_column__
              ])
        '''
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_groupby_series_ntile(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("grp"),
            PrimitiveTdsColumn.integer_column("val"),
            PrimitiveTdsColumn.integer_column("other"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        result = frame.groupby("grp")["val"].ntile_legend_ext(num_buckets=3)

        expected = '''
            SELECT
                "root"."val__pylegend_olap_column__" AS "val"
            FROM
                (
                    SELECT
                        ntile(3) OVER (PARTITION BY "root"."grp" ORDER BY "root"."val") AS "val__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".grp AS "grp",
                                "root".val AS "val",
                                "root".other AS "other"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''
        expected = dedent(expected).strip()
        assert result.to_sql_query(FrameToSqlConfig()) == expected
