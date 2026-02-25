# Copyright 2025 Goldman Sachs
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

import pandas as pd
import pytest

from pylegend._typing import PyLegendDict, PyLegendUnion
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_relation_person_service_frame_pandas_api


TEST_PURE: bool = False
USE_LEGEND_ENGINE: bool = False


class TestErrorsOnBaseFrame:
    def test_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.shift(axis=1)

        expected_msg = "The 'axis' argument of the shift function must be 0 or 'index', but got: axis=1"
        assert v.value.args[0] == expected_msg

    def test_frequency_not_none(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.shift(freq='D')

        expected_msg = "The 'freq' argument of the shift function is not supported, but got: freq='D'"
        assert v.value.args[0] == expected_msg

    def test_suffix_with_int_periods(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.shift(periods=-1, suffix='abcd')

        expected_msg = "Cannot specify the 'suffix' argument of the shift function if the 'periods' argument is an int."
        assert v.value.args[0] == expected_msg

    def test_fill_value_argument(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("int_col"),
                   PrimitiveTdsColumn.string_column("str_col"),
                   PrimitiveTdsColumn.boolean_column("bool_col"),
                   PrimitiveTdsColumn.date_column("date_col"),
                   PrimitiveTdsColumn.datetime_column("datetime_col"),
                   PrimitiveTdsColumn.strictdate_column("strictdate_col"),
                   PrimitiveTdsColumn.float_column("float_col"),
                   PrimitiveTdsColumn.number_column("num_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.shift(fill_value="default_fill")

        expected_msg = (
            "The 'fill_value' argument of the shift function is not supported, but got: fill_value='default_fill'")
        assert v.value.args[0] == expected_msg

        with pytest.raises(NotImplementedError) as v:
            frame.shift(fill_value=-1)

        expected_msg = (
            "The 'fill_value' argument of the shift function is not supported, but got: fill_value=-1")
        assert v.value.args[0] == expected_msg

    def test_periods_argument_as_unsupported_int(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.shift(periods=0)

        expected_msg = (
            "The 'periods' argument of the shift function is only supported for the values of [1, -1]"
            " or a list of these, but got: periods=0")
        assert v.value.args[0] == expected_msg

    def test_periods_argument_as_unsupported_list(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.shift(periods=[1, -1, 2])

        expected_msg = (
            "The 'periods' argument of the shift function is only supported for the values of [1, -1]"
            " or a list of these, but got: periods=[1, -1, 2]")
        assert v.value.args[0] == expected_msg

    def test_periods_list_with_repitition(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.shift(periods=[1, -1, 1])

        expected_msg = (
            "The 'periods' argument of the shift function cannot contain duplicate values, but got: "
            "periods=[1, -1, 1]")
        assert v.value.args[0] == expected_msg


class TestErrorsOnGroupbyFrame:
    def test_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("group_col").shift(axis=1)

        expected_msg = "The 'axis' argument of the shift function must be 0 or 'index', but got: axis=1"
        assert v.value.args[0] == expected_msg

    def test_frequency_not_none(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("group_col").shift(freq='D')

        expected_msg = "The 'freq' argument of the shift function is not supported, but got: freq='D'"
        assert v.value.args[0] == expected_msg

    def test_fill_value_not_none(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("group_col")["val_col"].shift(fill_value="default_fill")

        expected_msg = (
            "The 'fill_value' argument of the shift function is not supported, but got: fill_value='default_fill'")
        assert v.value.args[0] == expected_msg


class TestUsageOnBaseFrame:
    if USE_LEGEND_ENGINE:
        @pytest.fixture(autouse=True)
        def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
            self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_no_arguments(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift()

        expected = '''
            SELECT
                "root"."col1__INTERNAL_PYLEGEND_COLUMN__" AS "col1"
            FROM
                (
                    SELECT
                        lag("root"."col1", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col1__INTERNAL_PYLEGEND_COLUMN__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                0 AS "__INTERNAL_PYLEGEND_COLUMN__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r|0})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~col1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).col1})
              ->project(~[col1:p|$p.col1__INTERNAL_PYLEGEND_COLUMN__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_periods_argument_multiple_columns(self) -> None:
        columns = [PrimitiveTdsColumn.number_column("col1"), PrimitiveTdsColumn.float_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=1)

        expected = '''
            SELECT
                "root"."col1__INTERNAL_PYLEGEND_COLUMN__" AS "col1",
                "root"."col2__INTERNAL_PYLEGEND_COLUMN__" AS "col2"
            FROM
                (
                    SELECT
                        lag("root"."col1", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col1__INTERNAL_PYLEGEND_COLUMN__",
                        lag("root"."col2", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col2__INTERNAL_PYLEGEND_COLUMN__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__INTERNAL_PYLEGEND_COLUMN__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r|0})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~col1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).col1})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~col2__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).col2})
              ->project(~[col1:p|$p.col1__INTERNAL_PYLEGEND_COLUMN__, col2:p|$p.col2__INTERNAL_PYLEGEND_COLUMN__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_negative_periods_argument(self) -> None:
        columns = [PrimitiveTdsColumn.date_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=-1)

        expected = '''
            SELECT
                "root"."col1__INTERNAL_PYLEGEND_COLUMN__" AS "col1"
            FROM
                (
                    SELECT
                        lead("root"."col1", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col1__INTERNAL_PYLEGEND_COLUMN__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                0 AS "__INTERNAL_PYLEGEND_COLUMN__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r|0})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~col1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lead($r).col1})
              ->project(~[col1:p|$p.col1__INTERNAL_PYLEGEND_COLUMN__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_list_periods_no_suffix(self) -> None:
        columns = [PrimitiveTdsColumn.strictdate_column("col1"), PrimitiveTdsColumn.datetime_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=[1, -1])

        expected = '''
            SELECT
                "root"."col1_1__INTERNAL_PYLEGEND_COLUMN__" AS "col1_1",
                "root"."col2_1__INTERNAL_PYLEGEND_COLUMN__" AS "col2_1",
                "root"."col1_-1__INTERNAL_PYLEGEND_COLUMN__" AS "col1_-1",
                "root"."col2_-1__INTERNAL_PYLEGEND_COLUMN__" AS "col2_-1"
            FROM
                (
                    SELECT
                        lag("root"."col1", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col1_1__INTERNAL_PYLEGEND_COLUMN__",
                        lag("root"."col2", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col2_1__INTERNAL_PYLEGEND_COLUMN__",
                        lead("root"."col1", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col1_-1__INTERNAL_PYLEGEND_COLUMN__",
                        lead("root"."col2", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col2_-1__INTERNAL_PYLEGEND_COLUMN__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__INTERNAL_PYLEGEND_COLUMN__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r|0})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~col1_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).col1})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~col2_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).col2})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~'col1_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r).col1})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~'col2_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r).col2})
              ->project(~[col1_1:p|$p.col1_1__INTERNAL_PYLEGEND_COLUMN__, col2_1:p|$p.col2_1__INTERNAL_PYLEGEND_COLUMN__, 'col1_-1':p|$p.'col1_-1__INTERNAL_PYLEGEND_COLUMN__', 'col2_-1':p|$p.'col2_-1__INTERNAL_PYLEGEND_COLUMN__'])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_list_periods_with_suffix(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"), PrimitiveTdsColumn.integer_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=[-1, 1], suffix="_custom_suffix")

        expected = '''
            SELECT
                "root"."col1_custom_suffix_-1__INTERNAL_PYLEGEND_COLUMN__" AS "col1_custom_suffix_-1",
                "root"."col2_custom_suffix_-1__INTERNAL_PYLEGEND_COLUMN__" AS "col2_custom_suffix_-1",
                "root"."col1_custom_suffix_1__INTERNAL_PYLEGEND_COLUMN__" AS "col1_custom_suffix_1",
                "root"."col2_custom_suffix_1__INTERNAL_PYLEGEND_COLUMN__" AS "col2_custom_suffix_1"
            FROM
                (
                    SELECT
                        lead("root"."col1", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col1_custom_suffix_-1__INTERNAL_PYLEGEND_COLUMN__",
                        lead("root"."col2", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col2_custom_suffix_-1__INTERNAL_PYLEGEND_COLUMN__",
                        lag("root"."col1", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col1_custom_suffix_1__INTERNAL_PYLEGEND_COLUMN__",
                        lag("root"."col2", 1) OVER (ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "col2_custom_suffix_1__INTERNAL_PYLEGEND_COLUMN__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__INTERNAL_PYLEGEND_COLUMN__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r|0})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~'col1_custom_suffix_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r).col1})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~'col2_custom_suffix_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r).col2})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~col1_custom_suffix_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).col1})
              ->extend(over([ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~col2_custom_suffix_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).col2})
              ->project(~['col1_custom_suffix_-1':p|$p.'col1_custom_suffix_-1__INTERNAL_PYLEGEND_COLUMN__', 'col2_custom_suffix_-1':p|$p.'col2_custom_suffix_-1__INTERNAL_PYLEGEND_COLUMN__', col1_custom_suffix_1:p|$p.col1_custom_suffix_1__INTERNAL_PYLEGEND_COLUMN__, col2_custom_suffix_1:p|$p.col2_custom_suffix_1__INTERNAL_PYLEGEND_COLUMN__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected


class TestUsageOnGroupbyFrame:
    if USE_LEGEND_ENGINE:
        @pytest.fixture(autouse=True)
        def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
            self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_no_selection(self) -> None:
        columns = columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col").shift(1)

        expected = '''
            SELECT
                "root"."val_col__INTERNAL_PYLEGEND_COLUMN__" AS "val_col",
                "root"."random_col__INTERNAL_PYLEGEND_COLUMN__" AS "random_col"
            FROM
                (
                    SELECT
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "val_col__INTERNAL_PYLEGEND_COLUMN__",
                        lag("root"."random_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "random_col__INTERNAL_PYLEGEND_COLUMN__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
                                0 AS "__INTERNAL_PYLEGEND_COLUMN__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r|0})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~val_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).val_col})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~random_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).random_col})
              ->project(~[val_col:p|$p.val_col__INTERNAL_PYLEGEND_COLUMN__, random_col:p|$p.random_col__INTERNAL_PYLEGEND_COLUMN__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_single_selection(self) -> None:
        columns = columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")[["val_col"]].shift(1)

        expected = '''
            SELECT
                "root"."val_col__INTERNAL_PYLEGEND_COLUMN__" AS "val_col"
            FROM
                (
                    SELECT
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "val_col__INTERNAL_PYLEGEND_COLUMN__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
                                0 AS "__INTERNAL_PYLEGEND_COLUMN__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r|0})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~val_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).val_col})
              ->project(~[val_col:p|$p.val_col__INTERNAL_PYLEGEND_COLUMN__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_selection_same_as_groupby(self) -> None:
        columns = columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")[["group_col"]].shift(1)

        expected = '''
            SELECT
                "root"."group_col__INTERNAL_PYLEGEND_COLUMN__" AS "group_col"
            FROM
                (
                    SELECT
                        lag("root"."group_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "group_col__INTERNAL_PYLEGEND_COLUMN__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
                                0 AS "__INTERNAL_PYLEGEND_COLUMN__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r|0})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~group_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).group_col})
              ->project(~[group_col:p|$p.group_col__INTERNAL_PYLEGEND_COLUMN__])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_multiple_periods(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col"),
            PrimitiveTdsColumn.float_column("random_col_2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")[["val_col", "random_col"]].shift([1, -1])

        expected = '''
            SELECT
                "root"."val_col_1__INTERNAL_PYLEGEND_COLUMN__" AS "val_col_1",
                "root"."random_col_1__INTERNAL_PYLEGEND_COLUMN__" AS "random_col_1",
                "root"."val_col_-1__INTERNAL_PYLEGEND_COLUMN__" AS "val_col_-1",
                "root"."random_col_-1__INTERNAL_PYLEGEND_COLUMN__" AS "random_col_-1"
            FROM
                (
                    SELECT
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "val_col_1__INTERNAL_PYLEGEND_COLUMN__",
                        lag("root"."random_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "random_col_1__INTERNAL_PYLEGEND_COLUMN__",
                        lead("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "val_col_-1__INTERNAL_PYLEGEND_COLUMN__",
                        lead("root"."random_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "random_col_-1__INTERNAL_PYLEGEND_COLUMN__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
                                "root".random_col_2 AS "random_col_2",
                                0 AS "__INTERNAL_PYLEGEND_COLUMN__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r|0})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~val_col_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).val_col})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~random_col_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).random_col})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~'val_col_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r).val_col})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~'random_col_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r).random_col})
              ->project(~[val_col_1:p|$p.val_col_1__INTERNAL_PYLEGEND_COLUMN__, random_col_1:p|$p.random_col_1__INTERNAL_PYLEGEND_COLUMN__, 'val_col_-1':p|$p.'val_col_-1__INTERNAL_PYLEGEND_COLUMN__', 'random_col_-1':p|$p.'random_col_-1__INTERNAL_PYLEGEND_COLUMN__'])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_suffix(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col"),
            PrimitiveTdsColumn.float_column("random_col_2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")[["val_col", "random_col"]].shift([1, -1], suffix="_sfx")

        expected = '''
            SELECT
                "root"."val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__" AS "val_col_sfx_1",
                "root"."random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__" AS "random_col_sfx_1",
                "root"."val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__" AS "val_col_sfx_-1",
                "root"."random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__" AS "random_col_sfx_-1"
            FROM
                (
                    SELECT
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__",
                        lag("root"."random_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__",
                        lead("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__",
                        lead("root"."random_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
                                "root".random_col_2 AS "random_col_2",
                                0 AS "__INTERNAL_PYLEGEND_COLUMN__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r|0})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).val_col})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).random_col})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~'val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r).val_col})
              ->extend(over(~[group_col], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~'random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r).random_col})
              ->project(~[val_col_sfx_1:p|$p.val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__, random_col_sfx_1:p|$p.random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__, 'val_col_sfx_-1':p|$p.'val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__', 'random_col_sfx_-1':p|$p.'random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__'])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_multiple_grouping(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.string_column("group_col_2"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col"),
            PrimitiveTdsColumn.float_column("random_col_2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = (
            frame.groupby(["group_col", "group_col_2"])[["group_col", "val_col", "random_col"]]
            .shift([1, -1], suffix="_sfx"))

        expected = '''
            SELECT
                "root"."group_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__" AS "group_col_sfx_1",
                "root"."val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__" AS "val_col_sfx_1",
                "root"."random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__" AS "random_col_sfx_1",
                "root"."group_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__" AS "group_col_sfx_-1",
                "root"."val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__" AS "val_col_sfx_-1",
                "root"."random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__" AS "random_col_sfx_-1"
            FROM
                (
                    SELECT
                        lag("root"."group_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "group_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__",
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__",
                        lag("root"."random_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__",
                        lead("root"."group_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "group_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__",
                        lead("root"."val_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__",
                        lead("root"."random_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."__INTERNAL_PYLEGEND_COLUMN__") AS "random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".group_col_2 AS "group_col_2",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
                                "root".random_col_2 AS "random_col_2",
                                0 AS "__INTERNAL_PYLEGEND_COLUMN__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r|0})
              ->extend(over(~[group_col, group_col_2], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~group_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).group_col})
              ->extend(over(~[group_col, group_col_2], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).val_col})
              ->extend(over(~[group_col, group_col_2], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r).random_col})
              ->extend(over(~[group_col, group_col_2], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~'group_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r).group_col})
              ->extend(over(~[group_col, group_col_2], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~'val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r).val_col})
              ->extend(over(~[group_col, group_col_2], [ascending(~__INTERNAL_PYLEGEND_COLUMN__)]), ~'random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r).random_col})
              ->project(~[group_col_sfx_1:p|$p.group_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__, val_col_sfx_1:p|$p.val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__, random_col_sfx_1:p|$p.random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__, 'group_col_sfx_-1':p|$p.'group_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__', 'val_col_sfx_-1':p|$p.'val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__', 'random_col_sfx_-1':p|$p.'random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__'])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected


@pytest.fixture(scope="class")
def pandas_df_simple_person() -> pd.DataFrame:
    rows = [
        {"values": ["Peter", "Smith", 23, "Firm X"]},
        {"values": ["John", "Johnson", 22, "Firm X"]},
        {"values": ["John", "Hill", 12, "Firm X"]},
        {"values": ["Anthony", "Allen", 22, "Firm X"]},
        {"values": ["Fabrice", "Roberts", 34, "Firm A"]},
        {"values": ["Oliver", "Hill", 32, "Firm B"]},
        {"values": ["David", "Harris", 35, "Firm C"]},
    ]

    return pd.DataFrame(
        [row["values"] for row in rows],
        columns=["First Name", "Last Name", "Age", "Firm/Legal Name"],
    )


def assert_frame_equal(left: pd.DataFrame, right: pd.DataFrame) -> None:
    # Ensure both DataFrames have the exact same columns
    missing_in_right = set(left.columns) - set(right.columns)
    missing_in_left = set(right.columns) - set(left.columns)
    assert not missing_in_right, f"Right DataFrame is missing columns: {missing_in_right}"
    assert not missing_in_left, f"Left DataFrame is missing columns: {missing_in_left}"

    left = left[right.columns]  # To align columns
    left = left.reset_index(drop=True)
    right = right.reset_index(drop=True)

    # This standardizes np.nan and pd.NA
    left = left.convert_dtypes()
    right = right.convert_dtypes()

    pd.testing.assert_frame_equal(
        left,
        right,
        check_dtype=False,             # Ignores type differences (Int64 vs Float64)
        check_exact=False,             # Allows for minor precision differences in floats
        check_categorical=False,       # Ignores categorical vs string differences
        check_datetimelike_compat=True # Treats datetime objects and matching strings as equal
    )


class TestEndToEndUsageOnBaseFrame:

    def test_no_arguments(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_frame = frame.shift()
        pandas_frame = pandas_df_simple_person.shift()

        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": [None, None, None, None]},
                {"values": ['Peter', 'Smith', 23, 'Firm X']},
                {"values": ['John', 'Johnson', 22, 'Firm X']},
                {"values": ['John', 'Hill', 12, 'Firm X']},
                {"values": ['Anthony', 'Allen', 22, 'Firm X']},
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A']},
                {"values": ['Oliver', 'Hill', 32, 'Firm B']},
            ]
        }
        res = pylegend_frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        assert_frame_equal(pylegend_frame.execute_frame_to_pandas_df(), pandas_frame)

    def test_negative_periods(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_frame = frame.shift(periods=-1)
        pandas_frame = pandas_df_simple_person.shift(periods=-1)

        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ['John', 'Johnson', 22, 'Firm X']},
                {"values": ['John', 'Hill', 12, 'Firm X']},
                {"values": ['Anthony', 'Allen', 22, 'Firm X']},
                {"values": ['Fabrice', 'Roberts', 34, 'Firm A']},
                {"values": ['Oliver', 'Hill', 32, 'Firm B']},
                {'values': ['David', 'Harris', 35, 'Firm C']},
                {"values": [None, None, None, None]},
            ]
        }
        res = pylegend_frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        assert_frame_equal(pylegend_frame.execute_frame_to_pandas_df(), pandas_frame)

    def test_list_periods(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_frame = frame.shift(periods=[1, -1])
        pandas_frame = pandas_df_simple_person.shift(periods=[1, -1])

        expected = {
            'columns': ['First Name_1', 'Last Name_1', 'Age_1', 'Firm/Legal Name_1',
                        'First Name_-1', 'Last Name_-1', 'Age_-1', 'Firm/Legal Name_-1'],
            'rows': [
                {'values': [None,      None,      None, None,     'John',    'Johnson', 22,   'Firm X']},
                {'values': ['Peter',   'Smith',   23,   'Firm X', 'John',    'Hill',    12,   'Firm X']},
                {'values': ['John',    'Johnson', 22,   'Firm X', 'Anthony', 'Allen',   22,   'Firm X']},
                {'values': ['John',    'Hill',    12,   'Firm X', 'Fabrice', 'Roberts', 34,   'Firm A']},
                {'values': ['Anthony', 'Allen',   22,   'Firm X', 'Oliver',  'Hill',    32,   'Firm B']},
                {'values': ['Fabrice', 'Roberts', 34,   'Firm A', 'David',   'Harris',  35,   'Firm C']},
                {'values': ['Oliver',  'Hill',    32,   'Firm B', None,      None,      None, None    ]}
            ]
        }
        res = pylegend_frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        assert_frame_equal(pylegend_frame.execute_frame_to_pandas_df(), pandas_frame)

    def test_list_periods_with_suffix(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_frame = frame.shift(periods=[1, -1], suffix="_shifted")
        pandas_frame = pandas_df_simple_person.shift(periods=[1, -1], suffix="_shifted")  # type: ignore[call-arg]

        expected = {
            'columns': ['First Name_shifted_1', 'Last Name_shifted_1', 'Age_shifted_1', 'Firm/Legal Name_shifted_1',
                        'First Name_shifted_-1', 'Last Name_shifted_-1', 'Age_shifted_-1', 'Firm/Legal Name_shifted_-1'],
            'rows': [
                {'values': [None,      None,      None, None,     'John',    'Johnson', 22,   'Firm X']},
                {'values': ['Peter',   'Smith',   23,   'Firm X', 'John',    'Hill',    12,   'Firm X']},
                {'values': ['John',    'Johnson', 22,   'Firm X', 'Anthony', 'Allen',   22,   'Firm X']},
                {'values': ['John',    'Hill',    12,   'Firm X', 'Fabrice', 'Roberts', 34,   'Firm A']},
                {'values': ['Anthony', 'Allen',   22,   'Firm X', 'Oliver',  'Hill',    32,   'Firm B']},
                {'values': ['Fabrice', 'Roberts', 34,   'Firm A', 'David',   'Harris',  35,   'Firm C']},
                {'values': ['Oliver',  'Hill',    32,   'Firm B', None,      None,      None, None    ]}
            ]
        }
        res = pylegend_frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        assert_frame_equal(pylegend_frame.execute_frame_to_pandas_df(), pandas_frame)


class TestEndToEndUsageOnGroupbyFrame:

    def test_no_arguments(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_frame = frame.groupby("Firm/Legal Name").shift()
        pandas_frame = pandas_df_simple_person.groupby("Firm/Legal Name").shift()

        expected = {
            'columns': ['First Name', 'Last Name', 'Age'],
            'rows': [
                {'values': [None,    None,      None]},  # Firm X
                {'values': ['Peter', 'Smith',   23  ]},  # Firm X
                {'values': ['John',  'Johnson', 22  ]},  # Firm X
                {'values': ['John',  'Hill',    12  ]},  # Firm X
                {'values': [None,    None,      None]},  # Firm A
                {'values': [None,    None,      None]},  # Firm B
                {'values': [None,    None,      None]},  # Firm C
            ]
        }
        res = pylegend_frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        assert_frame_equal(pylegend_frame.execute_frame_to_pandas_df(), pandas_frame)

    def test_negative_periods_with_selection(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_frame = frame.groupby("Firm/Legal Name")[["First Name", "Last Name"]].shift(-1)
        pandas_frame = pandas_df_simple_person.groupby("Firm/Legal Name")[["First Name", "Last Name"]].shift(-1)

        expected = {
            'columns': ['First Name', 'Last Name'],
            'rows': [
                {'values': ['John',    'Johnson']},  # Firm X
                {'values': ['John',    'Hill'   ]},  # Firm X
                {'values': ['Anthony', 'Allen'  ]},  # Firm X
                {'values': [None,      None     ]},  # Firm X
                {'values': [None,      None     ]},  # Firm A
                {'values': [None,      None     ]},  # Firm B
                {'values': [None,      None     ]},  # Firm C
            ]
        }
        res = pylegend_frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        assert_frame_equal(pylegend_frame.execute_frame_to_pandas_df(), pandas_frame)

    def test_list_periods_with_groupby_column_selected(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_frame = frame.groupby("Firm/Legal Name")[["Firm/Legal Name"]].shift([1, -1])
        pandas_frame = pandas_df_simple_person.groupby("Firm/Legal Name")[["Firm/Legal Name"]].shift([1, -1])

        expected = {
            'columns': ['Firm/Legal Name_1', 'Firm/Legal Name_-1'],
            'rows': [
                {'values': [None,     'Firm X']},  # Firm X
                {'values': ['Firm X', 'Firm X']},  # Firm X
                {'values': ['Firm X', 'Firm X']},  # Firm X
                {'values': ['Firm X', None    ]},  # Firm X
                {'values': [None,     None    ]},  # Firm A
                {'values': [None,     None    ]},  # Firm B
                {'values': [None,     None    ]},  # Firm C
            ]
        }
        res = pylegend_frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        assert_frame_equal(pylegend_frame.execute_frame_to_pandas_df(), pandas_frame)

    def test_list_periods_with_multiple_groupby_and_suffix(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_frame = frame.groupby("Firm/Legal Name")[["Firm/Legal Name", "First Name"]].shift([1, -1], suffix="_shifted")
        pandas_frame = (
            pandas_df_simple_person.groupby("Firm/Legal Name")[["Firm/Legal Name", "First Name"]]
            .shift([1, -1], suffix="_shifted")
        )

        expected = {
            'columns': ['First Name_shifted_1', 'Firm/Legal Name_shifted_1',
                        'First Name_shifted_-1', 'Firm/Legal Name_shifted_-1'],
            'rows': [
                {'values': [None,    None,     'John',    'Firm X']},  # Firm X
                {'values': ['Peter', 'Firm X', 'John',    'Firm X']},  # Firm X
                {'values': ['John',  'Firm X', 'Anthony', 'Firm X']},  # Firm X
                {'values': ['John',  'Firm X', None,       None   ]},  # Firm X
                {'values': [None,    None,     None,       None   ]},  # Firm A
                {'values': [None,    None,     None,       None   ]},  # Firm B
                {'values': [None,    None,     None,       None   ]},  # Firm C
            ]
        }
        res = pylegend_frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        assert_frame_equal(pylegend_frame.execute_frame_to_pandas_df(), pandas_frame)
