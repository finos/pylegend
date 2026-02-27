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

from textwrap import dedent
import pytest
from pylegend._typing import PyLegendDict, PyLegendUnion
from pylegend.core.language.pandas_api.pandas_api_groupby_series import GroupbySeries
from pylegend.core.language.pandas_api.pandas_api_series import Series
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile


TEST_PURE: bool = True
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

    def test_periods_list_with_repitition(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.shift(periods=[1, -1, 1])

        expected_msg = (
            "The 'periods' argument of the shift function cannot contain duplicate values, but got: "
            "periods=[1, -1, 1]")
        assert v.value.args[0] == expected_msg

    def test_to_sql(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.shift().to_sql_query()

        expected_msg = "SQL query execution is not supported for the shift function"
        assert v.value.args[0] == expected_msg

    def test_sql_assign(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame["new_col"] = frame["col1"].shift().to_sql_query()

        expected_msg = "SQL query execution is not supported for the shift function"
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
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_no_arguments(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift()

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).col1})
              ->project(~[
                col1:p|$p.col1__INTERNAL_PYLEGEND_COLUMN__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_periods_argument_multiple_columns(self) -> None:
        columns = [PrimitiveTdsColumn.number_column("col1"), PrimitiveTdsColumn.float_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=1)

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).col1})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col2__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).col2})
              ->project(~[
                col1:p|$p.col1__INTERNAL_PYLEGEND_COLUMN__,
                col2:p|$p.col2__INTERNAL_PYLEGEND_COLUMN__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_negative_periods_argument(self) -> None:
        columns = [PrimitiveTdsColumn.date_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=-3)

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lead($r, 3).col1})
              ->project(~[
                col1:p|$p.col1__INTERNAL_PYLEGEND_COLUMN__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_list_periods_no_suffix(self) -> None:
        columns = [PrimitiveTdsColumn.strictdate_column("col1"), PrimitiveTdsColumn.datetime_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=[3, -2])

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col1_3__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 3).col1})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col2_3__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 3).col2})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~'col1_-2__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 2).col1})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~'col2_-2__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 2).col2})
              ->project(~[
                col1_3:p|$p.col1_3__INTERNAL_PYLEGEND_COLUMN__,
                col2_3:p|$p.col2_3__INTERNAL_PYLEGEND_COLUMN__,
                'col1_-2':p|$p.'col1_-2__INTERNAL_PYLEGEND_COLUMN__',
                'col2_-2':p|$p.'col2_-2__INTERNAL_PYLEGEND_COLUMN__'
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_list_periods_with_suffix(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"), PrimitiveTdsColumn.integer_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=[-5, 10], suffix="_suffix")

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~'col1_suffix_-5__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 5).col1})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~'col2_suffix_-5__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 5).col2})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col1_suffix_10__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 10).col1})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col2_suffix_10__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 10).col2})
              ->project(~[
                'col1_suffix_-5':p|$p.'col1_suffix_-5__INTERNAL_PYLEGEND_COLUMN__',
                'col2_suffix_-5':p|$p.'col2_suffix_-5__INTERNAL_PYLEGEND_COLUMN__',
                col1_suffix_10:p|$p.col1_suffix_10__INTERNAL_PYLEGEND_COLUMN__,
                col2_suffix_10:p|$p.col2_suffix_10__INTERNAL_PYLEGEND_COLUMN__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_series_datatype_conversion_and_full_query_generation(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.integer_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame1 = frame["col1"].shift(2)
        assert isinstance(frame1, Series)
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->select(~[col1])
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 2).col1})
              ->project(~[
                col1:p|$p.col1__INTERNAL_PYLEGEND_COLUMN__
              ])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame1.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame1, FrameToPureConfig(), self.legend_client) == expected_pure

        frame1 += 5  # type: ignore[operator, assignment]
        assert isinstance(frame1, Series)
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 2).col1})
              ->project(~[col1:c|(toOne($c.col1__INTERNAL_PYLEGEND_COLUMN__) + 5)])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame1.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame1, FrameToPureConfig(), self.legend_client) == expected_pure

        frame2 = frame["col1"].shift([1])
        assert isinstance(frame2, PandasApiTdsFrame)
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->select(~[col1])
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col1_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).col1})
              ->project(~[
                col1_1:p|$p.col1_1__INTERNAL_PYLEGEND_COLUMN__
              ])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame2.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame2, FrameToPureConfig(), self.legend_client) == expected_pure

        frame3 = frame["col1"].shift([5, -3])
        assert isinstance(frame3, PandasApiTdsFrame)
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->select(~[col1])
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col1_5__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 5).col1})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~'col1_-3__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 3).col1})
              ->project(~[
                col1_5:p|$p.col1_5__INTERNAL_PYLEGEND_COLUMN__,
                'col1_-3':p|$p.'col1_-3__INTERNAL_PYLEGEND_COLUMN__'
              ])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame3.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame3, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_series_assign(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"), PrimitiveTdsColumn.integer_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame["shifted_col1"] = frame["col1"].shift(10)  # type: ignore[assignment]
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 10).col1})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, shifted_col1:c|$c.col1__INTERNAL_PYLEGEND_COLUMN__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

        frame["col2"] = frame["col2"].shift(-3) + 5  # type: ignore[operator]
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 10).col1})
              ->project(~[col1:c|$c.col1, col2:c|$c.col2, shifted_col1:c|$c.col1__INTERNAL_PYLEGEND_COLUMN__])
              ->extend(~__INTERNAL_PYLEGEND_COLUMN__:{r | 0})
              ->extend(over(~[__INTERNAL_PYLEGEND_COLUMN__], []), ~col2__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lead($r, 3).col2})
              ->project(~[col1:c|$c.col1, col2:c|(toOne($c.col2__INTERNAL_PYLEGEND_COLUMN__) + 5), shifted_col1:c|$c.shifted_col1])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure


class TestUsageOnGroupbyFrame:
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_no_selection(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col").shift(1)

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], []), ~val_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).val_col})
              ->extend(over(~[group_col], []), ~random_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).random_col})
              ->project(~[
                val_col:p|$p.val_col__INTERNAL_PYLEGEND_COLUMN__,
                random_col:p|$p.random_col__INTERNAL_PYLEGEND_COLUMN__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_single_selection(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")[["val_col"]].shift(3)

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], []), ~val_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 3).val_col})
              ->project(~[
                val_col:p|$p.val_col__INTERNAL_PYLEGEND_COLUMN__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_selection_same_as_groupby(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.groupby("group_col")[["group_col"]].shift(1)

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], []), ~group_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).group_col})
              ->project(~[
                group_col:p|$p.group_col__INTERNAL_PYLEGEND_COLUMN__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
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
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], []), ~val_col_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).val_col})
              ->extend(over(~[group_col], []), ~random_col_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).random_col})
              ->extend(over(~[group_col], []), ~'val_col_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 1).val_col})
              ->extend(over(~[group_col], []), ~'random_col_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 1).random_col})
              ->project(~[
                val_col_1:p|$p.val_col_1__INTERNAL_PYLEGEND_COLUMN__,
                random_col_1:p|$p.random_col_1__INTERNAL_PYLEGEND_COLUMN__,
                'val_col_-1':p|$p.'val_col_-1__INTERNAL_PYLEGEND_COLUMN__',
                'random_col_-1':p|$p.'random_col_-1__INTERNAL_PYLEGEND_COLUMN__'
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
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
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], []), ~val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).val_col})
              ->extend(over(~[group_col], []), ~random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).random_col})
              ->extend(over(~[group_col], []), ~'val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 1).val_col})
              ->extend(over(~[group_col], []), ~'random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 1).random_col})
              ->project(~[
                val_col_sfx_1:p|$p.val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__,
                random_col_sfx_1:p|$p.random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__,
                'val_col_sfx_-1':p|$p.'val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__',
                'random_col_sfx_-1':p|$p.'random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__'
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
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
            .shift([1, -1], suffix="_sfx")
        )

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col, group_col_2], []), ~group_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).group_col})
              ->extend(over(~[group_col, group_col_2], []), ~val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).val_col})
              ->extend(over(~[group_col, group_col_2], []), ~random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).random_col})
              ->extend(over(~[group_col, group_col_2], []), ~'group_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 1).group_col})
              ->extend(over(~[group_col, group_col_2], []), ~'val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 1).val_col})
              ->extend(over(~[group_col, group_col_2], []), ~'random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 1).random_col})
              ->project(~[
                group_col_sfx_1:p|$p.group_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__,
                val_col_sfx_1:p|$p.val_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__,
                random_col_sfx_1:p|$p.random_col_sfx_1__INTERNAL_PYLEGEND_COLUMN__,
                'group_col_sfx_-1':p|$p.'group_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__',
                'val_col_sfx_-1':p|$p.'val_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__',
                'random_col_sfx_-1':p|$p.'random_col_sfx_-1__INTERNAL_PYLEGEND_COLUMN__'
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_series_datatype_conversion_and_full_query_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.string_column("group_col_2"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame1 = frame.groupby("group_col")["val_col"].shift(5)
        assert isinstance(frame1, GroupbySeries)
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], []), ~val_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 5).val_col})
              ->project(~[
                val_col:p|$p.val_col__INTERNAL_PYLEGEND_COLUMN__
              ])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame1.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame1, FrameToPureConfig(), self.legend_client) == expected_pure

        frame1 += 5  # type: ignore[operator, assignment]
        assert isinstance(frame1, GroupbySeries)
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], []), ~val_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 5).val_col})
              ->project(~[val_col:c|(toOne($c.val_col__INTERNAL_PYLEGEND_COLUMN__) + 5)])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame1.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame1, FrameToPureConfig(), self.legend_client) == expected_pure

        frame2 = frame.groupby("group_col")["val_col"].shift([1])
        assert isinstance(frame2, PandasApiTdsFrame)
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], []), ~val_col_1__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 1).val_col})
              ->project(~[
                val_col_1:p|$p.val_col_1__INTERNAL_PYLEGEND_COLUMN__
              ])
        '''
        expected_pure = dedent(expected_pure).strip()
        assert frame2.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame2, FrameToPureConfig(), self.legend_client) == expected_pure

        frame3 = frame.groupby(["group_col", "group_col_2"])["val_col"].shift([5, -3])
        assert isinstance(frame3, PandasApiTdsFrame)
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col, group_col_2], []), ~val_col_5__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 5).val_col})
              ->extend(over(~[group_col, group_col_2], []), ~'val_col_-3__INTERNAL_PYLEGEND_COLUMN__':{p,w,r | $p->lead($r, 3).val_col})
              ->project(~[
                val_col_5:p|$p.val_col_5__INTERNAL_PYLEGEND_COLUMN__,
                'val_col_-3':p|$p.'val_col_-3__INTERNAL_PYLEGEND_COLUMN__'
              ])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame3.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame3, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_series_assign(self) -> None:
        columns = [
            PrimitiveTdsColumn.string_column("group_col"),
            PrimitiveTdsColumn.string_column("group_col_2"),
            PrimitiveTdsColumn.integer_column("val_col"),
            PrimitiveTdsColumn.integer_column("random_col"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame["shifted_col1"] = frame.groupby(["group_col", "group_col_2"])["val_col"].shift(10)  # type: ignore[assignment]
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col, group_col_2], []), ~val_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 10).val_col})
              ->project(~[group_col:c|$c.group_col, group_col_2:c|$c.group_col_2, val_col:c|$c.val_col, random_col:c|$c.random_col, shifted_col1:c|$c.val_col__INTERNAL_PYLEGEND_COLUMN__])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

        frame["col2"] = frame.groupby(["group_col", "group_col_2"])["val_col"].shift(-3) + 5  # type: ignore[operator]
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col, group_col_2], []), ~val_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lag($r, 10).val_col})
              ->project(~[group_col:c|$c.group_col, group_col_2:c|$c.group_col_2, val_col:c|$c.val_col, random_col:c|$c.random_col, shifted_col1:c|$c.val_col__INTERNAL_PYLEGEND_COLUMN__])
              ->extend(over(~[group_col, group_col_2], []), ~val_col__INTERNAL_PYLEGEND_COLUMN__:{p,w,r | $p->lead($r, 3).val_col})
              ->project(~[group_col:c|$c.group_col, group_col_2:c|$c.group_col_2, val_col:c|$c.val_col, random_col:c|$c.random_col, shifted_col1:c|$c.shifted_col1, col2:c|(toOne($c.val_col__INTERNAL_PYLEGEND_COLUMN__) + 5)])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure
