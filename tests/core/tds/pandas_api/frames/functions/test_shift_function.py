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

from textwrap import dedent

import pandas as pd
import pytest
from pylegend._typing import (
    PyLegendDict,
    PyLegendList,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
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
                "root"."col1__internal_sql_column_name__" AS "col1"
            FROM
                (
                    SELECT
                        lag("root"."col1", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col1__internal_sql_column_name__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                0 AS "__internal_sql_column_name__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over([ascending(~col1)]), ~col1__internal_pure_col_name__:{p,w,r | $p->rank($w, $r)})
              ->project(~[col1:p|$p.col1__internal_pure_col_name__])
        '''
        expected = dedent(expected).strip()
        if TEST_PURE:
            assert frame.to_pure_query(FrameToPureConfig()) == expected
            if USE_LEGEND_ENGINE:
                assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_periods_argument_multiple_columns(self) -> None:
        columns = [PrimitiveTdsColumn.number_column("col1"), PrimitiveTdsColumn.boolean_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=1)

        expected = '''
            SELECT
                "root"."col1__internal_sql_column_name__" AS "col1",
                "root"."col2__internal_sql_column_name__" AS "col2"
            FROM
                (
                    SELECT
                        lag("root"."col1", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col1__internal_sql_column_name__",
                        lag("root"."col2", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col2__internal_sql_column_name__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__internal_sql_column_name__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test_negative_periods_argument(self) -> None:
        columns = [PrimitiveTdsColumn.date_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=-1)

        expected = '''
            SELECT
                "root"."col1__internal_sql_column_name__" AS "col1"
            FROM
                (
                    SELECT
                        lead("root"."col1", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col1__internal_sql_column_name__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                0 AS "__internal_sql_column_name__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test_list_periods_no_suffix(self) -> None:
        columns = [PrimitiveTdsColumn.strictdate_column("col1"), PrimitiveTdsColumn.datetime_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=[1, -1])

        expected = '''
            SELECT
                "root"."col1_1__internal_sql_column_name__" AS "col1_1",
                "root"."col2_1__internal_sql_column_name__" AS "col2_1",
                "root"."col1_-1__internal_sql_column_name__" AS "col1_-1",
                "root"."col2_-1__internal_sql_column_name__" AS "col2_-1"
            FROM
                (
                    SELECT
                        lag("root"."col1", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col1_1__internal_sql_column_name__",
                        lag("root"."col2", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col2_1__internal_sql_column_name__",
                        lead("root"."col1", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col1_-1__internal_sql_column_name__",
                        lead("root"."col2", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col2_-1__internal_sql_column_name__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__internal_sql_column_name__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected

    def test_list_periods_with_suffix(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"), PrimitiveTdsColumn.integer_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(periods=[-1, 1], suffix="_custom_suffix")

        expected = '''
            SELECT
                "root"."col1_custom_suffix_-1__internal_sql_column_name__" AS "col1_custom_suffix_-1",
                "root"."col2_custom_suffix_-1__internal_sql_column_name__" AS "col2_custom_suffix_-1",
                "root"."col1_custom_suffix_1__internal_sql_column_name__" AS "col1_custom_suffix_1",
                "root"."col2_custom_suffix_1__internal_sql_column_name__" AS "col2_custom_suffix_1"
            FROM
                (
                    SELECT
                        lead("root"."col1", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col1_custom_suffix_-1__internal_sql_column_name__",
                        lead("root"."col2", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col2_custom_suffix_-1__internal_sql_column_name__",
                        lag("root"."col1", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col1_custom_suffix_1__internal_sql_column_name__",
                        lag("root"."col2", 1) OVER (ORDER BY "root"."__internal_sql_column_name__") AS "col2_custom_suffix_1__internal_sql_column_name__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__internal_sql_column_name__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query(FrameToSqlConfig()) == expected


@pytest.fixture(scope="class")
def pandas_df_simple_person() -> pd.DataFrame:
    rows: PyLegendList[PyLegendDict[str, PyLegendList[PyLegendUnion[str, int]]]] = [
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
    pd.testing.assert_frame_equal(
        left=left,
        right=right,
        check_dtype=False,
        check_exact=False,
        check_like=True
    )


class TestEndToEndUsageOnBaseFrame:

    def test_no_arguments(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_output = frame.shift().execute_frame_to_pandas_df()
        pandas_output = pandas_df_simple_person.shift()

        assert_frame_equal(left=pylegend_output, right=pandas_output)

    def test_negative_periods(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_output = frame.shift(periods=-1).execute_frame_to_pandas_df()
        pandas_output = pandas_df_simple_person.shift(periods=-1)

        assert_frame_equal(left=pylegend_output, right=pandas_output)

    def test_list_periods(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_output = frame.shift(periods=[1, -1]).execute_frame_to_pandas_df()
        pandas_output = pandas_df_simple_person.shift(periods=[1, -1])

        assert_frame_equal(left=pylegend_output, right=pandas_output)

    def test_list_periods_with_suffix(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int,]],
            pandas_df_simple_person: pd.DataFrame
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        pylegend_output = frame.shift(periods=[1, -1], suffix="_shifted").execute_frame_to_pandas_df()
        pandas_output = pandas_df_simple_person.shift(periods=[1, -1], suffix="_shifted")  # type: ignore[call-arg]

        assert_frame_equal(left=pylegend_output, right=pandas_output)
