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

from pylegend import LegendClient
from pylegend._typing import PyLegendDict, PyLegendUnion
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_relation_person_service_frame_pandas_api


class TestErrorsOnBaseFrame:
    def test_invalid_periods_value(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v1:
            frame.shift(order_by="col1", periods=2)
        assert v1.value.args[0] == (
            "The 'periods' argument of the shift function only supports these values (or a list of them): {1, -1}\n"
            "But got these unsupported values: {2}."
        )

        with pytest.raises(NotImplementedError) as v2:
            frame.shift(order_by="col1", periods=[-1, 3])
        assert v2.value.args[0] == (
            "The 'periods' argument of the shift function only supports these values (or a list of them): {1, -1}\n"
            "But got these unsupported values: {3}."
        )

        with pytest.raises(ValueError) as v3:
            frame.shift(order_by="col1", periods=[-1, 1, -1])
        assert v3.value.args[0] == (
            "The 'periods' argument of the shift function cannot contain duplicate values, but got: periods=[-1, 1, -1]"
        )

    def test_invalid_order_by(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.shift(order_by="col2")
        assert v.value.args[0] == \
               "The following columns in the 'order_by' argument are not present in the base_frame: {'col2'}"

    def test_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.shift(order_by="col1", axis=1)

        expected_msg = "The 'axis' argument of the shift function must be 0 or 'index', but got: axis=1"
        assert v.value.args[0] == expected_msg

    def test_frequency_not_none(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.shift(order_by="col1", freq='D')

        expected_msg = "The 'freq' argument of the shift function is not supported, but got: freq='D'"
        assert v.value.args[0] == expected_msg

    def test_suffix_with_int_periods(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.shift(order_by="col1", periods=-1, suffix='abcd')

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
            frame.shift(order_by="int_col", fill_value="default_fill")

        expected_msg = (
            "The 'fill_value' argument of the shift function is not supported, but got: fill_value='default_fill'")
        assert v.value.args[0] == expected_msg

        with pytest.raises(NotImplementedError) as v:
            frame.shift(order_by="int_col", fill_value=-1)

        expected_msg = (
            "The 'fill_value' argument of the shift function is not supported, but got: fill_value=-1")
        assert v.value.args[0] == expected_msg

    def test_periods_list_with_repitition(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(ValueError) as v:
            frame.shift(order_by="col1", periods=[1, -1, 1])

        expected_msg = (
            "The 'periods' argument of the shift function cannot contain duplicate values, but got: "
            "periods=[1, -1, 1]")
        assert v.value.args[0] == expected_msg

    def test_kwargs_on_pct_change(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"),
                   PrimitiveTdsColumn.date_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.pct_change(order_by="col2", periods=5, axis=0)
        assert v.value.args[0] == "Extra keyword arguments are not supported in pct_change. Received: ['axis']"


class TestErrorsOnGroupbyFrame:
    def test_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("group_col").shift(order_by="val_col", axis=1)

        expected_msg = "The 'axis' argument of the shift function must be 0 or 'index', but got: axis=1"
        assert v.value.args[0] == expected_msg

    def test_frequency_not_none(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("group_col").shift(order_by="val_col", freq='D')

        expected_msg = "The 'freq' argument of the shift function is not supported, but got: freq='D'"
        assert v.value.args[0] == expected_msg

    def test_fill_value_not_none(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("group_col"),
                   PrimitiveTdsColumn.integer_column("val_col"),
                   PrimitiveTdsColumn.integer_column("random_col")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(NotImplementedError) as v:
            frame.groupby("group_col")[["val_col"]].shift(order_by="val_col", fill_value="default_fill")

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
        frame = frame.shift(order_by="col1")

        expected = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        lag("root"."col1", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query() == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r | 0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col1})
              ->project(~[
                col1:c|$c.col1__pylegend_olap_column__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_negative_periods_argument(self) -> None:
        columns = [PrimitiveTdsColumn.date_column("col1"), PrimitiveTdsColumn.float_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(order_by="col1", periods=-1)

        expected = '''
            SELECT
                "root"."col1__pylegend_olap_column__" AS "col1",
                "root"."col2__pylegend_olap_column__" AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        lead("root"."col1", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col1__pylegend_olap_column__",
                        lead("root"."col2", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query() == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r | 0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->lead($r, 1).col1})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col2__pylegend_olap_column__:{p,w,r | $p->lead($r, 1).col2})
              ->project(~[
                col1:c|$c.col1__pylegend_olap_column__,
                col2:c|$c.col2__pylegend_olap_column__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_list_periods_no_suffix(self) -> None:
        columns = [PrimitiveTdsColumn.strictdate_column("col1"), PrimitiveTdsColumn.datetime_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(order_by="col1", periods=[1, -1])

        expected = '''
            SELECT
                "root"."col1_1__pylegend_olap_column__" AS "col1_1",
                "root"."col2_1__pylegend_olap_column__" AS "col2_1",
                "root"."col1_-1__pylegend_olap_column__" AS "col1_-1",
                "root"."col2_-1__pylegend_olap_column__" AS "col2_-1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        lag("root"."col1", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col1_1__pylegend_olap_column__",
                        lag("root"."col2", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col2_1__pylegend_olap_column__",
                        lead("root"."col1", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col1_-1__pylegend_olap_column__",
                        lead("root"."col2", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col2_-1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query() == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r | 0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col1_1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col1})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col2_1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col2})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~'col1_-1__pylegend_olap_column__':{p,w,r | $p->lead($r, 1).col1})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~'col2_-1__pylegend_olap_column__':{p,w,r | $p->lead($r, 1).col2})
              ->project(~[
                col1_1:c|$c.col1_1__pylegend_olap_column__,
                col2_1:c|$c.col2_1__pylegend_olap_column__,
                'col1_-1':c|$c.'col1_-1__pylegend_olap_column__',
                'col2_-1':c|$c.'col2_-1__pylegend_olap_column__'
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_list_periods_with_suffix(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"), PrimitiveTdsColumn.integer_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.shift(order_by="col1", periods=[-1, 1], suffix="_suffix")

        expected = '''
            SELECT
                "root"."col1_suffix_-1__pylegend_olap_column__" AS "col1_suffix_-1",
                "root"."col2_suffix_-1__pylegend_olap_column__" AS "col2_suffix_-1",
                "root"."col1_suffix_1__pylegend_olap_column__" AS "col1_suffix_1",
                "root"."col2_suffix_1__pylegend_olap_column__" AS "col2_suffix_1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        lead("root"."col1", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col1_suffix_-1__pylegend_olap_column__",
                        lead("root"."col2", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col2_suffix_-1__pylegend_olap_column__",
                        lag("root"."col1", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col1_suffix_1__pylegend_olap_column__",
                        lag("root"."col2", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col2_suffix_1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query() == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r | 0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~'col1_suffix_-1__pylegend_olap_column__':{p,w,r | $p->lead($r, 1).col1})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~'col2_suffix_-1__pylegend_olap_column__':{p,w,r | $p->lead($r, 1).col2})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col1_suffix_1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col1})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col2_suffix_1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col2})
              ->project(~[
                'col1_suffix_-1':c|$c.'col1_suffix_-1__pylegend_olap_column__',
                'col2_suffix_-1':c|$c.'col2_suffix_-1__pylegend_olap_column__',
                col1_suffix_1:c|$c.col1_suffix_1__pylegend_olap_column__,
                col2_suffix_1:c|$c.col2_suffix_1__pylegend_olap_column__
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_diff(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"),
                   PrimitiveTdsColumn.integer_column("col2"),
                   PrimitiveTdsColumn.integer_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame = frame.diff(order_by="col1", periods=1)

        expected = '''
            SELECT
                ("root"."col1" - "root"."col1__pylegend_olap_column__") AS "col1",
                ("root"."col2" - "root"."col2__pylegend_olap_column__") AS "col2",
                ("root"."col3" - "root"."col3__pylegend_olap_column__") AS "col3"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3",
                        lag("root"."col1", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col1__pylegend_olap_column__",
                        lag("root"."col2", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col2__pylegend_olap_column__",
                        lag("root"."col3", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col3__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query() == expected

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r | 0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col1})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col2__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col2})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col3__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col3})
              ->project(~[
                col1:c|(toOne($c.col1) - toOne($c.col1__pylegend_olap_column__)),
                col2:c|(toOne($c.col2) - toOne($c.col2__pylegend_olap_column__)),
                col3:c|(toOne($c.col3) - toOne($c.col3__pylegend_olap_column__))
              ])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert frame.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_pct_change(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"),
                   PrimitiveTdsColumn.integer_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame = frame.pct_change(order_by="col1", periods=-1)

        expected = '''
            SELECT
                (((1.0 * "root"."col1") / "root"."col1__pylegend_olap_column__") - 1) AS "col1",
                (((1.0 * "root"."col2") / "root"."col2__pylegend_olap_column__") - 1) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        lead("root"."col1", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col1__pylegend_olap_column__",
                        lead("root"."col2", 1) OVER (PARTITION BY "root"."__pylegend_zero_column__" ORDER BY "root"."col1") AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                0 AS "__pylegend_zero_column__"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query() == expected

        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(~__pylegend_zero_column__:{r | 0})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col1__pylegend_olap_column__:{p,w,r | $p->lead($r, 1).col1})
              ->extend(over(~[__pylegend_zero_column__], [ascending(~col1)]), ~col2__pylegend_olap_column__:{p,w,r | $p->lead($r, 1).col2})
              ->project(~[
                col1:c|((toOne($c.col1) / toOne($c.col1__pylegend_olap_column__)) - 1),
                col2:c|((toOne($c.col2) / toOne($c.col2__pylegend_olap_column__)) - 1)
              ])
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
        frame = frame.groupby("group_col").shift(order_by="val_col", periods=1)

        expected = '''
            SELECT
                "root"."val_col__pylegend_olap_column__" AS "val_col",
                "root"."random_col__pylegend_olap_column__" AS "random_col"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col",
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__pylegend_olap_column__",
                        lag("root"."random_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "random_col__pylegend_olap_column__"
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
        assert frame.to_sql_query() == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).val_col})
              ->extend(over(~[group_col], [ascending(~val_col)]), ~random_col__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).random_col})
              ->project(~[
                val_col:c|$c.val_col__pylegend_olap_column__,
                random_col:c|$c.random_col__pylegend_olap_column__
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
        frame = frame.groupby("group_col")[["val_col"]].shift(order_by="val_col", periods=1)

        expected = '''
            SELECT
                "root"."val_col__pylegend_olap_column__" AS "val_col"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col",
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "val_col__pylegend_olap_column__"
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
        assert frame.to_sql_query() == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~val_col__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).val_col})
              ->project(~[
                val_col:c|$c.val_col__pylegend_olap_column__
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
        frame = frame.groupby("group_col")[["group_col"]].shift(order_by="val_col", periods=-1)

        expected = '''
            SELECT
                "root"."group_col__pylegend_olap_column__" AS "group_col"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col",
                        lead("root"."group_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."val_col") AS "group_col__pylegend_olap_column__"
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
        assert frame.to_sql_query() == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~val_col)]), ~group_col__pylegend_olap_column__:{p,w,r | $p->lead($r, 1).group_col})
              ->project(~[
                group_col:c|$c.group_col__pylegend_olap_column__
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
        frame = frame.groupby("group_col")[["val_col", "random_col"]].shift(order_by="random_col", periods=[1, -1])

        expected = '''
            SELECT
                "root"."val_col_1__pylegend_olap_column__" AS "val_col_1",
                "root"."random_col_1__pylegend_olap_column__" AS "random_col_1",
                "root"."val_col_-1__pylegend_olap_column__" AS "val_col_-1",
                "root"."random_col_-1__pylegend_olap_column__" AS "random_col_-1"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col",
                        "root".random_col_2 AS "random_col_2",
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "val_col_1__pylegend_olap_column__",
                        lag("root"."random_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col_1__pylegend_olap_column__",
                        lead("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "val_col_-1__pylegend_olap_column__",
                        lead("root"."random_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col_-1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
                                "root".random_col_2 AS "random_col_2"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query() == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~random_col)]), ~val_col_1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).val_col})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col_1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).random_col})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~'val_col_-1__pylegend_olap_column__':{p,w,r | $p->lead($r, 1).val_col})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~'random_col_-1__pylegend_olap_column__':{p,w,r | $p->lead($r, 1).random_col})
              ->project(~[
                val_col_1:c|$c.val_col_1__pylegend_olap_column__,
                random_col_1:c|$c.random_col_1__pylegend_olap_column__,
                'val_col_-1':c|$c.'val_col_-1__pylegend_olap_column__',
                'random_col_-1':c|$c.'random_col_-1__pylegend_olap_column__'
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
        frame = \
            frame.groupby("group_col")[["val_col", "random_col"]].shift(order_by="random_col", periods=[1, -1], suffix="_sfx")

        expected = '''
            SELECT
                "root"."val_col_sfx_1__pylegend_olap_column__" AS "val_col_sfx_1",
                "root"."random_col_sfx_1__pylegend_olap_column__" AS "random_col_sfx_1",
                "root"."val_col_sfx_-1__pylegend_olap_column__" AS "val_col_sfx_-1",
                "root"."random_col_sfx_-1__pylegend_olap_column__" AS "random_col_sfx_-1"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col",
                        "root".random_col_2 AS "random_col_2",
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "val_col_sfx_1__pylegend_olap_column__",
                        lag("root"."random_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col_sfx_1__pylegend_olap_column__",
                        lead("root"."val_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "val_col_sfx_-1__pylegend_olap_column__",
                        lead("root"."random_col", 1) OVER (PARTITION BY "root"."group_col" ORDER BY "root"."random_col") AS "random_col_sfx_-1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
                                "root".random_col_2 AS "random_col_2"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query() == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col], [ascending(~random_col)]), ~val_col_sfx_1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).val_col})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~random_col_sfx_1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).random_col})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~'val_col_sfx_-1__pylegend_olap_column__':{p,w,r | $p->lead($r, 1).val_col})
              ->extend(over(~[group_col], [ascending(~random_col)]), ~'random_col_sfx_-1__pylegend_olap_column__':{p,w,r | $p->lead($r, 1).random_col})
              ->project(~[
                val_col_sfx_1:c|$c.val_col_sfx_1__pylegend_olap_column__,
                random_col_sfx_1:c|$c.random_col_sfx_1__pylegend_olap_column__,
                'val_col_sfx_-1':c|$c.'val_col_sfx_-1__pylegend_olap_column__',
                'random_col_sfx_-1':c|$c.'random_col_sfx_-1__pylegend_olap_column__'
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
            .shift(order_by="val_col", periods=[1, -1], suffix="_sfx")
        )

        expected = '''
            SELECT
                "root"."group_col_sfx_1__pylegend_olap_column__" AS "group_col_sfx_1",
                "root"."val_col_sfx_1__pylegend_olap_column__" AS "val_col_sfx_1",
                "root"."random_col_sfx_1__pylegend_olap_column__" AS "random_col_sfx_1",
                "root"."group_col_sfx_-1__pylegend_olap_column__" AS "group_col_sfx_-1",
                "root"."val_col_sfx_-1__pylegend_olap_column__" AS "val_col_sfx_-1",
                "root"."random_col_sfx_-1__pylegend_olap_column__" AS "random_col_sfx_-1"
            FROM
                (
                    SELECT
                        "root".group_col AS "group_col",
                        "root".group_col_2 AS "group_col_2",
                        "root".val_col AS "val_col",
                        "root".random_col AS "random_col",
                        "root".random_col_2 AS "random_col_2",
                        lag("root"."group_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."val_col") AS "group_col_sfx_1__pylegend_olap_column__",
                        lag("root"."val_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."val_col") AS "val_col_sfx_1__pylegend_olap_column__",
                        lag("root"."random_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."val_col") AS "random_col_sfx_1__pylegend_olap_column__",
                        lead("root"."group_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."val_col") AS "group_col_sfx_-1__pylegend_olap_column__",
                        lead("root"."val_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."val_col") AS "val_col_sfx_-1__pylegend_olap_column__",
                        lead("root"."random_col", 1) OVER (PARTITION BY "root"."group_col", "root"."group_col_2" ORDER BY "root"."val_col") AS "random_col_sfx_-1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".group_col AS "group_col",
                                "root".group_col_2 AS "group_col_2",
                                "root".val_col AS "val_col",
                                "root".random_col AS "random_col",
                                "root".random_col_2 AS "random_col_2"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_sql_query() == expected

        expected = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[group_col, group_col_2], [ascending(~val_col)]), ~group_col_sfx_1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).group_col})
              ->extend(over(~[group_col, group_col_2], [ascending(~val_col)]), ~val_col_sfx_1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).val_col})
              ->extend(over(~[group_col, group_col_2], [ascending(~val_col)]), ~random_col_sfx_1__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).random_col})
              ->extend(over(~[group_col, group_col_2], [ascending(~val_col)]), ~'group_col_sfx_-1__pylegend_olap_column__':{p,w,r | $p->lead($r, 1).group_col})
              ->extend(over(~[group_col, group_col_2], [ascending(~val_col)]), ~'val_col_sfx_-1__pylegend_olap_column__':{p,w,r | $p->lead($r, 1).val_col})
              ->extend(over(~[group_col, group_col_2], [ascending(~val_col)]), ~'random_col_sfx_-1__pylegend_olap_column__':{p,w,r | $p->lead($r, 1).random_col})
              ->project(~[
                group_col_sfx_1:c|$c.group_col_sfx_1__pylegend_olap_column__,
                val_col_sfx_1:c|$c.val_col_sfx_1__pylegend_olap_column__,
                random_col_sfx_1:c|$c.random_col_sfx_1__pylegend_olap_column__,
                'group_col_sfx_-1':c|$c.'group_col_sfx_-1__pylegend_olap_column__',
                'val_col_sfx_-1':c|$c.'val_col_sfx_-1__pylegend_olap_column__',
                'random_col_sfx_-1':c|$c.'random_col_sfx_-1__pylegend_olap_column__'
              ])
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert frame.to_pure_query(FrameToPureConfig()) == expected
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected

    def test_groupby_diff(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"),
                   PrimitiveTdsColumn.integer_column("col2"),
                   PrimitiveTdsColumn.integer_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        groupby_frame_diff = frame.groupby("col1").diff(order_by="col2", periods=1)
        expected = '''
            SELECT
                ("root"."col2" - "root"."col2__pylegend_olap_column__") AS "col2",
                ("root"."col3" - "root"."col3__pylegend_olap_column__") AS "col3"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3",
                        lag("root"."col2", 1) OVER (PARTITION BY "root"."col1" ORDER BY "root"."col2") AS "col2__pylegend_olap_column__",
                        lag("root"."col3", 1) OVER (PARTITION BY "root"."col1" ORDER BY "root"."col2") AS "col3__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert groupby_frame_diff.to_sql_query() == expected
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[col1], [ascending(~col2)]), ~col2__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col2})
              ->extend(over(~[col1], [ascending(~col2)]), ~col3__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col3})
              ->project(~[
                col2:c|(toOne($c.col2) - toOne($c.col2__pylegend_olap_column__)),
                col3:c|(toOne($c.col3) - toOne($c.col3__pylegend_olap_column__))
              ])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert groupby_frame_diff.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(groupby_frame_diff, FrameToPureConfig(), self.legend_client) == expected_pure

        groupby_frame_diff = frame.groupby("col1")[["col1"]].diff(order_by="col2", periods=-1)
        expected = '''
            SELECT
                ("root"."col1" - "root"."col1__pylegend_olap_column__") AS "col1"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3",
                        lead("root"."col1", 1) OVER (PARTITION BY "root"."col1" ORDER BY "root"."col2") AS "col1__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert groupby_frame_diff.to_sql_query() == expected
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[col1], [ascending(~col2)]), ~col1__pylegend_olap_column__:{p,w,r | $p->lead($r, 1).col1})
              ->project(~[
                col1:c|(toOne($c.col1) - toOne($c.col1__pylegend_olap_column__))
              ])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert groupby_frame_diff.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(groupby_frame_diff, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_pct_change(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"),
                   PrimitiveTdsColumn.integer_column("col2"),
                   PrimitiveTdsColumn.integer_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        groupby_frame_pct_change = frame.groupby("col1").pct_change(order_by="col2", periods=1)
        expected = '''
            SELECT
                (((1.0 * "root"."col2") / "root"."col2__pylegend_olap_column__") - 1) AS "col2",
                (((1.0 * "root"."col3") / "root"."col3__pylegend_olap_column__") - 1) AS "col3"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3",
                        lag("root"."col2", 1) OVER (PARTITION BY "root"."col1" ORDER BY "root"."col2") AS "col2__pylegend_olap_column__",
                        lag("root"."col3", 1) OVER (PARTITION BY "root"."col1" ORDER BY "root"."col2") AS "col3__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert groupby_frame_pct_change.to_sql_query() == expected
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[col1], [ascending(~col2)]), ~col2__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col2})
              ->extend(over(~[col1], [ascending(~col2)]), ~col3__pylegend_olap_column__:{p,w,r | $p->lag($r, 1).col3})
              ->project(~[
                col2:c|((toOne($c.col2) / toOne($c.col2__pylegend_olap_column__)) - 1),
                col3:c|((toOne($c.col3) / toOne($c.col3__pylegend_olap_column__)) - 1)
              ])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert groupby_frame_pct_change.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(groupby_frame_pct_change, FrameToPureConfig(), self.legend_client) == \
               expected_pure

        groupby_frame_pct_change = frame.groupby("col1")[["col1", "col2"]].pct_change(order_by="col2", periods=-1)
        expected = '''
            SELECT
                (((1.0 * "root"."col1") / "root"."col1__pylegend_olap_column__") - 1) AS "col1",
                (((1.0 * "root"."col2") / "root"."col2__pylegend_olap_column__") - 1) AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3",
                        lead("root"."col1", 1) OVER (PARTITION BY "root"."col1" ORDER BY "root"."col2") AS "col1__pylegend_olap_column__",
                        lead("root"."col2", 1) OVER (PARTITION BY "root"."col1" ORDER BY "root"."col2") AS "col2__pylegend_olap_column__"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2",
                                "root".col3 AS "col3"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "root"
                ) AS "root"
        '''  # noqa: E501
        expected = dedent(expected).strip()
        assert groupby_frame_pct_change.to_sql_query() == expected
        expected_pure = '''
            #Table(test_schema.test_table)#
              ->extend(over(~[col1], [ascending(~col2)]), ~col1__pylegend_olap_column__:{p,w,r | $p->lead($r, 1).col1})
              ->extend(over(~[col1], [ascending(~col2)]), ~col2__pylegend_olap_column__:{p,w,r | $p->lead($r, 1).col2})
              ->project(~[
                col1:c|((toOne($c.col1) / toOne($c.col1__pylegend_olap_column__)) - 1),
                col2:c|((toOne($c.col2) / toOne($c.col2__pylegend_olap_column__)) - 1)
              ])
        '''  # noqa: E501
        expected_pure = dedent(expected_pure).strip()
        assert groupby_frame_pct_change.to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(groupby_frame_pct_change, FrameToPureConfig(), self.legend_client) == \
               expected_pure


class TestEndToEndUsageOnBaseFrame:

    def test_no_arguments(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame = frame.shift(order_by="Age")

        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {'values': ['Anthony', 'Allen', 22, 'Firm X']},
                {'values': ['John', 'Hill', 12, 'Firm X']},
                {'values': [None, None, None, None]},
                {'values': ['John', 'Johnson', 22, 'Firm X']},
                {'values': ['Oliver', 'Hill', 32, 'Firm B']},
                {'values': ['Peter', 'Smith', 23, 'Firm X']},
                {'values': ['Fabrice', 'Roberts', 34, 'Firm A']}
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_negative_periods(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame = frame.sort_values("Age").shift(order_by="Age", periods=-1)

        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {'values': ['John', 'Johnson', 22, 'Firm X']},
                {'values': ['Anthony', 'Allen', 22, 'Firm X']},
                {'values': ['Peter', 'Smith', 23, 'Firm X']},
                {'values': ['Oliver', 'Hill', 32, 'Firm B']},
                {'values': ['Fabrice', 'Roberts', 34, 'Firm A']},
                {'values': ['David', 'Harris', 35, 'Firm C']},
                {'values': [None, None, None, None]}
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_list_periods(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame = frame.sort_values("Age").shift(order_by="Age", periods=[1, -1])

        expected = {
            'columns': ['First Name_1', 'Last Name_1', 'Age_1', 'Firm/Legal Name_1',
                        'First Name_-1', 'Last Name_-1', 'Age_-1', 'Firm/Legal Name_-1'],
            'rows': [
                {'values': [None, None, None, None, 'John', 'Johnson', 22, 'Firm X']},
                {'values': ['John', 'Hill', 12, 'Firm X', 'Anthony', 'Allen', 22, 'Firm X']},
                {'values': ['John', 'Johnson', 22, 'Firm X', 'Peter', 'Smith', 23, 'Firm X']},
                {'values': ['Anthony', 'Allen', 22, 'Firm X', 'Oliver', 'Hill', 32, 'Firm B']},
                {'values': ['Peter', 'Smith', 23, 'Firm X', 'Fabrice', 'Roberts', 34, 'Firm A']},
                {'values': ['Oliver', 'Hill', 32, 'Firm B', 'David', 'Harris', 35, 'Firm C']},
                {'values': ['Fabrice', 'Roberts', 34, 'Firm A', None, None, None, None]}
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_list_periods_with_suffix(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame = frame.sort_values("Age").shift(order_by="Age", periods=[1, -1], suffix="_shifted")

        expected = {
            'columns': ['First Name_shifted_1', 'Last Name_shifted_1', 'Age_shifted_1', 'Firm/Legal Name_shifted_1',
                        'First Name_shifted_-1', 'Last Name_shifted_-1', 'Age_shifted_-1', 'Firm/Legal Name_shifted_-1'],
            'rows': [
                {'values': [None, None, None, None, 'John', 'Johnson', 22, 'Firm X']},
                {'values': ['John', 'Hill', 12, 'Firm X', 'Anthony', 'Allen', 22, 'Firm X']},
                {'values': ['John', 'Johnson', 22, 'Firm X', 'Peter', 'Smith', 23, 'Firm X']},
                {'values': ['Anthony', 'Allen', 22, 'Firm X', 'Oliver', 'Hill', 32, 'Firm B']},
                {'values': ['Peter', 'Smith', 23, 'Firm X', 'Fabrice', 'Roberts', 34, 'Firm A']},
                {'values': ['Oliver', 'Hill', 32, 'Firm B', 'David', 'Harris', 35, 'Firm C']},
                {'values': ['Fabrice', 'Roberts', 34, 'Firm A', None, None, None, None]}
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected


class TestEndToEndUsageOnGroupbyFrame:

    def test_no_arguments(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame = frame.sort_values("Age").groupby("Firm/Legal Name").shift("Age")

        expected = {
            'columns': ['First Name', 'Last Name', 'Age'],
            'rows': [
                {'values': [None, None, None]},
                {'values': ['John', 'Hill', 12]},
                {'values': ['John', 'Johnson', 22]},
                {'values': ['Anthony', 'Allen', 22]},
                {'values': [None, None, None]},
                {'values': [None, None, None]},
                {'values': [None, None, None]},
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_negative_periods_with_selection(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame = (
            frame.sort_values("Age")
            .groupby("Firm/Legal Name")[["First Name", "Last Name"]]
            .shift("Age", -1)
        )

        expected = {
            'columns': ['First Name', 'Last Name'],
            'rows': [
                {'values': ['John', 'Johnson']},
                {'values': ['Anthony', 'Allen']},
                {'values': ['Peter', 'Smith']},
                {'values': [None, None]},
                {'values': [None, None]},
                {'values': [None, None]},
                {'values': [None, None]},
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_list_periods_with_groupby_column_selected(
            self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame = (
            frame.sort_values("Age")
            .groupby("Firm/Legal Name")[["Firm/Legal Name"]]
            .shift("Age", [1, -1])
        )

        expected = {
            'columns': ['Firm/Legal Name_1', 'Firm/Legal Name_-1'],
            'rows': [
                {'values': [None, 'Firm X']},
                {'values': ['Firm X', 'Firm X']},
                {'values': ['Firm X', 'Firm X']},
                {'values': ['Firm X', None]},
                {'values': [None, None]},
                {'values': [None, None]},
                {'values': [None, None]},
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_list_periods_with_multiple_groupby_and_suffix(
            self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]
    ) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame = (
            frame.sort_values("Age")
            .groupby("Firm/Legal Name")[["Firm/Legal Name", "First Name"]]
            .shift("Age", [1, -1], suffix="_shifted")
        )

        expected = {
            'columns': ['First Name_shifted_1', 'Firm/Legal Name_shifted_1',
                        'First Name_shifted_-1', 'Firm/Legal Name_shifted_-1'],
            'rows': [
                {'values': [None, None, 'John', 'Firm X']},
                {'values': ['John', 'Firm X', 'Anthony', 'Firm X']},
                {'values': ['John', 'Firm X', 'Peter', 'Firm X']},
                {'values': ['Anthony', 'Firm X', None, None]},
                {'values': [None, None, None, None]},
                {'values': [None, None, None, None]},
                {'values': [None, None, None, None]},
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    @pytest.mark.skip(reason="Legend server doesn't execute this SQL because of arithmetic operation on OLAP column.")
    def test_e2e_diff(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame = frame.sort_values("Age")[["Age"]].diff("Age", 1)  # type: ignore[union-attr]
        expected = {
            'columns': ['Age'],
            'rows': [
                {'values': [None]},
                {'values': [10]},
                {'values': [0]},
                {'values': [1]},
                {'values': [9]},
                {'values': [2]},
                {'values': [1]}
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    @pytest.mark.skip(reason="Legend server doesn't execute this SQL because of arithmetic operation on OLAP column.")
    def test_e2e_pct_change(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_relation_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame = frame.sort_values("Age")[["Age"]].pct_change("Age", 1)  # type: ignore[union-attr]
        expected = {
            'columns': ['Age'],
            'rows': [
                {'values': [None]},
                {'values': [0.8333333333333334]},
                {'values': [0.0]},
                {'values': [0.045454545454545456]},
                {'values': [0.391304347826087]},
                {'values': [0.0625]},
                {'values': [0.029411764705882353]}
            ]
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
