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
import pytest

from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient

from tests.test_helpers.test_legend_service_frames import (
    simple_person_service_frame_pandas_api,
    simple_trade_service_frame_pandas_api,
)


class TestSortValuesFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_invalid_column_name(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.sort_values(["col3"])
        assert v.value.args[0] == "Column - 'col3' in sort_values columns list doesn't exist in the current frame. " \
                                  "Current frame columns: ['col1', 'col2']"

    def test_unequal_elements_in_by_and_ascending(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.sort_values(by=["col1"], ascending=[True, False])
        assert v.value.args[
                   0] == ("The number of columns in 'by' must equal the number of values in 'ascending' for "
                          "sort_values function.")

    def test_invalid_axis_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.sort_values(by=["col1"], axis=1)
        assert v.value.args[0] == "Axis parameter of sort_values function must be 0 or 'index'"

    def test_invalid_inplace_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.sort_values(by=["col1"], inplace=True)
        assert v.value.args[0] == "Inplace parameter of sort_values function must be False"

    def test_unsupported_kind_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.sort_values(by=["col1"], kind="mergesort")
        assert v.value.args[0] == "Kind parameter of sort_values function is not supported"

    def test_unsupported_key_parameter(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.sort_values(by=["col1"], key=lambda x: x)
        assert v.value.args[0] == "Key parameter of sort_values function is not supported"

    def test_simple_query_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.sort_values(["col2", "col1"])
        expected = '''\
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    ORDER BY
                        "root".col2,
                        "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->sort([~col2->ascending(), ~col1->ascending()])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False),
                                               self.legend_client) == ("#Table(test_schema.test_table)#->sort(["
                                                                       "~col2->ascending(), ~col1->ascending()])")

    def test_ascending_descending(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.sort_values(by=["col1", "col2"], ascending=[True, False])
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            ORDER BY
                "root".col1,
                "root".col2 DESC'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->sort([~col1->ascending(), ~col2->descending()])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False),
                                               self.legend_client) == ("#Table(test_schema.test_table)#->sort(["
                                                                       "~col1->ascending(), ~col2->descending()])")

    def test_single_column(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.sort_values(by=["col1"], ascending=True)
        expected = '''\
                SELECT
                    "root".col1 AS "col1",
                    "root".col2 AS "col2"
                FROM
                    test_schema.test_table AS "root"
                ORDER BY
                    "root".col1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
                #Table(test_schema.test_table)#
                  ->sort([~col1->ascending()])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False),
                                               self.legend_client) == ("#Table(test_schema.test_table)#->sort(["
                                                                       "~col1->ascending()])")

    def test_e2e_single_column(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.sort_values("Age")
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [
                        {'values': ['John', 'Hill', 12, 'Firm X']},
                        {'values': ['John', 'Johnson', 22, 'Firm X']},
                        {'values': ['Anthony', 'Allen', 22, 'Firm X']},
                        {'values': ['Peter', 'Smith', 23, 'Firm X']},
                        {'values': ['Oliver', 'Hill', 32, 'Firm B']},
                        {'values': ['Fabrice', 'Roberts', 34, 'Firm A']},
                        {'values': ['David', 'Harris', 35, 'Firm C']}
                    ]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_column_order_preservation(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.sort_values(by=["Age", "First Name"])
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [
                        {'values': ['John', 'Hill', 12, 'Firm X']},
                        {'values': ['Anthony', 'Allen', 22, 'Firm X']},
                        {'values': ['John', 'Johnson', 22, 'Firm X']},
                        {'values': ['Peter', 'Smith', 23, 'Firm X']},
                        {'values': ['Oliver', 'Hill', 32, 'Firm B']},
                        {'values': ['Fabrice', 'Roberts', 34, 'Firm A']},
                        {'values': ['David', 'Harris', 35, 'Firm C']}
                    ]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
        

    def test_e2e_date_column(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])
        frame = frame.sort_values(by=["Settlement Date Time", "Date"])

        expected = {'columns': ['Id', 'Date', 'Quantity', 'Settlement Date Time', 'Product/Name', 'Account/Name'],
                    'rows': [
                        {'values': [10, '2014-12-04', 38.0, None, 'Firm C', 'Account 2']},
                        {'values': [11, '2014-12-05', 5.0, None, None, None]},
                        {'values': [1, '2014-12-01', 25.0, '2014-12-02T21:00:00.000000000+0000', 'Firm X',
                                    'Account 1']},
                        {'values': [2, '2014-12-01', 320.0, '2014-12-02T21:00:00.000000000+0000', 'Firm X',
                                    'Account 2']},
                        {'values': [3, '2014-12-01', 11.0, '2014-12-02T21:00:00.000000000+0000', 'Firm A',
                                    'Account 1']},
                        {'values': [4, '2014-12-02', 23.0, '2014-12-03T21:00:00.000000000+0000', 'Firm A',
                                    'Account 2']},
                        {'values': [5, '2014-12-02', 32.0, '2014-12-03T21:00:00.000000000+0000', 'Firm A',
                                    'Account 1']},
                        {'values': [7, '2014-12-03', 44.0, '2014-12-04T15:22:23.123456789+0000', 'Firm C',
                                    'Account 1']},
                        {'values': [6, '2014-12-03', 27.0, '2014-12-04T21:00:00.000000000+0000', 'Firm C',
                                    'Account 1']},
                        {'values': [8, '2014-12-04', 22.0, '2014-12-05T21:00:00.000000000+0000', 'Firm C',
                                    'Account 2']},
                        {'values': [9, '2014-12-04', 45.0, '2014-12-05T21:00:00.000000000+0000', 'Firm C', 'Account 2']}
                    ]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
