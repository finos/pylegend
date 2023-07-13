# Copyright 2023 Goldman Sachs
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
import pytest
from textwrap import dedent

from pylegend.core.tds.pandas_api.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.legend_api.frames.legend_api_table_spec_input_frame import LegendApiTableSpecInputFrame
from pylegend.tests.test_helpers.legend_service_frame import simple_person_service_frame
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)


class TestFilterAppliedFunction:
    '''Also tests greater GT, GE, LT, LE and EQ magic methods'''

    def test_sql_gen_string_filter_eq_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.filter(lambda x: x["col2"] == "ISIN")

        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                (col2 = 'ISIN')'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_string_filter_gt_lt_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.filter(lambda x: (x["col2"] > "AAA") & (x["col2"] < "ZZZ"))  # type: ignore[operator]

        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                ((col2 > 'AAA') AND (col2 < 'ZZZ'))'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_string_filter_ge_le_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.filter(lambda x: (x["col2"] >= "AAA") & (x["col2"] <= "ZZZ"))  # type: ignore[operator]

        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                ((col2 >= 'AAA') AND (col2 <= 'ZZZ'))'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_integer_gt_lt_filter_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.filter(lambda x: (x["col1"] > 30) & (x["col1"] < 90))  # type: ignore[operator]

        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                ((col1 > 30) AND (col1 < 90))'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_integer_ge_le_filter_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.filter(lambda x: (x["col1"] >= 30) & (x["col1"] <= 90))  # type: ignore[operator]

        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                ((col1 >= 30) AND (col1 <= 90))'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_complex_filter_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.filter(lambda x: (x["col1"] > 20) & (x["col2"] == 'John'))  # type: ignore[operator]

        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                ((col1 > 20) AND (col2 = 'John'))'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_complex_or_filter_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.filter(lambda x: (x["col1"] > 20) | (x["col2"] == 'John'))  # type: ignore[operator]

        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                ((col1 > 20) OR (col2 = 'John'))'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_e2e_filter_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.filter(lambda x: x["First Name"] == 'John')
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_take_function_existing_filter(self,
                                               legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.filter(lambda x: x["First Name"] == 'John')
        frame = frame.filter(lambda x: x["Last Name"] == 'Hill')
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['John', 'Hill', 12, 'Firm X']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
