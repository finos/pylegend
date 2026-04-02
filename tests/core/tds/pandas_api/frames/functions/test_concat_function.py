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
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api


class TestConcatLegendExtErrors:

    def test_concat_error_on_different_size_frames(self) -> None:
        columns1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns1)

        columns2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3")
        ]
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns2)

        with pytest.raises(ValueError) as v:
            frame1.concat_legend_ext(frame2)

        expected = (
            'Cannot concatenate two Tds Frames with different column counts. \n'
            'Frame 1 cols - (Count: 2) - [TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String)] \n'
            'Frame 2 cols - (Count: 3) - [TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), '
            'TdsColumn(Name: col3, Type: String)] \n'
        )
        assert v.value.args[0] == expected

    def test_concat_error_on_column_name_mismatch(self) -> None:
        columns1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns1)

        columns2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col3")
        ]
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns2)

        with pytest.raises(ValueError) as v:
            frame1.concat_legend_ext(frame2)

        expected = (
            'Column name/type mismatch when concatenating Tds Frames at index 1. '
            'Frame 1 column - TdsColumn(Name: col2, Type: String), Frame 2 column - TdsColumn(Name: col3, Type: String)'
        )
        assert v.value.args[0] == expected

    def test_concat_error_on_column_type_mismatch(self) -> None:
        columns1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns1)

        columns2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns2)

        with pytest.raises(ValueError) as v:
            frame1.concat_legend_ext(frame2)

        expected = (
            'Column name/type mismatch when concatenating Tds Frames at index 1. '
            'Frame 1 column - TdsColumn(Name: col2, Type: String), Frame 2 column - TdsColumn(Name: col2, Type: Float)'
        )
        assert v.value.args[0] == expected

    def test_concat_error_on_non_frame(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(TypeError) as v:
            frame.concat_legend_ext(123)  # type: ignore

        assert "Can only concatenate TdsFrame objects" in v.value.args[0]


class TestConcatLegendExtOnFrame:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_concat_simple_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_2'], columns)

        result = frame1.concat_legend_ext(frame2)

        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2"
            FROM
                (
                    SELECT
                        "left"."col1" AS "col1",
                        "left"."col2" AS "col2"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "left"
                    UNION ALL
                    SELECT
                        "right"."col1" AS "col1",
                        "right"."col2" AS "col2"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2"
                            FROM
                                test_schema.test_table_2 AS "root"
                        ) AS "right"
                ) AS "root"'''
        assert result.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_concat_simple_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_2'], columns)

        result = frame1.concat_legend_ext(frame2)

        expected = '''\
            #Table(test_schema.test_table)#
              ->concatenate(
                #Table(test_schema.test_table_2)#
              )'''
        assert result.to_pure_query(FrameToPureConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == dedent(expected)

    def test_concat_with_head(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame1 = frame1.head(2)
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2 = frame2.iloc[2:4]

        result = frame1.concat_legend_ext(frame2)

        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2"
            FROM
                (
                    SELECT
                        "left"."col1" AS "col1",
                        "left"."col2" AS "col2"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2"
                            FROM
                                test_schema.test_table AS "root"
                            LIMIT 2
                            OFFSET 0
                        ) AS "left"
                    UNION ALL
                    SELECT
                        "right"."col1" AS "col1",
                        "right"."col2" AS "col2"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1",
                                "root".col2 AS "col2"
                            FROM
                                test_schema.test_table AS "root"
                            LIMIT 2
                            OFFSET 2
                        ) AS "right"
                ) AS "root"'''
        assert result.to_sql_query(FrameToSqlConfig()) == dedent(expected)

        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->slice(0, 2)
              ->concatenate(
                #Table(test_schema.test_table)#
                  ->slice(2, 4)
                  ->select(~[col1, col2])
              )'''
        assert result.to_pure_query(FrameToPureConfig()) == dedent(expected_pure)
        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)


class TestConcatLegendExtOnSeries:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_series_concat_sql(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_2'], columns)

        series1 = frame1["col1"]
        series2 = frame2["col1"]
        result = series1.concat_legend_ext(series2)

        expected = '''\
            SELECT
                "root"."col1" AS "col1"
            FROM
                (
                    SELECT
                        "left"."col1" AS "col1"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1"
                            FROM
                                test_schema.test_table AS "root"
                        ) AS "left"
                    UNION ALL
                    SELECT
                        "right"."col1" AS "col1"
                    FROM
                        (
                            SELECT
                                "root".col1 AS "col1"
                            FROM
                                test_schema.test_table_2 AS "root"
                        ) AS "right"
                ) AS "root"'''
        assert result.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_series_concat_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table_2'], columns)

        series1 = frame1["col1"]
        series2 = frame2["col1"]
        result = series1.concat_legend_ext(series2)

        expected = '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])
              ->concatenate(
                #Table(test_schema.test_table_2)#
                  ->select(~[col1])
              )'''
        assert result.to_pure_query(FrameToPureConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(result, FrameToPureConfig(), self.legend_client) == dedent(expected)


class TestConcatLegendExtEndToEnd:

    def test_e2e_concat_frame(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        result = frame.concat_legend_ext(frame)
        result = result.filter(items=["First Name", "Firm/Legal Name"])

        expected = {
            'columns': ['First Name', 'Firm/Legal Name'],
            'rows': [
                {'values': ['Peter', 'Firm X']},
                {'values': ['John', 'Firm X']},
                {'values': ['John', 'Firm X']},
                {'values': ['Anthony', 'Firm X']},
                {'values': ['Fabrice', 'Firm A']},
                {'values': ['Oliver', 'Firm B']},
                {'values': ['David', 'Firm C']},
                {'values': ['Peter', 'Firm X']},
                {'values': ['John', 'Firm X']},
                {'values': ['John', 'Firm X']},
                {'values': ['Anthony', 'Firm X']},
                {'values': ['Fabrice', 'Firm A']},
                {'values': ['Oliver', 'Firm B']},
                {'values': ['David', 'Firm C']},
            ]
        }
        res = result.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_concat_with_head(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame1 = frame.head(3)
        frame2 = frame.iloc[3:5]

        result = frame1.concat_legend_ext(frame2)
        result = result.filter(items=["First Name", "Firm/Legal Name"])

        expected = {
            'columns': ['First Name', 'Firm/Legal Name'],
            'rows': [
                {'values': ['Peter', 'Firm X']},
                {'values': ['John', 'Firm X']},
                {'values': ['John', 'Firm X']},
                {'values': ['Anthony', 'Firm X']},
                {'values': ['Fabrice', 'Firm A']},
            ]
        }
        res = result.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_concat_series(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        series1 = frame.head(3)["First Name"]
        series2 = frame.iloc[3:5]["First Name"]

        result = series1.concat_legend_ext(series2)

        expected = {
            'columns': ['First Name'],
            'rows': [
                {'values': ['Peter']},
                {'values': ['John']},
                {'values': ['John']},
                {'values': ['Anthony']},
                {'values': ['Fabrice']},
            ]
        }
        res = result.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
