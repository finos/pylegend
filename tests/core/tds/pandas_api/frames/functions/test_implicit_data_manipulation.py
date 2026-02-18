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
from datetime import date, datetime

import pytest

from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend.core.request.legend_client import LegendClient


class TestImplicitDataManipulationFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_implicit_function_errors(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # Type Error
        with pytest.raises(TypeError) as t:
            frame[1] = 2  # type: ignore
        assert t.value.args[0] == "Column name must be a string, got: <class 'int'>"

        # Cross frame assignment
        with pytest.raises(ValueError) as v:
            frame['col1'] = frame2['col2']  # type: ignore
        assert v.value.args[0] == "Assignment from a different frame is not allowed"

    def test_implicit_function_sql_pure(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.float_column("col4"),
            PrimitiveTdsColumn.float_column("col5")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame['col1'] = frame['col1'] + 10  # type: ignore
        expected_sql = '''\
                         SELECT
                             ("root".col1 + 10) AS "col1",
                             "root".col2 AS "col2",
                             "root".col3 AS "col3",
                             "root".col4 AS "col4",
                             "root".col5 AS "col5"
                         FROM
                             test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|(toOne($c.col1) + 10), col2:c|$c.col2, col3:c|$c.col3, "
            "col4:c|$c.col4, col5:c|$c.col5])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

    @pytest.mark.skip(reason="Boolean not yet supported in PURE")
    def test_implicit_function_boolean(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.boolean_column("col4"),
            PrimitiveTdsColumn.date_column("col5"),
            PrimitiveTdsColumn.datetime_column("col6"),
            PrimitiveTdsColumn.strictdate_column("col7")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame['col4'] = ~frame['col4']  # type: ignore
        expected_sql = '''\
                         SELECT
                             "root".col1 AS "col1",
                             "root".col2 AS "col2",
                             "root".col3 AS "col3",
                             NOT("root".col4) AS "col4",
                             "root".col5 AS "col5",
                             "root".col6 AS "col6",
                             "root".col7 AS "col7"
                         FROM
                             test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col4:c|toOne($c.col4)->not(), col5:c|$c.col5, "
            "col6:c|$c.col6, col7:c|$c.col7])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

    def test_implicit_function_float(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.date_column("col5"),
            PrimitiveTdsColumn.datetime_column("col6"),
            PrimitiveTdsColumn.strictdate_column("col7")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame['col3'] = frame['col3'] + 1.5  # type: ignore

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|(toOne($c.col3) + 1.5), col5:c|$c.col5, "
            "col6:c|$c.col6, col7:c|$c.col7])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        expected_sql = '''\
                        SELECT
                            "root".col1 AS "col1",
                            "root".col2 AS "col2",
                            ("root".col3 + 1.5) AS "col3",
                            "root".col5 AS "col5",
                            "root".col6 AS "col6",
                            "root".col7 AS "col7"
                        FROM
                            test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_implicit_function_date(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.date_column("col5"),
            PrimitiveTdsColumn.datetime_column("col6"),
            PrimitiveTdsColumn.strictdate_column("col7")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame['col5'] = date(2024, 1, 1)
        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col5:c|%2024-01-01, "
            "col6:c|$c.col6, col7:c|$c.col7])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        expected_sql = '''\
                        SELECT
                            "root".col1 AS "col1",
                            "root".col2 AS "col2",
                            "root".col3 AS "col3",
                            CAST('2024-01-01' AS DATE) AS "col5",
                            "root".col6 AS "col6",
                            "root".col7 AS "col7"
                        FROM
                            test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_implicit_function_strict_date(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.date_column("col5"),
            PrimitiveTdsColumn.datetime_column("col6"),
            PrimitiveTdsColumn.strictdate_column("col7")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame['col7'] = date(2024, 1, 1)
        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col5:c|$c.col5, "
            "col6:c|$c.col6, col7:c|%2024-01-01])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        expected_sql = '''\
                        SELECT
                            "root".col1 AS "col1",
                            "root".col2 AS "col2",
                            "root".col3 AS "col3",
                            "root".col5 AS "col5",
                            "root".col6 AS "col6",
                            CAST('2024-01-01' AS DATE) AS "col7"
                        FROM
                            test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

    def test_implicit_function_datetime(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.date_column("col5"),
            PrimitiveTdsColumn.datetime_column("col6"),
            PrimitiveTdsColumn.strictdate_column("col7")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame['col4'] = datetime(2024, 1, 1, 12, 30, 0)

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col5:c|$c.col5, col6:c|$c.col6, col7:c|$c.col7, "
            "col4:c|%2024-01-01T12:30:00])"
        )

        expected_sql = '''\
        SELECT
            "root".col1 AS "col1",
            "root".col2 AS "col2",
            "root".col3 AS "col3",
            "root".col5 AS "col5",
            "root".col6 AS "col6",
            "root".col7 AS "col7",
            CAST('2024-01-01T12:30:00' AS TIMESTAMP) AS "col4"
        FROM
            test_schema.test_table AS "root"'''

        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

        frame['col6'] = datetime(2024, 1, 1, 12, 30, 0)
        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col5:c|$c.col5, "
            "col6:c|$c.col6, col7:c|$c.col7, col4:c|%2024-01-01T12:30:00])\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col5:c|$c.col5, "
            "col6:c|%2024-01-01T12:30:00, col7:c|$c.col7, col4:c|$c.col4])"
        )

        expected_sql = '''\
        SELECT
            "root".col1 AS "col1",
            "root".col2 AS "col2",
            "root".col3 AS "col3",
            "root".col5 AS "col5",
            CAST('2024-01-01T12:30:00' AS TIMESTAMP) AS "col6",
            "root".col7 AS "col7",
            CAST('2024-01-01T12:30:00' AS TIMESTAMP) AS "col4"
        FROM
            test_schema.test_table AS "root"'''

        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

    def test_implicit_function_lambda(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2"),
            PrimitiveTdsColumn.float_column("col3"),
            PrimitiveTdsColumn.date_column("col5"),
            PrimitiveTdsColumn.datetime_column("col6"),
            PrimitiveTdsColumn.integer_column("col7")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        frame['col7'] = lambda x: x["col1"] + x["col2"]  # type: ignore
        expected_sql = '''\
        SELECT
            "root".col1 AS "col1",
            "root".col2 AS "col2",
            "root".col3 AS "col3",
            "root".col5 AS "col5",
            "root".col6 AS "col6",
            ("root".col1 + "root".col2) AS "col7"
        FROM
            test_schema.test_table AS "root"'''

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, col3:c|$c.col3, col5:c|$c.col5, col6:c|$c.col6, "
            "col7:c|(toOne($c.col1) + toOne($c.col2))])"
        )

        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

    def test_e2e_integer(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame['Age'] = frame['Age'] + 1  # type: ignore
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Smith", 24, "Firm X"]},
                {"values": ["John", "Johnson", 23, "Firm X"]},
                {"values": ["John", "Hill", 13, "Firm X"]},
                {"values": ["Anthony", "Allen", 23, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 35, "Firm A"]},
                {"values": ["Oliver", "Hill", 33, "Firm B"]},
                {"values": ["David", "Harris", 36, "Firm C"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        frame['Age'] = frame['Age'] * 2  # type: ignore
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Smith", 48, "Firm X"]},
                {"values": ["John", "Johnson", 46, "Firm X"]},
                {"values": ["John", "Hill", 26, "Firm X"]},
                {"values": ["Anthony", "Allen", 46, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 70, "Firm A"]},
                {"values": ["Oliver", "Hill", 66, "Firm B"]},
                {"values": ["David", "Harris", 72, "Firm C"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        frame['Age'] = 0
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Smith", 0, "Firm X"]},
                {"values": ["John", "Johnson", 0, "Firm X"]},
                {"values": ["John", "Hill", 0, "Firm X"]},
                {"values": ["Anthony", "Allen", 0, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 0, "Firm A"]},
                {"values": ["Oliver", "Hill", 0, "Firm B"]},
                {"values": ["David", "Harris", 0, "Firm C"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_string(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame['First Name'] = frame['Last Name']  # type: ignore
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Smith", "Smith", 23, "Firm X"]},
                {"values": ["Johnson", "Johnson", 22, "Firm X"]},
                {"values": ["Hill", "Hill", 12, "Firm X"]},
                {"values": ["Allen", "Allen", 22, "Firm X"]},
                {"values": ["Roberts", "Roberts", 34, "Firm A"]},
                {"values": ["Hill", "Hill", 32, "Firm B"]},
                {"values": ["Harris", "Harris", 35, "Firm C"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        frame['Firm/Legal Name'] = frame['First Name'] + frame['Last Name']  # type: ignore
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Smith", "Smith", 23, "SmithSmith"]},
                {"values": ["Johnson", "Johnson", 22, "JohnsonJohnson"]},
                {"values": ["Hill", "Hill", 12, "HillHill"]},
                {"values": ["Allen", "Allen", 22, "AllenAllen"]},
                {"values": ["Roberts", "Roberts", 34, "RobertsRoberts"]},
                {"values": ["Hill", "Hill", 32, "HillHill"]},
                {"values": ["Harris", "Harris", 35, "HarrisHarris"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        frame['Last Name'] = frame['Last Name'].upper()  # type: ignore
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Smith", "SMITH", 23, "SmithSmith"]},
                {"values": ["Johnson", "JOHNSON", 22, "JohnsonJohnson"]},
                {"values": ["Hill", "HILL", 12, "HillHill"]},
                {"values": ["Allen", "ALLEN", 22, "AllenAllen"]},
                {"values": ["Roberts", "ROBERTS", 34, "RobertsRoberts"]},
                {"values": ["Hill", "HILL", 32, "HillHill"]},
                {"values": ["Harris", "HARRIS", 35, "HarrisHarris"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        frame['lower'] = frame['First Name'].lower()  # type: ignore
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name", "lower"],
            "rows": [
                {"values": ["Smith", "SMITH", 23, "SmithSmith", "smith"]},
                {"values": ["Johnson", "JOHNSON", 22, "JohnsonJohnson", "johnson"]},
                {"values": ["Hill", "HILL", 12, "HillHill", "hill"]},
                {"values": ["Allen", "ALLEN", 22, "AllenAllen", "allen"]},
                {"values": ["Roberts", "ROBERTS", 34, "RobertsRoberts", "roberts"]},
                {"values": ["Hill", "HILL", 32, "HillHill", "hill"]},
                {"values": ["Harris", "HARRIS", 35, "HarrisHarris", "harris"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected


class TestSeriesArithmetic:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_series_class_type(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.datetime_column("col3")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        col1_plus_10_series = frame["col1"] + 10  # type: ignore[operator]
        assert type(col1_plus_10_series).__name__ == "NumberSeries"

        col2_len_series = frame["col2"].len()  # type: ignore[union-attr]
        assert type(col2_len_series).__name__ == "IntegerSeries"

        col3_year_series = frame["col3"].year()  # type: ignore[union-attr]
        assert type(col3_year_series).__name__ == "IntegerSeries"

        assert type(frame["col2"]).__name__ == "StringSeries"
        frame["col2"] = frame["col2"].parse_float()  # type: ignore[union-attr]
        assert type(frame["col2"]).__name__ == "FloatSeries"

    def test_arithmetic(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.datetime_column("col3")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        col1_series = frame["col1"] + 5 + 10  # type: ignore[operator]

        expected_sql = '''
            SELECT
                (("root".col1 + 5) + 10) AS "col1"
            FROM
                test_schema.test_table AS "root"
        '''
        expected_pure_pretty = '''
            #Table(test_schema.test_table)#
              ->project(~[col1:c|((toOne($c.col1) + 5) + 10)])
        '''
        expected_pure = '''
            #Table(test_schema.test_table)#->project(~[col1:c|((toOne($c.col1) + 5) + 10)])
        '''

        assert col1_series.to_sql_query() == dedent(expected_sql).strip()  # type: ignore[attr-defined]
        assert (
            generate_pure_query_and_compile(
                col1_series, FrameToPureConfig(), self.legend_client  # type: ignore[arg-type]
            )
            == dedent(expected_pure_pretty).strip()
        )
        assert (
            generate_pure_query_and_compile(
                col1_series, FrameToPureConfig(pretty=False), self.legend_client  # type: ignore[arg-type]
            )
            == dedent(expected_pure).strip()
        )

    def test_data_type_conversion(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.datetime_column("col3"),
            PrimitiveTdsColumn.strictdate_column("col4")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        col2_series = frame["col2"].len()  # type: ignore[union-attr]

        expected_sql = '''
            SELECT
                CHAR_LENGTH("root".col2) AS "col2"
            FROM
                test_schema.test_table AS "root"
        '''
        expected_pure_pretty = '''
            #Table(test_schema.test_table)#
              ->project(~[col2:c|toOne($c.col2)->length()])
        '''
        expected_pure = '''
            #Table(test_schema.test_table)#->project(~[col2:c|toOne($c.col2)->length()])
        '''

        assert col2_series.to_sql_query() == dedent(expected_sql).strip()
        assert (
            generate_pure_query_and_compile(
                col2_series, FrameToPureConfig(), self.legend_client
            )
            == dedent(expected_pure_pretty).strip()
        )
        assert (
            generate_pure_query_and_compile(
                col2_series, FrameToPureConfig(pretty=False), self.legend_client
            )
            == dedent(expected_pure).strip()
        )

    def test_multiple_columns_arithmetic(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.datetime_column("col3"),
            PrimitiveTdsColumn.strictdate_column("col4")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        combined_series = frame["col3"].year() + frame["col4"].year()  # type: ignore[union-attr]

        expected_sql = '''
            SELECT
                (DATE_PART('year', "root".col3) + DATE_PART('year', "root".col4)) AS "col3"
            FROM
                test_schema.test_table AS "root"
        '''
        expected_pure_pretty = '''
            #Table(test_schema.test_table)#
              ->project(~[col3:c|(toOne($c.col3)->year() + toOne($c.col4)->year())])
        '''
        expected_pure = '''
            #Table(test_schema.test_table)#->project(~[col3:c|(toOne($c.col3)->year() + toOne($c.col4)->year())])
        '''  # noqa: E501

        assert combined_series.to_sql_query() == dedent(expected_sql).strip()
        assert (
            generate_pure_query_and_compile(
                combined_series, FrameToPureConfig(), self.legend_client
            )
            == dedent(expected_pure_pretty).strip()
        )
        assert (
            generate_pure_query_and_compile(
                combined_series, FrameToPureConfig(pretty=False), self.legend_client
            )
            == dedent(expected_pure).strip()
        )

    def test_e2e_assign(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        frame["First Name"] = frame["First Name"].len()  # type: ignore[union-attr]
        frame["Last Name"] = frame["Last Name"].len()  # type: ignore[union-attr]
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": [5, 5, 23, "Firm X"]},
                {"values": [4, 7, 22, "Firm X"]},
                {"values": [4, 4, 12, "Firm X"]},
                {"values": [7, 5, 22, "Firm X"]},
                {"values": [7, 7, 34, "Firm A"]},
                {"values": [6, 4, 32, "Firm B"]},
                {"values": [5, 6, 35, "Firm C"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
