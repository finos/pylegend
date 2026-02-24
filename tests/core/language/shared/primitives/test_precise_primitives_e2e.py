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

import pytest

from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn, PrimitiveType
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend.core.request.legend_client import LegendClient


class TestPrecisePrimitivesE2E:

    _AGE_PLUS_1_ROWS = [
        {"values": ["Peter", "Smith", 24, "Firm X"]},
        {"values": ["John", "Johnson", 23, "Firm X"]},
        {"values": ["John", "Hill", 13, "Firm X"]},
        {"values": ["Anthony", "Allen", 23, "Firm X"]},
        {"values": ["Fabrice", "Roberts", 35, "Firm A"]},
        {"values": ["Oliver", "Hill", 33, "Firm B"]},
        {"values": ["David", "Harris", 36, "Firm C"]},
    ]

    _AGE_MINUS_5_ROWS = [
        {"values": ["Peter", "Smith", 18, "Firm X"]},
        {"values": ["John", "Johnson", 17, "Firm X"]},
        {"values": ["John", "Hill", 7, "Firm X"]},
        {"values": ["Anthony", "Allen", 17, "Firm X"]},
        {"values": ["Fabrice", "Roberts", 29, "Firm A"]},
        {"values": ["Oliver", "Hill", 27, "Firm B"]},
        {"values": ["David", "Harris", 30, "Firm C"]},
    ]

    _AGE_TIMES_2_ROWS = [
        {"values": ["Peter", "Smith", 46, "Firm X"]},
        {"values": ["John", "Johnson", 44, "Firm X"]},
        {"values": ["John", "Hill", 24, "Firm X"]},
        {"values": ["Anthony", "Allen", 44, "Firm X"]},
        {"values": ["Fabrice", "Roberts", 68, "Firm A"]},
        {"values": ["Oliver", "Hill", 64, "Firm B"]},
        {"values": ["David", "Harris", 70, "Firm C"]},
    ]

    _COLUMNS = ["First Name", "Last Name", "Age", "Firm/Legal Name"]

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_e2e_tinyint_addition(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"Age": PrimitiveType.TinyInt})
        frame['Age'] = frame['Age'] + 1  # type: ignore
        expected = {"columns": self._COLUMNS, "rows": self._AGE_PLUS_1_ROWS}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_utinyint_subtraction(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"Age": PrimitiveType.UTinyInt})
        frame['Age'] = frame['Age'] - 5  # type: ignore
        expected = {"columns": self._COLUMNS, "rows": self._AGE_MINUS_5_ROWS}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_smallint_addition(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"Age": PrimitiveType.SmallInt})
        frame['Age'] = frame['Age'] + 1  # type: ignore
        expected = {"columns": self._COLUMNS, "rows": self._AGE_PLUS_1_ROWS}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_usmallint_multiplication(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"Age": PrimitiveType.USmallInt})
        frame['Age'] = frame['Age'] * 2  # type: ignore
        expected = {"columns": self._COLUMNS, "rows": self._AGE_TIMES_2_ROWS}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_int_addition(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"Age": PrimitiveType.Int})
        frame['Age'] = frame['Age'] + 1  # type: ignore
        expected = {"columns": self._COLUMNS, "rows": self._AGE_PLUS_1_ROWS}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_uint_subtraction(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"Age": PrimitiveType.UInt})
        frame['Age'] = frame['Age'] - 5  # type: ignore
        expected = {"columns": self._COLUMNS, "rows": self._AGE_MINUS_5_ROWS}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_bigint_addition(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"Age": PrimitiveType.BigInt})
        frame['Age'] = frame['Age'] + 1  # type: ignore
        expected = {"columns": self._COLUMNS, "rows": self._AGE_PLUS_1_ROWS}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_ubigint_multiplication(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"Age": PrimitiveType.UBigInt})
        frame['Age'] = frame['Age'] * 2  # type: ignore
        expected = {"columns": self._COLUMNS, "rows": self._AGE_TIMES_2_ROWS}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_varchar_string_length(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"First Name": PrimitiveType.Varchar})
        frame["First Name"] = frame["First Name"].len()  # type: ignore[union-attr]
        expected = {
            "columns": self._COLUMNS,
            "rows": [
                {"values": [5, "Smith", 23, "Firm X"]},
                {"values": [4, "Johnson", 22, "Firm X"]},
                {"values": [4, "Hill", 12, "Firm X"]},
                {"values": [7, "Allen", 22, "Firm X"]},
                {"values": [7, "Roberts", 34, "Firm A"]},
                {"values": [6, "Hill", 32, "Firm B"]},
                {"values": [5, "Harris", 35, "Firm C"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_timestamp_cast(self) -> None:
        columns = [
            PrimitiveTdsColumn.datetime_column("event_time"),
            PrimitiveTdsColumn.string_column("name"),
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        result = frame.cast({"event_time": PrimitiveType.Timestamp})
        assert "TdsColumn(Name: event_time, Type: Timestamp)" == str(result[0])
        assert "TdsColumn(Name: name, Type: String)" == str(result[1])

    def test_e2e_float4_addition(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"Age": PrimitiveType.Float4})
        frame['Age'] = frame['Age'] + 0.5  # type: ignore
        expected = {
            "columns": self._COLUMNS,
            "rows": [
                {"values": ["Peter", "Smith", 23.5, "Firm X"]},
                {"values": ["John", "Johnson", 22.5, "Firm X"]},
                {"values": ["John", "Hill", 12.5, "Firm X"]},
                {"values": ["Anthony", "Allen", 22.5, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34.5, "Firm A"]},
                {"values": ["Oliver", "Hill", 32.5, "Firm B"]},
                {"values": ["David", "Harris", 35.5, "Firm C"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_double_subtraction(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"Age": PrimitiveType.Double})
        frame['Age'] = frame['Age'] - 0.5  # type: ignore
        expected = {
            "columns": self._COLUMNS,
            "rows": [
                {"values": ["Peter", "Smith", 22.5, "Firm X"]},
                {"values": ["John", "Johnson", 21.5, "Firm X"]},
                {"values": ["John", "Hill", 11.5, "Firm X"]},
                {"values": ["Anthony", "Allen", 21.5, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 33.5, "Firm A"]},
                {"values": ["Oliver", "Hill", 31.5, "Firm B"]},
                {"values": ["David", "Harris", 34.5, "Firm C"]},
            ],
        }
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_numeric_addition(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])
        frame.cast({"Age": PrimitiveType.Numeric})
        frame['Age'] = frame['Age'] + 1  # type: ignore
        expected = {"columns": self._COLUMNS, "rows": self._AGE_PLUS_1_ROWS}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
