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
import pytest
from pylegend._typing import (
    PyLegendUnion,
    PyLegendDict,
)
from pylegend import LegendClient
from pylegend.extensions.tds.legendql_api.frames.legendql_api_csv_input_frame import (
    LegendQLApiCsvNonExecutableInputTdsFrame,
)
from tests.test_helpers import generate_pure_query_and_compile
from pylegend.core.tds.tds_frame import FrameToPureConfig


class TestLegendQLApiCsvInputFrame:
    test_csv_string = 'id,grp,name\n1,1,A\n3,1,B'

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_csv_non_executable_input_frame_creation(self) -> None:
        frame = LegendQLApiCsvNonExecutableInputTdsFrame(
            csv_string=self.test_csv_string
        )

        expected_pure = (
            '#TDS\n'
            'id,grp,name\n'
            '1,1,A\n'
            '3,1,B#'
        )
        assert frame.get_all_tds_frames()[0].to_pure_query() == expected_pure
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_csv_non_executable_input_frame_sql_generation_error(self) -> None:
        frame = LegendQLApiCsvNonExecutableInputTdsFrame(
            csv_string=self.test_csv_string
        )

        with pytest.raises(RuntimeError) as v:
            frame.to_sql_query()
        assert v.value.args[0] == "SQL generation for csv tds frames is not supported yet."

        expected_pure = (
            '#TDS\n'
            'id,grp,name\n'
            '1,1,A\n'
            '3,1,B#'
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == expected_pure

    def test_csv_non_executable_input_frame_extend(self) -> None:
        frame = LegendQLApiCsvNonExecutableInputTdsFrame(
            csv_string=self.test_csv_string
        )

        expected_pure = (
            '#TDS\n'
            'id,grp,name\n'
            '1,1,A\n'
            '3,1,B#\n'
            '  ->extend(~col4:{r | toOne($r.id) + 1})'
        )

        assert generate_pure_query_and_compile(
            frame.extend(("col4", lambda r: r.get_integer('id') + 1)),
            FrameToPureConfig(),
            self.legend_client) == expected_pure
