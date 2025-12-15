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
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
import pytest
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api


class TestHeadFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_head_error(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        # type error
        with pytest.raises(TypeError) as t:
            frame.head("5")
        assert t.value.args[0] == "n must be an int, got str"

        # negative n
        with pytest.raises(NotImplementedError) as n:
            frame.head(-3)
        assert n.value.args[0] == "Negative n is not supported yet in Pandas API head"

    def test_head_sql_pure(self)  -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.head()

        expected_pure = dedent("""\
            #Table(test_schema.test_table)#
              ->slice(0, 5)""")

        expected_sql = dedent("""\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            LIMIT 5
            OFFSET 0""")

        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

        frame = frame.head(0)
        expected_pure = dedent("""\
            #Table(test_schema.test_table)#
              ->slice(0, 0)""")

        expected_sql = dedent("""\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            LIMIT 0
            OFFSET 0""")

        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql

