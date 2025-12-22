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

from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile


class TestDropnaFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_dropna_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(RuntimeError) as r:
            frame.assign(newcol=lambda x: [1, 2])  # type: ignore
        assert r.value.args[0] == "Type not supported"

    def test_drop(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.assign(sumColumn=lambda x: x.get_integer("col1") + x.get_integer("col2"))

        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                ("root".col1 + "root".col2) AS "sumColumn"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, sumColumn:c|(toOne($c.col1) + toOne($c.col2))])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        frame = frame.assign(col1=lambda x: x['col2']+5)  # type: ignore
        expected_pure = (
            "#Table(test_schema.test_table)#\n"
            "  ->project(~[col1:c|$c.col1, col2:c|$c.col2, sumColumn:c|(toOne($c.col1) + "
            "toOne($c.col2))])\n"
            "  ->project(~[col1:c|(toOne($c.col2) + 5), "
            "col2:c|$c.col2, sumColumn:c|$c.sumColumn])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)
        expected_sql = (
            'SELECT\n'
            '    ("root".col2 + 5) AS "col1",\n'
            '    "root".col2 AS "col2",\n'
            '    ("root".col1 + "root".col2) AS "sumColumn"\n'
            'FROM\n'
            '    test_schema.test_table AS "root"'
        )
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)