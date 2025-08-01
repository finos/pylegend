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
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legacy_api.frames.legacy_api_tds_frame import LegacyApiTdsFrame
from pylegend.extensions.tds.legacy_api.frames.legacy_api_table_spec_input_frame import LegacyApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_legacy_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile


class TestSliceAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_slice_error_on_param_values(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.slice(-1, 10)
        assert v.value.args[0] == "Start row argument of slice function cannot be negative. Start row: -1"

        with pytest.raises(ValueError) as v:
            frame.slice(0, -1)
        assert v.value.args[0] == \
               "End row argument of slice function cannot be less than or equal to start row argument. " \
               "Start row: 0, End row: -1"

    def test_query_gen_slice_function_no_offset(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.slice(2, 10)
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            LIMIT 8
            OFFSET 2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->slice(2, 10)'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->slice(2, 10)')

    def test_query_gen_slice_function_existing_offset(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.drop(10)
        frame = frame.slice(2, 10)
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    OFFSET 10
                ) AS "root"
            LIMIT 8
            OFFSET 2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->drop(10)
              ->slice(2, 10)'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->drop(10)->slice(2, 10)')

    def test_e2e_slice_function_no_offset(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegacyApiTdsFrame = simple_person_service_frame_legacy_api(legend_test_server["engine_port"])
        frame = frame.slice(2, 5)
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_slice_function_existing_offset(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) \
            -> None:
        frame: LegacyApiTdsFrame = simple_person_service_frame_legacy_api(legend_test_server["engine_port"])
        frame = frame.drop(3)
        frame = frame.slice(1, 3)
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Fabrice', 'Roberts', 34, 'Firm A']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
