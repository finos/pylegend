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

import importlib
import json
import pytest
from textwrap import dedent
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_tds_frame import LegendApiTdsFrame
from pylegend.extensions.tds.legend_api.frames.legend_api_table_spec_input_frame import LegendApiTableSpecInputFrame
from pylegend.tests.test_helpers.legend_service_frame import simple_person_service_frame
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)

postgres_ext = 'pylegend.extensions.database.vendors.postgres.postgres_sql_to_string'
importlib.import_module(postgres_ext)


class TestDropAppliedFunction:

    def test_sql_gen_drop_function_no_offset(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.drop(10)
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            OFFSET 10'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_drop_function_existing_offset(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.drop(10)
        frame = frame.drop(20)
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
            OFFSET 20'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_sql_gen_drop_function_existing_top(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.limit(20)
        frame = frame.drop(10)
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
                    LIMIT 20
                ) AS "root"
            OFFSET 10'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    @pytest.mark.skip(reason="Offset not handled by server")  # TODO: Enable this test after server support is added
    def test_e2e_drop_function_no_offset(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.drop(3)
        expected = """\
        {
           "columns": [
              "First Name",
              "Last Name",
              "Age",
              "Firm/Legal Name"
           ],
           "rows": [
              {
                 "values": [
                    "Peter",
                    "Smith",
                    23,
                    "Firm X"
                 ]
              },
              {
                 "values": [
                    "John",
                    "Johnson",
                    22,
                    "Firm X"
                 ]
              },
              {
                 "values": [
                    "John",
                    "Hill",
                    12,
                    "Firm X"
                 ]
              }
           ]
        }"""
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == json.loads(dedent(expected))

    @pytest.mark.skip(reason="Offset not handled by server")  # TODO: Enable this test after server support is added
    def test_e2e_drop_function_existing_offset(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> \
            None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.drop(3)
        frame = frame.drop(1)
        expected = """\
        {
           "columns": [
              "First Name",
              "Last Name",
              "Age",
              "Firm/Legal Name"
           ],
           "rows": [
              {
                 "values": [
                    "Peter",
                    "Smith",
                    23,
                    "Firm X"
                 ]
              },
              {
                 "values": [
                    "John",
                    "Johnson",
                    22,
                    "Firm X"
                 ]
              },
              {
                 "values": [
                    "John",
                    "Hill",
                    12,
                    "Firm X"
                 ]
              }
           ]
        }"""
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == json.loads(dedent(expected))

    @pytest.mark.skip(reason="Offset not handled by server")  # TODO: Enable this test after server support is added
    def test_e2e_drop_function_existing_top(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> \
            None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.take(3)
        frame = frame.drop(1)
        expected = """\
        {
           "columns": [
              "First Name",
              "Last Name",
              "Age",
              "Firm/Legal Name"
           ],
           "rows": [
              {
                 "values": [
                    "Peter",
                    "Smith",
                    23,
                    "Firm X"
                 ]
              },
              {
                 "values": [
                    "John",
                    "Johnson",
                    22,
                    "Firm X"
                 ]
              },
              {
                 "values": [
                    "John",
                    "Hill",
                    12,
                    "Firm X"
                 ]
              }
           ]
        }"""
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == json.loads(dedent(expected))
