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


class TestRestrictAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_query_gen_restrict_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.restrict(["col1"])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == "[TdsColumn(Name: col1, Type: Integer)]"
        expected = '''\
            SELECT
                "root".col1 AS "col1"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[col1])')

    @pytest.mark.skip(reason="Quoted select col spec not supported by server")
    def test_query_gen_restrict_function_with_col_name_with_spaces(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1 with spaces"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.restrict(['col1 with spaces'])
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~['col1 with spaces'])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[\'col1 with spaces\'])')

    def test_query_gen_restrict_function_column_order(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.restrict(["col2", "col1"])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: col2, Type: String), TdsColumn(Name: col1, Type: Integer)]"
        expected = '''\
            SELECT
                "root".col2 AS "col2",
                "root".col1 AS "col1"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->select(~[col2, col1])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->select(~[col2, col1])')

    def test_restrict_error_on_unknown_col(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame.restrict(["unknown_col"])
        assert r.value.args[0] == "Column - 'unknown_col' in restrict columns list doesn't exist in the current frame."\
                                  " Current frame columns: ['col1', 'col2']"

    def test_query_gen_restrict_function_after_distinct_creates_subquery(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegacyApiTdsFrame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.distinct()
        frame = frame.restrict(["col1"])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == "[TdsColumn(Name: col1, Type: Integer)]"
        expected = '''\
            SELECT
                "root"."col1" AS "col1"
            FROM
                (
                    SELECT DISTINCT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->distinct()
              ->select(~[col1])'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->distinct()->select(~[col1])')

    def test_e2e_restrict_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegacyApiTdsFrame = simple_person_service_frame_legacy_api(legend_test_server["engine_port"])
        frame = frame.take(5)
        frame = frame.restrict(["First Name", "Firm/Legal Name"])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Firm/Legal Name, Type: String)]"
        expected = {'columns': ['First Name', 'Firm/Legal Name'],
                    'rows': [{'values': ['Peter', 'Firm X']},
                             {'values': ['John', 'Firm X']},
                             {'values': ['John', 'Firm X']},
                             {'values': ['Anthony', 'Firm X']},
                             {'values': ['Fabrice', 'Firm A']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_restrict_function_column_order(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) \
            -> None:
        frame: LegacyApiTdsFrame = simple_person_service_frame_legacy_api(legend_test_server["engine_port"])
        frame = frame.take(5)
        frame = frame.restrict(["Firm/Legal Name", "First Name"])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Firm/Legal Name, Type: String), TdsColumn(Name: First Name, Type: String)]"
        expected = {'columns': ['Firm/Legal Name', 'First Name'],
                    'rows': [{'values': ['Firm X', 'Peter']},
                             {'values': ['Firm X', 'John']},
                             {'values': ['Firm X', 'John']},
                             {'values': ['Firm X', 'Anthony']},
                             {'values': ['Firm A', 'Fabrice']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    # TODO: Add tests for subquery generation when groupBy/orderBy/having operations are present
