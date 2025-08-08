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
import pytest
from textwrap import dedent
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame import LegendQLApiTdsFrame
from pylegend.extensions.tds.legendql_api.frames.legendql_api_table_spec_input_frame import LegendQLApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_legendql_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile


class TestRenameColumnsAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_rename_columns_error_incorrect(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as v:
            frame.rename(lambda r: [r.col2])  # type: ignore
        assert v.value.args[0] == \
               ('Sort lambda incompatible. Each element in rename list should be a tuple with '
                'first element being a string (existing column name) or a simple column '
                'expression and second element being a string (renamed column name). E.g - '
                "frame.rename(lambda r: [('c1', 'nc1'), (r.c2, 'nc2')]). Element at index 0 in "
                'the list is incompatible.')

    def test_rename_columns_error_on_duplicates_in_columns(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.rename(lambda r: [(r.col1, 'col3'), (r.col1, 'col4')])
        assert v.value.args[0] == \
               ("column_names list shouldn't have duplicates when renaming columns.\n"
                "column_names list - (Count: 2) - ['col1', 'col1']\n")

    def test_rename_columns_error_on_duplicates_in_renamed_columns(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.rename(lambda r: [(r.col1, 'col3'), (r.col2, 'col3')])
        assert v.value.args[0] == \
               ("renamed_column_names_list list shouldn't have duplicates when renaming columns.\n"
                "renamed_column_names_list - (Count: 2) - ['col3', 'col3']\n")

    def test_query_gen_rename_columns_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        for variation in [
            lambda r: (r.col2, 'col3'),
            lambda r: [(r['col2'], 'col3')],
            lambda r: ('col2', 'col3'),
            lambda r: [('col2', 'col3')],
        ]:
            frame1 = frame.rename(variation)  # type: ignore
            assert "[" + ", ".join([str(c) for c in frame1.columns()]) + "]" == \
                   "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col3, Type: String)]"
            expected = '''\
                SELECT
                    "root".col1 AS "col1",
                    "root".col2 AS "col3"
                FROM
                    test_schema.test_table AS "root"'''
            assert frame1.to_sql_query(FrameToSqlConfig()) == dedent(expected)
            assert generate_pure_query_and_compile(frame1, FrameToPureConfig(), self.legend_client) == dedent(
                '''\
                #Table(test_schema.test_table)#
                  ->rename(~col2, ~col3)'''
            )
            assert generate_pure_query_and_compile(frame1, FrameToPureConfig(pretty=False), self.legend_client) == \
                   ('#Table(test_schema.test_table)#->rename(~col2, ~col3)')

        frame = frame.rename(lambda r: [("col1", "col4"), (r.col2, "col5 with spaces")])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: col4, Type: Integer), TdsColumn(Name: col5 with spaces, Type: String)]"
        expected = '''\
            SELECT
                "root".col1 AS "col4",
                "root".col2 AS "col5 with spaces"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->rename(~col1, ~col4)
              ->rename(~col2, ~'col5 with spaces')'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#'
                '->rename(~col1, ~col4)->rename(~col2, ~\'col5 with spaces\')')

    def test_e2e_rename_columns_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.head(5)
        frame = frame.rename(lambda r: [(r["First Name"], "Name"), (r["Firm/Legal Name"], "Firm Name")])
        frame = frame.select(lambda r: ["Name", "Firm Name"])
        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == \
               "[TdsColumn(Name: Name, Type: String), TdsColumn(Name: Firm Name, Type: String)]"
        expected = {'columns': ['Name', 'Firm Name'],
                    'rows': [{'values': ['Peter', 'Firm X']},
                             {'values': ['John', 'Firm X']},
                             {'values': ['John', 'Firm X']},
                             {'values': ['Anthony', 'Firm X']},
                             {'values': ['Fabrice', 'Firm A']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
