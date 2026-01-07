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


class TestDistinctAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_query_gen_distinct_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.integer_column("col3"),
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        distinct_frame = frame.distinct()
        expected = '''\
                    SELECT DISTINCT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2",
                        "root".col3 AS "col3"
                    FROM
                        test_schema.test_table AS "root"'''
        assert distinct_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                "root".col3 AS "col3"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(distinct_frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->distinct()'''
        )
        assert generate_pure_query_and_compile(distinct_frame, FrameToPureConfig(False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->distinct()')

        distinct_col1_frame = frame.distinct("col1")

        expected = '''\
                   SELECT DISTINCT
                       "root"."col1" AS "col1"
                   FROM
                       (
                           SELECT
                               "root".col1 AS "col1",
                               "root".col2 AS "col2",
                               "root".col3 AS "col3"
                           FROM
                               test_schema.test_table AS "root"
                       ) AS "root"'''
        assert distinct_col1_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert len(distinct_col1_frame.columns()) == 1 and distinct_col1_frame.columns()[0].get_name() == "col1"
        assert generate_pure_query_and_compile(distinct_col1_frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->distinct(~[col1])'''
        )

        distinct_col1_col2_frame = frame.distinct(['col1', 'col2'])

        expected = '''\
                   SELECT DISTINCT
                       "root"."col1" AS "col1",
                       "root"."col2" AS "col2"
                   FROM
                       (
                           SELECT
                               "root".col1 AS "col1",
                               "root".col2 AS "col2",
                               "root".col3 AS "col3"
                           FROM
                               test_schema.test_table AS "root"
                       ) AS "root"'''
        assert distinct_col1_col2_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert (
                len(distinct_col1_col2_frame.columns()) == 2 and
                {col.get_name() for col in distinct_col1_col2_frame.columns()} == {"col1", "col2"})
        assert generate_pure_query_and_compile(distinct_col1_col2_frame, FrameToPureConfig(),
                                               self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->distinct(~[col1, col2])'''
        )

        distinct_all_col_frame = frame.distinct(lambda r: [r.col3, r["col1"], r.col2])

        expected = '''\
                   SELECT DISTINCT
                       "root"."col3" AS "col3",
                       "root"."col1" AS "col1",
                       "root"."col2" AS "col2"
                   FROM
                       (
                           SELECT
                               "root".col1 AS "col1",
                               "root".col2 AS "col2",
                               "root".col3 AS "col3"
                           FROM
                               test_schema.test_table AS "root"
                       ) AS "root"'''
        assert distinct_all_col_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert len(distinct_all_col_frame.columns()) == 3 and {col.get_name() for col in
                                                               distinct_all_col_frame.columns()} == {"col1", "col2",
                                                                                                     "col3"}
        assert generate_pure_query_and_compile(distinct_all_col_frame, FrameToPureConfig(),
                                               self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->distinct(~[col3, col1, col2])'''
        )

    def test_query_gen_distinct_function_existing_top(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.head(5)
        frame = frame.distinct()
        expected = '''\
            SELECT DISTINCT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 5
                ) AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->limit(5)
              ->distinct()'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table)#->limit(5)->distinct()')

    def test_e2e_distinct_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.select(["First Name", "Firm/Legal Name"])
        distinct_all_frame = frame.distinct()
        distinct_two_col_expected = {'columns': ['First Name', 'Firm/Legal Name'],
                                     'rows': [{'values': ['Anthony', 'Firm X']},
                                              {'values': ['David', 'Firm C']},
                                              {'values': ['Fabrice', 'Firm A']},
                                              {'values': ['John', 'Firm X']},
                                              {'values': ['Oliver', 'Firm B']},
                                              {'values': ['Peter', 'Firm X']}]}
        distinct_all_res = distinct_all_frame.execute_frame_to_string()
        assert json.loads(distinct_all_res)["result"] == distinct_two_col_expected

        distinct_one_col_frame = frame.distinct("Firm/Legal Name")
        distinct_one_col_expected = {'columns': ['Firm/Legal Name'],
                                     'rows': [{'values': ['Firm A']},
                                              {'values': ['Firm B']},
                                              {'values': ['Firm C']},
                                              {'values': ['Firm X']}]}
        distinct_one_col_res = distinct_one_col_frame.execute_frame_to_string()
        assert json.loads(distinct_one_col_res)["result"] == distinct_one_col_expected

        distinct_two_col_frame = frame.distinct(lambda r: [r["First Name"], r["Firm/Legal Name"]])
        distinct_two_col_res = distinct_two_col_frame.execute_frame_to_string()
        assert json.loads(distinct_two_col_res)["result"] == distinct_two_col_expected

    def test_e2e_distinct_function_existing_top(self,
                                                legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: LegendQLApiTdsFrame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        frame = frame.select(["First Name", "Firm/Legal Name"])
        frame = frame.head(3)
        frame = frame.distinct()
        expected = {'columns': ['First Name', 'Firm/Legal Name'],
                    'rows': [{'values': ['John', 'Firm X']},
                             {'values': ['Peter', 'Firm X']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
