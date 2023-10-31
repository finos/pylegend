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
from textwrap import dedent
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.project_cooridnates import VersionedProjectCoordinates
from pylegend.extensions.tds.legend_api.frames.legend_api_legend_function_input_frame import (
    LegendApiLegendFunctionInputFrame,
)


class TestLegendApiLegendFunctionFrame:

    def test_legend_api_legend_function_frame_sql_gen(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = LegendApiLegendFunctionInputFrame(
            path="pylegend::test::function::SimplePersonFunction__TabularDataSet_1_",
            project_coordinates=VersionedProjectCoordinates(
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-test-models",
                version="0.0.1-SNAPSHOT"
            ),
            legend_client=LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)
        )
        sql = frame.to_sql_query(FrameToSqlConfig())

        expected = '''\
        SELECT
            "root"."First Name" AS "First Name",
            "root"."Last Name" AS "Last Name",
            "root"."Age" AS "Age",
            "root"."Firm/Legal Name" AS "Firm/Legal Name"
        FROM
            func(
                path => 'pylegend::test::function::SimplePersonFunction__TabularDataSet_1_',
                coordinates => 'org.finos.legend.pylegend:pylegend-test-models:0.0.1-SNAPSHOT'
            ) AS "root"'''

        assert sql == dedent(expected)

    def test_legend_api_legend_function_frame_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = LegendApiLegendFunctionInputFrame(
            path="pylegend::test::function::SimplePersonFunction__TabularDataSet_1_",
            project_coordinates=VersionedProjectCoordinates(
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-test-models",
                version="0.0.1-SNAPSHOT"
            ),
            legend_client=LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)
        )
        res = frame.execute_frame_to_string()
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X']},
                             {'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B']},
                             {'values': ['David', 'Harris', 35, 'Firm C']}]}
        assert json.loads(res)["result"] == expected
