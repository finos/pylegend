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
from typing import List
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.project_cooridnates import VersionedProjectCoordinates
from pylegend.extensions.tds.legendql_api.frames.legendql_api_legend_function_input_frame import (
    LegendQLApiLegendFunctionInputFrame,
)


class TestLegendQLApiLegendFunctionFrame:

    def test_legendql_api_legend_function_frame_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = LegendQLApiLegendFunctionInputFrame(
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

    def test_legendql_api_legend_function_frame_pure_gen(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = LegendQLApiLegendFunctionInputFrame(
            path="pylegend::test::function::SimplePersonFunction__TabularDataSet_1_",
            project_coordinates=VersionedProjectCoordinates(
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-test-models",
                version="0.0.1-SNAPSHOT"
            ),
            legend_client=LegendClient(
                "localhost", legend_test_server["engine_port"], secure_http=False,
                depot_server_host="localhost", depot_server_port=legend_test_server["metadata_port"]
            )
        )
        assert (frame.to_pure(FrameToPureConfig()) ==
                "|pylegend::test::function::SimplePersonFunction__TabularDataSet_1_()")

    def test_legendql_api_legend_function_frame_pure_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = LegendQLApiLegendFunctionInputFrame(
            path="pylegend::test::function::SimplePersonFunction__TabularDataSet_1_",
            project_coordinates=VersionedProjectCoordinates(
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-test-models",
                version="0.0.1-SNAPSHOT"
            ),
            legend_client=LegendClient(
                "localhost", legend_test_server["engine_port"], secure_http=False,
                depot_server_host="localhost", depot_server_port=legend_test_server["metadata_port"]
            )
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

    def test_legendql_api_legend_function_frame_pure_execution_uses_execute_pure_string(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]],
            monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        pure_call_count: List[int] = [0]

        legend_client = LegendClient(
            "localhost", legend_test_server["engine_port"], secure_http=False,
            depot_server_host="localhost", depot_server_port=legend_test_server["metadata_port"]
        )

        original_execute_pure = legend_client.execute_pure_string

        def counting_execute_pure_string(pure: str, project_coordinates: object,
                                         chunk_size: object = None) -> object:
            pure_call_count[0] += 1
            return original_execute_pure(pure, project_coordinates, chunk_size=chunk_size)  # type: ignore[arg-type]

        def failing_execute_sql_string(sql: str, chunk_size: object = None) -> object:
            raise AssertionError("execute_sql_string must not be called on the Pure path")

        monkeypatch.setattr(legend_client, "execute_pure_string", counting_execute_pure_string)
        monkeypatch.setattr(legend_client, "execute_sql_string", failing_execute_sql_string)

        frame = LegendQLApiLegendFunctionInputFrame(
            path="pylegend::test::function::SimplePersonFunction__TabularDataSet_1_",
            project_coordinates=VersionedProjectCoordinates(
                group_id="org.finos.legend.pylegend",
                artifact_id="pylegend-test-models",
                version="0.0.1-SNAPSHOT"
            ),
            legend_client=legend_client
        )
        res = frame.execute_frame_to_string()
        assert pure_call_count[0] >= 1
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['Peter', 'Smith', 23, 'Firm X']},
                             {'values': ['John', 'Johnson', 22, 'Firm X']},
                             {'values': ['John', 'Hill', 12, 'Firm X']},
                             {'values': ['Anthony', 'Allen', 22, 'Firm X']},
                             {'values': ['Fabrice', 'Roberts', 34, 'Firm A']},
                             {'values': ['Oliver', 'Hill', 32, 'Firm B']},
                             {'values': ['David', 'Harris', 35, 'Firm C']}]}
        assert json.loads(res)["result"] == expected
