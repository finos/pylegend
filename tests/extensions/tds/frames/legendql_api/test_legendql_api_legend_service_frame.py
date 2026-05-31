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
from typing import List
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.project_cooridnates import VersionedProjectCoordinates
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.extensions.tds.legendql_api.frames.legendql_api_legend_service_input_frame import (
    LegendQLApiLegendServiceInputFrame,
)
from tests.test_helpers.test_legend_service_frames import (
    simple_person_service_frame_legendql_api,
    simple_trade_service_frame_legendql_api,
    simple_product_service_frame_legendql_api,
)


class TestLegendQLApiLegendServiceFrame:

    def test_legendql_api_legend_service_frame_sql_gen(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
        sql = frame.to_sql_query(FrameToSqlConfig())

        expected = '''\
        SELECT
            "root"."First Name" AS "First Name",
            "root"."Last Name" AS "Last Name",
            "root"."Age" AS "Age",
            "root"."Firm/Legal Name" AS "Firm/Legal Name"
        FROM
            service(
                pattern => '/simplePersonService',
                coordinates => 'org.finos.legend.pylegend:pylegend-test-models:0.0.1-SNAPSHOT'
            ) AS "root"'''

        assert sql == dedent(expected)

    def test_legendql_api_legend_person_service_frame_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = simple_person_service_frame_legendql_api(legend_test_server["engine_port"])
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

    def test_legendql_api_legend_trade_service_frame_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = simple_trade_service_frame_legendql_api(legend_test_server["engine_port"])
        res = frame.execute_frame_to_string()
        expected = {'columns': ['Id',
                                'Date',
                                'Quantity',
                                'Settlement Date Time',
                                'Product/Name',
                                'Account/Name'],
                    'rows': [{'values': [1,
                                         '2014-12-01',
                                         25.0,
                                         '2014-12-02T21:00:00.000000000+0000',
                                         'Firm X',
                                         'Account 1']},
                             {'values': [2,
                                         '2014-12-01',
                                         320.0,
                                         '2014-12-02T21:00:00.000000000+0000',
                                         'Firm X',
                                         'Account 2']},
                             {'values': [3,
                                         '2014-12-01',
                                         11.0,
                                         '2014-12-02T21:00:00.000000000+0000',
                                         'Firm A',
                                         'Account 1']},
                             {'values': [4,
                                         '2014-12-02',
                                         23.0,
                                         '2014-12-03T21:00:00.000000000+0000',
                                         'Firm A',
                                         'Account 2']},
                             {'values': [5,
                                         '2014-12-02',
                                         32.0,
                                         '2014-12-03T21:00:00.000000000+0000',
                                         'Firm A',
                                         'Account 1']},
                             {'values': [6,
                                         '2014-12-03',
                                         27.0,
                                         '2014-12-04T21:00:00.000000000+0000',
                                         'Firm C',
                                         'Account 1']},
                             {'values': [7,
                                         '2014-12-03',
                                         44.0,
                                         '2014-12-04T15:22:23.123456789+0000',
                                         'Firm C',
                                         'Account 1']},
                             {'values': [8,
                                         '2014-12-04',
                                         22.0,
                                         '2014-12-05T21:00:00.000000000+0000',
                                         'Firm C',
                                         'Account 2']},
                             {'values': [9,
                                         '2014-12-04',
                                         45.0,
                                         '2014-12-05T21:00:00.000000000+0000',
                                         'Firm C',
                                         'Account 2']},
                             {'values': [10,
                                         '2014-12-04',
                                         38.0,
                                         None,
                                         'Firm C',
                                         'Account 2']},
                             {'values': [11,
                                         '2014-12-05',
                                         5.0,
                                         None,
                                         None,
                                         None]}]}
        assert json.loads(res)["result"] == expected

    def test_legendql_api_legend_product_service_frame_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = simple_product_service_frame_legendql_api(legend_test_server["engine_port"])
        res = frame.execute_frame_to_string()
        expected = {'columns': ['Name', 'Synonyms/Name', 'Synonyms/Type'],
                    'rows': [{'values': ['Firm X', 'CUSIP1', 'CUSIP']},
                             {'values': ['Firm X', 'ISIN1', 'ISIN']},
                             {'values': ['Firm A', 'CUSIP2', 'CUSIP']},
                             {'values': ['Firm A', 'ISIN2', 'ISIN']},
                             {'values': ['Firm C', 'CUSIP3', 'CUSIP']},
                             {'values': ['Firm C', 'ISIN3', 'ISIN']},
                             {'values': ['Firm D', None, None]}]}
        assert json.loads(res)["result"] == expected

    def test_legendql_api_legend_person_service_frame_pure_gen(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = LegendQLApiLegendServiceInputFrame(
            pattern="/simplePersonService",
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
        assert frame.to_pure(FrameToPureConfig()) == "|pylegend::test::SimplePersonService.all()"

    def test_legendql_api_legend_person_service_frame_pure_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = LegendQLApiLegendServiceInputFrame(
            pattern="/simplePersonService",
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

    def test_legendql_api_legend_trade_service_frame_pure_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = LegendQLApiLegendServiceInputFrame(
            pattern="/simpleTradeService",
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
        result = json.loads(res)["result"]
        rows = result["rows"]
        assert len(rows) == 11
        assert rows[0]["values"][0] == 1

    def test_legendql_api_legend_product_service_frame_pure_execution(
            self,
            legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]
    ) -> None:
        frame = LegendQLApiLegendServiceInputFrame(
            pattern="/simpleProductService",
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
        result = json.loads(res)["result"]
        rows = result["rows"]
        assert rows[0]["values"] == ["Firm X", "CUSIP1", "CUSIP"]

    def test_legendql_api_legend_person_service_frame_pure_execution_uses_execute_pure_string(
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

        frame = LegendQLApiLegendServiceInputFrame(
            pattern="/simplePersonService",
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
