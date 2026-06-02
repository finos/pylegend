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
import os
import pytest
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.project_cooridnates import VersionedProjectCoordinates
from pylegend._typing import PyLegendDict, PyLegendUnion


class TestLegendClientE2E:

    @pytest.mark.skipif(os.environ.get("JAVA_HOME") is None, reason="JAVA_HOME unset; requires legend_test_server")
    def test_e2e_pure_schema_api(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        client = LegendClient(
            "localhost", legend_test_server["engine_port"], secure_http=False,
            depot_server_host="localhost", depot_server_port=legend_test_server["metadata_port"]
        )
        coords = VersionedProjectCoordinates(
            "org.finos.legend.pylegend", "pylegend-test-models", "0.0.1-SNAPSHOT"
        )
        res = client.get_pure_string_schema("|pylegend::test::SimplePersonService.all()", coords)
        assert ", ".join([str(x) for x in res]) == \
            "TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), " \
            "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Firm/Legal Name, Type: String)"

    @pytest.mark.skipif(os.environ.get("JAVA_HOME") is None, reason="JAVA_HOME unset; requires legend_test_server")
    def test_e2e_pure_execute_api(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        client = LegendClient(
            "localhost", legend_test_server["engine_port"], secure_http=False,
            depot_server_host="localhost", depot_server_port=legend_test_server["metadata_port"]
        )
        coords = VersionedProjectCoordinates(
            "org.finos.legend.pylegend", "pylegend-test-models", "0.0.1-SNAPSHOT"
        )
        res = client.execute_pure_string("|pylegend::test::SimplePersonService.all()", coords)
        parsed = json.loads(b"".join(res))
        assert parsed["result"]["columns"] == ["First Name", "Last Name", "Age", "Firm/Legal Name"]
        assert parsed["result"]["rows"][0]["values"] == ["Peter", "Smith", 23, "Firm X"]
        assert len(parsed["result"]["rows"]) == 7

    def test_e2e_parse_model_api(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)
        res = client.parse_model(
            "function my::test():Integer[1] {1 + 2}"
        )
        assert any([x["_type"] == "function" and x["name"] == "test__Integer_1_" for x in json.loads(res)["elements"]])

    def test_e2e_compile_api(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)
        client.parse_and_compile_model(
            "function my::test():Integer[1] {1 + 2}"
        )
