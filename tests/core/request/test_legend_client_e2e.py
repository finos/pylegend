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
from pylegend.core.request.legend_client import LegendClient
from pylegend._typing import PyLegendDict, PyLegendUnion


class TestLegendClientE2E:

    def test_e2e_schema_string_api(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)
        res = client.get_sql_string_schema(
            "SELECT * FROM "
            "   service("
            "       pattern => '/simplePersonService', "
            "       coordinates => 'org.finos.legend.pylegend:pylegend-test-models:0.0.1-SNAPSHOT'"
            "   )"
        )

        assert ", ".join([str(x) for x in res]) == \
            "TdsColumn(Name: First Name, Type: String), TdsColumn(Name: Last Name, Type: String), " \
            "TdsColumn(Name: Age, Type: Integer), TdsColumn(Name: Firm/Legal Name, Type: String)"

    def test_e2e_execute_string_api(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)
        res = client.execute_sql_string(
            "SELECT * FROM "
            "   service("
            "       pattern => '/simplePersonService', "
            "       coordinates => 'org.finos.legend.pylegend:pylegend-test-models:0.0.1-SNAPSHOT'"
            "   )"
        )
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
              },
              {
                 "values": [
                    "Anthony",
                    "Allen",
                    22,
                    "Firm X"
                 ]
              },
              {
                 "values": [
                    "Fabrice",
                    "Roberts",
                    34,
                    "Firm A"
                 ]
              },
              {
                 "values": [
                    "Oliver",
                    "Hill",
                    32,
                    "Firm B"
                 ]
              },
              {
                 "values": [
                    "David",
                    "Harris",
                    35,
                    "Firm C"
                 ]
              }
           ]
        }"""

        assert json.loads(b"".join(res))["result"] == json.loads(expected)

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
