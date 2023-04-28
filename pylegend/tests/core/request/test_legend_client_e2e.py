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
        client = LegendClient("localhost", legend_test_server["engine_port"], False)
        res = client.get_sql_string_schema(
            "SELECT * FROM "
            "   legend_service("
            "       '/simplePersonService', "
            "       groupId => 'org.finos.legend.pylegend',"
            "       artifactId => 'pylegend-test-models',"
            "       version => '0.0.1-SNAPSHOT'"
            "   )"
        )
        expected = """\
        {
           "__TYPE": "meta::external::query::sql::Schema",
           "columns": [
              {
                 "__TYPE": "meta::external::query::sql::PrimitiveValueSchemaColumn",
                 "type": "String",
                 "name": "First Name"
              },
              {
                 "__TYPE": "meta::external::query::sql::PrimitiveValueSchemaColumn",
                 "type": "String",
                 "name": "Last Name"
              },
              {
                 "__TYPE": "meta::external::query::sql::PrimitiveValueSchemaColumn",
                 "type": "Integer",
                 "name": "Age"
              },
              {
                 "__TYPE": "meta::external::query::sql::PrimitiveValueSchemaColumn",
                 "type": "String",
                 "name": "Firm/Legal Name"
              }
           ],
           "enums": []
        }"""

        assert json.loads(res) == json.loads(expected)

    def test_e2e_execute_string_api(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        client = LegendClient("localhost", legend_test_server["engine_port"], False)
        res = client.execute_sql_string(
            "SELECT * FROM "
            "   legend_service("
            "       '/simplePersonService', "
            "       groupId => 'org.finos.legend.pylegend',"
            "       artifactId => 'pylegend-test-models',"
            "       version => '0.0.1-SNAPSHOT'"
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

        assert json.loads(res.text)["result"] == json.loads(expected)
