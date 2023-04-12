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
from pylegend.tests.test_helpers.e2e_test_with_legend_server import E2ETestWithLegendServer


class TestLegendClientE2E(E2ETestWithLegendServer):

    def test_e2e_schema_api(self) -> None:
        client = LegendClient("localhost", self.engine_port, False)
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