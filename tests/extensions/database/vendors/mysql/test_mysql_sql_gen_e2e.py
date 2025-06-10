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


# type: ignore
import pytest
import sqlalchemy
from testcontainers.mysql import MySqlContainer
from tests.core.database.test_sql_gen_e2e import E2EDbSpecificSqlGenerationTest
from pylegend.extensions.database.vendors.mysql.mysql_sql_to_string import MySQLSqlToStringGenerator


@pytest.mark.skip(reason="MySQL not supported ")
class TestMySQLE2ESqlGeneration(E2EDbSpecificSqlGenerationTest):
    extension = MySQLSqlToStringGenerator.create_sql_generator().get_db_extension()

    @pytest.fixture(scope='module')
    def db_test(self):
        with MySqlContainer() as c:
            engine = sqlalchemy.create_engine(c.get_connection_url())
            yield {
                "engine": engine
            }

    def execute_sql(self, db_test, sql):
        with db_test["engine"].connect() as c:
            return c.execute(sqlalchemy.text(sql))

    def db_extension(self):
        return self.extension
