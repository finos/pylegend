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


from pylegend.core.databse.sql_to_string import SqlToStringGenerator
from pylegend.extensions.database.vendors.postgres.postgres_sql_to_string import PostgresSqlToStringGenerator


class TestPostgresSqlToString:

    def test_postgres_sql_to_string_extension_loaded(self) -> None:
        generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type("Postgres")
        assert isinstance(generator, PostgresSqlToStringGenerator)
