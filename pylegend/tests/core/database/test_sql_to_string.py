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

import pytest  # type: ignore
from pylegend.core.databse.sql_to_string import SqlToStringGenerator, SqlToStringConfig
from pylegend.core.sql.metamodel import QuerySpecification, Select


class TestSqlToStringGenerator(SqlToStringGenerator):
    @classmethod
    def database_type(cls) -> str:
        return "TestDB"

    @classmethod
    def create_sql_generator(cls) -> "SqlToStringGenerator":
        return TestSqlToStringGenerator()

    def generate_sql_string(self, sql: QuerySpecification, config: "SqlToStringConfig") -> str:
        return ""


class TestSqlToString:
    def test_find_sql_to_string_generator(self) -> None:
        generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type("TestDB")
        assert isinstance(generator, TestSqlToStringGenerator)
        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), _from=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert "" == generator.generate_sql_string(query, SqlToStringConfig())

    def test_find_sql_to_string_generator_error(self) -> None:
        with pytest.raises(
            RuntimeError,
            match="Found no \\(or multiple\\) sql to string generators for database type 'UnknownDB'. " +
                  "Found generators: \\[\\]"
        ):
            SqlToStringGenerator.find_sql_to_string_generator_for_db_type("UnknownDB")
