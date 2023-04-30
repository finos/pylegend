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


from pylegend.extensions.database.vendors.postgres.postgres_sql_to_string import PostgresSqlToStringGenerator
from pylegend.core.databse.sql_to_string import (
    SqlToStringGenerator,
    SqlToStringConfig,
    SqlToStringFormat
)
from pylegend.core.sql.metamodel import (
    QuerySpecification,
    Select,
    IntegerLiteral
)


class TestPostgresSqlToString:

    def test_postgres_sql_to_string_extension_loaded(self) -> None:
        generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type("Postgres")
        assert isinstance(generator, PostgresSqlToStringGenerator)

    def test_postgres_process_top(self) -> None:
        generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type("Postgres")
        extension = generator.get_db_extension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), from_=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert extension.process_top(query, config) == ""

        query.limit = IntegerLiteral(101)
        assert extension.process_top(query, config) == ""

        query.offset = IntegerLiteral(202)
        assert extension.process_top(query, config) == ""

    def test_postgres_process_limit(self) -> None:
        generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type("Postgres")
        extension = generator.get_db_extension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))

        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), from_=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert extension.process_limit(query, config) == ""

        query.limit = IntegerLiteral(101)
        assert extension.process_limit(query, config) == " LIMIT 101"

        query.offset = IntegerLiteral(202)
        assert extension.process_limit(query, config) == " LIMIT 101 OFFSET 202"

    def test_postgres_process_limit_pretty_format(self) -> None:
        generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type("Postgres")
        extension = generator.get_db_extension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))

        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), from_=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert extension.process_limit(query, config) == ""

        query.limit = IntegerLiteral(101)
        assert extension.process_limit(query, config) == "\nLIMIT 101"

        query.offset = IntegerLiteral(202)
        assert extension.process_limit(query, config) == "\nLIMIT 101\nOFFSET 202"
