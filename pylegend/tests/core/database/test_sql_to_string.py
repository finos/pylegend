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
from pylegend.core.databse.sql_to_string import (
    SqlToStringGenerator,
    SqlToStringConfig,
    SqlToStringDbExtension,
    SqlToStringFormat
)
from pylegend.core.sql.metamodel import (
    AllColumns,
    IntegerLiteral,
    LongLiteral,
    StringLiteral,
    DoubleLiteral,
    NullLiteral,
    BooleanLiteral,
    QuerySpecification,
    Select,
    SingleColumn
)


class TestSqlToStringDbExtension(SqlToStringDbExtension):
    @classmethod
    def process_query_specification(cls, query: QuerySpecification, config: SqlToStringConfig) -> str:
        return ""


class TestSqlToStringGenerator(SqlToStringGenerator):
    @classmethod
    def database_type(cls) -> str:
        return "TestDB"

    @classmethod
    def create_sql_generator(cls) -> "SqlToStringGenerator":
        return TestSqlToStringGenerator()

    def get_db_extension(self) -> SqlToStringDbExtension:
        return TestSqlToStringDbExtension()


class TestSqlToString:
    def test_find_sql_to_string_generator(self) -> None:
        generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type("TestDB")
        assert isinstance(generator, TestSqlToStringGenerator)
        query = QuerySpecification(
            select=Select(selectItems=[], distinct=False), _from=[], where=None, groupBy=[],
            having=None, orderBy=[], limit=None, offset=None
        )
        assert generator.generate_sql_string(query, SqlToStringConfig(SqlToStringFormat(multi_line=False), False)) == ""

    def test_find_sql_to_string_generator_error(self) -> None:
        with pytest.raises(
            RuntimeError,
            match="Found no \\(or multiple\\) sql to string generators for database type 'UnknownDB'. " +
                  "Found generators: \\[\\]"
        ):
            SqlToStringGenerator.find_sql_to_string_generator_for_db_type("UnknownDB")


class TestSqlToStringDbExtensionProcessing:
    def test_process_identifier(self) -> None:
        class ExtensionWithBackTick(SqlToStringDbExtension):
            @classmethod
            def quote_character(cls) -> str:
                return "`"

        plain_extension = SqlToStringDbExtension()
        backtick_extension = ExtensionWithBackTick()
        config = SqlToStringConfig(SqlToStringFormat(multi_line=False), False)
        config_quoted_identifiers = SqlToStringConfig(SqlToStringFormat(multi_line=False), True)
        assert plain_extension.process_identifier("A", config) == "A"
        assert plain_extension.process_identifier("date", config) == '"date"'
        assert plain_extension.process_identifier("A", config_quoted_identifiers) == '"A"'
        assert backtick_extension.process_identifier("A", config_quoted_identifiers) == '`A`'

    def test_process_all_columns(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(multi_line=False), False)
        assert extension.process_all_columns(AllColumns(prefix=None), config) == "*"
        assert extension.process_all_columns(AllColumns(prefix="root"), config) == '"root".*'

    def test_process_single_column(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(multi_line=False), False)
        single_column_without_alias = SingleColumn(expression=IntegerLiteral(101), alias=None)
        single_column_with_alias = SingleColumn(expression=IntegerLiteral(101), alias="a")
        assert extension.process_single_column(single_column_without_alias, config) == "101"
        assert extension.process_single_column(single_column_with_alias, config) == '101 as "a"'

    def test_process_select_item(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(multi_line=False), False)
        single_column = SingleColumn(expression=IntegerLiteral(101), alias="a")
        all_columns = AllColumns(prefix="root")
        assert extension.process_select_item(single_column, config) == '101 as "a"'
        assert extension.process_select_item(all_columns, config) == '"root".*'

    def test_process_select(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(multi_line=False), False)
        select_with_distinct = Select(
            selectItems=[
                SingleColumn(expression=IntegerLiteral(101), alias="a"),
                SingleColumn(expression=IntegerLiteral(202), alias=None),
            ],
            distinct=True
        )
        assert extension.process_select(select_with_distinct, config) == 'distinct 101 as "a", 202'

        select_without_distinct = Select(
            selectItems=[
                AllColumns(prefix="alias"),
            ],
            distinct=False
        )
        assert extension.process_select(select_without_distinct, config) == '"alias".*'

    def test_process_literal(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(multi_line=False), False)
        assert extension.process_literal(IntegerLiteral(101), config) == '101'
        assert extension.process_literal(LongLiteral(202), config) == '202'
        assert extension.process_literal(DoubleLiteral(303.3), config) == "303.3"
        assert extension.process_literal(StringLiteral("a'b", quoted=False), config) == "'a''b'"
        assert extension.process_literal(BooleanLiteral(True), config) == "true"
        assert extension.process_literal(BooleanLiteral(False), config) == "false"
        assert extension.process_literal(NullLiteral(), config) == "null"
