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

from pylegend.core.databse.sql_to_string import (
    SqlToStringGenerator,
    SqlToStringDbExtension,
    SqlToStringConfig
)
from pylegend.core.sql.metamodel import (
    QuerySpecification
)
from pylegend._typing import PyLegendSequence


__all__: PyLegendSequence[str] = [
    "PostgresSqlToStringGenerator"
]


def limit_processor(
    query: QuerySpecification,
    extension: "SqlToStringDbExtension",
    config: SqlToStringConfig
) -> str:
    sep0 = config.format.separator(0)
    limit = f"{sep0}LIMIT {extension.process_expression(query.limit, config)}" \
        if query.limit else ""
    offset = f"{sep0}OFFSET {extension.process_expression(query.offset, config)}" \
        if query.offset else ""
    return f"{limit}{offset}"


class PostgresSqlToStringDbExtension(SqlToStringDbExtension):
    def process_top(self, query: QuerySpecification, config: SqlToStringConfig) -> str:
        return ""

    def process_limit(self, query: QuerySpecification, config: SqlToStringConfig) -> str:
        return limit_processor(query, self, config)


class PostgresSqlToStringGenerator(SqlToStringGenerator):
    __sql_to_string_db_extension: SqlToStringDbExtension = PostgresSqlToStringDbExtension()

    @classmethod
    def database_type(cls) -> str:
        return "Postgres"

    @classmethod
    def create_sql_generator(cls) -> SqlToStringGenerator:
        return PostgresSqlToStringGenerator()

    def get_db_extension(self) -> SqlToStringDbExtension:
        return self.__sql_to_string_db_extension
