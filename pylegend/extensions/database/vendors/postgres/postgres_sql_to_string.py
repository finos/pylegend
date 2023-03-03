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
    SqlToStringConfig
)
from pylegend.core.sql.metamodel import (
    QuerySpecification
)
from pylegend._typing import (
    PyLegendSequence
)


__all__: PyLegendSequence[str] = [
    "PostgresSqlToStringGenerator"
]


class PostgresSqlToStringGenerator(SqlToStringGenerator):

    @classmethod
    def database_type(cls) -> str:
        return "Postgres"

    @classmethod
    def create_sql_generator(cls) -> SqlToStringGenerator:
        return PostgresSqlToStringGenerator()

    def generate_sql_string(self, sql: QuerySpecification, config: "SqlToStringConfig") -> str:
        return ""