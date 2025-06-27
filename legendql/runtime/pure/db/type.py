# Copyright 2025 Goldman Sachs
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

from typing import List
from abc import ABC, abstractmethod
from legendql.model.schema import Database, Table
import pyarrow as pa

class DatabaseDefinition(ABC):
    def generate_model(self, database: Database) -> str:
        return f"{self.generate_pure_runtime(database)}\n\n{self.generate_pure_connection(database)}\n\n{self.generate_pure_database(database)}"

    @abstractmethod
    def get_type_name(self) -> str:
        pass

    @abstractmethod
    def generate_pure_runtime(self, database: Database) -> str:
        pass

    @abstractmethod
    def generate_pure_connection(self, database: Database) -> str:
        pass

    def generate_pure_database(self, database: Database) -> str:
        all_tables = [x for xs in map(lambda c: [c] if isinstance(c, Table) else c, database.children) for x in xs]
        schemaless_tables = [x for x in all_tables if x.schema is None]
        tables_by_schema = {}

        for table in [x for x in all_tables if x.schema is not None]:
            schema_name = table.schema.name
            if schema_name in tables_by_schema:
                tables_by_schema[schema_name].append(table)
            else:
                tables_by_schema[schema_name] = [table]

        schemas = "".join(map(lambda sn, ts: self._schema_to_pure(sn, ts), tables_by_schema.keys(), tables_by_schema.values()))
        tables = "".join(map(lambda t: self._table_to_pure(t), schemaless_tables))

        return f"""
###Relational
Database {database.name}
(
    {schemas}
    {tables}
)
            """

    def _schema_to_pure(self, name: str, tables: List[Table]) -> str:
        return f"""
    Schema {name}
    (
        {"        ".join(map(lambda t: self._table_to_pure(t), tables))}
    )
"""

    def _table_to_pure(self, table: Table) -> str:
        columns = []
        for column in table.columns:
            columns.append(f"{column.name} {self._arrow_field_to_pure_db_type(column)}")
        return f"""
        Table {table.table}
        (
            {("," + chr(10) + "            ").join(columns)}
        )
        """

    @staticmethod
    def _arrow_field_to_pure_db_type(field: pa.Field):
        if pa.types.is_string(field.type):
            return "VARCHAR(0)"
        if pa.types.is_integer(field.type):
            return "BIGINT"
        if pa.types.is_date(field.type):
            return "DATE"
        if pa.types.is_floating(field.type):
            return "DOUBLE"
        if pa.types.is_boolean(field.type):
            return "BOOLEAN"
        if pa.types.is_timestamp(field.type):
            return "TIMESTAMP"
        raise ValueError(f"Unsupported type {field.type}")
