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

from dataclasses import dataclass


from legendql.model.schema import Database
from legendql.runtime.pure.db.type import DatabaseDefinition

@dataclass
class DuckDBDatabaseDefinition(DatabaseDefinition):
    path: str

    def get_type_name(self) -> str:
        return "DuckDB"

    def generate_pure_runtime(self, database: Database) -> str:
        return f"""
###Runtime
Runtime legendql::Runtime
{{
  mappings:
  [
  ];
  connections:
  [
    {database.name}:
    [
      connection: legendql::Connection
    ]
  ];
}}
"""

    def generate_pure_connection(self, database: Database) -> str:
        return f"""
###Connection       
RelationalDatabaseConnection legendql::Connection
{{
  type: DuckDB;
  specification: DuckDB
  {{
    path: '{self.path}';
  }};
  auth: Test;
}}    
"""
