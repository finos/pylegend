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

import json
from dataclasses import dataclass
from typing import List

from pylegend.core.request.legend_client import LegendClient

import pyarrow as pa

from legendql.model.metamodel import Clause
from legendql.model.schema import Database, Table, Schema
from legendql.result import Result
from legendql.runtime.pure.db.type import DatabaseDefinition

from legendql.dialect.type import DialectType
from pylegend.core.request.service_client import RequestMethod


@dataclass
class LegendExecutionServer:
    database_type: DatabaseDefinition
    database: Database
    client: LegendClient

    def execute(self, clauses: List[Clause]) -> Result:
        lam = DialectType.PURE_RELATION.to_string(clauses)
        model = self._generate_model()
        query = self._parse_lambda(lam, self.client)
        pmcd = self._parse_model(model, self.client)
        runtime = pmcd["elements"][0]["runtimeValue"]

        execution_input = {"clientVersion": "vX_X_X", "context": {"_type": "BaseExecutionContext"}, "function": query, "runtime": runtime, "model": pmcd}
        return LegendExecutionServer._parse_execution_response(model, lam, self._execute(execution_input, self.client))

    def _parse_model(self, model: str, client: LegendClient) -> dict:
        return client._execute_service(
            path="/api/pure/v1/grammar/grammarToJson/model",
            method=RequestMethod.POST,
            headers={'Content-Type': 'text/plain'},
            data=model,
            stream=False
        ).json()

    def _parse_lambda(self, lam: str, client: LegendClient) -> dict:
        return client._execute_service(
            path="/api/pure/v1/grammar/grammarToJson/lambda",
            method=RequestMethod.POST,
            headers={'Content-Type': 'text/plain'},
            data="|" + lam,
            stream=False
        ).json()

    def _execute(self, input: dict, client: LegendClient) -> dict:
        return client._execute_service(
            path="/api/pure/v1/execution/execute?serializationFormat=DEFAULT",
            method=RequestMethod.POST,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(input),
            stream=False
        ).json()

    def _generate_model(self) -> str:
        return self.database_type.generate_model(self.database)

    @staticmethod
    def _generate_schema_patterns(table: str, schema: str = None):
        patterns = [{"tablePattern": table}]
        if schema:
            patterns[0].update({"schemaPattern": schema})
        return patterns

    def infer_database(self, database_def: DatabaseDefinition, table: str, schema: str = None) -> Database:
        connection = database_def.generate_pure_connection(self.database)
        connection_pmcd = self._parse_model(connection, self.client)["elements"][0]["connectionValue"]
        patterns = LegendExecutionServer._generate_schema_patterns(table, schema)
        schema_exploration_input = {
            "config": {
                "enrichTables": True,
                "enrichPrimaryKeys": True,
                "enrichColumns": True,
                "patterns": patterns
            },
            "connection": connection_pmcd
        }
        response = self.client._execute_service(
            path="/api/pure/v1/utilities/database/schemaExploration",
            method=RequestMethod.POST,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(schema_exploration_input),
            stream=False
        ).json()

        return LegendExecutionServer._parse_schema_exploration_response(response)

    @staticmethod
    def _parse_schema_exploration_response(response: dict) -> Database:
        schema_defs = response["elements"][0]["schemas"]
        schemas = []
        for schema_def in schema_defs:
            name = schema_def["name"]
            table_defs = schema_def["tables"]
            schema = Schema(name=name)
            schemas.append(schema)
            for table_def in table_defs:
                table_name = table_def["name"]
                column_defs = table_def["columns"]
                columns = []
                for column_def in column_defs:
                    column_name = column_def["name"]
                    column_type = column_def["type"]
                    column_nullable = column_def["nullable"]
                    columns.append(pa.field(column_name, LegendExecutionServer._arrow_type_from_pure_relational_type(column_type), column_nullable))
                schema.append(Table(table_name, columns))

        default_schemas = [x for x in schemas if x.name == "default"]
        default_schema = default_schemas[0] if len(default_schemas) > 0 else None
        schemas_and_tables = []
        if default_schema:
            schemas.remove(default_schema)
            schemas_and_tables = list(default_schema)
        schemas_and_tables.extend(schemas)

        return Database("legendql::Database", schemas_and_tables)

    @staticmethod
    def _parse_execution_response(model: str, relation: str, result: dict) -> Result:
        sql = result["activities"][0]["sql"]
        headers = result["result"]["columns"]
        data = [[] for _ in range(len(headers))]
        for row in result["result"]["rows"]:
            values = row["values"]
            for col_num in range(len(values)):
                data[col_num].append(values[col_num])
        return Result(model, relation, sql, pa.Table.from_arrays(arrays=data, names=headers))

    @staticmethod
    def _arrow_type_from_pure_relational_type(type_info: dict) -> pa.DataType:
        type_name = type_info["_type"]
        if type_name == "Varchar":
            return pa.utf8()
        elif type_name == "BigInt":
            return pa.int32()
        elif type_name == "Double":
            return pa.float64()
        elif type_name == "Boolean":
            return pa.bool_()
        elif type_name == "Timestamp":
            return pa.timestamp("ms", "UTC")
        elif type_name == "Date":
            return pa.date64()
        else:
            raise ValueError(f"Unsupported Pure Relational Type: {type_name}")
