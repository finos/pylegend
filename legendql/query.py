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

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple

from pylegend.core.request.legend_client import LegendClient

from legendql.dialect import DialectType
from legendql.model.metamodel import SelectionClause, FilterClause, ExtendClause, GroupByClause, \
    LimitClause, JoinClause, JoinType, JoinExpression, Clause, DatabaseFromClause, Expression, IntegerLiteral, \
    GroupByExpression, ColumnReferenceExpression, RenameClause, ColumnAliasExpression, OffsetClause, OrderByExpression, \
    OrderByClause
from legendql.model.schema import Table, Database, Schema
from legendql.result import Result
from legendql.runtime.pure.db.type import DatabaseDefinition
from legendql.runtime.pure.executionserver.runtime import LegendExecutionServer

@dataclass
class Query:
    _database_definition: DatabaseDefinition
    _database: Database
    _table_history: []
    _table: Table
    _clauses: List[Clause]

    @classmethod
    def from_table(cls, database_definition: DatabaseDefinition, database: Database, table: str) -> Query:
        all_tables = [x for xs in map(lambda c: [c] if isinstance(c, Table) else c, database.children) for x in xs]
        try:
            found_table = next(t for t in all_tables if t.table == table)
        except StopIteration:
            raise ValueError(f"Table {table} not found in the database.")
        cloned_table = Table(found_table.table, found_table.columns.copy(), found_table.schema)
        return Query(database_definition, database, [cloned_table], cloned_table, [DatabaseFromClause(database.name, cloned_table.table, cloned_table.schema.name if cloned_table.schema else None)])

    def execute(self, database_definition: DatabaseDefinition = None, client: LegendClient = None) -> Result:
        if database_definition is None and self._database_definition is None:
            raise ValueError("Database definition is required to execute the query.")
        d = database_definition if database_definition else self._database_definition
        runtime = LegendExecutionServer(d, self._database, client)
        return runtime.execute(self._clauses)

    def to_string(self, dialect: DialectType = DialectType.PURE_RELATION) -> str:
        return dialect.to_string(self._clauses)

    def _add_clause(self, clause: Clause) -> None:
        self._clauses.append(clause)

    def _update_table(self, table: Table) -> None:
        self._table_history.append(table)
        self._table = table

    def _update_db(self, database: Database) -> None:
        new_children = database.children
        current_schemas = dict([(s.name, s) for s in self._database.children if isinstance(s, Schema)])
        current_tables = dict([(t.table, t) for t in self._database.children if isinstance(t, Table)])
        for child in database.children:
            if isinstance(child, Table) and child.table not in current_tables.keys():
                new_children.append(child)
            elif isinstance(child, Schema):
                if child.name in current_schemas.keys():
                    schema = current_schemas[child.name]
                    schema_tables = dict([(t.table, t) for t in schema if isinstance(t, Table)])
                    for table in child:
                        if table.table not in schema_tables.keys():
                            schema.append(table)
                else:
                    new_children.append(child)
        self._database.children = new_children

    def select(self, *names: str) -> Query:
        self._add_clause(SelectionClause(list(map(lambda name: ColumnReferenceExpression(name), names))))
        return self

    def rename(self, *renames: Tuple[str, str]) -> Query:
        self._add_clause(RenameClause(list(map(lambda rename: ColumnAliasExpression(alias=rename[1], reference=ColumnReferenceExpression(name=rename[0])), renames))))
        return self

    def extend(self, extend: List[Expression]) -> Query:
        self._add_clause(ExtendClause(extend))
        return self

    def filter(self, filter_clause: Expression) -> Query:
        self._add_clause(FilterClause(filter_clause))
        return self

    def group_by(self, selections: List[Expression], group_by: List[Expression], having: Expression = None) -> Query:
        self._add_clause(GroupByClause(GroupByExpression(selections, group_by)))
        if having:
            self.filter(having)
        return self

    def limit(self, limit: int) -> Query:
        self._add_clause(LimitClause(IntegerLiteral(limit)))
        return self

    def offset(self, offset: int) -> Query:
        self._add_clause(OffsetClause(IntegerLiteral(offset)))
        return self

    def order_by(self, *ordering: OrderByExpression) -> Query:
        self._add_clause(OrderByClause(list(ordering)))
        return self

    def join(self, database: str, schema: str, table: str, join_type: JoinType, on_clause: Expression) -> Query:
        self._add_clause(JoinClause([DatabaseFromClause(database, table, schema)], join_type, JoinExpression(on_clause)))
        return self
