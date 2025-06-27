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

from typing import Callable, Dict, List, Union

from pylegend.core.request.legend_client import LegendClient

from legendql import parser
from legendql.dialect import DialectType
from legendql.model.metamodel import OrderByClause, LimitClause, IntegerLiteral, OffsetClause, RenameClause, \
    LeftJoinType, InnerJoinType
from legendql.parser import ParseType
from legendql.model.metamodel import SelectionClause, ExtendClause, FilterClause, GroupByClause, JoinClause, JoinType
from legendql.model.schema import Table, Database
from legendql.query import Query
from legendql.result import Result
from legendql.runtime.pure.db.type import DatabaseDefinition
from legendql.runtime.pure.executionserver.runtime import LegendExecutionServer
from legendql.store import Store

import pyarrow as pa


def table(table_name: str, columns: List[pa.Field]) -> LegendQL:
    db_table = Table(table_name, columns)
    database = Database("legendql::Database", [db_table])
    return LegendQL.using_db_def(database, table_name, None)


def db(tables: Dict[str, List[pa.Field]]) -> [LegendQL]:
    db_tables = [Table(table_name, columns) for (table_name, columns) in tables.items()]
    database = Database("legendql::Database", db_tables)
    return [LegendQL.using_db_def(database, db_table.table) for db_table in db_tables]


def using(store_or_def: Union[Store, DatabaseDefinition], table: str, schema: str = None, client: LegendClient = None) -> LegendQL:
    database_def = store_or_def.database_definition() if isinstance(store_or_def, Store) else store_or_def
    runtime = LegendExecutionServer(database_def, Database("legendql::Database", []), client)
    database = runtime.infer_database(database_def, table, schema)
    return using_db_def(database_def, database, table)


def using_db_def(database_definition: DatabaseDefinition, database: Database, table: str) -> LegendQL:
    return LegendQL.using_db_def(database, table, database_definition)


def using_db(database: Database, table: str) -> LegendQL:
    return LegendQL.using_db_def(database, table, None)


class LegendQL:

    def __init__(self, database_definition: DatabaseDefinition, database: Database, table: str):
        self._query = Query.from_table(database_definition, database, table)

    @classmethod
    def using_db_def(cls, database: Database, table: str, database_definition: DatabaseDefinition = None) -> LegendQL:
        return LegendQL(database_definition, database, table)

    def execute(self, store_or_def: Union[Store | DatabaseDefinition] = None, client: LegendClient = None) -> Result:
        database_def = None
        if store_or_def:
            if isinstance(store_or_def, Store):
                database_def = store_or_def.database_definition()
            else:
                database_def = store_or_def
        return self._query.execute(database_def, client)

    def to_string(self, dialect: DialectType = DialectType.PURE_RELATION) -> str:
        return self._query.to_string(dialect)

    def select(self, columns: Callable) -> LegendQL:
        exp, t = parser.Parser.parse(columns, [self._query._table], ParseType.select)
        self._query._add_clause(SelectionClause(exp))
        self._query._update_table(t)
        return self

    def extend(self, columns: Callable) -> LegendQL:
        exp, t = parser.Parser.parse(columns, [self._query._table], ParseType.extend)
        self._query._add_clause(ExtendClause(exp))
        self._query._update_table(t)
        return self

    def rename(self, columns: Callable) -> LegendQL:
        exp, t = parser.Parser.parse(columns, [self._query._table], ParseType.rename)
        self._query._add_clause(RenameClause(exp))
        self._query._update_table(t)
        return self

    def filter(self, condition: Callable) -> LegendQL:
        exp, t = parser.Parser.parse(condition, [self._query._table], ParseType.filter)
        self._query._add_clause(FilterClause(exp))
        self._query._update_table(t)
        return self

    def group_by(self, aggr: Callable) -> LegendQL:
        exps, t = parser.Parser.parse(aggr, [self._query._table], ParseType.group_by)
        self._query._add_clause(GroupByClause(exps[0]))
        if len(exps) == 2:
            self._query._add_clause(FilterClause(exps[1]))
        self._query._update_table(t)
        return self

    def _join(self, lq: LegendQL, join: Callable, join_type: JoinType) -> LegendQL:
        exp, t = parser.Parser.parse(join, [self._query._table, lq._query._table], ParseType.join)
        self._query._add_clause(JoinClause(lq._query._clauses, join_type, exp))
        self._query._update_db(lq._query._database)
        self._query._update_table(t)
        return self

    def join(self, lq: LegendQL, join: Callable) -> LegendQL:
        return self._join(lq, join, InnerJoinType())

    def left_join(self, lq: LegendQL, join: Callable) -> LegendQL:
        return self._join(lq, join, LeftJoinType())

    def order_by(self, columns: Callable) -> LegendQL:
        exp, t = parser.Parser.parse(columns, [self._query._table], ParseType.order_by)
        self._query._add_clause(OrderByClause(exp))
        self._query._update_table(t)
        return self

    def limit(self, limit: int) -> LegendQL:
        clause = LimitClause(IntegerLiteral(limit))
        self._query._add_clause(clause)
        return self

    def offset(self, offset: int) -> LegendQL:
        clause = OffsetClause(IntegerLiteral(offset))
        self._query._add_clause(clause)
        return self

    def take(self, offset: int, limit: int) -> LegendQL:
        clause = OffsetClause(IntegerLiteral(offset))
        self._query._add_clause(clause)

        clause = LimitClause(IntegerLiteral(limit))
        self._query._add_clause(clause)
        return self


query = LegendQL
