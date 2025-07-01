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
from typing import List, Optional
import sqlglot
from sqlglot import exp, parse_one

from legendql.model.metamodel import (
    SelectionClause, DatabaseFromClause, ColumnReferenceExpression, 
    Expression, Clause
)
from legendql.model.schema import Database, Table, Schema
from legendql.query import Query
from legendql.runtime.pure.db.type import DatabaseDefinition


class SQLToLegendQLConverter:
    """Converts SQL statements to LegendQL metamodel using SQLGlot parsing."""
    
    def __init__(self, database_definition: DatabaseDefinition, database: Database):
        self.database_definition = database_definition
        self.database = database
    
    def parse_sql_to_query(self, sql: str) -> Query:
        """Parse SQL string and convert to LegendQL Query object."""
        try:
            parsed = parse_one(sql)
            if not isinstance(parsed, exp.Select):
                raise ValueError(f"Only SELECT statements are currently supported, got: {type(parsed)}")
            
            return self._convert_select_to_query(parsed)
        except Exception as e:
            raise ValueError(f"Failed to parse SQL: {sql}. Error: {str(e)}")
    
    def _convert_select_to_query(self, select_ast: exp.Select) -> Query:
        """Convert SQLGlot Select AST to LegendQL Query."""
        table_info = self._extract_table_info(select_ast)
        
        query = Query.from_table(
            self.database_definition, 
            self.database, 
            table_info['table_name']
        )
        
        if select_ast.expressions:
            selection_expressions = []
            for expr in select_ast.expressions:
                legend_expr = self._convert_expression(expr)
                selection_expressions.append(legend_expr)
            
            query._clauses = [query._clauses[0]]  # Keep the FROM clause
            query._add_clause(SelectionClause(selection_expressions))
        
        return query
    
    def _extract_table_info(self, select_ast: exp.Select) -> dict:
        """Extract table name and schema information from SELECT AST."""
        table_node = select_ast.find(exp.Table)
        if not table_node:
            raise ValueError("No table found in SELECT statement")
        
        return {
            'table_name': table_node.name,
            'schema_name': table_node.db if hasattr(table_node, 'db') else None,
            'database_name': table_node.catalog if hasattr(table_node, 'catalog') else None
        }
    
    def _convert_expression(self, expr: exp.Expression) -> Expression:
        """Convert SQLGlot expression to LegendQL Expression."""
        if isinstance(expr, exp.Column):
            return ColumnReferenceExpression(expr.name)
        elif isinstance(expr, exp.Star):
            raise NotImplementedError("SELECT * is not yet implemented")
        else:
            return ColumnReferenceExpression(str(expr))


def sql_to_pure_relation(sql: str, database_definition: DatabaseDefinition, database: Database) -> str:
    """
    Convert SQL statement to Pure Relation code.
    
    Args:
        sql: SQL statement to convert
        database_definition: Database definition for execution
        database: Database schema information
        
    Returns:
        Pure Relation code string
    """
    converter = SQLToLegendQLConverter(database_definition, database)
    query = converter.parse_sql_to_query(sql)
    return query.to_string()
