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

from typing import List, Optional, Union
import sqlglot
from sqlglot import expressions as exp

from legendql.model.metamodel import (
    SelectionClause, FilterClause, LimitClause, OffsetClause, OrderByClause,
    ColumnReferenceExpression, LiteralExpression, BinaryExpression,
    IntegerLiteral, StringLiteral, BooleanLiteral, DateLiteral,
    EqualsBinaryOperator, NotEqualsBinaryOperator, GreaterThanBinaryOperator,
    LessThanBinaryOperator, GreaterThanEqualsBinaryOperator, LessThanEqualsBinaryOperator,
    AndBinaryOperator, OrBinaryOperator, AddBinaryOperator, SubtractBinaryOperator,
    MultiplyBinaryOperator, DivideBinaryOperator, OperandExpression,
    OrderByExpression, AscendingOrderType, DescendingOrderType,
    DatabaseFromClause, Clause, Expression
)
from legendql.model.schema import Database, Table
import pyarrow as pa


class SQLParser:
    """Parser that converts SQL statements to LegendQL metamodel using SQLGlot."""
    
    def __init__(self, database: Database):
        self.database = database
        
    def parse_sql(self, sql: str) -> List[Clause]:
        """Parse SQL statement and return list of LegendQL clauses."""
        try:
            parsed = sqlglot.parse_one(sql)
            
            if not isinstance(parsed, exp.Select):
                raise ValueError(f"Only SELECT statements are currently supported, got: {type(parsed)}")
            
            clauses = []
            
            from_clause = self._parse_from_clause(parsed)
            if from_clause:
                clauses.append(from_clause)
            
            select_clause = self._parse_select_clause(parsed)
            if select_clause:
                clauses.append(select_clause)
            
            where_clause = self._parse_where_clause(parsed)
            if where_clause:
                clauses.append(where_clause)
            
            order_by_clause = self._parse_order_by_clause(parsed)
            if order_by_clause:
                clauses.append(order_by_clause)
            
            limit_clause = self._parse_limit_clause(parsed)
            if limit_clause:
                clauses.append(limit_clause)
            
            offset_clause = self._parse_offset_clause(parsed)
            if offset_clause:
                clauses.append(offset_clause)
            
            return clauses
            
        except Exception as e:
            raise ValueError(f"Failed to parse SQL: {sql}. Error: {str(e)}")
    
    def _parse_from_clause(self, select_stmt: exp.Select) -> Optional[DatabaseFromClause]:
        """Parse FROM clause and return DatabaseFromClause."""
        if not select_stmt.find(exp.From):
            return None
        
        from_expr = select_stmt.find(exp.From)
        table_expr = from_expr.find(exp.Table)
        
        if not table_expr:
            raise ValueError("FROM clause must specify a table")
        
        table_name = table_expr.name
        schema_name = table_expr.db if table_expr.db else None
        
        database_name = self.database.name
        
        return DatabaseFromClause(database_name, table_name, schema_name or "")
    
    def _parse_select_clause(self, select_stmt: exp.Select) -> Optional[SelectionClause]:
        """Parse SELECT clause and return SelectionClause."""
        expressions = []
        
        for select_expr in select_stmt.expressions:
            if isinstance(select_expr, exp.Star):
                raise ValueError("SELECT * is not currently supported. Please specify column names.")
            elif isinstance(select_expr, exp.Column):
                expressions.append(ColumnReferenceExpression(select_expr.name))
            elif hasattr(select_expr, 'alias') and select_expr.alias:
                expressions.append(ColumnReferenceExpression(select_expr.alias))
            else:
                expr = self._parse_expression(select_expr)
                if isinstance(expr, ColumnReferenceExpression):
                    expressions.append(expr)
                else:
                    if hasattr(select_expr, 'name'):
                        expressions.append(ColumnReferenceExpression(select_expr.name))
        
        return SelectionClause(expressions) if expressions else None
    
    def _parse_where_clause(self, select_stmt: exp.Select) -> Optional[FilterClause]:
        """Parse WHERE clause and return FilterClause."""
        where_expr = select_stmt.find(exp.Where)
        if not where_expr:
            return None
        
        condition = self._parse_expression(where_expr.this)
        return FilterClause(condition)
    
    def _parse_order_by_clause(self, select_stmt: exp.Select) -> Optional[OrderByClause]:
        """Parse ORDER BY clause and return OrderByClause."""
        order_by = select_stmt.find(exp.Order)
        if not order_by:
            return None
        
        ordering = []
        for ordered_expr in order_by.expressions:
            column_expr = self._parse_expression(ordered_expr.this)
            direction = DescendingOrderType() if ordered_expr.args.get('desc') else AscendingOrderType()
            ordering.append(OrderByExpression(direction, column_expr))
        
        return OrderByClause(ordering)
    
    def _parse_limit_clause(self, select_stmt: exp.Select) -> Optional[LimitClause]:
        """Parse LIMIT clause and return LimitClause."""
        limit_expr = select_stmt.find(exp.Limit)
        if not limit_expr:
            return None
        
        limit_value = self._parse_expression(limit_expr.expression)
        if isinstance(limit_value, LiteralExpression) and isinstance(limit_value.literal, IntegerLiteral):
            return LimitClause(limit_value.literal)
        
        raise ValueError("LIMIT value must be an integer")
    
    def _parse_offset_clause(self, select_stmt: exp.Select) -> Optional[OffsetClause]:
        """Parse OFFSET clause and return OffsetClause."""
        offset_expr = select_stmt.find(exp.Offset)
        if not offset_expr:
            return None
        
        offset_value = self._parse_expression(offset_expr.expression)
        if isinstance(offset_value, LiteralExpression) and isinstance(offset_value.literal, IntegerLiteral):
            return OffsetClause(offset_value.literal)
        
        raise ValueError("OFFSET value must be an integer")
    
    def _parse_expression(self, expr) -> Expression:
        """Parse SQLGlot expression and return LegendQL Expression."""
        if isinstance(expr, exp.Column):
            return ColumnReferenceExpression(expr.name)
        elif isinstance(expr, exp.Literal):
            return self._parse_literal(expr)
        elif isinstance(expr, exp.Binary):
            return self._parse_binary_expression(expr)
        else:
            if hasattr(expr, 'name'):
                return ColumnReferenceExpression(expr.name)
            raise ValueError(f"Unsupported expression type: {type(expr)}")
    
    def _parse_literal(self, literal: exp.Literal) -> LiteralExpression:
        """Parse SQLGlot literal and return LiteralExpression."""
        if literal.is_int:
            return LiteralExpression(IntegerLiteral(int(literal.this)))
        elif literal.is_string:
            return LiteralExpression(StringLiteral(literal.this))
        else:
            return LiteralExpression(StringLiteral(str(literal.this)))
    
    def _parse_binary_expression(self, binary: exp.Binary) -> BinaryExpression:
        """Parse SQLGlot binary expression and return BinaryExpression."""
        left = OperandExpression(self._parse_expression(binary.left))
        right = OperandExpression(self._parse_expression(binary.right))
        
        operator_map = {
            exp.EQ: EqualsBinaryOperator(),
            exp.NEQ: NotEqualsBinaryOperator(),
            exp.GT: GreaterThanBinaryOperator(),
            exp.LT: LessThanBinaryOperator(),
            exp.GTE: GreaterThanEqualsBinaryOperator(),
            exp.LTE: LessThanEqualsBinaryOperator(),
            exp.And: AndBinaryOperator(),
            exp.Or: OrBinaryOperator(),
            exp.Add: AddBinaryOperator(),
            exp.Sub: SubtractBinaryOperator(),
            exp.Mul: MultiplyBinaryOperator(),
            exp.Div: DivideBinaryOperator(),
        }
        
        operator_class = type(binary)
        if operator_class in operator_map:
            operator = operator_map[operator_class]
        else:
            raise ValueError(f"Unsupported binary operator: {operator_class}")
        
        return BinaryExpression(left, right, operator)
