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

import unittest
import pyarrow as pa
from legendql.sql_parser import SQLParser
from legendql.model.schema import Database, Table
from legendql.model.metamodel import (
    SelectionClause, FilterClause, LimitClause, OffsetClause, OrderByClause,
    DatabaseFromClause, ColumnReferenceExpression, LiteralExpression,
    BinaryExpression, IntegerLiteral, StringLiteral,
    EqualsBinaryOperator, GreaterThanBinaryOperator, OperandExpression,
    OrderByExpression, AscendingOrderType, DescendingOrderType
)


class TestSQLParser(unittest.TestCase):
    
    def setUp(self):
        """Set up test database and parser."""
        columns = [
            pa.field("colA", pa.string()),
            pa.field("colB", pa.int64()),
            pa.field("colC", pa.float64())
        ]
        table = Table("tableC", columns)
        self.database = Database("test_db", [table])
        self.parser = SQLParser(self.database)
    
    def test_basic_select_statement(self):
        """Test parsing basic SELECT statement."""
        sql = "SELECT colA, colB FROM tableC"
        clauses = self.parser.parse_sql(sql)
        
        self.assertEqual(len(clauses), 2)
        
        from_clause = clauses[0]
        self.assertIsInstance(from_clause, DatabaseFromClause)
        self.assertEqual(from_clause.database, "test_db")
        self.assertEqual(from_clause.table, "tableC")
        self.assertEqual(from_clause.schema, "")
        
        select_clause = clauses[1]
        self.assertIsInstance(select_clause, SelectionClause)
        self.assertEqual(len(select_clause.expressions), 2)
        self.assertIsInstance(select_clause.expressions[0], ColumnReferenceExpression)
        self.assertEqual(select_clause.expressions[0].name, "colA")
        self.assertIsInstance(select_clause.expressions[1], ColumnReferenceExpression)
        self.assertEqual(select_clause.expressions[1].name, "colB")
    
    def test_select_with_where_clause(self):
        """Test parsing SELECT statement with WHERE clause."""
        sql = "SELECT colA FROM tableC WHERE colB = 42"
        clauses = self.parser.parse_sql(sql)
        
        self.assertEqual(len(clauses), 3)
        
        where_clause = clauses[2]
        self.assertIsInstance(where_clause, FilterClause)
        self.assertIsInstance(where_clause.expression, BinaryExpression)
        
        binary_expr = where_clause.expression
        self.assertIsInstance(binary_expr.left.expression, ColumnReferenceExpression)
        self.assertEqual(binary_expr.left.expression.name, "colB")
        self.assertIsInstance(binary_expr.right.expression, LiteralExpression)
        self.assertIsInstance(binary_expr.right.expression.literal, IntegerLiteral)
        self.assertEqual(binary_expr.right.expression.literal.value(), 42)
        self.assertIsInstance(binary_expr.operator, EqualsBinaryOperator)
    
    def test_select_with_order_by(self):
        """Test parsing SELECT statement with ORDER BY clause."""
        sql = "SELECT colA FROM tableC ORDER BY colB ASC, colC DESC"
        clauses = self.parser.parse_sql(sql)
        
        self.assertEqual(len(clauses), 3)
        
        order_clause = clauses[2]
        self.assertIsInstance(order_clause, OrderByClause)
        self.assertEqual(len(order_clause.ordering), 2)
        
        first_order = order_clause.ordering[0]
        self.assertIsInstance(first_order, OrderByExpression)
        self.assertIsInstance(first_order.direction, AscendingOrderType)
        self.assertIsInstance(first_order.expression, ColumnReferenceExpression)
        self.assertEqual(first_order.expression.name, "colB")
        
        second_order = order_clause.ordering[1]
        self.assertIsInstance(second_order, OrderByExpression)
        self.assertIsInstance(second_order.direction, DescendingOrderType)
        self.assertIsInstance(second_order.expression, ColumnReferenceExpression)
        self.assertEqual(second_order.expression.name, "colC")
    
    def test_select_with_limit_and_offset(self):
        """Test parsing SELECT statement with LIMIT and OFFSET clauses."""
        sql = "SELECT colA FROM tableC LIMIT 10 OFFSET 5"
        clauses = self.parser.parse_sql(sql)
        
        self.assertEqual(len(clauses), 4)
        
        limit_clause = clauses[2]
        self.assertIsInstance(limit_clause, LimitClause)
        self.assertIsInstance(limit_clause.value, IntegerLiteral)
        self.assertEqual(limit_clause.value.value(), 10)
        
        offset_clause = clauses[3]
        self.assertIsInstance(offset_clause, OffsetClause)
        self.assertIsInstance(offset_clause.value, IntegerLiteral)
        self.assertEqual(offset_clause.value.value(), 5)
    
    def test_complex_where_condition(self):
        """Test parsing SELECT statement with complex WHERE condition."""
        sql = "SELECT colA FROM tableC WHERE colB > 10 AND colC = 'test'"
        clauses = self.parser.parse_sql(sql)
        
        self.assertEqual(len(clauses), 3)
        
        where_clause = clauses[2]
        self.assertIsInstance(where_clause, FilterClause)
        self.assertIsInstance(where_clause.expression, BinaryExpression)
        
        and_expr = where_clause.expression
        self.assertIsInstance(and_expr.left.expression, BinaryExpression)
        self.assertIsInstance(and_expr.right.expression, BinaryExpression)
    
    def test_unsupported_sql_statement(self):
        """Test that unsupported SQL statements raise appropriate errors."""
        sql = "INSERT INTO tableC VALUES (1, 2, 3)"
        
        with self.assertRaises(ValueError) as context:
            self.parser.parse_sql(sql)
        
        self.assertIn("Only SELECT statements are currently supported", str(context.exception))
    
    def test_select_star_not_supported(self):
        """Test that SELECT * raises appropriate error."""
        sql = "SELECT * FROM tableC"
        
        with self.assertRaises(ValueError) as context:
            self.parser.parse_sql(sql)
        
        self.assertIn("SELECT * is not currently supported", str(context.exception))
    
    def test_missing_from_clause(self):
        """Test that SQL without FROM clause raises appropriate error."""
        sql = "SELECT 1"
        clauses = self.parser.parse_sql(sql)
        
        self.assertEqual(len(clauses), 1)
        self.assertIsInstance(clauses[0], SelectionClause)
    
    def test_invalid_sql_syntax(self):
        """Test that invalid SQL syntax raises appropriate error."""
        sql = "SELECT colA FROM"
        
        with self.assertRaises(ValueError) as context:
            self.parser.parse_sql(sql)
        
        self.assertIn("Failed to parse SQL", str(context.exception))


if __name__ == '__main__':
    unittest.main()
