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
from legendql.query import Query
from legendql.model.schema import Database, Table
from legendql.model.metamodel import (
    SelectionClause, FilterClause, DatabaseFromClause,
    ColumnReferenceExpression, BinaryExpression
)


class TestQueryFromSQL(unittest.TestCase):
    
    def setUp(self):
        """Set up test database."""
        columns = [
            pa.field("colA", pa.string()),
            pa.field("colB", pa.int64()),
            pa.field("colC", pa.float64())
        ]
        table = Table("tableC", columns)
        self.database = Database("test_db", [table])
    
    def test_query_from_sql_basic(self):
        """Test creating Query from basic SQL statement."""
        sql = "SELECT colA, colB FROM tableC"
        query = Query.from_sql(sql, None, self.database)
        
        self.assertIsNotNone(query)
        self.assertEqual(len(query._clauses), 2)
        
        from_clause = query._clauses[0]
        self.assertIsInstance(from_clause, DatabaseFromClause)
        self.assertEqual(from_clause.table, "tableC")
        
        select_clause = query._clauses[1]
        self.assertIsInstance(select_clause, SelectionClause)
        self.assertEqual(len(select_clause.expressions), 2)
    
    def test_query_from_sql_with_where(self):
        """Test creating Query from SQL statement with WHERE clause."""
        sql = "SELECT colA FROM tableC WHERE colB = 42"
        query = Query.from_sql(sql, None, self.database)
        
        self.assertIsNotNone(query)
        self.assertEqual(len(query._clauses), 3)
        
        where_clause = query._clauses[2]
        self.assertIsInstance(where_clause, FilterClause)
        self.assertIsInstance(where_clause.expression, BinaryExpression)
    
    def test_query_from_sql_table_not_found(self):
        """Test that Query.from_sql raises error for non-existent table."""
        sql = "SELECT colA FROM nonexistent_table"
        
        with self.assertRaises(ValueError) as context:
            Query.from_sql(sql, None, self.database)
        
        self.assertIn("Table nonexistent_table not found", str(context.exception))
    
    def test_query_from_sql_no_from_clause(self):
        """Test that Query.from_sql raises error when no FROM clause."""
        sql = "SELECT 1"
        
        with self.assertRaises(ValueError) as context:
            Query.from_sql(sql, None, self.database)
        
        self.assertIn("SQL statement must include a FROM clause", str(context.exception))
    
    def test_query_to_string_from_sql(self):
        """Test that Query created from SQL can be converted to string."""
        sql = "SELECT colA, colB FROM tableC"
        query = Query.from_sql(sql, None, self.database)
        
        result_string = query.to_string()
        self.assertIsInstance(result_string, str)
        self.assertIn("tableC", result_string)
        self.assertIn("colA", result_string)
        self.assertIn("colB", result_string)


if __name__ == '__main__':
    unittest.main()
