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
from legendql import from_sql
from legendql.ql import LegendQL
from legendql.model.schema import Database, Table


class TestLegendQLFromSQL(unittest.TestCase):
    
    def setUp(self):
        """Set up test database."""
        columns = [
            pa.field("colA", pa.string()),
            pa.field("colB", pa.int64()),
            pa.field("colC", pa.float64())
        ]
        table = Table("tableC", columns)
        self.database = Database("test_db", [table])
    
    def test_from_sql_basic(self):
        """Test creating LegendQL from basic SQL statement."""
        sql = "SELECT colA, colB FROM tableC"
        lql = from_sql(sql, self.database)
        
        self.assertIsInstance(lql, LegendQL)
        self.assertIsNotNone(lql._query)
        self.assertEqual(len(lql._query._clauses), 2)
    
    def test_from_sql_with_where(self):
        """Test creating LegendQL from SQL statement with WHERE clause."""
        sql = "SELECT colA FROM tableC WHERE colB = 42"
        lql = from_sql(sql, self.database)
        
        self.assertIsInstance(lql, LegendQL)
        self.assertIsNotNone(lql._query)
        self.assertEqual(len(lql._query._clauses), 3)
    
    def test_from_sql_to_string(self):
        """Test that LegendQL created from SQL can be converted to string."""
        sql = "SELECT colA, colB FROM tableC"
        lql = from_sql(sql, self.database)
        
        result_string = lql.to_string()
        self.assertIsInstance(result_string, str)
        self.assertIn("tableC", result_string)
        self.assertIn("colA", result_string)
        self.assertIn("colB", result_string)
    
    def test_from_sql_chaining(self):
        """Test that LegendQL created from SQL supports method chaining."""
        sql = "SELECT colA FROM tableC"
        lql = from_sql(sql, self.database)
        
        result = lql.limit(10)
        self.assertIsInstance(result, LegendQL)
        self.assertEqual(len(result._query._clauses), 3)  # FROM, SELECT, LIMIT
    
    def test_from_sql_invalid_table(self):
        """Test that from_sql raises error for non-existent table."""
        sql = "SELECT colA FROM nonexistent_table"
        
        with self.assertRaises(ValueError) as context:
            from_sql(sql, self.database)
        
        self.assertIn("Table nonexistent_table not found", str(context.exception))


if __name__ == '__main__':
    unittest.main()
