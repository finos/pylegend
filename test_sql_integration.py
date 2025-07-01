#!/usr/bin/env python3
"""Test script for SQLGlot integration with LegendQL."""

import pyarrow as pa
from legendql.sql_parser import sql_to_pure_relation
from legendql.model.schema import Database, Table
from legendql.runtime.pure.db.h2 import H2DatabaseDefinition

def test_basic_sql_conversion():
    """Test basic SQL SELECT conversion to Pure Relation code."""
    
    table = Table(
        table="tableC",
        columns=[
            pa.field("colA", pa.utf8()),
            pa.field("colB", pa.int32())
        ]
    )
    database = Database(name="testdb", children=[table])
    
    db_def = H2DatabaseDefinition(sqls=[])
    
    sql = "SELECT colA, colB FROM tableC"
    
    try:
        pure_relation_code = sql_to_pure_relation(sql, db_def, database)
        print("SQL:", sql)
        print("Pure Relation Code:", pure_relation_code)
        print("✅ Basic SQL conversion successful!")
        return True
    except Exception as e:
        print(f"❌ Error converting SQL: {e}")
        return False

if __name__ == "__main__":
    success = test_basic_sql_conversion()
    if success:
        print("\n🎉 SQLGlot integration test passed!")
    else:
        print("\n💥 SQLGlot integration test failed!")
