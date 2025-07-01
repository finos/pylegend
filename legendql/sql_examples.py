"""Examples demonstrating SQL to Pure Relation conversion using SQLGlot."""

import pyarrow as pa
from legendql.sql_parser import sql_to_pure_relation
from legendql.model.schema import Database, Table
from legendql.runtime.pure.db.h2 import H2DatabaseDefinition

employees_table = Table(
    table="employees",
    columns=[
        pa.field("id", pa.int32()),
        pa.field("name", pa.utf8()),
        pa.field("department", pa.utf8()),
        pa.field("salary", pa.float32())
    ]
)

database = Database(name="company", children=[employees_table])
db_def = H2DatabaseDefinition(sqls=[])

sql1 = "SELECT id, name FROM employees"
pure_code1 = sql_to_pure_relation(sql1, db_def, database)
print("Example 1:")
print(f"SQL: {sql1}")
print(f"Pure Relation: {pure_code1}")
print()

sql2 = "SELECT name, department, salary FROM employees"
pure_code2 = sql_to_pure_relation(sql2, db_def, database)
print("Example 2:")
print(f"SQL: {sql2}")
print(f"Pure Relation: {pure_code2}")
