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

from legendql.model.schema import Table, Database
from legendql.runtime.pure.db.h2 import H2DatabaseDefinition
import legendql as lq
import pyarrow as pa

from pylegend import LegendClient

table = Table("employees", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
database = Database("legendql::Database", [table])
h2 = H2DatabaseDefinition([
    'DROP TABLE IF EXISTS EMPLOYEES',
    'CREATE TABLE EMPLOYEES(id INT, departmentId INT, first VARCHAR(255), last VARCHAR(255))',
    'INSERT INTO EMPLOYEES VALUES(1, 1, \'John\', \'Doe\')',
    'INSERT INTO EMPLOYEES VALUES(2, 1, \'Jane\', \'Doe\')',])


@unittest.skip
class TestExecutionServerEvaluation(unittest.TestCase):
    def test_execution_against_execution_server(self):
        tds = (lq.using_db(database, table.table)
               ._query
               .select("id", "departmentId", "first", "last")
               .execute(h2, LegendClient("localhost", self.dynamic_port, secure_http=False)))

        self.assertEqual(tds.relation, "#>{legendql::Database.employees}#->select(~[id, departmentId, first, last])->from(legendql::Runtime)")
        self.assertEqual(tds.sql, 'select "employees_0".id as "id", "employees_0".departmentId as "departmentId", "employees_0".first as "first", "employees_0".last as "last" from employees as "employees_0"')
        self.assertEqual(",".join(tds.data.column_names), "id,departmentId,first,last")
        self.assertEqual(len(tds.data.columns), 4)
        self.assertEqual(",".join(map(lambda r: str(r), tds.data.columns[0])), "1,2")
        self.assertEqual(",".join(map(lambda r: str(r), tds.data.columns[1])), "1,1")
        self.assertEqual(",".join(map(lambda r: str(r), tds.data.columns[2])), "John,Jane")
        self.assertEqual(",".join(map(lambda r: str(r), tds.data.columns[3])), "Doe,Doe")
