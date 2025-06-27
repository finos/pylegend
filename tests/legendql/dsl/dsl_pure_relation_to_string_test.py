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

from legendql.functions import aggregate
from legendql.model.schema import Table, Database
import legendql as lq
import pyarrow as pa

class TestDslToPureRelationDialect(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_select(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)
                 .select(lambda e: [e.id, e.departmentId]))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId])->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_filter(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)
                 .select(lambda e: [e.id, e.departmentId])
                 .filter(lambda e: e.id == 1))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId])->filter(e | $e.id==1)->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_and_condition_in_filter(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)
                 .select(lambda e: [e.id, e.departmentId])
                 .filter(lambda e: (e.id == 1) and (e.id == 2)))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId])->filter(e | $e.id==1 && $e.id==2)->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_or_condition_in_filter(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)
                 .select(lambda e: [e.id, e.departmentId])
                 .filter(lambda e: (e.id == 1) or (e.id == 2)))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId])->filter(e | $e.id==1 || $e.id==2)->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_composite_filter(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)
                 .select(lambda e: [e.id, e.departmentId, e.first])
                 .filter(lambda e: (e.id == 1) or (e.id == 2))
                 .filter(lambda e: e.first == 'Yossarian'))

        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId, first])->filter(e | $e.id==1 || $e.id==2)->filter(e | $e.first=='Yossarian')->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_extend(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)
                 .select(lambda e: [e.id, e.departmentId])
                 .extend(lambda e: [new_col := e.id + 1]))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId])->extend(~[new_col:e | $e.id+1])->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_groupBy(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)
                 .select(lambda e: [e.id, e.departmentId, e.first, e.last])
                 .group_by(lambda r: aggregate(
                    [r.last],
                    [sum_of_id := sum(r.id + 1)])))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId, first, last])->groupBy(~[last], ~[sum_of_id:r | $r.id+1 : r | $r->sum()])->from(legendql::Runtime)",
            pure_relation)

    def test_simple_select_with_groupBy_with_having(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)
                 .select(lambda e: [e.id, e.departmentId, e.first, e.last])
                 .group_by(lambda r: aggregate(
                    [r.last],
                    [sum_of_id := sum(r.id + 1)],
                    r.sum_of_id > 0)))
        pure_relation = query.to_string()
        self.assertEqual(
            "#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId, first, last])->groupBy(~[last], ~[sum_of_id:r | $r.id+1 : r | $r->sum()])->filter(r | $r.sum_of_id>0)->from(legendql::Runtime)",
            pure_relation)

    def test_simple_select_with_limit(self):
        table = Table("table", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [table])
        query = (lq.using_db(database, table.table)
                 .select(lambda e: [e.id, e.departmentId])
                 .limit(1))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[id, departmentId])->limit(1)->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_join_on_table(self):
        emp = Table("emp", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        dep = Table("dep", [pa.field("id", pa.int32(), pa.field("name", pa.utf8()))])
        database = Database("local::DuckDuckDatabase", [emp, dep])
        query = (lq.using_db(database, emp.table)
                 .select(lambda e: [e.id, e.departmentId])
                 .join(lq.using_db(database, dep.table), lambda e, d: e.departmentId == d.id))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.emp}#->select(~[id, departmentId])->join(#>{local::DuckDuckDatabase.dep}#, JoinKind.INNER, {e, d | $e.departmentId==$d.id})->from(legendql::Runtime)", pure_relation)

    def test_simple_select_with_join_on_expression(self):
        emp = Table("emp", [pa.field("id", pa.int32()), pa.field("departmentId", pa.int32()), pa.field("first", pa.utf8()), pa.field("last", pa.utf8())])
        dep = Table("dep", [pa.field("id", pa.int32()), pa.field("name", pa.utf8())])
        database = Database("local::DuckDuckDatabase", [emp, dep])
        dep_query = (lq.using_db(database, dep.table).select(lambda d: [d.id, d.name]))
        query = (lq.using_db(database, emp.table)
                 .select(lambda e: [e.id, e.departmentId])
                 .join(dep_query, lambda e, d: e.departmentId == d.id))
        pure_relation = query.to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.emp}#->select(~[id, departmentId])->join(#>{local::DuckDuckDatabase.dep}#->select(~[id, name]), JoinKind.INNER, {e, d | $e.departmentId==$d.id})->from(legendql::Runtime)", pure_relation)