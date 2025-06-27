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

import legendql as lq
import pyarrow as pa
from legendql.functions import over, avg, rows, aggregate, unbounded, count, left


'''
    Example DSL with Join using Fluent-style API
'''
# [emp, dep] = lq.db({
#     "employees": [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("dept_id", pa.utf8()), pa.field("salary", pa.float32()), pa.field("title", pa.utf8())],
#     "department": [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("city", pa.utf8()), pa.field("code", pa.utf8()), pa.field("location", pa.utf8())]
# })
#
# (emp
#  .filter(lambda r: r.id > 10)
#  .left_join(dep, lambda e, d: (
#     e.dept_id == d.id,
#     (department_name := d.name, department_id := d.id)))
#  .extend(lambda r:[
#     (ids := r.id + r.dept_id),
#     (avg_val := over(r.location, avg(r.salary),
#     sort=[r.name, -r.location],
#     frame=rows(0, unbounded())))])
#  .group_by(lambda r: aggregate(
#     [ r.id, r.name ],
#     [sum_salary := sum(r.salary), count_dept := count(r.department_name) ],
#     having=sum_salary > 100_000))
#  .filter(lambda r: r.id > 100)
#  .extend(lambda r: (calc_col := r.id + r.sum_salary)))
#
# for clause in emp._query._clauses:
#     print(clause)


'''
    PRQL example from https://prql-lang.org/book/index.html
    Using reassignment for each expression
'''
q = lq.table("employees", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("title", pa.utf8()), pa.field("country", pa.utf8()), pa.field("dept_id", pa.utf8()), pa.field("salary", pa.float32()), pa.field("start_date", pa.utf8()), pa.field("benefits", pa.utf8())])

q = q.filter(lambda e: e.start_date > '2021-01-01')
q = q.extend(lambda e: [
        (gross_salary := e.salary + 10),
        (gross_cost := gross_salary + e.benefits) ])
q = q.filter(lambda e: e.gross_cost > 0)
q = q.group_by(lambda e: aggregate(
        [e.title, e.country],
        [avg_gross_salary := avg(e.gross_salary), sum_gross_cost := sum(e.gross_cost)],
        having=sum_gross_cost > 100_000))
q = q.extend(lambda e: (new_id := f"{e.title}_{e.country}"))
q = q.extend(lambda e: (country_code := left(e.country, 2)))
q = q.order_by(lambda e: [e.sum_gross_cost, -e.country])
q = q.limit(10)

for clause in q._query._clauses:
    print(clause)

'''
    Same PRQL example using Fluent API, spacing and line breaks are important
'''

emp = lq.table("employees", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("title", pa.utf8()), pa.field("country", pa.utf8()), pa.field("dept_id", pa.utf8()), pa.field("salary", pa.float32()), pa.field("start_date", pa.utf8()), pa.field("benefits", pa.utf8())])

(emp
 .filter(lambda e: e.start_date > '2021-01-01')
 .extend(lambda e: [
    (gross_salary := e.salary + 10),
    (gross_cost := gross_salary + e.benefits) ])
 .filter(lambda e: e.gross_cost > 0)
 .group_by(lambda e: aggregate(
    [e.title, e.country],
    [avg_gross_salary := avg(e.gross_salary), sum_gross_cost := sum(e.gross_cost)],
    having=sum_gross_cost > 100_000))
 .extend(lambda e: (new_id := f"{e.title}_{e.country}"))
 .extend(lambda e: (country_code := left(e.country, 2)))
 .order_by(lambda e: [e.sum_gross_cost, -e.country])
 .limit(10))

for clause in emp._query._clauses:
    print(clause)


'''
    Example Window as an expression
'''
# emp = lq.table("employees", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("dept_id", pa.utf8()), pa.field("salary", pa.float32()), pa.field("location", pa.utf8())])
#
# (emp
#  .extend(lambda r: (
#     avg_val := over(
#         r.location,
#         (avg_val := avg(r.salary)),
#         sort=[r.name, -r.location],
#         frame=rows(0, unbounded()),
#         qualify=avg_val > 100_000)))
#  )
#
# for clause in emp._query._clauses:
#     print(clause)

emp = lq.table("employees", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("dept_id", pa.utf8()), pa.field("salary", pa.float32()), pa.field("location", pa.utf8())])

emp = emp.extend(lambda r: (avg_val := over(r.location, avg(r.salary))))

for clause in emp._query._clauses:
    print(clause)

emp = lq.table("employees", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("title", pa.utf8()), pa.field("dept_id", pa.float32()), pa.field("salary", pa.float32())])
emp = emp.group_by(lambda r: aggregate(r.title, avg_salary := avg(r.salary)))

for clause in emp._query._clauses:
    print(clause)

emp = lq.table("employees", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("title", pa.utf8()), pa.field("dept_id", pa.float32()), pa.field("salary", pa.float32())])
emp = emp.group_by(lambda r: aggregate([r.title, r.dept_id], avg_salary := avg(r.salary)))

for clause in emp._query._clauses:
    print(clause)

emp = lq.table("employees", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("title", pa.utf8()), pa.field("dept_id", pa.float32()), pa.field("salary", pa.float32())])
emp = emp.group_by(lambda r: aggregate(r.title, avg_salary := avg(r.salary), having=avg_salary > 100_000))

for clause in emp._query._clauses:
    print(clause)

# emp = lq.from_("employees", {"id": int, "name": str, "dept_id": str, "salary": float, "title": str})
# dep = lq.from_("department", {"id": int, "name": str, "city": str, "code": str, "location": str})
# loc = lq.from_("location", {"id": int, "name": str, "country": str, "code": str})

# [emp, dep, loc] = lq.db({
#     "employees": [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("title", pa.utf8()), pa.field("dept_id", pa.float32()), pa.field("salary", pa.float32())],
#     "department": [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("city", pa.utf8()), pa.field("code", pa.utf8()), pa.field("location", pa.utf8())],
#     "location": [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("country", pa.utf8()), pa.field("code", pa.utf8())]
# })
#
# emp = (emp
#  .left_join(dep, lambda e, d: (e.dept_id == d.id, (new_dept_id := d.id, new_dept_name := d.name)))
#  .left_join(loc, lambda d, l: (d.city == l.id, (new_loc_id := l.id, new_loc_name := l.name, new_loc_code := l.code))))
#
# for clause in emp._query._clauses:
#     print(clause)

# [emp, dep, loc] = lq.db({
#     "employees": [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("dept_id", pa.float32()), pa.field("salary", pa.float32())],
#     "department": [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("city", pa.utf8()), pa.field("code", pa.utf8())],
#     "location": [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("country", pa.utf8()),pa.field("code", pa.utf8())]
# })
#
# emp = (emp
#  .left_join(dep, lambda e, d: (e.dept_id == d.id, [(new_dept_id := d.id), (new_dept_name := d.name)]))
#  .left_join(loc, lambda d, l: (d.city == l.id, (new_loc_id := l.id, new_loc_name := l.name, new_loc_code := l.code))))
#
# for clause in emp._query._clauses:
#     print(clause)

dep = lq.table("department", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("city", pa.utf8()), pa.field("code", pa.utf8())])
dep.extend(lambda e: (id_plus_one := e.id + 1))

print(dep._query._table.columns)

for clause in dep._query._clauses:
    print(clause)

# [emp, dep] = lq.db({
#     "employees": [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("dept_id", pa.float32()), pa.field("salary", pa.float32())],
#     "department": [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("city", pa.utf8()), pa.field("code", pa.utf8())]
# })
# emp = emp.left_join(dep, lambda e, d: (e.dept_id == d.id, [(new_dept_id := d.id), (new_dept_name := d.name)]))
#
# print(emp._query._table.columns)

dep = lq.table("department", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("city", pa.utf8()), pa.field("code", pa.utf8())])
dep.rename(lambda d: (new_dept_id := d.id, new_dept_name := d.name))

print(dep._query._table.columns)

for clause in dep._query._clauses:
    print(clause)


dep = lq.table("department", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("city", pa.utf8()), pa.field("code", pa.utf8())])
dep.select(lambda d: [d.id, d.name])

print(dep._query._table.columns)

for clause in dep._query._clauses:
    print(clause)


dep = lq.table("department", [pa.field("id", pa.int32()), pa.field("name", pa.utf8()), pa.field("city", pa.utf8()), pa.field("code", pa.utf8())])
dep.group_by(lambda d: aggregate([d.id, d.name], [sum_test := sum(d.code), count_test := count(d.city)]))

print(dep._query._table.columns)

for clause in dep._query._clauses:
    print(clause)
