# Copyright 2023 Goldman Sachs
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


# type: ignore
from abc import ABCMeta, abstractmethod
import pytest
from pylegend.core.databse.sql_to_string import SqlToStringFormat, SqlToStringConfig
from pylegend.core.sql.metamodel import (
    IntegerLiteral,
    LongLiteral,
    StringLiteral,
    DoubleLiteral,
    NullLiteral,
    BooleanLiteral,
    QuerySpecification,
    Select,
    SingleColumn
)


class E2EDbSpecificSqlGenerationTest(metaclass=ABCMeta):
    config = SqlToStringConfig(SqlToStringFormat(pretty=True))

    @pytest.fixture(scope='module')
    def db_test(self):
        pass

    def test_literal(self, db_test):
        result = self.wrap_and_execute_expression(db_test, IntegerLiteral(101)).fetchall()
        assert (len(result) == 1) and (len(result[0]) == 1) and (result[0][0] == 101)
        result = self.wrap_and_execute_expression(db_test, LongLiteral(202)).fetchall()
        assert (len(result) == 1) and (len(result[0]) == 1) and (result[0][0] == 202)
        result = self.wrap_and_execute_expression(db_test, DoubleLiteral(303.3)).fetchall()
        assert (len(result) == 1) and (len(result[0]) == 1) and (float(result[0][0]) == 303.3)
        result = self.wrap_and_execute_expression(db_test, StringLiteral("a'b", quoted=False)).fetchall()
        assert (len(result) == 1) and (len(result[0]) == 1) and (result[0][0] == "a'b")
        result = self.wrap_and_execute_expression(db_test, BooleanLiteral(True)).fetchall()
        assert (len(result) == 1) and (len(result[0]) == 1) and (result[0][0])
        result = self.wrap_and_execute_expression(db_test, BooleanLiteral(False)).fetchall()
        assert (len(result) == 1) and (len(result[0]) == 1) and (not result[0][0])
        result = self.wrap_and_execute_expression(db_test, NullLiteral()).fetchall()
        assert (len(result) == 1) and (len(result[0]) == 1) and (result[0][0] is None)

    def wrap_and_execute_expression(self, db_test, expr):
        wrapped = QuerySpecification(
            select=Select(
                selectItems=[SingleColumn(alias=self.db_extension().quote_identifier("res"), expression=expr)],
                distinct=False
            ),
            from_=[],
            where=None,
            groupBy=[],
            having=None,
            orderBy=[],
            limit=None,
            offset=None
        )
        sql = self.db_extension().process_query_specification(wrapped, self.config)
        return self.execute_sql(db_test, sql)

    @abstractmethod
    def execute_sql(self, db_test, sql):
        pass

    @abstractmethod
    def db_extension(self):
        pass
