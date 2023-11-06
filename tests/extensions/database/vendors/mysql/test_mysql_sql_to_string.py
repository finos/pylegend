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


import importlib
from pylegend.core.databse.sql_to_string import (
    SqlToStringGenerator,
    SqlToStringConfig,
    SqlToStringFormat,
)

mysql_ext = 'pylegend.extensions.database.vendors.mysql.mysql_sql_to_string'
importlib.import_module(mysql_ext)


class TestMySQLSqlToString:
    generator = SqlToStringGenerator.find_sql_to_string_generator_for_db_type("MySQL")
    db_ext = generator.get_db_extension()

    def test_find_mysql_sql_generator(self) -> None:
        assert self.generator is not None
        assert (str(type(self.generator)) ==
                "<class 'pylegend.extensions.database.vendors.mysql.mysql_sql_to_string.MySQLSqlToStringGenerator'>")

    def test_process_identifier(self) -> None:
        config = SqlToStringConfig(SqlToStringFormat(pretty=False))
        assert self.db_ext.process_identifier("A", config, True) == "`A`"
