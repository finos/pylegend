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

import pytest
from pylegend._typing import PyLegendCallable
from pylegend.core.databse.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.legend_api.frames.legend_api_table_spec_input_frame import LegendApiTableSpecInputFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.language import TdsRow, PyLegendPrimitive


class TestPyLegendBoolean:
    frame_to_sql_config = FrameToSqlConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.boolean_column("col1"),
        PrimitiveTdsColumn.boolean_column("col2")
    ])
    tds_row = TdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    def test_boolean_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_boolean("col2")) == '"root".col2'

    def test_boolean_or_operation(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_boolean("col2") | x.get_boolean("col1")) == \
               '("root".col2 OR "root".col1)'

    def test_boolean_or_operation_with_literal(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_boolean("col2") | True) == \
               '("root".col2 OR true)'

    def test_boolean_error_message(self) -> None:
        with pytest.raises(TypeError) as t:
            self.__generate_sql_string(lambda x: x.get_boolean("col2") | 1)  # type: ignore
        assert t.value.args[0] == ("Boolean OR (|) parameter should be a bool or a boolean expression "
                                   "(PyLegendBoolean). Got value 1 of type: <class 'int'>")

    def test_boolean_reverse_or_operation_with_literal(self) -> None:
        assert self.__generate_sql_string(lambda x: True | x.get_boolean("col2")) == \
               '(true OR "root".col2)'

    def __generate_sql_string(self, f: PyLegendCallable[[TdsRow], PyLegendPrimitive]) -> str:
        return self.db_extension.process_expression(
            f(self.tds_row).to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
