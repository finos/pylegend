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

import typing
import pytest
from pylegend._typing import PyLegendCallable
from pylegend.core.databse.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.extensions.tds.legend_api.frames.legend_api_table_spec_input_frame import LegendApiTableSpecInputFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.language import LegendApiTdsRow, LegendApiPrimitive


class TestLegendApiBoolean:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.boolean_column("col1"),
        PrimitiveTdsColumn.boolean_column("col2")
    ])
    tds_row = LegendApiTdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    def test_boolean_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_boolean("col2")) == '"root".col2'
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2")) == '$t.col2'

    def test_boolean_error_message(self) -> None:
        with pytest.raises(TypeError) as t:
            self.__generate_sql_string(lambda x: x.get_boolean("col2") | 1)  # type: ignore
        assert t.value.args[0] == ("Boolean OR (|) parameter should be a bool or a boolean expression "
                                   "(PyLegendBoolean). Got value 1 of type: <class 'int'>")
        with pytest.raises(TypeError) as t:
            self.__generate_pure_string(lambda x: x.get_boolean("col2") | 1)  # type: ignore
        assert t.value.args[0] == ("Boolean OR (|) parameter should be a bool or a boolean expression "
                                   "(PyLegendBoolean). Got value 1 of type: <class 'int'>")

    def test_boolean_or_operation(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_boolean("col2") | x.get_boolean("col1")) == \
               '("root".col2 OR "root".col1)'
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2") | x.get_boolean("col1")) == \
               '($t.col2 || $t.col1)'

    def test_boolean_or_operation_with_literal(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_boolean("col2") | True) == \
               '("root".col2 OR true)'
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2") | True) == \
               '($t.col2 || true)'

    def test_boolean_reverse_or_operation_with_literal(self) -> None:
        assert self.__generate_sql_string(lambda x: True | x.get_boolean("col2")) == \
               '(true OR "root".col2)'
        assert self.__generate_pure_string(lambda x: True | x.get_boolean("col2")) == \
               '(true || $t.col2)'

    def test_boolean_and_operation(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_boolean("col2") & x.get_boolean("col1")) == \
               '("root".col2 AND "root".col1)'
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2") & x.get_boolean("col1")) == \
               '($t.col2 && $t.col1)'

    def test_boolean_and_operation_with_literal(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_boolean("col2") & True) == \
               '("root".col2 AND true)'
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2") & True) == \
               '($t.col2 && true)'

    def test_boolean_reverse_and_operation_with_literal(self) -> None:
        assert self.__generate_sql_string(lambda x: False & x.get_boolean("col2")) == \
               '(false AND "root".col2)'
        assert self.__generate_pure_string(lambda x: False & x.get_boolean("col2")) == \
               '(false && $t.col2)'

    def test_boolean_not_operation(self) -> None:
        assert self.__generate_sql_string(lambda x: ~x.get_boolean("col2")) == \
               'NOT("root".col2)'
        assert self.__generate_pure_string(lambda x: ~x.get_boolean("col2")) == \
               '(!$t.col2)'

    @typing.no_type_check
    def test_boolean_equals_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x["col2"] == x["col1"]) == \
               '("root".col2 = "root".col1)'
        assert self.__generate_sql_string(lambda x: x["col2"] == True) == '("root".col2 = true)'  # noqa: E712
        assert self.__generate_sql_string(lambda x: True == x["col2"]) == '("root".col2 = true)'  # noqa: E712
        assert self.__generate_sql_string(lambda x: True == (x["col2"] & x["col1"])) == \
               '(("root".col2 AND "root".col1) = true)'
        assert self.__generate_pure_string(lambda x: x["col2"] == x["col1"]) == \
               '($t.col2 == $t.col1)'
        assert self.__generate_pure_string(lambda x: x["col2"] == True) == '($t.col2 == true)'  # noqa: E712
        assert self.__generate_pure_string(lambda x: True == x["col2"]) == '($t.col2 == true)'  # noqa: E712
        assert self.__generate_pure_string(lambda x: True == (x["col2"] & x["col1"])) == \
               '(($t.col2 && $t.col1) == true)'

    def __generate_sql_string(self, f: PyLegendCallable[[LegendApiTdsRow], LegendApiPrimitive]) -> str:
        return self.db_extension.process_expression(
            f(self.tds_row).to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )

    def __generate_pure_string(self, f: PyLegendCallable[[LegendApiTdsRow], LegendApiPrimitive]) -> str:
        return f(self.tds_row).to_pure_expression(self.frame_to_pure_config)
