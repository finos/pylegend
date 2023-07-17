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


class TestPyLegendString:
    frame_to_sql_config = FrameToSqlConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.string_column("col1"),
        PrimitiveTdsColumn.string_column("col2")
    ])
    tds_row = TdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    def test_string_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2")) == '"root".col2'

    def test_string_length_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").len()) == 'CHAR_LENGTH("root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2").length()) == 'CHAR_LENGTH("root".col2)'

    def test_string_startswith_expr(self) -> None:
        with pytest.raises(TypeError) as t:
            self.__generate_sql_string(lambda x: x.get_string("col2").startswith(x.get_string("col2")))  # type: ignore
        assert t.value.args[0].startswith("startswith prefix parameter should be a str")
        assert self.__generate_sql_string(lambda x: x.get_string("col2").startswith("Abc")) == \
               "(\"root\".col2 LIKE 'Abc%')"
        assert self.__generate_sql_string(lambda x: x.get_string("col2").startswith("A_b%c")) == \
               "(\"root\".col2 LIKE 'A\\_b\\%c%')"

    def test_string_endswith_expr(self) -> None:
        with pytest.raises(TypeError) as t:
            self.__generate_sql_string(lambda x: x.get_string("col2").endswith(x.get_string("col2")))  # type: ignore
        assert t.value.args[0].startswith("endswith suffix parameter should be a str")
        assert self.__generate_sql_string(lambda x: x.get_string("col2").endswith("Abc")) == \
               "(\"root\".col2 LIKE '%Abc')"
        assert self.__generate_sql_string(lambda x: x.get_string("col2").endswith("A_b%c")) == \
               "(\"root\".col2 LIKE '%A\\_b\\%c')"

    def test_string_contains_expr(self) -> None:
        with pytest.raises(TypeError) as t:
            self.__generate_sql_string(lambda x: x.get_string("col2").contains(x.get_string("col2")))  # type: ignore
        assert t.value.args[0].startswith("contains/in other parameter should be a str")
        assert self.__generate_sql_string(lambda x: x.get_string("col2").contains("Abc")) == \
               "(\"root\".col2 LIKE '%Abc%')"
        assert self.__generate_sql_string(lambda x: x.get_string("col2").contains("A_b%c")) == \
               "(\"root\".col2 LIKE '%A\\_b\\%c%')"

    def test_string_upper_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").upper()) == 'UPPER("root".col2)'

    def test_string_lower_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").lower()) == 'LOWER("root".col2)'

    def test_string_lstrip_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").lstrip()) == 'LTRIM("root".col2)'

    def test_string_rstrip_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").rstrip()) == 'RTRIM("root".col2)'

    def test_string_strip_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").strip()) == 'BTRIM("root".col2)'

    def test_string_index_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").index(x.get_string("col1"))) == \
               'STRPOS("root".col2, "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2").index("Abc")) == \
               'STRPOS("root".col2, \'Abc\')'
        assert self.__generate_sql_string(lambda x: x.get_string("col2").index_of("Abc")) == \
               'STRPOS("root".col2, \'Abc\')'

    def test_string_parse_int_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").parse_int()) == \
               'CAST("root".col2 AS INTEGER)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2").parse_integer()) == \
               'CAST("root".col2 AS INTEGER)'

    def test_string_parse_float_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").parse_float()) == \
               'CAST("root".col2 AS DOUBLE PRECISION)'

    def __generate_sql_string(self, f: PyLegendCallable[[TdsRow], PyLegendPrimitive]) -> str:
        return self.db_extension.process_expression(
            f(self.tds_row).to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
