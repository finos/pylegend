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
import typing
from pylegend._typing import PyLegendCallable
from pylegend.core.database.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.language import PyLegendPrimitive, PyLegendInteger
from pylegend.core.request.legend_client import LegendClient
from pylegend._typing import PyLegendDict, PyLegendUnion
from tests.core.language.shared import TestTableSpecInputFrame, TestTdsRow


class TestPyLegendInteger:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.integer_column("col1"),
        PrimitiveTdsColumn.integer_column("col2")
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_integer_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_integer("col2")) == '"root".col2'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2")) == '$t.col2'

    def test_integer_add_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_integer("col2") + x.get_integer("col1")) == \
               '("root".col2 + "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_integer("col2") + 10) == \
               '("root".col2 + 10)'
        assert self.__generate_sql_string(lambda x: 10 + x.get_integer("col2")) == \
               '(10 + "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") + x.get_integer("col1")) == \
               '(toOne($t.col2) + toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") + 10) == \
               '(toOne($t.col2) + 10)'
        assert self.__generate_pure_string(lambda x: 10 + x.get_integer("col2")) == \
               '(10 + toOne($t.col2))'

    def test_integer_float_add_expr(self) -> None:
        assert self.__generate_sql_string_no_integer_assert(lambda x: x.get_integer("col2") + 1.2) == \
               '("root".col2 + 1.2)'
        assert self.__generate_sql_string_no_integer_assert(lambda x: 1.2 + x.get_integer("col2")) == \
               '(1.2 + "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") + 1.2) == \
               '(toOne($t.col2) + 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 + x.get_integer("col2")) == \
               '(1.2 + toOne($t.col2))'

    def test_integer_subtract_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_integer("col2") - x.get_integer("col1")) == \
               '("root".col2 - "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_integer("col2") - 10) == \
               '("root".col2 - 10)'
        assert self.__generate_sql_string(lambda x: 10 - x.get_integer("col2")) == \
               '(10 - "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") - x.get_integer("col1")) == \
               '(toOne($t.col2) - toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") - 10) == \
               '(toOne($t.col2) - 10)'
        assert self.__generate_pure_string(lambda x: 10 - x.get_integer("col2")) == \
               '(10 - toOne($t.col2))'

    def test_integer_float_subtract_expr(self) -> None:
        assert self.__generate_sql_string_no_integer_assert(lambda x: x.get_integer("col2") - 1.2) == \
               '("root".col2 - 1.2)'
        assert self.__generate_sql_string_no_integer_assert(lambda x: 1.2 - x.get_integer("col2")) == \
               '(1.2 - "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") - 1.2) == \
               '(toOne($t.col2) - 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 - x.get_integer("col2")) == \
               '(1.2 - toOne($t.col2))'

    def test_integer_multiply_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_integer("col2") * x.get_integer("col1")) == \
               '("root".col2 * "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_integer("col2") * 10) == \
               '("root".col2 * 10)'
        assert self.__generate_sql_string(lambda x: 10 * x.get_integer("col2")) == \
               '(10 * "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") * x.get_integer("col1")) == \
               '(toOne($t.col2) * toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") * 10) == \
               '(toOne($t.col2) * 10)'
        assert self.__generate_pure_string(lambda x: 10 * x.get_integer("col2")) == \
               '(10 * toOne($t.col2))'

    def test_integer_float_multiply_expr(self) -> None:
        assert self.__generate_sql_string_no_integer_assert(lambda x: x.get_integer("col2") * 1.2) == \
               '("root".col2 * 1.2)'
        assert self.__generate_sql_string_no_integer_assert(lambda x: 1.2 * x.get_integer("col2")) == \
               '(1.2 * "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") * 1.2) == \
               '(toOne($t.col2) * 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 * x.get_integer("col2")) == \
               '(1.2 * toOne($t.col2))'

    def test_integer_modulo_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_integer("col2") % x.get_integer("col1")) == \
               'MOD((MOD("root".col2, "root".col1) + "root".col1), "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_integer("col2") % 10) == \
               'MOD((MOD("root".col2, 10) + 10), 10)'
        assert self.__generate_sql_string(lambda x: 10 % x.get_integer("col2")) == \
               'MOD((MOD(10, "root".col2) + "root".col2), "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") % x.get_integer("col1")) == \
               'toOne($t.col2)->mod(toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") % 10) == \
               'toOne($t.col2)->mod(10)'
        assert self.__generate_pure_string(lambda x: 10 % x.get_integer("col2")) == \
               '10->mod(toOne($t.col2))'

    def test_integer_abs_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: abs(x.get_integer("col2"))) == \
               'ABS("root".col2)'
        assert self.__generate_sql_string(lambda x: abs(x.get_integer("col2") + x.get_integer("col1"))) == \
               'ABS(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: abs(x.get_integer("col2"))) == \
               'toOne($t.col2)->abs()'
        assert self.__generate_pure_string(lambda x: abs(x.get_integer("col2") + x.get_integer("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->abs()'

    def test_integer_neg_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: -x.get_integer("col2")) == \
               '(0 - "root".col2)'
        assert self.__generate_sql_string(lambda x: -(x.get_integer("col2") + x.get_integer("col1"))) == \
               '(0 - ("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: -x.get_integer("col2")) == \
               'toOne($t.col2)->minus()'
        assert self.__generate_pure_string(lambda x: -(x.get_integer("col2") + x.get_integer("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->minus()'

    def test_integer_pos_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: + x.get_integer("col2")) == \
               '"root".col2'
        assert self.__generate_sql_string(lambda x: +(x.get_integer("col2") + x.get_integer("col1"))) == \
               '("root".col2 + "root".col1)'
        assert self.__generate_pure_string(lambda x: + x.get_integer("col2")) == \
               '$t.col2'
        assert self.__generate_pure_string(lambda x: +(x.get_integer("col2") + x.get_integer("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))'

    @typing.no_type_check
    def test_integer_equals_expr(self) -> None:
        assert self.__generate_sql_string_no_integer_assert(lambda x: x["col2"] == x["col1"]) == \
               '("root".col2 = "root".col1)'
        assert self.__generate_sql_string_no_integer_assert(lambda x: x["col2"] == 1) == \
               '("root".col2 = 1)'
        assert self.__generate_sql_string_no_integer_assert(lambda x: 1 == x["col2"]) == \
               '("root".col2 = 1)'
        assert self.__generate_sql_string_no_integer_assert(lambda x: 1 == (x["col2"] + x["col1"])) == \
               '(("root".col2 + "root".col1) = 1)'
        assert self.__generate_pure_string(lambda x: x["col2"] == x["col1"]) == \
               '($t.col2 == $t.col1)'
        assert self.__generate_pure_string(lambda x: x["col2"] == 1) == \
               '($t.col2 == 1)'
        assert self.__generate_pure_string(lambda x: 1 == x["col2"]) == \
               '($t.col2 == 1)'
        assert self.__generate_pure_string(lambda x: 1 == (x["col2"] + x["col1"])) == \
               '((toOne($t.col2) + toOne($t.col1)) == 1)'

    def test_integer_to_string_expr(self) -> None:
        assert self.__generate_sql_string_no_integer_assert(lambda x: x.get_integer("col2").to_string()) == \
               'CAST("root".col2 AS TEXT)'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2").to_string()) == \
               'toOne($t.col2)->toString()'

    def test_integer_to_char_expr(self) -> None:
        assert self.__generate_sql_string_no_integer_assert(lambda x: x.get_integer("col2").char()) == \
               'CHR("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2").char()) == \
               'toOne($t.col2)->char()'

    def __generate_sql_string(self, f: PyLegendCallable[[TestTdsRow], PyLegendPrimitive]) -> str:
        ret = f(self.tds_row)
        assert isinstance(ret, PyLegendInteger)
        return self.db_extension.process_expression(
            ret.to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )

    def __generate_sql_string_no_integer_assert(self, f: PyLegendCallable[[TestTdsRow], PyLegendPrimitive]) -> str:
        ret = f(self.tds_row)
        return self.db_extension.process_expression(
            ret.to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )

    def __generate_pure_string(self, f) -> str:  # type: ignore
        expr = str(f(self.tds_row).to_pure_expression(self.frame_to_pure_config))
        model_code = """
        function test::testFunc(): Any[*]
        {
            []->toOne()->cast(
                @meta::pure::metamodel::relation::Relation<(col1: Integer[0..1], col2: Integer[0..1])>
            )
            ->extend(~new_col:t|<<expression>>)
        }
        """
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr
