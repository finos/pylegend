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
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.request.legend_client import LegendClient
from pylegend._typing import PyLegendDict, PyLegendUnion
from tests.core.language.shared import TestTableSpecInputFrame, TestTdsRow


class TestPyLegendInteger:
    frame_to_pure_config = FrameToPureConfig()
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.integer_column("col1"),
        PrimitiveTdsColumn.integer_column("col2")
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_integer_col_access(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_integer("col2")) == '$t.col2'

    def test_integer_add_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") + x.get_integer("col1")) == \
               '(toOne($t.col2) + toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") + 10) == \
               '(toOne($t.col2) + 10)'
        assert self.__generate_pure_string(lambda x: 10 + x.get_integer("col2")) == \
               '(10 + toOne($t.col2))'

    def test_integer_float_add_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") + 1.2) == \
               '(toOne($t.col2) + 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 + x.get_integer("col2")) == \
               '(1.2 + toOne($t.col2))'

    def test_integer_subtract_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") - x.get_integer("col1")) == \
               '(toOne($t.col2) - toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") - 10) == \
               '(toOne($t.col2) - 10)'
        assert self.__generate_pure_string(lambda x: 10 - x.get_integer("col2")) == \
               '(10 - toOne($t.col2))'

    def test_integer_float_subtract_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") - 1.2) == \
               '(toOne($t.col2) - 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 - x.get_integer("col2")) == \
               '(1.2 - toOne($t.col2))'

    def test_integer_multiply_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") * x.get_integer("col1")) == \
               '(toOne($t.col2) * toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") * 10) == \
               '(toOne($t.col2) * 10)'
        assert self.__generate_pure_string(lambda x: 10 * x.get_integer("col2")) == \
               '(10 * toOne($t.col2))'

    def test_integer_float_multiply_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") * 1.2) == \
               '(toOne($t.col2) * 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 * x.get_integer("col2")) == \
               '(1.2 * toOne($t.col2))'

    def test_integer_modulo_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") % x.get_integer("col1")) == \
               'toOne($t.col2)->mod(toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2") % 10) == \
               'toOne($t.col2)->mod(10)'
        assert self.__generate_pure_string(lambda x: 10 % x.get_integer("col2")) == \
               '10->mod(toOne($t.col2))'

    def test_integer_abs_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: abs(x.get_integer("col2"))) == \
               'toOne($t.col2)->abs()'
        assert self.__generate_pure_string(lambda x: abs(x.get_integer("col2") + x.get_integer("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->abs()'

    def test_integer_neg_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: -x.get_integer("col2")) == \
               'toOne($t.col2)->minus()'
        assert self.__generate_pure_string(lambda x: -(x.get_integer("col2") + x.get_integer("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->minus()'

    def test_integer_pos_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: + x.get_integer("col2")) == \
               '$t.col2'
        assert self.__generate_pure_string(lambda x: +(x.get_integer("col2") + x.get_integer("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))'

    @typing.no_type_check
    def test_integer_equals_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x["col2"] == x["col1"]) == \
               '($t.col2 == $t.col1)'
        assert self.__generate_pure_string(lambda x: x["col2"] == 1) == \
               '($t.col2 == 1)'
        assert self.__generate_pure_string(lambda x: 1 == x["col2"]) == \
               '($t.col2 == 1)'
        assert self.__generate_pure_string(lambda x: 1 == (x["col2"] + x["col1"])) == \
               '((toOne($t.col2) + toOne($t.col1)) == 1)'

    def test_integer_to_string_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_integer("col2").to_string()) == \
               'toOne($t.col2)->toString()'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2").toString()) == \
               'toOne($t.col2)->toString()'

    def test_integer_to_char_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_integer("col2").char()) == \
               'toOne($t.col2)->char()'

    def test_integer_invert_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: ~x.get_integer("col2")) == \
               'toOne($t.col2)->bitNot()'

    @pytest.mark.parametrize(
        "py_op, sql_op, pure_fn",
        [
            ("&", "&", "bitAnd"),
            ("|", "|", "bitOr"),
            ("^", "#", "bitXor"),
            ("<<", "<<", "bitShiftLeft"),
            (">>", ">>", "bitShiftRight"),
        ],
    )
    def test_integer_bitwise_binary_expr(
            self,
            py_op: str,
            sql_op: str,
            pure_fn: str) -> None:
        assert self.__generate_pure_string(
            lambda x: eval(f'x.get_integer("col2") {py_op} x.get_integer("col1")')
        ) == f'toOne($t.col2)->{pure_fn}(toOne($t.col1))'

        assert self.__generate_pure_string(
            lambda x: eval(f'x.get_integer("col2") {py_op} 10')
        ) == f'toOne($t.col2)->{pure_fn}(10)'

        assert self.__generate_pure_string(
            lambda x: eval(f'10 {py_op} x.get_integer("col2")')
        ) == f'10->{pure_fn}(toOne($t.col2))'

    def test_integer_in_list_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_integer("col2").in_list([1, 2, 3])) == \
               '$t.col2->in([1, 2, 3])'
        assert self.__generate_pure_string(lambda x: x.get_integer("col2").in_list([42])) == \
               '$t.col2->in([42])'

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
