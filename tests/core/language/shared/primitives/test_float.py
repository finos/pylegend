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


class TestPyLegendFloat:
    frame_to_pure_config = FrameToPureConfig()
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.float_column("col1"),
        PrimitiveTdsColumn.float_column("col2")
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_float_col_access(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_float("col1")) == '$t.col1'

    def test_float_add_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_float("col2") + x.get_float("col1")) == \
               '(toOne($t.col2) + toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_float("col2") + 1.2) == \
               '(toOne($t.col2) + 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 + x.get_float("col2")) == \
               '(1.2 + toOne($t.col2))'

    def test_float_integer_add_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_float("col2") + 10) == \
               '(toOne($t.col2) + 10)'
        assert self.__generate_pure_string(lambda x: 10 + x.get_float("col2")) == \
               '(10 + toOne($t.col2))'

    def test_float_subtract_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_float("col2") - x.get_float("col1")) == \
               '(toOne($t.col2) - toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_float("col2") - 1.2) == \
               '(toOne($t.col2) - 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 - x.get_float("col2")) == \
               '(1.2 - toOne($t.col2))'

    def test_float_integer_subtract_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_float("col2") - 10) == \
               '(toOne($t.col2) - 10)'
        assert self.__generate_pure_string(lambda x: 10 - x.get_float("col2")) == \
               '(10 - toOne($t.col2))'

    def test_float_multiply_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_float("col2") * x.get_float("col1")) == \
               '(toOne($t.col2) * toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_float("col2") * 1.2) == \
               '(toOne($t.col2) * 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 * x.get_float("col2")) == \
               '(1.2 * toOne($t.col2))'

    def test_float_integer_multiply_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_float("col2") * 10) == \
               '(toOne($t.col2) * 10)'
        assert self.__generate_pure_string(lambda x: 10 * x.get_float("col2")) == \
               '(10 * toOne($t.col2))'

    def test_float_abs_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: abs(x.get_float("col2"))) == \
               'toOne($t.col2)->abs()'
        assert self.__generate_pure_string(lambda x: abs(x.get_float("col2") + x.get_float("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->abs()'

    def test_float_neg_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: -x.get_float("col2")) == \
               'toOne($t.col2)->minus()'
        assert self.__generate_pure_string(lambda x: -(x.get_float("col2") + x.get_float("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->minus()'

    def test_float_pos_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: + x.get_float("col2")) == \
               '$t.col2'
        assert self.__generate_pure_string(lambda x: +(x.get_float("col2") + x.get_float("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))'

    @typing.no_type_check
    def test_float_equals_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x["col2"] == x["col1"]) == \
               '($t.col2 == $t.col1)'
        assert self.__generate_pure_string(lambda x: x["col2"] == 1) == \
               '($t.col2 == 1)'
        assert self.__generate_pure_string(lambda x: 1 == x["col2"]) == \
               '($t.col2 == 1)'
        assert self.__generate_pure_string(lambda x: 1 == (x["col2"] + x["col1"])) == \
               '((toOne($t.col2) + toOne($t.col1)) == 1)'

    def test_float_to_string_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_float("col2").to_string()) == \
               'toOne($t.col2)->toString()'

    def test_float_in_list_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_float("col2").in_list([1.1, 2.2, 3.3])) == \
               '$t.col2->in([1.1, 2.2, 3.3])'
        assert self.__generate_pure_string(lambda x: x.get_float("col2").in_list([4.2])) == \
               '$t.col2->in([4.2])'

    def __generate_pure_string(self, f) -> str:  # type: ignore
        expr = str(f(self.tds_row).to_pure_expression(self.frame_to_pure_config))
        model_code = """
        function test::testFunc(): Any[*]
        {
            []->toOne()->cast(
                @meta::pure::metamodel::relation::Relation<(col1: Float[0..1], col2: Float[0..1])>
            )
            ->extend(~new_col:t|<<expression>>)
        }
        """
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr
