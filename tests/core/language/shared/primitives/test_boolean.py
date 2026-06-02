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
from datetime import date, datetime
from decimal import Decimal
from pylegend._typing import PyLegendCallable
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.language import PyLegendPrimitive
from pylegend.core.request.legend_client import LegendClient
from pylegend._typing import PyLegendDict, PyLegendUnion
from tests.core.language.shared import TestTableSpecInputFrame, TestTdsRow


class TestPyLegendBoolean:
    frame_to_pure_config = FrameToPureConfig()
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.boolean_column("col1"),
        PrimitiveTdsColumn.boolean_column("col2")
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_boolean_col_access(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2")) == '$t.col2'

    def test_boolean_error_message(self) -> None:
        with pytest.raises(TypeError):
            self.__generate_pure_string(lambda x: x.get_boolean("col2") | 1)  # type: ignore

    def test_boolean_or_operation(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2") | x.get_boolean("col1")) == \
               '(toOne($t.col2) || toOne($t.col1))'

    def test_boolean_or_operation_with_literal(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2") | True) == \
               '(toOne($t.col2) || true)'

    def test_boolean_reverse_or_operation_with_literal(self) -> None:
        assert self.__generate_pure_string(lambda x: True | x.get_boolean("col2")) == \
               '(true || toOne($t.col2))'

    def test_boolean_and_operation(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2") & x.get_boolean("col1")) == \
               '(toOne($t.col2) && toOne($t.col1))'

    def test_boolean_and_operation_with_literal(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2") & True) == \
               '(toOne($t.col2) && true)'

    def test_boolean_reverse_and_operation_with_literal(self) -> None:
        assert self.__generate_pure_string(lambda x: False & x.get_boolean("col2")) == \
               '(false && toOne($t.col2))'

    def test_boolean_not_operation(self) -> None:
        assert self.__generate_pure_string(lambda x: ~x.get_boolean("col2")) == \
               'toOne($t.col2)->not()'
        assert self.__generate_pure_string(lambda x: ~(x.get_boolean("col2") | x.get_boolean("col1"))) == \
               '(toOne($t.col2) || toOne($t.col1))->not()'

    @pytest.mark.parametrize(
        "py_op, sql_op",
        [
            ("<",  "<"),
            ("<=", "<="),
            (">",  ">"),
            (">=", ">="),
        ],
    )
    def test_boolean_comparison_operations(
            self,
            py_op: str,
            sql_op: str) -> None:

        assert self.__generate_pure_string(
            lambda x: eval(f'x.get_boolean("col2") {py_op} x.get_boolean("col1")')
        ) == f'($t.col2 {sql_op} $t.col1)'

    def test_boolean_xor_operation(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2") ^ x.get_boolean("col1")) == \
               'toOne($t.col2)->xor(toOne($t.col1))'

        assert self.__generate_pure_string(lambda x: False ^ x.get_boolean("col1")) == \
               'false->xor(toOne($t.col1))'

        assert self.__generate_pure_string(lambda x: x.get_boolean("col2") ^ True) == \
               'toOne($t.col2)->xor(true)'

    @typing.no_type_check
    def test_boolean_equals_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x["col2"] == x["col1"]) == \
               '($t.col2 == $t.col1)'
        assert self.__generate_pure_string(lambda x: x["col2"] == True) == '($t.col2 == true)'  # noqa: E712
        assert self.__generate_pure_string(lambda x: True == x["col2"]) == '($t.col2 == true)'  # noqa: E712
        assert self.__generate_pure_string(lambda x: True == (x["col2"] & x["col1"])) == \
               '((toOne($t.col2) && toOne($t.col1)) == true)'

    def test_boolean_to_string_expr(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_boolean("col2").to_string()) == \
               'toOne($t.col2)->toString()'

    def test_case(self) -> None:
        assert self.__generate_pure_string(lambda x: x.get_boolean("col1").case(True, False)) == \
               'if(toOne($t.col1), |true, |false)'

        assert self.__generate_pure_string(
            lambda x: x.get_boolean("col1").case(x.get_boolean("col2"), x.get_boolean("col1"))
        ) == 'if(toOne($t.col1), |$t.col2, |$t.col1)'

        assert self.__generate_pure_string(lambda x: x.get_boolean("col1").case(1, 0)) == \
               'if(toOne($t.col1), |1, |0)'

        assert self.__generate_pure_string(lambda x: x.get_boolean("col1").case(1.5, 2.5)) == \
               'if(toOne($t.col1), |1.5, |2.5)'

        assert self.__generate_pure_string(
            lambda x: x.get_boolean("col1").case(Decimal("1.5"), Decimal("2.5"))
        ) == 'if(toOne($t.col1), |1.5D, |2.5D)'

        assert self.__generate_pure_string(lambda x: x.get_boolean("col1").case("yes", "no")) == \
               "if(toOne($t.col1), |'yes', |'no')"

        assert self.__generate_pure_string(
            lambda x: x.get_boolean("col1").case(date(2025, 1, 1), date(2025, 12, 31))
        ) == 'if(toOne($t.col1), |%2025-01-01, |%2025-12-31)'

        assert self.__generate_pure_string(
            lambda x: x.get_boolean("col1").case(
                datetime(2025, 1, 1, 10, 30, 0), datetime(2025, 12, 31, 23, 59, 59)
            )
        ) == 'if(toOne($t.col1), |%2025-01-01T10:30:00, |%2025-12-31T23:59:59)'

        with pytest.raises(TypeError) as t:
            self.tds_row.get_boolean("col1").case([1, 2], "no")  # type: ignore[arg-type]
        assert "case if_true parameter should be a primitive value or PyLegendPrimitive expression" in \
               t.value.args[0]

        with pytest.raises(TypeError) as t:
            self.tds_row.get_boolean("col1").case("yes", [1, 2])  # type: ignore[arg-type]
        assert "case if_false parameter should be a primitive value or PyLegendPrimitive expression" in \
               t.value.args[0]

        with pytest.raises(TypeError) as t:
            self.tds_row.get_boolean("col1").case(1, "no")
        assert "case if_true and if_false parameters must be of the same type." in t.value.args[0]

        with pytest.raises(TypeError) as t:
            self.tds_row.get_boolean("col1").case(True, 1.5)
        assert "case if_true and if_false parameters must be of the same type." in t.value.args[0]

        # date literal with datetime literal -> Date case (mixed date subtypes)

        # Number col with integer literal (mixed numeric types -> Number case)
        number_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
            PrimitiveTdsColumn.number_column("num_col"),
            PrimitiveTdsColumn.boolean_column("bool_col")
        ])
        number_row = TestTdsRow.from_tds_frame("t", number_frame)
        result = number_row.get_boolean("bool_col").case(number_row.get_number("num_col"), 0)
        assert str(result.to_pure_expression(self.frame_to_pure_config)) == 'if(toOne($t.bool_col), |$t.num_col, |0)'

        # DateTime col with StrictDate col (mixed date subtypes -> Date case)
        date_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
            PrimitiveTdsColumn.datetime_column("dt_col"),
            PrimitiveTdsColumn.strictdate_column("sd_col"),
            PrimitiveTdsColumn.boolean_column("bool_col")
        ])
        date_row = TestTdsRow.from_tds_frame("t", date_frame)
        result = date_row.get_boolean("bool_col").case(
            date_row.get_datetime("dt_col"), date_row.get_strictdate("sd_col")
        )

    def __generate_pure_string(self, f: PyLegendCallable[[TestTdsRow], PyLegendPrimitive]) -> str:
        expr = str(f(self.tds_row).to_pure_expression(self.frame_to_pure_config))
        model_code = """
        function test::testFunc(): Any[*]
        {
            []->toOne()->cast(
                @meta::pure::metamodel::relation::Relation<(col1: Boolean[0..1], col2: Boolean[0..1])>
            )
            ->extend(~new_col:t|<<expression>>)
        }
        """
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr
