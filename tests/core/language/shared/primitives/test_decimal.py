# Copyright 2026 Goldman Sachs
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

import pytest
import typing
from decimal import Decimal as PythonDecimal
from pylegend._typing import PyLegendCallable
from pylegend.core.database.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.language import PyLegendPrimitive, PyLegendDecimal
from pylegend.core.request.legend_client import LegendClient
from pylegend._typing import PyLegendDict, PyLegendUnion
from tests.core.language.shared import TestTableSpecInputFrame, TestTdsRow


class TestPyLegendDecimal:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.decimal_column("col1"),
        PrimitiveTdsColumn.decimal_column("col2")
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_decimal_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col2")) == '"root".col2'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col1")) == '$t.col1'

    def test_decimal_add_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col2") + x.get_decimal("col1")) == \
               '("root".col2 + "root".col1)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2") + 1.2) == \
               '("root".col2 + 1.2)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: 1.2 + x.get_decimal("col2")) == \
               '(1.2 + "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") + x.get_decimal("col1")) == \
               '(toOne($t.col2) + toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") + 1.2) == \
               '(toOne($t.col2) + 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 + x.get_decimal("col2")) == \
               '(1.2 + toOne($t.col2))'

    def test_decimal_integer_add_expr(self) -> None:
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2") + 10) == \
               '("root".col2 + 10)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: 10 + x.get_decimal("col2")) == \
               '(10 + "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") + 10) == \
               '(toOne($t.col2) + 10)'
        assert self.__generate_pure_string(lambda x: 10 + x.get_decimal("col2")) == \
               '(10 + toOne($t.col2))'

    def test_decimal_subtract_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col2") - x.get_decimal("col1")) == \
               '("root".col2 - "root".col1)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2") - 1.2) == \
               '("root".col2 - 1.2)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: 1.2 - x.get_decimal("col2")) == \
               '(1.2 - "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") - x.get_decimal("col1")) == \
               '(toOne($t.col2) - toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") - 1.2) == \
               '(toOne($t.col2) - 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 - x.get_decimal("col2")) == \
               '(1.2 - toOne($t.col2))'

    def test_decimal_integer_subtract_expr(self) -> None:
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2") - 10) == \
               '("root".col2 - 10)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: 10 - x.get_decimal("col2")) == \
               '(10 - "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") - 10) == \
               '(toOne($t.col2) - 10)'
        assert self.__generate_pure_string(lambda x: 10 - x.get_decimal("col2")) == \
               '(10 - toOne($t.col2))'

    def test_decimal_multiply_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col2") * x.get_decimal("col1")) == \
               '("root".col2 * "root".col1)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2") * 1.2) == \
               '("root".col2 * 1.2)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: 1.2 * x.get_decimal("col2")) == \
               '(1.2 * "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") * x.get_decimal("col1")) == \
               '(toOne($t.col2) * toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") * 1.2) == \
               '(toOne($t.col2) * 1.2)'
        assert self.__generate_pure_string(lambda x: 1.2 * x.get_decimal("col2")) == \
               '(1.2 * toOne($t.col2))'

    def test_decimal_integer_multiply_expr(self) -> None:
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2") * 10) == \
               '("root".col2 * 10)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: 10 * x.get_decimal("col2")) == \
               '(10 * "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") * 10) == \
               '(toOne($t.col2) * 10)'
        assert self.__generate_pure_string(lambda x: 10 * x.get_decimal("col2")) == \
               '(10 * toOne($t.col2))'

    def test_decimal_abs_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: abs(x.get_decimal("col2"))) == \
               'ABS("root".col2)'
        assert self.__generate_sql_string(lambda x: abs(x.get_decimal("col2") + x.get_decimal("col1"))) == \
               'ABS(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: abs(x.get_decimal("col2"))) == \
               'toOne($t.col2)->abs()'
        assert self.__generate_pure_string(lambda x: abs(x.get_decimal("col2") + x.get_decimal("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->abs()'

    def test_decimal_neg_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: -x.get_decimal("col2")) == \
               '(0 - "root".col2)'
        assert self.__generate_sql_string(lambda x: -(x.get_decimal("col2") + x.get_decimal("col1"))) == \
               '(0 - ("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: -x.get_decimal("col2")) == \
               'toOne($t.col2)->minus()'
        assert self.__generate_pure_string(lambda x: -(x.get_decimal("col2") + x.get_decimal("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->minus()'

    def test_decimal_pos_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: + x.get_decimal("col2")) == \
               '"root".col2'
        assert self.__generate_sql_string(lambda x: +(x.get_decimal("col2") + x.get_decimal("col1"))) == \
               '("root".col2 + "root".col1)'
        assert self.__generate_pure_string(lambda x: + x.get_decimal("col2")) == \
               '$t.col2'
        assert self.__generate_pure_string(lambda x: +(x.get_decimal("col2") + x.get_decimal("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))'

    @typing.no_type_check
    def test_decimal_equals_expr(self) -> None:
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x["col2"] == x["col1"]) == \
               '("root".col2 = "root".col1)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x["col2"] == 1) == \
               '("root".col2 = 1)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: 1 == x["col2"]) == \
               '("root".col2 = 1)'
        assert self.__generate_sql_string_no_decimal_assert(lambda x: 1 == (x["col2"] + x["col1"])) == \
               '(("root".col2 + "root".col1) = 1)'
        assert self.__generate_pure_string(lambda x: x["col2"] == x["col1"]) == \
               '($t.col2 == $t.col1)'
        assert self.__generate_pure_string(lambda x: x["col2"] == 1) == \
               '($t.col2 == 1)'
        assert self.__generate_pure_string(lambda x: 1 == x["col2"]) == \
               '($t.col2 == 1)'
        assert self.__generate_pure_string(lambda x: 1 == (x["col2"] + x["col1"])) == \
               '((toOne($t.col2) + toOne($t.col1)) == 1)'

    def test_decimal_to_string_expr(self) -> None:
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2").to_string()) == \
               'CAST("root".col2 AS TEXT)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2").to_string()) == \
               'toOne($t.col2)->toString()'

    def test_decimal_python_decimal_add_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col2") + PythonDecimal("1.5")) == \
               '("root".col2 + CAST(\'1.5\' AS DECIMAL))'
        assert self.__generate_sql_string(lambda x: PythonDecimal("1.5") + x.get_decimal("col2")) == \
               '(CAST(\'1.5\' AS DECIMAL) + "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") + PythonDecimal("1.5")) == \
               '(toOne($t.col2) + 1.5D)'
        assert self.__generate_pure_string(lambda x: PythonDecimal("1.5") + x.get_decimal("col2")) == \
               '(1.5D + toOne($t.col2))'

    def test_decimal_python_decimal_subtract_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col2") - PythonDecimal("2.5")) == \
               '("root".col2 - CAST(\'2.5\' AS DECIMAL))'
        assert self.__generate_sql_string(lambda x: PythonDecimal("2.5") - x.get_decimal("col2")) == \
               '(CAST(\'2.5\' AS DECIMAL) - "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") - PythonDecimal("2.5")) == \
               '(toOne($t.col2) - 2.5D)'
        assert self.__generate_pure_string(lambda x: PythonDecimal("2.5") - x.get_decimal("col2")) == \
               '(2.5D - toOne($t.col2))'

    def test_decimal_python_decimal_multiply_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_decimal("col2") * PythonDecimal("3.0")) == \
               '("root".col2 * CAST(\'3.0\' AS DECIMAL))'
        assert self.__generate_sql_string(lambda x: PythonDecimal("3.0") * x.get_decimal("col2")) == \
               '(CAST(\'3.0\' AS DECIMAL) * "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") * PythonDecimal("3.0")) == \
               '(toOne($t.col2) * 3.0D)'
        assert self.__generate_pure_string(lambda x: PythonDecimal("3.0") * x.get_decimal("col2")) == \
               '(3.0D * toOne($t.col2))'

    def test_decimal_divide_expr(self) -> None:
        # Decimal / Decimal
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2") / x.get_decimal("col1")) == \
               '((1.0 * "root".col2) / "root".col1)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") / x.get_decimal("col1")) == \
               '(toOne($t.col2) / toOne($t.col1))'

        # Decimal / PythonDecimal
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2") / PythonDecimal("2.0")) == \
               '((1.0 * "root".col2) / CAST(\'2.0\' AS DECIMAL))'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") / PythonDecimal("2.0")) == \
               '(toOne($t.col2) / 2.0D)'

        # PythonDecimal / Decimal
        assert self.__generate_sql_string_no_decimal_assert(lambda x: PythonDecimal("2.0") / x.get_decimal("col2")) == \
               '((1.0 * CAST(\'2.0\' AS DECIMAL)) / "root".col2)'
        assert self.__generate_pure_string(lambda x: PythonDecimal("2.0") / x.get_decimal("col2")) == \
               '(2.0D / toOne($t.col2))'

    def test_decimal_divide_with_float_and_int(self) -> None:
        # Decimal / float
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2") / 2.0) == \
               '((1.0 * "root".col2) / 2.0)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") / 2.0) == \
               '(toOne($t.col2) / 2.0)'

        # float / Decimal
        assert self.__generate_sql_string_no_decimal_assert(lambda x: 2.0 / x.get_decimal("col2")) == \
               '((1.0 * 2.0) / "root".col2)'
        assert self.__generate_pure_string(lambda x: 2.0 / x.get_decimal("col2")) == \
               '(2.0 / toOne($t.col2))'

        # Decimal / int
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2") / 3) == \
               '((1.0 * "root".col2) / 3)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2") / 3) == \
               '(toOne($t.col2) / 3)'

        # int / Decimal
        assert self.__generate_sql_string_no_decimal_assert(lambda x: 3 / x.get_decimal("col2")) == \
               '((1.0 * 3) / "root".col2)'
        assert self.__generate_pure_string(lambda x: 3 / x.get_decimal("col2")) == \
               '(3 / toOne($t.col2))'

    def test_decimal_divide_scaled_expr(self) -> None:
        assert self.__generate_sql_string(
            lambda x: x.get_decimal("col2").divide(x.get_decimal("col1"), 2)
        ) == 'ROUND(((1.0 * "root".col2) / "root".col1), 2)'
        assert self.__generate_pure_string(
            lambda x: x.get_decimal("col2").divide(x.get_decimal("col1"), 2)
        ) == 'toOne($t.col2)->divide(toOne($t.col1), 2)'

        assert self.__generate_sql_string(
            lambda x: x.get_decimal("col2").divide(PythonDecimal("0.1"), 3)
        ) == 'ROUND(((1.0 * "root".col2) / CAST(\'0.1\' AS DECIMAL)), 3)'
        assert self.__generate_pure_string(
            lambda x: x.get_decimal("col2").divide(PythonDecimal("0.1"), 3)
        ) == 'toOne($t.col2)->divide(0.1D, 3)'

    def test_decimal_divide_scaled_type_error(self) -> None:
        with pytest.raises(TypeError) as t:
            self.__generate_sql_string(
                lambda x: x.get_decimal("col2").divide(x.get_decimal("col1"), 2.5)  # type: ignore
            )
        assert "Divide scale parameter should be an int" in t.value.args[0]

    def test_decimal_round_expr(self) -> None:
        # round() no args
        assert self.__generate_sql_string(lambda x: x.get_decimal("col2").round()) == \
               'ROUND("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2").round()) == \
               'toOne($t.col2)->round()'

        # round(0)
        assert self.__generate_sql_string(lambda x: x.get_decimal("col2").round(0)) == \
               'ROUND("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2").round(0)) == \
               'toOne($t.col2)->round()'

        # round(2)
        assert self.__generate_sql_string(lambda x: x.get_decimal("col2").round(2)) == \
               'ROUND("root".col2, 2)'
        assert self.__generate_pure_string(lambda x: x.get_decimal("col2").round(2)) == \
               'toOne($t.col2)->round(2)'

        # __round__ via built-in round()
        assert self.__generate_sql_string(lambda x: round(x.get_decimal("col2"))) == \
               'ROUND("root".col2)'
        assert self.__generate_pure_string(lambda x: round(x.get_decimal("col2"))) == \
               'toOne($t.col2)->round()'
        assert self.__generate_sql_string(lambda x: round(x.get_decimal("col2"), 2)) == \
               'ROUND("root".col2, 2)'
        assert self.__generate_pure_string(lambda x: round(x.get_decimal("col2"), 2)) == \
               'toOne($t.col2)->round(2)'

    def test_decimal_round_type_error(self) -> None:
        with pytest.raises(TypeError) as t:
            self.__generate_sql_string(lambda x: x.get_decimal("col2").round(2.5))  # type: ignore
        assert "Round parameter should be an int" in t.value.args[0]

    @typing.no_type_check
    def test_decimal_python_decimal_equals_expr(self) -> None:
        assert self.__generate_sql_string_no_decimal_assert(lambda x: x["col2"] == PythonDecimal("-1.5")) == \
               '("root".col2 = CAST(\'-1.5\' AS DECIMAL))'
        assert self.__generate_pure_string(lambda x: x["col2"] == PythonDecimal("-1.5")) == \
               '($t.col2 == minus(1.5D))'

    def test_decimal_in_list_expr(self) -> None:
        assert self.__generate_sql_string_no_decimal_assert(
            lambda x: x.get_decimal("col2").in_list([PythonDecimal("1.1"), PythonDecimal("2.2")])) == \
            '"root".col2 IN (CAST(\'1.1\' AS DECIMAL), CAST(\'2.2\' AS DECIMAL))'
        assert self.__generate_sql_string_no_decimal_assert(
            lambda x: x.get_decimal("col2").in_list([PythonDecimal("4.2")])) == \
            '"root".col2 IN (CAST(\'4.2\' AS DECIMAL))'
        assert self.__generate_sql_string_no_decimal_assert(
            lambda x: x.get_decimal("col2").in_list([PythonDecimal("1.5"), x.get_decimal("col1")])) == \
            '"root".col2 IN (CAST(\'1.5\' AS DECIMAL), "root".col1)'
        assert (self.__generate_pure_string(
            lambda x: x.get_decimal("col2").in_list([PythonDecimal("1.1"), PythonDecimal("2.2")])) ==
                '$t.col2->in([1.1D, 2.2D])')
        assert (self.__generate_pure_string(
            lambda x: x.get_decimal("col2").in_list([PythonDecimal("4.2")])) ==
                '$t.col2->in([4.2D])')

        with pytest.raises(ValueError) as v:
            self.__generate_sql_string_no_decimal_assert(lambda x: x.get_decimal("col2").in_list([]))
        assert v.value.args[0] == "in_list parameter should be a non-empty list of number values."

        with pytest.raises(ValueError) as v:
            self.__generate_sql_string_no_decimal_assert(
                lambda x: x.get_decimal("col2").in_list("not_a_list")  # type: ignore[arg-type]
            )
        assert v.value.args[0] == "in_list parameter should be a non-empty list of number values."

        with pytest.raises(TypeError) as t:
            self.__generate_sql_string_no_decimal_assert(
                lambda x: x.get_decimal("col2").in_list(["a", "b"])  # type: ignore[list-item]
            )
        assert t.value.args[0].startswith("in_list list element should be a int/float/decimal.Decimal")

    def __generate_sql_string(self, f: PyLegendCallable[[TestTdsRow], PyLegendPrimitive]) -> str:
        ret = f(self.tds_row)
        assert isinstance(ret, PyLegendDecimal)
        return self.db_extension.process_expression(
            ret.to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )

    def __generate_sql_string_no_decimal_assert(self, f: PyLegendCallable[[TestTdsRow], PyLegendPrimitive]) -> str:
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
                @meta::pure::metamodel::relation::Relation<(col1: Decimal[0..1], col2: Decimal[0..1])>
            )
            ->extend(~new_col:t|<<expression>>)
        }
        """
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr


class TestPyLegendDecimalUnit:
    frame_to_sql_config = FrameToSqlConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.decimal_column("col1"),
        PrimitiveTdsColumn.decimal_column("col2")
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    def test_decimal_validate_param_to_be_decimal(self) -> None:
        """Covers PyLegendDecimal.__validate__param_to_be_decimal (decimal.py line 172)"""
        PyLegendDecimal._PyLegendDecimal__validate__param_to_be_decimal(10, "test param")  # type: ignore
        PyLegendDecimal._PyLegendDecimal__validate__param_to_be_decimal(1.5, "test param")  # type: ignore
        PyLegendDecimal._PyLegendDecimal__validate__param_to_be_decimal(PythonDecimal("3.14"), "test param")  # type: ignore

    def test_number_convert_python_decimal_branch(self) -> None:
        num_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
            PrimitiveTdsColumn.number_column("n"),
        ])
        num_row = TestTdsRow.from_tds_frame("t", num_frame)
        base_query = num_frame.to_sql_query_object(self.frame_to_sql_config)
        result = self.db_extension.process_expression(
            (num_row.get_number("n") + PythonDecimal("1.5")).to_sql_expression(  # type: ignore
                {"t": base_query}, self.frame_to_sql_config
            ),
            config=self.sql_to_string_config
        )
        assert result == '("root".n + CAST(\'1.5\' AS DECIMAL))'

    def test_decimal_collection_count(self) -> None:
        """Verifies PyLegendDecimalCollection supports count (inherited)"""
        from pylegend.core.language.shared.primitive_collection import create_primitive_collection
        from pylegend.core.language import PyLegendInteger
        dec = self.tds_row.get_decimal("col1")
        collection = create_primitive_collection(dec)
        result = collection.count()
        assert isinstance(result, PyLegendInteger)

    def test_decimal_to_decimal_expr(self) -> None:
        """Covers PyLegendNumber.to_decimal() called on a Decimal column"""
        result = self.tds_row.get_decimal("col1").to_decimal()
        assert isinstance(result, PyLegendDecimal)
        sql = self.db_extension.process_expression(
            result.to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert sql == 'CAST("root".col1 AS DECIMAL)'

    def test_decimal_to_float_expr(self) -> None:
        """Covers PyLegendNumber.to_float() called on a Decimal column"""
        from pylegend.core.language import PyLegendFloat
        result = self.tds_row.get_decimal("col1").to_float()
        assert isinstance(result, PyLegendFloat)
        sql = self.db_extension.process_expression(
            result.to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert sql == 'CAST("root".col1 AS DOUBLE PRECISION)'

    def test_decimal_round_unit(self) -> None:
        """Covers decimal round SQL without legend server"""
        result = self.tds_row.get_decimal("col1").round()
        assert isinstance(result, PyLegendDecimal)
        sql = self.db_extension.process_expression(
            result.to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert sql == 'ROUND("root".col1)'

        result2 = self.tds_row.get_decimal("col1").round(3)
        sql2 = self.db_extension.process_expression(
            result2.to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert sql2 == 'ROUND("root".col1, 3)'

    def test_decimal_divide_scaled_unit(self) -> None:
        """Covers decimal divide(other, scale) SQL without legend server"""
        result = self.tds_row.get_decimal("col1").divide(self.tds_row.get_decimal("col2"), 2)
        assert isinstance(result, PyLegendDecimal)
        # Verify the underlying expression is non-nullable
        assert result.value().is_non_nullable() is True
        sql = self.db_extension.process_expression(
            result.to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )
        assert sql == 'ROUND(((1.0 * "root".col1) / "root".col2), 2)'
