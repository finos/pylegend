# Copyright 2025 Goldman Sachs
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
from datetime import date, datetime
from decimal import Decimal
from pylegend._typing import PyLegendCallable, PyLegendDict, PyLegendUnion
from pylegend.core.database.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.language import PyLegendPrimitive
from pylegend.core.request.legend_client import LegendClient
from tests.core.language.shared import TestTableSpecInputFrame, TestTdsRow
import pylegend


class TestCasesFunction:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))

    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.boolean_column("b1"),
        PrimitiveTdsColumn.boolean_column("b2"),
        PrimitiveTdsColumn.boolean_column("b3"),
        PrimitiveTdsColumn.string_column("s1"),
        PrimitiveTdsColumn.integer_column("i1"),
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    # ------------------------------------------------------------------
    # String branches
    # ------------------------------------------------------------------

    def test_cases_string_pure(self) -> None:
        expr = pylegend.cases(
            (self.tds_row.get_boolean("b1"), "Rule1"),
            (self.tds_row.get_boolean("b2"), "Rule2"),
            (self.tds_row.get_boolean("b3"), "Rule3"),
            default="Default",
        )
        assert self.__pure_str(expr) == "if(toOne($t.b1), |'Rule1', |if(toOne($t.b2), |'Rule2', |if(toOne($t.b3), |'Rule3', |'Default')))"  # noqa: E501

    def test_cases_string_sql(self) -> None:
        expr = pylegend.cases(
            (self.tds_row.get_boolean("b1"), "Rule1"),
            (self.tds_row.get_boolean("b2"), "Rule2"),
            default="Default",
        )
        assert self.__sql_str(expr) == (
            "CASE\n"
            "    WHEN\n        \"root\".b1\n    THEN\n        'Rule1'\n"
            "    WHEN\n        \"root\".b2\n    THEN\n        'Rule2'\n"
            "    ELSE\n        'Default'\n"
            "END"
        )

    # ------------------------------------------------------------------
    # Integer branches
    # ------------------------------------------------------------------

    def test_cases_integer_pure(self) -> None:
        expr = pylegend.cases(
            (self.tds_row.get_boolean("b1"), 1),
            (self.tds_row.get_boolean("b2"), 2),
            default=0,
        )
        assert self.__pure_str(expr) == "if(toOne($t.b1), |1, |if(toOne($t.b2), |2, |0))"

    # ------------------------------------------------------------------
    # Single pair (degenerate case)
    # ------------------------------------------------------------------

    def test_cases_single_pair(self) -> None:
        expr = pylegend.cases(
            (self.tds_row.get_boolean("b1"), "yes"),
            default="no",
        )
        assert self.__pure_str(expr) == "if(toOne($t.b1), |'yes', |'no')"

    # ------------------------------------------------------------------
    # Date branch
    # ------------------------------------------------------------------

    def test_cases_date_pure(self) -> None:
        expr = pylegend.cases(
            (self.tds_row.get_boolean("b1"), date(2025, 1, 1)),
            (self.tds_row.get_boolean("b2"), date(2025, 6, 1)),
            default=date(2025, 12, 31),
        )
        assert self.__pure_str(expr) == (
            "if(toOne($t.b1), |%2025-01-01, |if(toOne($t.b2), |%2025-06-01, |%2025-12-31))"
        )

    # ------------------------------------------------------------------
    # Numeric widening: Number col + int literal -> Number
    # ------------------------------------------------------------------

    def test_cases_numeric_widening_to_number(self) -> None:
        number_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
            PrimitiveTdsColumn.number_column("num_col"),
            PrimitiveTdsColumn.boolean_column("bool_col"),
        ])
        number_row = TestTdsRow.from_tds_frame("t", number_frame)
        expr = pylegend.cases(
            (number_row.get_boolean("bool_col"), number_row.get_number("num_col")),
            default=0,
        )
        assert str(expr.to_pure_expression(self.frame_to_pure_config)) == "if(toOne($t.bool_col), |$t.num_col, |0)"

    # ------------------------------------------------------------------
    # Error paths
    # ------------------------------------------------------------------

    def test_cases_no_pairs_raises(self) -> None:
        with pytest.raises(TypeError, match="at least one"):
            pylegend.cases(default="x")  # type: ignore[call-arg]

    def test_cases_non_tuple_pair_raises(self) -> None:
        with pytest.raises(TypeError, match="2-tuple"):
            pylegend.cases("not_a_tuple", default="x")  # type: ignore[arg-type]

    def test_cases_non_boolean_condition_raises(self) -> None:
        with pytest.raises(TypeError, match="boolean expression"):
            pylegend.cases(
                (self.tds_row.get_string("s1"), "Rule1"),  # type: ignore[arg-type]
                default="Default",
            )

    def test_cases_mismatched_types_raises(self) -> None:
        with pytest.raises(TypeError, match="same type family"):
            pylegend.cases(
                (self.tds_row.get_boolean("b1"), "string_val"),
                (self.tds_row.get_boolean("b2"), 999),
                default="Default",
            )

    def test_cases_invalid_value_raises(self) -> None:
        with pytest.raises(TypeError, match="primitive value"):
            pylegend.cases(
                (self.tds_row.get_boolean("b1"), [1, 2]),  # type: ignore[arg-type]
                default="Default",
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def __sql_str(self, expr: PyLegendPrimitive) -> str:
        return self.db_extension.process_expression(
            expr.to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config,
        )

    def __pure_str(self, expr: PyLegendPrimitive) -> str:
        result = str(expr.to_pure_expression(self.frame_to_pure_config))
        model_code = """
        function test::testFunc(): Any[*]
        {
            []->toOne()->cast(
                @meta::pure::metamodel::relation::Relation<(b1: Boolean[0..1], b2: Boolean[0..1], b3: Boolean[0..1], s1: String[0..1], i1: Integer[0..1])>
            )
            ->extend(~new_col:t|<<expression>>)
        }
        """
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", result))
        return result
