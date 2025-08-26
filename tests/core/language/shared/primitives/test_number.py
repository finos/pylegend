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
import math
from pylegend.core.database.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.request.legend_client import LegendClient
from pylegend._typing import PyLegendDict, PyLegendUnion
from tests.core.language.shared import TestTableSpecInputFrame, TestTdsRow


class TestPyLegendNumber:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.number_column("col1"),
        PrimitiveTdsColumn.number_column("col2")
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_number_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2")) == '"root".col2'
        assert self.__generate_pure_string(lambda x: x.get_number("col2")) == '$t.col2'

    def test_number_add_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2") + x.get_number("col1")) == \
               '("root".col2 + "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2") + 10) == \
               '("root".col2 + 10)'
        assert self.__generate_sql_string(lambda x: 1.2 + x.get_number("col2")) == \
               '(1.2 + "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") + x.get_number("col1")) == \
               '(toOne($t.col2) + toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") + 10) == \
               '(toOne($t.col2) + 10)'
        assert self.__generate_sql_string(lambda x: 1.2 + x.get_number("col2")) == \
               '(1.2 + "root".col2)'

    def test_number_multiply_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2") * x.get_number("col1")) == \
               '("root".col2 * "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2") * 10) == \
               '("root".col2 * 10)'
        assert self.__generate_sql_string(lambda x: 1.2 * x.get_number("col2")) == \
               '(1.2 * "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") * x.get_number("col1")) == \
               '(toOne($t.col2) * toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") * 10) == \
               '(toOne($t.col2) * 10)'
        assert self.__generate_pure_string(lambda x: 1.2 * x.get_number("col2")) == \
               '(1.2 * toOne($t.col2))'

    def test_number_divide_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2") / x.get_number("col1")) == \
               '((1.0 * "root".col2) / "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2") / 10) == \
               '((1.0 * "root".col2) / 10)'
        assert self.__generate_sql_string(lambda x: 1.2 / x.get_number("col2")) == \
               '((1.0 * 1.2) / "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") / x.get_number("col1")) == \
               '$t.col2->map(op | $op / toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") / 10) == \
               '$t.col2->map(op | $op / 10)'
        assert self.__generate_pure_string(lambda x: 1.2 / x.get_number("col2")) == \
               '1.2->map(op | $op / toOne($t.col2))'

    def test_number_subtract_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2") - x.get_number("col1")) == \
               '("root".col2 - "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2") - 10) == \
               '("root".col2 - 10)'
        assert self.__generate_sql_string(lambda x: 1.2 - x.get_number("col2")) == \
               '(1.2 - "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") - x.get_number("col1")) == \
               '(toOne($t.col2) - toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") - 10) == \
               '(toOne($t.col2) - 10)'
        assert self.__generate_pure_string(lambda x: 1.2 - x.get_number("col2")) == \
               '(1.2 - toOne($t.col2))'

    def test_number_lt_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2") < x.get_number("col1")) == \
               '("root".col2 < "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2") < 10) == \
               '("root".col2 < 10)'
        assert self.__generate_sql_string(lambda x: 1.2 < x.get_number("col2")) == \
               '("root".col2 > 1.2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") < x.get_number("col1")) == \
               '($t.col2 < $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") < 10) == \
               '($t.col2 < 10)'
        assert self.__generate_pure_string(lambda x: 1.2 < x.get_number("col2")) == \
               '($t.col2 > 1.2)'

    def test_number_le_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2") <= x.get_number("col1")) == \
               '("root".col2 <= "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2") <= 10) == \
               '("root".col2 <= 10)'
        assert self.__generate_sql_string(lambda x: 1.2 <= x.get_number("col2")) == \
               '("root".col2 >= 1.2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") <= x.get_number("col1")) == \
               '($t.col2 <= $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") <= 10) == \
               '($t.col2 <= 10)'
        assert self.__generate_pure_string(lambda x: 1.2 <= x.get_number("col2")) == \
               '($t.col2 >= 1.2)'

    def test_number_gt_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2") > x.get_number("col1")) == \
               '("root".col2 > "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2") > 10) == \
               '("root".col2 > 10)'
        assert self.__generate_sql_string(lambda x: 1.2 > x.get_number("col2")) == \
               '("root".col2 < 1.2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") > x.get_number("col1")) == \
               '($t.col2 > $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") > 10) == \
               '($t.col2 > 10)'
        assert self.__generate_pure_string(lambda x: 1.2 > x.get_number("col2")) == \
               '($t.col2 < 1.2)'

    def test_number_ge_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2") >= x.get_number("col1")) == \
               '("root".col2 >= "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2") >= 10) == \
               '("root".col2 >= 10)'
        assert self.__generate_sql_string(lambda x: 1.2 >= x.get_number("col2")) == \
               '("root".col2 <= 1.2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") >= x.get_number("col1")) == \
               '($t.col2 >= $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") >= 10) == \
               '($t.col2 >= 10)'
        assert self.__generate_pure_string(lambda x: 1.2 >= x.get_number("col2")) == \
               '($t.col2 <= 1.2)'

    def test_number_pos_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: + x.get_number("col2")) == \
               '"root".col2'
        assert self.__generate_sql_string(lambda x: +(x.get_number("col2") + x.get_number("col1"))) == \
               '("root".col2 + "root".col1)'
        assert self.__generate_pure_string(lambda x: + x.get_number("col2")) == \
               '$t.col2'
        assert self.__generate_pure_string(lambda x: +(x.get_number("col2") + x.get_number("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))'

    def test_number_neg_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: -x.get_number("col2")) == \
               '(0 - "root".col2)'
        assert self.__generate_sql_string(lambda x: -(x.get_number("col2") + x.get_number("col1"))) == \
               '(0 - ("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: -x.get_number("col2")) == \
               '$t.col2->map(op | $op->minus())'
        assert self.__generate_pure_string(lambda x: -(x.get_number("col2") + x.get_number("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->minus())'

    def test_number_abs_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: abs(x.get_number("col2"))) == \
               'ABS("root".col2)'
        assert self.__generate_sql_string(lambda x: abs(x.get_number("col2") + x.get_number("col1"))) == \
               'ABS(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: abs(x.get_number("col2"))) == \
               '$t.col2->map(op | $op->abs())'
        assert self.__generate_pure_string(lambda x: abs(x.get_number("col2") + x.get_number("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->abs())'

    def test_number_power_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2") ** x.get_number("col1")) == \
               'POWER("root".col2, "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2") ** 10) == \
               'POWER("root".col2, 10)'
        assert self.__generate_sql_string(lambda x: 1.2 ** x.get_number("col2")) == \
               'POWER(1.2, "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") ** x.get_number("col1")) == \
               '$t.col2->map(op | $op->pow(toOne($t.col1)))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") ** 10) == \
               '$t.col2->map(op | $op->pow(10))'
        assert self.__generate_pure_string(lambda x: 1.2 ** x.get_number("col2")) == \
               '1.2->map(op | $op->pow(toOne($t.col2)))'

    def test_number_ceil_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").ceil()) == \
               'CEIL("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).ceil()) == \
               'CEIL(("root".col2 + "root".col1))'
        assert self.__generate_sql_string(lambda x: math.ceil(x.get_number("col2"))) == \
               'CEIL("root".col2)'
        assert self.__generate_sql_string(lambda x: math.ceil(x.get_number("col2") + x.get_number("col1"))) == \
               'CEIL(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").ceil()) == \
               '$t.col2->map(op | $op->ceiling())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).ceil()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->ceiling())'
        assert self.__generate_pure_string(lambda x: math.ceil(x.get_number("col2"))) == \
               '$t.col2->map(op | $op->ceiling())'
        assert self.__generate_pure_string(lambda x: math.ceil(x.get_number("col2") + x.get_number("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->ceiling())'

    def test_number_floor_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").floor()) == \
               'FLOOR("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).floor()) == \
               'FLOOR(("root".col2 + "root".col1))'
        assert self.__generate_sql_string(lambda x: math.floor(x.get_number("col2"))) == \
               'FLOOR("root".col2)'
        assert self.__generate_sql_string(lambda x: math.floor(x.get_number("col2") + x.get_number("col1"))) == \
               'FLOOR(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").floor()) == \
               '$t.col2->map(op | $op->floor())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).floor()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->floor())'
        assert self.__generate_pure_string(lambda x: math.floor(x.get_number("col2"))) == \
               '$t.col2->map(op | $op->floor())'
        assert self.__generate_pure_string(lambda x: math.floor(x.get_number("col2") + x.get_number("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->floor())'

    def test_number_sqrt_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").sqrt()) == \
               'SQRT("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).sqrt()) == \
               'SQRT(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").sqrt()) == \
               '$t.col2->map(op | $op->sqrt())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).sqrt()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->sqrt())'

    def test_number_cbrt_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").cbrt()) == \
               'CBRT("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cbrt()) == \
               'CBRT(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").cbrt()) == \
               '$t.col2->map(op | $op->cbrt())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cbrt()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->cbrt())'

    def test_number_exp_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").exp()) == \
               'EXP("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).exp()) == \
               'EXP(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").exp()) == \
               '$t.col2->map(op | $op->exp())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).exp()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->exp())'

    def test_number_log_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").log()) == \
               'LN("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).log()) == \
               'LN(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").log()) == \
               '$t.col2->map(op | $op->log())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).log()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->log())'

    def test_number_remainder_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").rem(x.get_number("col1"))) == \
               'MOD("root".col2, "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2").rem(10)) == \
               'MOD("root".col2, 10)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").rem(x.get_number("col1"))) == \
               '$t.col2->map(op | $op->rem(toOne($t.col1)))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").rem(10)) == \
               '$t.col2->map(op | $op->rem(10))'

    def test_number_round_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").round()) == \
               'ROUND("root".col2)'
        assert self.__generate_sql_string(lambda x: round(x.get_number("col2"))) == \
               'ROUND("root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2").round(0)) == \
               'ROUND("root".col2)'
        assert self.__generate_sql_string(lambda x: round(x.get_number("col2"), 0)) == \
               'ROUND("root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2").round(2)) == \
               'ROUND("root".col2, 2)'
        assert self.__generate_sql_string(lambda x: round(x.get_number("col2"), 2)) == \
               'ROUND("root".col2, 2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").round()) == \
               '$t.col2->map(op | $op->round())'
        assert self.__generate_pure_string(lambda x: round(x.get_number("col2"))) == \
               '$t.col2->map(op | $op->round())'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").round(0)) == \
               '$t.col2->map(op | $op->round())'
        assert self.__generate_pure_string(lambda x: round(x.get_number("col2"), 0)) == \
               '$t.col2->map(op | $op->round())'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").round(2)) == \
               'cast($t.col2, @Float)->map(op | $op->round(2))'
        assert self.__generate_pure_string(lambda x: round(x.get_number("col2"), 2)) == \
               'cast($t.col2, @Float)->map(op | $op->round(2))'

        with pytest.raises(TypeError) as t:
            self.__generate_sql_string(lambda x: round(x.get_number("col2"), 2.1))  # type: ignore
        assert t.value.args[0] == "Round parameter should be an int. Passed - <class 'float'>"

        with pytest.raises(TypeError) as t:
            self.__generate_pure_string(lambda x: round(x.get_number("col2"), 2.1))  # type: ignore
        assert t.value.args[0] == "Round parameter should be an int. Passed - <class 'float'>"

    def test_number_sine_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").sin()) == \
               'SIN("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).sin()) == \
               'SIN(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").sin()) == \
               '$t.col2->map(op | $op->sin())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).sin()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->sin())'

    def test_number_arc_sine_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").asin()) == \
               'ASIN("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).asin()) == \
               'ASIN(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").asin()) == \
               '$t.col2->map(op | $op->asin())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).asin()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->asin())'

    def test_number_cosine_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").cos()) == \
               'COS("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cos()) == \
               'COS(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").cos()) == \
               '$t.col2->map(op | $op->cos())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cos()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->cos())'

    def test_number_arc_cosine_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").acos()) == \
               'ACOS("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).acos()) == \
               'ACOS(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").acos()) == \
               '$t.col2->map(op | $op->acos())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).acos()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->acos())'

    def test_number_tan_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").tan()) == \
               'TAN("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).tan()) == \
               'TAN(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").tan()) == \
               '$t.col2->map(op | $op->tan())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).tan()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->tan())'

    def test_number_arc_tan_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").atan()) == \
               'ATAN("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).atan()) == \
               'ATAN(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").atan()) == \
               '$t.col2->map(op | $op->atan())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).atan()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->atan())'

    def test_number_arc_tan2_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").atan2(0.5)) == \
               'ATAN2("root".col2, 0.5)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2")).atan2(x.get_number("col1"))) == \
               'ATAN2("root".col2, "root".col1)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").atan2(0.5)) == \
               '$t.col2->map(op | $op->atan2(0.5))'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2")).atan2(x.get_number("col1"))) == \
               '$t.col2->map(op | $op->atan2(toOne($t.col1)))'

    def test_number_cot_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").cot()) == \
               'COT("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cot()) == \
               'COT(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").cot()) == \
               '$t.col2->map(op | $op->cot())'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cot()) == \
               '(toOne($t.col2) + toOne($t.col1))->map(op | $op->cot())'

    def test_number_equals_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x["col2"] == x["col1"]) == \
               '("root".col2 = "root".col1)'
        assert self.__generate_sql_string(lambda x: x["col2"] == 1) == \
               '("root".col2 = 1)'
        assert self.__generate_sql_string(lambda x: 1 == x["col2"]) == \
               '("root".col2 = 1)'
        assert self.__generate_sql_string(lambda x: 1 == (x["col2"] + x["col1"])) == \
               '(("root".col2 + "root".col1) = 1)'
        assert self.__generate_pure_string(lambda x: x["col2"] == x["col1"]) == \
               '($t.col2 == $t.col1)'
        assert self.__generate_pure_string(lambda x: x["col2"] == 1) == \
               '($t.col2 == 1)'
        assert self.__generate_pure_string(lambda x: 1 == x["col2"]) == \
               '($t.col2 == 1)'
        assert self.__generate_pure_string(lambda x: 1 == (x["col2"] + x["col1"])) == \
               '((toOne($t.col2) + toOne($t.col1)) == 1)'

    def test_number_not_equals_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x["col2"] != x["col1"]) == \
               '("root".col2 <> "root".col1)'
        assert self.__generate_sql_string(lambda x: x["col2"] != 1) == \
               '("root".col2 <> 1)'
        assert self.__generate_sql_string(lambda x: 1 != x["col2"]) == \
               '("root".col2 <> 1)'
        assert self.__generate_sql_string(lambda x: 1 != (x["col2"] + x["col1"])) == \
               '(("root".col2 + "root".col1) <> 1)'
        assert self.__generate_pure_string(lambda x: x["col2"] != x["col1"]) == \
               '($t.col2 != $t.col1)'
        assert self.__generate_pure_string(lambda x: x["col2"] != 1) == \
               '($t.col2 != 1)'
        assert self.__generate_pure_string(lambda x: 1 != x["col2"]) == \
               '($t.col2 != 1)'
        assert self.__generate_pure_string(lambda x: 1 != (x["col2"] + x["col1"])) == \
               '((toOne($t.col2) + toOne($t.col1)) != 1)'

    def test_number_empty_not_empty_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x["col2"].is_not_empty()) == \
               '("root".col2 IS NOT NULL)'
        assert self.__generate_sql_string(lambda x: x["col2"].is_empty()) == \
               '("root".col2 IS NULL)'
        assert self.__generate_pure_string(lambda x: x["col2"].is_not_empty()) == \
               '$t.col2->isNotEmpty()'
        assert self.__generate_pure_string(lambda x: abs(x["col2"]).is_empty()) == \
               '$t.col2->map(op | $op->abs())->isEmpty()'

    def __generate_sql_string(self, f) -> str:  # type: ignore
        return self.db_extension.process_expression(
            f(self.tds_row).to_sql_expression({"t": self.base_query}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        )

    def __generate_pure_string(self, f) -> str:  # type: ignore
        expr = str(f(self.tds_row).to_pure_expression(self.frame_to_pure_config))
        model_code = """
        function test::testFunc(): Any[*]
        {
            []->toOne()->cast(
                @meta::pure::metamodel::relation::Relation<(col1: Number[0..1], col2: Number[0..1])>
            )
            ->extend(~new_col:t|<<expression>>)
        }
        """
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr
