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
from pylegend.core.language.shared.functions import pi


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
               '(toOne($t.col2) / toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") / 10) == \
               '(toOne($t.col2) / 10)'
        assert self.__generate_pure_string(lambda x: 1.2 / x.get_number("col2")) == \
               '(1.2 / toOne($t.col2))'

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
               'toOne($t.col2)->minus()'
        assert self.__generate_pure_string(lambda x: -(x.get_number("col2") + x.get_number("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->minus()'

    def test_number_abs_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: abs(x.get_number("col2"))) == \
               'ABS("root".col2)'
        assert self.__generate_sql_string(lambda x: abs(x.get_number("col2") + x.get_number("col1"))) == \
               'ABS(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: abs(x.get_number("col2"))) == \
               'toOne($t.col2)->abs()'
        assert self.__generate_pure_string(lambda x: abs(x.get_number("col2") + x.get_number("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->abs()'

    def test_number_power_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2") ** x.get_number("col1")) == \
               'POWER("root".col2, "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2") ** 10) == \
               'POWER("root".col2, 10)'
        assert self.__generate_sql_string(lambda x: 1.2 ** x.get_number("col2")) == \
               'POWER(1.2, "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") ** x.get_number("col1")) == \
               'toOne($t.col2)->pow(toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2") ** 10) == \
               'toOne($t.col2)->pow(10)'
        assert self.__generate_pure_string(lambda x: 1.2 ** x.get_number("col2")) == \
               '1.2->pow(toOne($t.col2))'

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
               'toOne($t.col2)->ceiling()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).ceil()) == \
               '(toOne($t.col2) + toOne($t.col1))->ceiling()'
        assert self.__generate_pure_string(lambda x: math.ceil(x.get_number("col2"))) == \
               'toOne($t.col2)->ceiling()'
        assert self.__generate_pure_string(lambda x: math.ceil(x.get_number("col2") + x.get_number("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->ceiling()'

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
               'toOne($t.col2)->floor()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).floor()) == \
               '(toOne($t.col2) + toOne($t.col1))->floor()'
        assert self.__generate_pure_string(lambda x: math.floor(x.get_number("col2"))) == \
               'toOne($t.col2)->floor()'
        assert self.__generate_pure_string(lambda x: math.floor(x.get_number("col2") + x.get_number("col1"))) == \
               '(toOne($t.col2) + toOne($t.col1))->floor()'

    def test_number_sqrt_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").sqrt()) == \
               'SQRT("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).sqrt()) == \
               'SQRT(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").sqrt()) == \
               'toOne($t.col2)->sqrt()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).sqrt()) == \
               '(toOne($t.col2) + toOne($t.col1))->sqrt()'

    def test_number_cbrt_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").cbrt()) == \
               'CBRT("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cbrt()) == \
               'CBRT(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").cbrt()) == \
               'toOne($t.col2)->cbrt()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cbrt()) == \
               '(toOne($t.col2) + toOne($t.col1))->cbrt()'

    def test_number_exp_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").exp()) == \
               'EXP("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).exp()) == \
               'EXP(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").exp()) == \
               'toOne($t.col2)->exp()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).exp()) == \
               '(toOne($t.col2) + toOne($t.col1))->exp()'

    def test_number_log_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").log()) == \
               'LN("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).log()) == \
               'LN(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").log()) == \
               'toOne($t.col2)->log()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).log()) == \
               '(toOne($t.col2) + toOne($t.col1))->log()'

    def test_number_remainder_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").rem(x.get_number("col1"))) == \
               'MOD("root".col2, "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_number("col2").rem(10)) == \
               'MOD("root".col2, 10)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").rem(x.get_number("col1"))) == \
               'toOne($t.col2)->rem(toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").rem(10)) == \
               'toOne($t.col2)->rem(10)'

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
               'toOne($t.col2)->round()'
        assert self.__generate_pure_string(lambda x: round(x.get_number("col2"))) == \
               'toOne($t.col2)->round()'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").round(0)) == \
               'toOne($t.col2)->round()'
        assert self.__generate_pure_string(lambda x: round(x.get_number("col2"), 0)) == \
               'toOne($t.col2)->round()'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").round(2)) == \
               'cast(toOne($t.col2), @Float)->round(2)'
        assert self.__generate_pure_string(lambda x: round(x.get_number("col2"), 2)) == \
               'cast(toOne($t.col2), @Float)->round(2)'

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
               'toOne($t.col2)->sin()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).sin()) == \
               '(toOne($t.col2) + toOne($t.col1))->sin()'

    def test_number_arc_sine_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").asin()) == \
               'ASIN("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).asin()) == \
               'ASIN(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").asin()) == \
               'toOne($t.col2)->asin()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).asin()) == \
               '(toOne($t.col2) + toOne($t.col1))->asin()'

    def test_number_cosine_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").cos()) == \
               'COS("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cos()) == \
               'COS(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").cos()) == \
               'toOne($t.col2)->cos()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cos()) == \
               '(toOne($t.col2) + toOne($t.col1))->cos()'

    def test_number_arc_cosine_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").acos()) == \
               'ACOS("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).acos()) == \
               'ACOS(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").acos()) == \
               'toOne($t.col2)->acos()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).acos()) == \
               '(toOne($t.col2) + toOne($t.col1))->acos()'

    def test_number_tan_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").tan()) == \
               'TAN("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).tan()) == \
               'TAN(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").tan()) == \
               'toOne($t.col2)->tan()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).tan()) == \
               '(toOne($t.col2) + toOne($t.col1))->tan()'

    def test_number_arc_tan_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").atan()) == \
               'ATAN("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).atan()) == \
               'ATAN(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").atan()) == \
               'toOne($t.col2)->atan()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).atan()) == \
               '(toOne($t.col2) + toOne($t.col1))->atan()'

    def test_number_arc_tan2_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").atan2(0.5)) == \
               'ATAN2("root".col2, 0.5)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2")).atan2(x.get_number("col1"))) == \
               'ATAN2("root".col2, "root".col1)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").atan2(0.5)) == \
               'toOne($t.col2)->atan2(0.5)'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2")).atan2(x.get_number("col1"))) == \
               'toOne($t.col2)->atan2(toOne($t.col1))'

    def test_number_cot_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").cot()) == \
               'COT("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cot()) == \
               'COT(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").cot()) == \
               'toOne($t.col2)->cot()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cot()) == \
               '(toOne($t.col2) + toOne($t.col1))->cot()'

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
               'toOne($t.col2)->abs()->isEmpty()'

    def test_number_log10_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").log10()) == \
               'LOG10("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).log10()) == \
               'LOG10(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").log10()) == \
               'toOne($t.col2)->log10()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).log10()) == \
               '(toOne($t.col2) + toOne($t.col1))->log10()'

    def test_number_degrees_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").degrees()) == \
               'DEGREES("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).degrees()) == \
               'DEGREES(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").degrees()) == \
               'toOne($t.col2)->toDegrees()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).degrees()) == \
               '(toOne($t.col2) + toOne($t.col1))->toDegrees()'

    def test_number_radians_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").radians()) == \
               'RADIANS("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).radians()) == \
               'RADIANS(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").radians()) == \
               'toOne($t.col2)->toRadians()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).radians()) == \
               '(toOne($t.col2) + toOne($t.col1))->toRadians()'

    def test_number_sign_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").sign()) == \
               'SIGN("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").sign()) == \
               'toOne($t.col2)->sign()'

    def test_number_sinh_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").sinh()) == \
               'SINH("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).sinh()) == \
               'SINH(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").sinh()) == \
               'toOne($t.col2)->sinh()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).sinh()) == \
               '(toOne($t.col2) + toOne($t.col1))->sinh()'

    def test_number_cosh_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").cosh()) == \
               'COSH("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cosh()) == \
               'COSH(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").cosh()) == \
               'toOne($t.col2)->cosh()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).cosh()) == \
               '(toOne($t.col2) + toOne($t.col1))->cosh()'

    def test_number_tanh_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_number("col2").tanh()) == \
               'TANH("root".col2)'
        assert self.__generate_sql_string(lambda x: (x.get_number("col2") + x.get_number("col1")).tanh()) == \
               'TANH(("root".col2 + "root".col1))'
        assert self.__generate_pure_string(lambda x: x.get_number("col2").tanh()) == \
               'toOne($t.col2)->tanh()'
        assert self.__generate_pure_string(lambda x: (x.get_number("col2") + x.get_number("col1")).tanh()) == \
               '(toOne($t.col2) + toOne($t.col1))->tanh()'

    def test_number_pi_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: pi()) == 'PI()'
        assert self.__generate_pure_string(lambda x: pi()) == 'pi()'

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
