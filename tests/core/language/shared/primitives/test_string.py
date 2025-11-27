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
from pylegend.core.database.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.language.shared.functions import current_user
from pylegend._typing import PyLegendDict, PyLegendUnion
from tests.core.language.shared import TestTableSpecInputFrame, TestTdsRow


class TestPyLegendString:
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    test_frame = TestTableSpecInputFrame(['test_schema', 'test_table'], [
        PrimitiveTdsColumn.string_column("col1"),
        PrimitiveTdsColumn.string_column("col2")
    ])
    tds_row = TestTdsRow.from_tds_frame("t", test_frame)
    base_query = test_frame.to_sql_query_object(frame_to_sql_config)

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_string_col_access(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2")) == '"root".col2'
        assert self.__generate_pure_string(lambda x: x.get_string("col2")) == '$t.col2'

    def test_string_length_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").len()) == 'CHAR_LENGTH("root".col2)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2").length()) == 'CHAR_LENGTH("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").len()) == 'toOne($t.col2)->length()'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").length()) == 'toOne($t.col2)->length()'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").substring(1).length()) == \
               'toOne(toOne($t.col2)->substring(1))->length()'

    def test_string_startswith_expr(self) -> None:
        with pytest.raises(TypeError) as t:
            self.__generate_sql_string(lambda x: x.get_string("col2").startswith(x.get_string("col2")))
        assert t.value.args[0].startswith("startswith prefix parameter should be a str")
        assert self.__generate_sql_string(lambda x: x.get_string("col2").startswith("Abc")) == \
               "(\"root\".col2 LIKE 'Abc%')"
        assert self.__generate_sql_string(lambda x: x.get_string("col2").startswith("A_b%c")) == \
               "(\"root\".col2 LIKE 'A\\_b\\%c%')"
        with pytest.raises(TypeError) as t:
            self.__generate_pure_string(lambda x: x.get_string("col2").startswith(x.get_string("col2")))
        assert t.value.args[0].startswith("startswith prefix parameter should be a str")
        assert self.__generate_pure_string(lambda x: x.get_string("col2").startswith("Abc")) == \
               "$t.col2->startsWith('Abc')"
        assert self.__generate_pure_string(lambda x: x.get_string("col2").startswith("A_b%c")) == \
               "$t.col2->startsWith('A_b%c')"

    def test_string_endswith_expr(self) -> None:
        with pytest.raises(TypeError) as t:
            self.__generate_sql_string(lambda x: x.get_string("col2").endswith(x.get_string("col2")))
        assert t.value.args[0].startswith("endswith suffix parameter should be a str")
        assert self.__generate_sql_string(lambda x: x.get_string("col2").endswith("Abc")) == \
               "(\"root\".col2 LIKE '%Abc')"
        assert self.__generate_sql_string(lambda x: x.get_string("col2").endswith("A_b%c")) == \
               "(\"root\".col2 LIKE '%A\\_b\\%c')"
        with pytest.raises(TypeError) as t:
            self.__generate_pure_string(lambda x: x.get_string("col2").endswith(x.get_string("col2")))
        assert t.value.args[0].startswith("endswith suffix parameter should be a str")
        assert self.__generate_pure_string(lambda x: x.get_string("col2").endswith("Abc")) == \
               "$t.col2->endsWith('Abc')"
        assert self.__generate_pure_string(lambda x: x.get_string("col2").endswith("A_b%c")) == \
               "$t.col2->endsWith('A_b%c')"

    def test_string_contains_expr(self) -> None:
        with pytest.raises(TypeError) as t:
            self.__generate_sql_string(lambda x: x.get_string("col2").contains(x.get_string("col2")))
        assert t.value.args[0].startswith("contains/in other parameter should be a str")
        assert self.__generate_sql_string(lambda x: x.get_string("col2").contains("Abc")) == \
               "(\"root\".col2 LIKE '%Abc%')"
        assert self.__generate_sql_string(lambda x: x.get_string("col2").contains("A_b%c")) == \
               "(\"root\".col2 LIKE '%A\\_b\\%c%')"
        with pytest.raises(TypeError) as t:
            self.__generate_pure_string(lambda x: x.get_string("col2").contains(x.get_string("col2")))
        assert t.value.args[0].startswith("contains/in other parameter should be a str")
        assert self.__generate_pure_string(lambda x: x.get_string("col2").contains("Abc")) == \
               "$t.col2->contains('Abc')"
        assert self.__generate_pure_string(lambda x: x.get_string("col2").contains("A_b%c")) == \
               "$t.col2->contains('A_b%c')"

    def test_string_upper_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").upper()) == 'UPPER("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").upper()) == 'toOne($t.col2)->toUpper()'

    def test_string_lower_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").lower()) == 'LOWER("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").lower()) == 'toOne($t.col2)->toLower()'

    def test_string_lstrip_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").lstrip()) == 'LTRIM("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").lstrip()) == 'toOne($t.col2)->ltrim()'

    def test_string_rstrip_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").rstrip()) == 'RTRIM("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").rstrip()) == 'toOne($t.col2)->rtrim()'

    def test_string_strip_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").strip()) == 'BTRIM("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").strip()) == 'toOne($t.col2)->trim()'

    def test_string_index_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").index(x.get_string("col1"))) == \
               'STRPOS("root".col2, "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2").index("Abc")) == \
               'STRPOS("root".col2, \'Abc\')'
        assert self.__generate_sql_string(lambda x: x.get_string("col2").index_of("Abc")) == \
               'STRPOS("root".col2, \'Abc\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").index(x.get_string("col1"))) == \
               'toOne($t.col2)->indexOf(toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").index("Abc")) == \
               'toOne($t.col2)->indexOf(\'Abc\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").index_of("Abc")) == \
               'toOne($t.col2)->indexOf(\'Abc\')'

    def test_string_parse_int_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").parse_int()) == \
               'CAST("root".col2 AS INTEGER)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2").parse_integer()) == \
               'CAST("root".col2 AS INTEGER)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").parse_int()) == \
               'toOne($t.col2)->parseInteger()'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").parse_integer()) == \
               'toOne($t.col2)->parseInteger()'

    def test_string_parse_float_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").parse_float()) == \
               'CAST("root".col2 AS DOUBLE PRECISION)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").parse_float()) == \
               'toOne($t.col2)->parseFloat()'

    def test_string_add_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2") + x.get_string("col1")) == \
               'CONCAT("root".col2, "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2") + "Abc") == \
               'CONCAT("root".col2, \'Abc\')'
        assert self.__generate_sql_string(lambda x: "Abc" + x.get_string("col2")) == \
               'CONCAT(\'Abc\', "root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2") + x.get_string("col1")) == \
               '(toOne($t.col2) + toOne($t.col1))'
        assert self.__generate_pure_string(lambda x: x.get_string("col2") + "Abc") == \
               '(toOne($t.col2) + \'Abc\')'
        assert self.__generate_pure_string(lambda x: "Abc" + x.get_string("col2")) == \
               '(\'Abc\' + toOne($t.col2))'

    def test_string_lt_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2") < x.get_string("col1")) == \
               '("root".col2 < "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2") < "Abc") == \
               '("root".col2 < \'Abc\')'
        assert self.__generate_sql_string(lambda x: "Abc" < x.get_string("col2")) == \
               '("root".col2 > \'Abc\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2") < x.get_string("col1")) == \
               '($t.col2 < $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2") < "Abc") == \
               '($t.col2 < \'Abc\')'
        assert self.__generate_pure_string(lambda x: "Abc" < x.get_string("col2")) == \
               '($t.col2 > \'Abc\')'

    def test_string_le_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2") <= x.get_string("col1")) == \
               '("root".col2 <= "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2") <= "Abc") == \
               '("root".col2 <= \'Abc\')'
        assert self.__generate_sql_string(lambda x: "Abc" <= x.get_string("col2")) == \
               '("root".col2 >= \'Abc\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2") <= x.get_string("col1")) == \
               '($t.col2 <= $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2") <= "Abc") == \
               '($t.col2 <= \'Abc\')'
        assert self.__generate_pure_string(lambda x: "Abc" <= x.get_string("col2")) == \
               '($t.col2 >= \'Abc\')'

    def test_string_gt_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2") > x.get_string("col1")) == \
               '("root".col2 > "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2") > "Abc") == \
               '("root".col2 > \'Abc\')'
        assert self.__generate_sql_string(lambda x: "Abc" > x.get_string("col2")) == \
               '("root".col2 < \'Abc\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2") > x.get_string("col1")) == \
               '($t.col2 > $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2") > "Abc") == \
               '($t.col2 > \'Abc\')'
        assert self.__generate_pure_string(lambda x: "Abc" > x.get_string("col2")) == \
               '($t.col2 < \'Abc\')'

    def test_string_ge_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2") >= x.get_string("col1")) == \
               '("root".col2 >= "root".col1)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2") >= "Abc") == \
               '("root".col2 >= \'Abc\')'
        assert self.__generate_sql_string(lambda x: "Abc" >= x.get_string("col2")) == \
               '("root".col2 <= \'Abc\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2") >= x.get_string("col1")) == \
               '($t.col2 >= $t.col1)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2") >= "Abc") == \
               '($t.col2 >= \'Abc\')'
        assert self.__generate_pure_string(lambda x: "Abc" >= x.get_string("col2")) == \
               '($t.col2 <= \'Abc\')'

    def test_string_equals_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x["col2"] == x["col1"]) == \
               '("root".col2 = "root".col1)'
        assert self.__generate_sql_string(lambda x: x["col2"] == 'Hello') == \
               '("root".col2 = \'Hello\')'
        assert self.__generate_sql_string(lambda x: 'Hello' == x["col2"]) == \
               '("root".col2 = \'Hello\')'
        assert self.__generate_sql_string(lambda x: 'Hello' == (x["col2"] + x["col1"])) == \
               '(CONCAT("root".col2, "root".col1) = \'Hello\')'
        assert self.__generate_pure_string(lambda x: x["col2"] == x["col1"]) == \
               '($t.col2 == $t.col1)'
        assert self.__generate_pure_string(lambda x: x["col2"] == 'Hello') == \
               '($t.col2 == \'Hello\')'
        assert self.__generate_pure_string(lambda x: 'Hello' == x["col2"]) == \
               '($t.col2 == \'Hello\')'
        assert self.__generate_pure_string(lambda x: 'Hello' == (x["col2"] + x["col1"])) == \
               '((toOne($t.col2) + toOne($t.col1)) == \'Hello\')'

    def test_string_current_user_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: current_user()) == 'CURRENT_USER'
        assert self.__generate_pure_string(lambda x: current_user()) == 'currentUserId()'
        assert self.__generate_pure_string(lambda x: current_user().lower()) == 'currentUserId()->toLower()'

    def test_string_null_not_null_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x["col2"].is_not_null()) == \
               '("root".col2 IS NOT NULL)'
        assert self.__generate_sql_string(lambda x: x["col2"].is_null()) == \
               '("root".col2 IS NULL)'
        assert self.__generate_pure_string(lambda x: x["col2"].is_not_null()) == \
               '$t.col2->isNotEmpty()'
        assert self.__generate_pure_string(lambda x: x["col2"].lower().is_null()) == \
               'toOne($t.col2)->toLower()->isEmpty()'

    def test_string_to_string_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").to_string()) == \
               'CAST("root".col2 AS TEXT)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").to_string()) == \
               'toOne($t.col2)->toString()'

    def test_string_parse_boolean_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").parse_boolean()) == \
               'CAST("root".col2 AS BOOLEAN)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").parse_boolean()) == \
               'toOne($t.col2)->parseBoolean()'

    def test_string_parse_datetime_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").parse_datetime()) == \
               'CAST("root".col2 AS TIMESTAMP)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").parse_datetime()) == \
               'toOne($t.col2)->parseDate()'

    def test_string_ascii_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").ascii()) == \
               'ASCII("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").ascii()) == \
               'toOne($t.col2)->ascii()'

    def test_string_b64decode_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").b64decode()) == \
               'CONVERT_FROM(DECODE("root".col2, \'BASE64\'), \'UTF8\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").b64decode()) == \
               'toOne($t.col2)->decodeBase64()'

    def test_string_b64encode_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").b64encode()) == \
               'ENCODE(CONVERT_TO("root".col2, \'UTF8\'), \'BASE64\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").b64encode()) == \
               'toOne($t.col2)->encodeBase64()'

    def test_string_reverse_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").reverse()) == \
               'REVERSE("root".col2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").reverse()) == \
               'toOne($t.col2)->reverseString()'

    def test_string_to_lower_first_character_expr(self) -> None:
        assert (self.__generate_sql_string(lambda x: x.get_string("col2").to_lower_first_character()) ==
                'CONCAT(LOWER(LEFT("root".col2, 1)), SUBSTR("root".col2, 2))')
        assert self.__generate_pure_string(lambda x: x.get_string("col2").to_lower_first_character()) == \
               'toOne($t.col2)->toLowerFirstCharacter()'

    def test_string_to_upper_first_character_expr(self) -> None:
        assert (self.__generate_sql_string(lambda x: x.get_string("col2").to_upper_first_character()) ==
               'CONCAT(UPPER(LEFT("root".col2, 1)), SUBSTR("root".col2, 2))')
        assert self.__generate_pure_string(lambda x: x.get_string("col2").to_upper_first_character()) == \
               'toOne($t.col2)->toUpperFirstCharacter()'

    def test_string_left_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").left(2)) == \
               'LEFT("root".col2, 2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").left(2)) == \
               'toOne($t.col2)->left(2)'

    def test_string_right_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").right(2)) == \
               'RIGHT("root".col2, 2)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").right(2)) == \
               'toOne($t.col2)->right(2)'

    def test_string_substr_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").substring(1)) == \
               'SUBSTR("root".col2, (1) + 1)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").substring(1)) == \
               'toOne($t.col2)->substring(1)'
        assert self.__generate_sql_string(lambda x: x.get_string("col2").substring(1, 3)) == \
               'SUBSTR("root".col2, (1) + 1, (3) - (1) + 1)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").substring(1, 3)) == \
               'toOne($t.col2)->substring(1, 3)'

    def test_string_replace_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").replace("ab", "ba")) == \
               'REPLACE("root".col2, \'ab\', \'ba\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").replace("ab", "ba")) == \
               'toOne($t.col2)->replace(\'ab\', \'ba\')'
        with pytest.raises(TypeError):
            self.__generate_pure_string(lambda x: x.get_string("col2").replace("s", 12))

    def test_string_rjust_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").rjust(3, "_")) == \
               'LPAD("root".col2, 3, \'_\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").rjust(3, "_")) == \
               'toOne($t.col2)->lpad(3, \'_\')'

    def test_string_ljust_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").ljust(3, "_")) == \
               'RPAD("root".col2, 3, \'_\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").ljust(3, "_")) == \
               'toOne($t.col2)->rpad(3, \'_\')'
        with pytest.raises(TypeError):
            self.__generate_pure_string(lambda x: x.get_string("col2").ljust("s", "_"))

    def test_string_split_part_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").split_part("_", 3)) == \
               'SPLIT_PART("root".col2, \'_\', 3)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").split_part("_", 3)) == \
               'toOne($t.col2)->splitPart(\'_\', 3)'

    def test_string_full_match_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").full_match("ab")) == \
               '("root".col2 ~~ \'ab\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").full_match("ab")) == \
               'toOne($t.col2)->matches(\'ab\')'

    @pytest.mark.skip(reason="regexpLike not supported by server")
    def test_string_match_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").match("ab")) == \
               '("root".col2 ~ \'ab\')'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").match("ab")) == \
               'toOne($t.col2)->regexpLike(\'ab\')'

    def test_string_repeat_expr(self) -> None:
        assert self.__generate_sql_string(lambda x: x.get_string("col2").repeat_string(3)) == \
               'REPEAT("root".col2, 3)'
        assert self.__generate_pure_string(lambda x: x.get_string("col2").repeat_string(3)) == \
               'toOne($t.col2)->repeatString(3)'

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
                @meta::pure::metamodel::relation::Relation<(col1: String[0..1], col2: String[0..1])>
            )
            ->extend(~new_col:t|<<expression>>)
        }
        """
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", expr))
        return expr
