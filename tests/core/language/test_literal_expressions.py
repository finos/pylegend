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
from datetime import date, datetime
from pylegend.core.language import (
    PyLegendBooleanLiteralExpression,
    PyLegendDateTimeLiteralExpression,
    PyLegendStrictDateLiteralExpression,
    PyLegendStringLiteralExpression,
)
from pylegend.core.database.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.language.shared.literal_expressions import PyLegendNullLiteralExpression
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.request.legend_client import LegendClient
from pylegend._typing import PyLegendDict, PyLegendUnion


class TestLiteralExpressions:
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    frame_to_sql_config = FrameToSqlConfig()
    frame_to_pure_config = FrameToPureConfig()

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.__legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_boolean_literal_expr(self) -> None:

        true_expr = PyLegendBooleanLiteralExpression(True)
        assert self.db_extension.process_expression(
            true_expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "true"
        assert self.__generate_pure_string(true_expr) == 'true'

        false_expr = PyLegendBooleanLiteralExpression(False)
        assert self.db_extension.process_expression(
            false_expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "false"
        assert self.__generate_pure_string(false_expr) == 'false'

    def test_datetime_literal_expr(self) -> None:
        expr = PyLegendDateTimeLiteralExpression(datetime(2023, 6, 1, 14, 45, 00))
        assert self.db_extension.process_expression(
            expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "CAST('2023-06-01T14:45:00' AS TIMESTAMP)"
        assert self.__generate_pure_string(expr) == "%2023-06-01T14:45:00"

    def test_strictdate_literal_expr(self) -> None:
        expr = PyLegendStrictDateLiteralExpression(date(2023, 6, 1))
        assert self.db_extension.process_expression(
            expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "CAST('2023-06-01' AS DATE)"
        assert self.__generate_pure_string(expr) == "%2023-06-01"

    def test_string_literal_expr(self) -> None:
        expr = PyLegendStringLiteralExpression("Hello, World!")
        assert self.db_extension.process_expression(
            expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "'Hello, World!'"
        assert self.__generate_pure_string(expr) == "'Hello, World!'"

        expr = PyLegendStringLiteralExpression("Hello,' World!")
        assert self.db_extension.process_expression(
            expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "'Hello,'' World!'"
        assert self.__generate_pure_string(expr) == "'Hello,\\\' World!'"

    def test_null_literal_expr(self) -> None:
        expr = PyLegendNullLiteralExpression()
        assert self.db_extension.process_expression(
            expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "null"
        assert self.__generate_pure_string(expr) == "[]"
        assert expr.get_sub_expressions() == [expr]

    def __generate_pure_string(self, expr) -> str:  # type: ignore
        e = str(expr.to_pure_expression(self.frame_to_pure_config))
        model_code = """
        function test::testFunc(): Any[*]
        {
            []->toOne()->cast(
                @meta::pure::metamodel::relation::Relation<(col1: String[0..1], col2: String[0..1])>
            )
            ->extend(~new_col:t|<<expression>>)
        }
        """
        self.__legend_client.parse_and_compile_model(model_code.replace("<<expression>>", e))
        return e
