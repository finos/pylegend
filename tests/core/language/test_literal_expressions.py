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

from datetime import date, datetime
from pylegend.core.language import (
    PyLegendBooleanLiteralExpression,
    PyLegendDateTimeLiteralExpression,
    PyLegendStrictDateLiteralExpression,
    PyLegendStringLiteralExpression,
)
from pylegend.core.databse.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig


class TestLiteralExpressions:
    db_extension = SqlToStringDbExtension()
    sql_to_string_config = SqlToStringConfig(SqlToStringFormat(pretty=True))
    frame_to_sql_config = FrameToSqlConfig()

    def test_boolean_literal_expr(self) -> None:

        true_expr = PyLegendBooleanLiteralExpression(True)
        assert self.db_extension.process_expression(
            true_expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "true"
        assert true_expr.to_pure_expression() == 'true'

        false_expr = PyLegendBooleanLiteralExpression(False)
        assert self.db_extension.process_expression(
            false_expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "false"
        assert false_expr.to_pure_expression() == 'false'

    def test_datetime_literal_expr(self) -> None:
        expr = PyLegendDateTimeLiteralExpression(datetime(2023, 6, 1, 14, 45, 00))
        assert self.db_extension.process_expression(
            expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "CAST('2023-06-01T14:45:00' AS TIMESTAMP)"
        assert expr.to_pure_expression() == "%2023-06-01T14:45:00"

    def test_strictdate_literal_expr(self) -> None:
        expr = PyLegendStrictDateLiteralExpression(date(2023, 6, 1))
        assert self.db_extension.process_expression(
            expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "CAST('2023-06-01' AS DATE)"
        assert expr.to_pure_expression() == "%2023-06-01"

    def test_string_literal_expr(self) -> None:
        expr = PyLegendStringLiteralExpression("Hello, World!")
        assert self.db_extension.process_expression(
            expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "'Hello, World!'"
        assert expr.to_pure_expression() == "'Hello, World!'"

        expr = PyLegendStringLiteralExpression("Hello,' World!")
        assert self.db_extension.process_expression(
            expr.to_sql_expression({}, self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == "'Hello,'' World!'"
        assert expr.to_pure_expression() == "'Hello,\\\' World!'"
