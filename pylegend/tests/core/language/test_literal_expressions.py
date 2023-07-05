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

from pylegend.core.language import (
    PyLegendBooleanLiteralExpression,
)
from pylegend.core.databse.sql_to_string import (
    SqlToStringFormat,
    SqlToStringConfig,
    SqlToStringDbExtension,
)


class TestLiteralExpressions:

    def test_boolean_literal_expr_sql_gen(self) -> None:
        extension = SqlToStringDbExtension()
        config = SqlToStringConfig(SqlToStringFormat(pretty=True))

        true_expr = PyLegendBooleanLiteralExpression(True)
        assert extension.process_expression(true_expr.to_sql_expression(), config=config) == "true"

        false_expr = PyLegendBooleanLiteralExpression(False)
        assert extension.process_expression(false_expr.to_sql_expression(), config=config) == "false"
