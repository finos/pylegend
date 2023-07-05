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


from pylegend._typing import (
    PyLegendSequence
)
from pylegend.core.language.expression import PyLegendExpressionBooleanReturn
from pylegend.core.sql.metamodel import (
    Expression,
    BooleanLiteral,
)


__all__: PyLegendSequence[str] = [
    "PyLegendLiteralExpressionBooleanReturn",
]


class PyLegendLiteralExpressionBooleanReturn(PyLegendExpressionBooleanReturn):
    __value: bool

    def __init__(self, value: bool) -> None:
        self.__value = value

    def to_sql_expression(self) -> Expression:
        return BooleanLiteral(value=self.__value)
