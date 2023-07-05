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


from abc import ABCMeta
from pylegend._typing import (
    PyLegendSequence
)
from pylegend.core.language.expression import (
    PyLegendExpression,
    PyLegendExpressionBooleanReturn,
)
from pylegend.core.sql.metamodel import (
    Expression,
)


__all__: PyLegendSequence[str] = [
    "PyLegendColumnExpression",
    "PyLegendBooleanColumnExpression"
]


class PyLegendColumnExpression(PyLegendExpression, metaclass=ABCMeta):
    __column: Expression

    def __init__(self, column: Expression) -> None:
        self.__column = column

    def to_sql_expression(self) -> Expression:
        return self.__column


class PyLegendBooleanColumnExpression(PyLegendColumnExpression, PyLegendExpressionBooleanReturn):

    def __init__(self, column: Expression) -> None:
        super().__init__(column=column)
