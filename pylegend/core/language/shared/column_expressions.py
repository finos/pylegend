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
    PyLegendSequence,
    PyLegendDict,
)
from pylegend.core.language.shared.expression import (
    PyLegendExpression,
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
)
from pylegend.core.sql.metamodel import (
    Expression,
    QuerySpecification,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.language.shared.helpers import escape_column_name
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pylegend.core.language.shared.tds_row import AbstractTdsRow


__all__: PyLegendSequence[str] = [
    "PyLegendColumnExpression",
    "PyLegendBooleanColumnExpression",
    "PyLegendStringColumnExpression",
    "PyLegendNumberColumnExpression",
    "PyLegendIntegerColumnExpression",
    "PyLegendFloatColumnExpression",
    "PyLegendDateColumnExpression",
    "PyLegendDateTimeColumnExpression",
    "PyLegendStrictDateColumnExpression",
]


class PyLegendColumnExpression(PyLegendExpression, metaclass=ABCMeta):
    __row: "AbstractTdsRow"
    __column: str

    def __init__(self, row: "AbstractTdsRow", column: str) -> None:
        self.__row = row
        self.__column = column

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return self.__row.column_sql_expression(self.__column, frame_name_to_base_query_map, config)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return f"{self.__row.to_pure_expression(config)}.{escape_column_name(self.__column)}"

    def get_column(self) -> str:
        return self.__column


class PyLegendBooleanColumnExpression(PyLegendColumnExpression, PyLegendExpressionBooleanReturn):

    def __init__(self, row: "AbstractTdsRow", column: str) -> None:
        super().__init__(row=row, column=column)


class PyLegendStringColumnExpression(PyLegendColumnExpression, PyLegendExpressionStringReturn):

    def __init__(self, row: "AbstractTdsRow", column: str) -> None:
        super().__init__(row=row, column=column)


class PyLegendNumberColumnExpression(PyLegendColumnExpression, PyLegendExpressionNumberReturn):

    def __init__(self, row: "AbstractTdsRow", column: str) -> None:
        super().__init__(row=row, column=column)


class PyLegendIntegerColumnExpression(PyLegendNumberColumnExpression, PyLegendExpressionIntegerReturn):

    def __init__(self, row: "AbstractTdsRow", column: str) -> None:
        super().__init__(row=row, column=column)


class PyLegendFloatColumnExpression(PyLegendNumberColumnExpression, PyLegendExpressionFloatReturn):

    def __init__(self, row: "AbstractTdsRow", column: str) -> None:
        super().__init__(row=row, column=column)


class PyLegendDateColumnExpression(PyLegendColumnExpression, PyLegendExpressionDateReturn):

    def __init__(self, row: "AbstractTdsRow", column: str) -> None:
        super().__init__(row=row, column=column)


class PyLegendDateTimeColumnExpression(PyLegendDateColumnExpression, PyLegendExpressionDateTimeReturn):

    def __init__(self, row: "AbstractTdsRow", column: str) -> None:
        super().__init__(row=row, column=column)


class PyLegendStrictDateColumnExpression(PyLegendDateColumnExpression, PyLegendExpressionStrictDateReturn):

    def __init__(self, row: "AbstractTdsRow", column: str) -> None:
        super().__init__(row=row, column=column)
