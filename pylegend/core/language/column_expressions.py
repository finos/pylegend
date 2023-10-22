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
from pylegend.core.language.expression import (
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
    SingleColumn,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig


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
    __frame_name: str
    __column: str

    def __init__(self, frame_name: str, column: str) -> None:
        self.__frame_name = frame_name
        self.__column = column

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        query = frame_name_to_base_query_map[self.__frame_name]
        db_extension = config.sql_to_string_generator().get_db_extension()
        filtered = [
            s for s in query.select.selectItems
            if (isinstance(s, SingleColumn) and
                s.alias == db_extension.quote_identifier(self.__column))
        ]
        if len(filtered) == 0:
            raise RuntimeError("Cannot find column: " + self.__column)  # pragma: no cover
        return filtered[0].expression


class PyLegendBooleanColumnExpression(PyLegendColumnExpression, PyLegendExpressionBooleanReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class PyLegendStringColumnExpression(PyLegendColumnExpression, PyLegendExpressionStringReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class PyLegendNumberColumnExpression(PyLegendColumnExpression, PyLegendExpressionNumberReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class PyLegendIntegerColumnExpression(PyLegendNumberColumnExpression, PyLegendExpressionIntegerReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class PyLegendFloatColumnExpression(PyLegendNumberColumnExpression, PyLegendExpressionFloatReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class PyLegendDateColumnExpression(PyLegendColumnExpression, PyLegendExpressionDateReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class PyLegendDateTimeColumnExpression(PyLegendDateColumnExpression, PyLegendExpressionDateTimeReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class PyLegendStrictDateColumnExpression(PyLegendDateColumnExpression, PyLegendExpressionStrictDateReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)
