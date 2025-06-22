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
    SingleColumn,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig


__all__: PyLegendSequence[str] = [
    "LegendApiColumnExpression",
    "LegendApiBooleanColumnExpression",
    "LegendApiStringColumnExpression",
    "LegendApiNumberColumnExpression",
    "LegendApiIntegerColumnExpression",
    "LegendApiFloatColumnExpression",
    "LegendApiDateColumnExpression",
    "LegendApiDateTimeColumnExpression",
    "LegendApiStrictDateColumnExpression",
]


class LegendApiColumnExpression(PyLegendExpression, metaclass=ABCMeta):
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

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        if self.__column.isidentifier():
            # Python identifier check is same as PURE identifier check
            return f"${self.__frame_name}.{self.__column}"
        else:
            escaped = self.__column.replace('\'', '\\\'')
            return f"${self.__frame_name}.'{escaped}'"


class LegendApiBooleanColumnExpression(LegendApiColumnExpression, PyLegendExpressionBooleanReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class LegendApiStringColumnExpression(LegendApiColumnExpression, PyLegendExpressionStringReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class LegendApiNumberColumnExpression(LegendApiColumnExpression, PyLegendExpressionNumberReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class LegendApiIntegerColumnExpression(LegendApiNumberColumnExpression, PyLegendExpressionIntegerReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class LegendApiFloatColumnExpression(LegendApiNumberColumnExpression, PyLegendExpressionFloatReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class LegendApiDateColumnExpression(LegendApiColumnExpression, PyLegendExpressionDateReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class LegendApiDateTimeColumnExpression(LegendApiDateColumnExpression, PyLegendExpressionDateTimeReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)


class LegendApiStrictDateColumnExpression(LegendApiDateColumnExpression, PyLegendExpressionStrictDateReturn):

    def __init__(self, frame_name: str, column: str) -> None:
        super().__init__(frame_name=frame_name, column=column)
