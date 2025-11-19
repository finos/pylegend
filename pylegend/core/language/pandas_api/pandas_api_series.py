# Copyright 2025 Goldman Sachs
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
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.language.shared.column_expressions import PyLegendColumnExpression
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


__all__: PyLegendSequence[str] = [
    "Series"
]


class Series(PyLegendColumnExpression, PyLegendPrimitive):
    def __init__(self, base_frame, column: str):
        row = PandasApiTdsRow.from_tds_frame("c", base_frame)
        PyLegendColumnExpression.__init__(self, row=row, column=column)
        self.__base_frame = base_frame

    def value(self) -> PyLegendColumnExpression:
        return self

    def to_sql_expression(
            self,
            frame_name_to_base_query_map: PyLegendDict[str, QuerySpecification],
            config: FrameToSqlConfig
    ) -> Expression:
        return super().to_sql_expression(frame_name_to_base_query_map, config)

    def to_pure_expression(self, config: FrameToPureConfig) -> str:
        return super().to_pure_expression(config)

class BooleanSeries(Series, PyLegendExpressionBooleanReturn):
    def __init__(self, base_frame, column: str):
        super().__init__(base_frame, column)

class StringSeries(Series, PyLegendExpressionStringReturn):
    def __init__(self, base_frame, column: str):
        super().__init__(base_frame, column)

class NumberSeries(Series, PyLegendExpressionNumberReturn):
    def __init__(self, base_frame, column: str):
        super().__init__(base_frame, column)

class IntegerSeries(NumberSeries, PyLegendExpressionIntegerReturn):
    def __init__(self, base_frame, column: str):
        super().__init__(base_frame, column)

class FloatSeries(NumberSeries, PyLegendExpressionFloatReturn):
    def __init__(self, base_frame, column: str):
        super().__init__(base_frame, column)

class DateSeries(Series, PyLegendExpressionDateReturn):
    def __init__(self, base_frame, column: str):
        super().__init__(base_frame, column)

class DateTimeSeries(DateSeries, PyLegendExpressionDateTimeReturn):
    def __init__(self, base_frame, column: str):
        super().__init__(base_frame, column)

class StrictDateSeries(DateSeries, PyLegendExpressionStrictDateReturn):
    def __init__(self, base_frame, column: str):
        super().__init__(base_frame, column)
