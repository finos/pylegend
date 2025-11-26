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

from typing import TYPE_CHECKING

import pandas as pd

from pylegend._typing import (
    PyLegendDict,
)
from pylegend._typing import (
    PyLegendSequence,
    PyLegendOptional,
    PyLegendTypeVar
)
from pylegend.core.language.pandas_api.pandas_api_tds_row import PandasApiTdsRow
from pylegend.core.language.shared.column_expressions import PyLegendColumnExpression
from pylegend.core.language.shared.expression import (
    PyLegendExpressionBooleanReturn,
    PyLegendExpressionStringReturn,
    PyLegendExpressionNumberReturn,
    PyLegendExpressionIntegerReturn,
    PyLegendExpressionFloatReturn,
    PyLegendExpressionDateReturn,
    PyLegendExpressionDateTimeReturn,
    PyLegendExpressionStrictDateReturn,
)
from pylegend.core.language.shared.primitives.boolean import PyLegendBoolean
from pylegend.core.language.shared.primitives.date import PyLegendDate
from pylegend.core.language.shared.primitives.datetime import PyLegendDateTime
from pylegend.core.language.shared.primitives.float import PyLegendFloat
from pylegend.core.language.shared.primitives.integer import PyLegendInteger
from pylegend.core.language.shared.primitives.number import PyLegendNumber
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive
from pylegend.core.language.shared.primitives.strictdate import PyLegendStrictDate
from pylegend.core.language.shared.primitives.string import PyLegendString
from pylegend.core.sql.metamodel import (
    Expression,
)
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.tds.abstract.frames.base_tds_frame import BaseTdsFrame
from pylegend.core.tds.result_handler import ResultHandler
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.result_handler import PandasDfReadConfig

if TYPE_CHECKING:
    from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame

__all__: PyLegendSequence[str] = [
    "Series"
]

R = PyLegendTypeVar('R')


class Series(PyLegendColumnExpression, PyLegendPrimitive, BaseTdsFrame):
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        row = PandasApiTdsRow.from_tds_frame("c", base_frame)
        PyLegendColumnExpression.__init__(self, row=row, column=column)

        self.__base_frame = base_frame
        self._filtered_frame = base_frame.filter(items=[column])

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

    def columns(self) -> PyLegendSequence[TdsColumn]:
        return self._filtered_frame.columns()

    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        return self._filtered_frame.to_sql_query(config)

    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        return self._filtered_frame.to_pure_query(config)

    def execute_frame(
            self,
            result_handler: ResultHandler[R],
            chunk_size: PyLegendOptional[int] = None
    ) -> R:
        return self._filtered_frame.execute_frame(result_handler, chunk_size)  # pragma: no cover

    def execute_frame_to_string(
            self,
            chunk_size: PyLegendOptional[int] = None
    ) -> str:
        return self._filtered_frame.execute_frame_to_string(chunk_size)

    def execute_frame_to_pandas_df(
            self,
            chunk_size: PyLegendOptional[int] = None,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> pd.DataFrame:
        return self._filtered_frame.execute_frame_to_pandas_df(chunk_size, pandas_df_read_config)  # pragma: no cover

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        return self._filtered_frame.to_sql_query_object(config)  # type: ignore

    def to_pure(self, config: FrameToPureConfig) -> str:
        return self._filtered_frame.to_pure(config)  # type: ignore

    def get_all_tds_frames(self) -> PyLegendSequence["BaseTdsFrame"]:
        return self._filtered_frame.get_all_tds_frames()  # type: ignore


class BooleanSeries(Series, PyLegendBoolean, PyLegendExpressionBooleanReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)  # pragma: no cover (Boolean column not supported in PURE)
        PyLegendBoolean.__init__(self, self)  # pragma: no cover (Boolean column not supported in PURE)


class StringSeries(Series, PyLegendString, PyLegendExpressionStringReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendString.__init__(self, self)


class NumberSeries(Series, PyLegendNumber, PyLegendExpressionNumberReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendNumber.__init__(self, self)


class IntegerSeries(NumberSeries, PyLegendInteger, PyLegendExpressionIntegerReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendInteger.__init__(self, self)


class FloatSeries(NumberSeries, PyLegendFloat, PyLegendExpressionFloatReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendFloat.__init__(self, self)


class DateSeries(Series, PyLegendDate, PyLegendExpressionDateReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendDate.__init__(self, self)


class DateTimeSeries(DateSeries, PyLegendDateTime, PyLegendExpressionDateTimeReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendDateTime.__init__(self, self)


class StrictDateSeries(DateSeries, PyLegendStrictDate, PyLegendExpressionStrictDateReturn):  # type: ignore
    def __init__(self, base_frame: "PandasApiTdsFrame", column: str):
        super().__init__(base_frame, column)
        PyLegendStrictDate.__init__(self, self)
