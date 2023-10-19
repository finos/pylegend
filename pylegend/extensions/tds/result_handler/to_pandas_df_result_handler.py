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

import ijson  # type: ignore
import pandas as pd
import numpy as np
from pylegend._typing import (
    PyLegendSequence,
    TYPE_CHECKING
)
if TYPE_CHECKING:
    from pylegend.core.tds.tds_frame import PyLegendTdsFrame  # pragma: no cover
from pylegend.core.tds.tds_column import TdsColumn, PrimitiveTdsColumn
from pylegend.core.tds.result_handler.result_handler import ResultHandler
from pylegend.core.request.response_reader import ResponseReader

__all__: PyLegendSequence[str] = [
    "ToPandasDfResultHandler",
    "PandasDfReadConfig"
]

DATE_TYPES = ["StrictDate", "DateTime", "Date"]

COLUMN_TYPE_DTYPE_MAP = {
    "Boolean": "boolean",
    "Integer": "Int64",
    "Float": "Float64",
    "Number": "Float64",
    "String": "object"
}


class PandasDfReadConfig:
    __parse_float_as_decimal: bool
    __rows_per_batch: int

    def __init__(self, parse_float_as_decimal: bool = False, rows_per_batch: int = 16_384) -> None:
        self.__parse_float_as_decimal = parse_float_as_decimal
        self.__rows_per_batch = rows_per_batch

    def rows_per_batch(self) -> int:
        return self.__rows_per_batch

    def parse_float_as_decimal(self) -> bool:
        return self.__parse_float_as_decimal


class ToPandasDfResultHandler(ResultHandler[pd.DataFrame]):
    __pandas_df_read_config: PandasDfReadConfig

    def __init__(
            self,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> None:
        self.__pandas_df_read_config = pandas_df_read_config

    def handle_result(self, frame: "PyLegendTdsFrame", result: ResponseReader) -> pd.DataFrame:
        df: pd.DataFrame = pd.concat(
            self._read_partial_dfs(
                frame,
                ijson.items(
                    result,
                    "result.rows.item.values",
                    use_float=not self.__pandas_df_read_config.parse_float_as_decimal()
                ),
            ),
            ignore_index=True
        )
        return df

    def _read_partial_dfs(self, frame: "PyLegendTdsFrame", row_iter):  # type: ignore
        all_values_list = []
        cnt = 0
        for row in row_iter:
            all_values_list.extend(row)
            cnt += 1
            if cnt == self.__pandas_df_read_config.rows_per_batch():
                yield ToPandasDfResultHandler._create_df_from_list(all_values_list, frame)  # type: ignore
                all_values_list = []
                cnt = 0

        if cnt > 0:
            yield ToPandasDfResultHandler._create_df_from_list(all_values_list, frame)  # type: ignore
            all_values_list = []
            cnt = 0

        if cnt == 0 and len(all_values_list) == 0:
            return
        else:
            raise RuntimeError("Unexpected state")  # pragma: no cover

    @staticmethod
    def _create_df_from_list(all_values_list, frame):  # type: ignore
        columns = frame.columns()
        columns_length = len(columns)
        column_series_list = [
            ToPandasDfResultHandler._create_series(all_values_list, x, i, columns_length)
            for (i, x) in enumerate(columns)
        ]
        return pd.concat(column_series_list, axis=1)

    @staticmethod
    def _create_series(all_values_list, column: TdsColumn, col_index: int, columns_length: int):  # type: ignore
        if isinstance(column, PrimitiveTdsColumn):
            if column.get_type() in DATE_TYPES:
                dtype = None
            elif column.get_type() in COLUMN_TYPE_DTYPE_MAP:
                dtype = COLUMN_TYPE_DTYPE_MAP[column.get_type()]
            else:
                raise RuntimeError(  # pragma: no cover
                    f"Cannot infer pandas column dtype for column '{column.get_name()}' with type '{column.get_type()}'"
                )
        else:
            dtype = "object"

        if dtype:
            series = pd.Series(
                data=all_values_list[col_index::columns_length],
                name=column.get_name(),
                dtype=dtype
            )
        else:
            series = pd.Series(
                data=all_values_list[col_index::columns_length],
                name=column.get_name()
            )

        if dtype == "object":
            series = series.fillna(np.nan)

        if column.get_type() in DATE_TYPES:
            series = pd.to_datetime(series, format="ISO8601")

        return series
