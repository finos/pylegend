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

from abc import ABCMeta
import pandas as pd
from pylegend._typing import (
    PyLegendSequence,
    PyLegendTypeVar,
    PyLegendOptional,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.core.tds.tds_column import TdsColumn
from pylegend.core.tds.result_handler import (
    ResultHandler,
)
from pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame import LegendQLApiTdsFrame
from pylegend.extensions.tds.result_handler import (
    PandasDfReadConfig,
)

__all__: PyLegendSequence[str] = [
    "LegendQLApiBaseTdsFrame"
]

R = PyLegendTypeVar('R')


class LegendQLApiBaseTdsFrame(LegendQLApiTdsFrame, metaclass=ABCMeta):
    __columns: PyLegendSequence[TdsColumn]

    def __init__(self, columns: PyLegendSequence[TdsColumn]) -> None:
        col_names = [c.get_name() for c in columns]
        if len(col_names) != len(set(col_names)):
            cols = "[" + ", ".join([str(c) for c in columns]) + "]"
            raise ValueError(f"TdsFrame cannot have duplicated column names. Passed columns: {cols}")
        self.__columns = [c.copy() for c in columns]

    def columns(self) -> PyLegendSequence[TdsColumn]:
        return [c.copy() for c in self.__columns]

    def to_pure_query(self, config: FrameToPureConfig = FrameToPureConfig()) -> str:
        raise RuntimeError("Not supported yet!")

    def execute_frame(self, result_handler: ResultHandler[R], chunk_size: PyLegendOptional[int] = None) -> R:
        raise RuntimeError("Not supported yet!")

    def execute_frame_to_string(self, chunk_size: PyLegendOptional[int] = None) -> str:
        raise RuntimeError("Not supported yet!")

    def execute_frame_to_pandas_df(
            self,
            chunk_size: PyLegendOptional[int] = None,
            pandas_df_read_config: PandasDfReadConfig = PandasDfReadConfig()
    ) -> pd.DataFrame:
        raise RuntimeError("Not supported yet!")

    def to_sql_query(self, config: FrameToSqlConfig = FrameToSqlConfig()) -> str:
        raise RuntimeError("Not supported yet!")
