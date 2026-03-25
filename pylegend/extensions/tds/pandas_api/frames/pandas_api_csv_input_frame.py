# Copyright 2026 Goldman Sachs
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
    PyLegendSequence,
    PyLegendType,
)
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.tds.pandas_api.frames.pandas_api_base_tds_frame import PandasApiBaseTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_input_tds_frame import (
    PandasApiNonExecutableInputTdsFrame,
)
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig, PyLegendTdsFrame
from pylegend.extensions.tds.abstract.csv_tds_frame import CsvInputFrameAbstract

__all__: PyLegendSequence[str] = [
    "PandasApiCsvNonExecutableInputTdsFrame",
]


class PandasApiCsvNonExecutableInputTdsFrame(
    CsvInputFrameAbstract,
    PandasApiNonExecutableInputTdsFrame
):

    def __init__(
            self,
            csv_string: str) -> None:
        CsvInputFrameAbstract.__init__(self, csv_string=csv_string)
        PandasApiNonExecutableInputTdsFrame.__init__(self, columns=self.columns())

    def get_super_type(self) -> PyLegendType[PyLegendTdsFrame]:
        return CsvInputFrameAbstract

    def to_pure(self, config: FrameToPureConfig) -> str:
        return PandasApiBaseTdsFrame.to_pure(self, config)

    def to_sql_query_object(self, config: FrameToSqlConfig) -> QuerySpecification:
        return PandasApiBaseTdsFrame.to_sql_query_object(self, config)
