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
import pandas as pd
from io import StringIO
from pylegend import LegendClient
from pylegend._typing import (
    PyLegendSequence,
)
from pylegend.core.tds.legendql_api.frames.legendql_api_input_tds_frame import (
    LegendQLApiExecutableInputTdsFrame,
    LegendQLApiNonExecutableInputTdsFrame)
from pylegend.core.tds.abstract.frames.csv_tds_frame import CsvTdsFrame
from pylegend.core.tds.tds_column import TdsColumn, tds_columns_from_pandas_df

__all__: PyLegendSequence[str] = [
    "LegendQLApiCsvExecutableInputTdsFrame",
    "LegendQLApiCsvNonExecutableInputTdsFrame"
]


class LegendQLApiCsvExecutableInputTdsFrame(
    LegendQLApiExecutableInputTdsFrame,
    CsvTdsFrame):

    def __init__(
            self,
            csv_string: str,
            legend_client: LegendClient,
            columns: PyLegendSequence[TdsColumn]) -> None:
        LegendQLApiExecutableInputTdsFrame.__init__(
            self,
            legend_client=legend_client,
            columns=columns)
        CsvTdsFrame.__init__(self, csv_string=csv_string, columns=columns)
        self.set_initialized(True)

    @staticmethod
    def from_csv_string(
            legend_client: LegendClient,
            csv_string: str) -> "LegendQLApiCsvExecutableInputTdsFrame":
        df = pd.read_csv(StringIO(csv_string))
        return LegendQLApiCsvExecutableInputTdsFrame(
            csv_string=csv_string,
            legend_client=legend_client,
            columns=tds_columns_from_pandas_df(df))


class LegendQLApiCsvNonExecutableInputTdsFrame(
    CsvTdsFrame,
    LegendQLApiNonExecutableInputTdsFrame,
):

    def __init__(self, csv_string: str, columns: PyLegendSequence[TdsColumn]) -> None:
        CsvTdsFrame.__init__(self, csv_string=csv_string, columns=columns)
        LegendQLApiNonExecutableInputTdsFrame.__init__(self, columns=columns)
        self.set_initialized(True)

    @staticmethod
    def from_csv_string(csv_string: str) -> "LegendQLApiCsvNonExecutableInputTdsFrame":
        df = pd.read_csv(StringIO(csv_string))
        return LegendQLApiCsvNonExecutableInputTdsFrame(
            csv_string=csv_string,
            columns=tds_columns_from_pandas_df(df))
