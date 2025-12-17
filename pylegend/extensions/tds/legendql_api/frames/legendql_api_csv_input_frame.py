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
from pylegend._typing import (
    PyLegendSequence,
)
from pylegend.core.tds.legendql_api.frames.legendql_api_input_tds_frame import (
    LegendQLApiNonExecutableInputTdsFrame,
)
from pylegend.core.tds.abstract.frames.csv_tds_frame import CsvTdsFrame

__all__: PyLegendSequence[str] = [
    "LegendQLApiCsvNonExecutableInputTdsFrame",
    "LegendQLApiCsvTdsFrame"
]


class LegendQLApiCsvTdsFrame(CsvTdsFrame):

    def __init__(self, csv_string: str) -> None:
        CsvTdsFrame.__init__(self, csv_string=csv_string)


class LegendQLApiCsvNonExecutableInputTdsFrame(
    LegendQLApiCsvTdsFrame,
    LegendQLApiNonExecutableInputTdsFrame
):

    def __init__(
            self,
            csv_string: str) -> None:
        LegendQLApiCsvTdsFrame.__init__(self, csv_string=csv_string)
        LegendQLApiNonExecutableInputTdsFrame.__init__(self, columns=self.columns())
