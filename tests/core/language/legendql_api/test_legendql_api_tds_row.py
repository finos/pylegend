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

from pylegend.core.tds.tds_column import PrimitiveTdsColumn, EnumTdsColumn
from pylegend.extensions.tds.legendql_api.frames.legendql_api_table_spec_input_frame import LegendQLApiTableSpecInputFrame
from pylegend.core.language import PyLegendString
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from tests.core.language.shared.test_tds_row import AbstractTestTdsRow
from pylegend._typing import PyLegendList


class TestLegendQLApiTdsRow(AbstractTestTdsRow):

    def get_tds_row(self, columns: PyLegendList[PrimitiveTdsColumn]) -> LegendQLApiTdsRow:
        frame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        return LegendQLApiTdsRow.from_tds_frame("t", frame)

    def test_enum_get_col_access_with_enum_tds_column(self) -> None:
        columns = [
            EnumTdsColumn('col2', 'my::EnumType', ['A', 'B'])
        ]
        frame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        tds_row = LegendQLApiTdsRow.from_tds_frame("t", frame)
        col_expr = tds_row.col2
        assert isinstance(col_expr, PyLegendString)
