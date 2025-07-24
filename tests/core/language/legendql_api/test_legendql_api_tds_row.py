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

from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.legendql_api.frames.legendql_api_table_spec_input_frame import LegendQLApiTableSpecInputFrame
from pylegend.core.language import PyLegendString
from pylegend.core.language.legendql_api.legendql_api_tds_row import LegendQLApiTdsRow
from tests.core.language.shared.test_tds_row import AbstractTestTdsRow
from pylegend._typing import PyLegendList, PyLegendDict


class TestLegendQLApiTdsRow(AbstractTestTdsRow):

    def get_tds_row(self, columns: PyLegendList[PrimitiveTdsColumn]) -> LegendQLApiTdsRow:
        frame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        return LegendQLApiTdsRow.from_tds_frame("t", frame)

    def get_frame_name_to_base_query_map(
            self,
            columns: PyLegendList[PrimitiveTdsColumn]
    ) -> PyLegendDict[str, QuerySpecification]:
        frame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        return {"t": frame.to_sql_query_object(config=FrameToSqlConfig())}

    def test_col_access_with_dot_operator(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        tds_row = self.get_tds_row(columns)
        col_expr = tds_row.col2

        assert isinstance(col_expr, PyLegendString)
        assert self.db_extension.process_expression(
            col_expr.to_sql_expression(self.get_frame_name_to_base_query_map(columns), self.frame_to_sql_config),
            config=self.sql_to_string_config
        ) == '"root".col2'
        assert col_expr.to_pure_expression(self.frame_to_pure_config) == '$t.col2'
