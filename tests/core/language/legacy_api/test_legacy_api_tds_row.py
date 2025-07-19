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

from pylegend.core.language.shared.tds_row import AbstractTdsRow
from pylegend.core.sql.metamodel import QuerySpecification
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.legacy_api.frames.legacy_api_table_spec_input_frame import LegacyApiTableSpecInputFrame
from pylegend.core.language.legacy_api.legacy_api_tds_row import LegacyApiTdsRow
from tests.core.language.shared.test_tds_row import AbstractTestTdsRow
from pylegend._typing import PyLegendList, PyLegendDict


class TestLegacyApiTdsRow(AbstractTestTdsRow):

    def get_tds_row(self, columns: PyLegendList[PrimitiveTdsColumn]) -> AbstractTdsRow:
        frame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        return LegacyApiTdsRow.from_tds_frame("t", frame)

    def get_frame_name_to_base_query_map(
            self,
            columns: PyLegendList[PrimitiveTdsColumn]
    ) -> PyLegendDict[str, QuerySpecification]:
        frame = LegacyApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        return {"t": frame.to_sql_query_object(config=FrameToSqlConfig())}
