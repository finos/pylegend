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

import pytest
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.extensions.tds.legendql_api.frames.legendql_api_table_spec_input_frame import LegendQLApiTableSpecInputFrame


class TestLegendQLApiTableSpecInputFrame:
    def test_table_spec_frame_creation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        assert frame.to_pure_query() == '#Table(test_schema.test_table)#'

    def test_table_spec_frame_creation_duplicated_columns_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col1")
        ]
        with pytest.raises(ValueError) as v:
            LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        assert v.value.args[0] == "TdsFrame cannot have duplicated column names. Passed columns: " \
                                  "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col1, Type: String)]"
