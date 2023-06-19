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

import pytest
import importlib
from textwrap import dedent
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.legend_api.frames.legend_api_table_spec_input_frame import LegendApiTableSpecInputFrame

postgres_ext = 'pylegend.extensions.database.vendors.postgres.postgres_sql_to_string'
importlib.import_module(postgres_ext)


class TestTableSpecInputFrame:
    def test_table_spec_frame_creation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        expected = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_table_spec_frame_execution_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        with pytest.raises(ValueError) as v:
            frame.execute_frame(lambda res: b"".join(res))
        assert v.value.args[0] == "Cannot execute frame as its built on top of non-executable " \
                                  "input frames: [TableSpecInputFrame(test_schema.test_table)]"

        with pytest.raises(ValueError) as v:
            new_frame = frame.head(10)
            new_frame.execute_frame(lambda r: b"".join(r))
        assert v.value.args[0] == "Cannot execute frame as its built on top of non-executable " \
                                  "input frames: [TableSpecInputFrame(test_schema.test_table)]"

    def test_table_spec_frame_creation_duplicated_columns_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col1")
        ]
        with pytest.raises(ValueError) as v:
            LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        assert v.value.args[0] == "TdsFrame cannot have duplicated column names. Passed columns: " \
                                  "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col1, Type: String)]"
