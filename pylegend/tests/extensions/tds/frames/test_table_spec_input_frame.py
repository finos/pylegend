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

import importlib
from textwrap import dedent
from pylegend.core.tds.tds_column import PyLegendTdsColumn
from pylegend.core.tds.tds_frame import PyLegendTdsFrame, FrameToSqlConfig
from pylegend.extensions.tds.frames.table_spec_input_frame import TableSpecInputFrame

postgres_ext = 'pylegend.extensions.database.vendors.postgres.postgres_sql_to_string'
importlib.import_module(postgres_ext)


class TestTableSpecInputFrame:
    def test_frame_creation(self) -> None:
        columns = [
            PyLegendTdsColumn("col1", "Integer"),
            PyLegendTdsColumn("col2", "String")
        ]
        frame = TableSpecInputFrame(['test_schema', 'test_table'], columns)
        expected = """\
            select
                "root".col1 as "col1",
                "root".col2 as "col2"
            from
                test_schema.test_table as "root" """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected).rstrip()

    def test_frame_with_head_operation(self) -> None:
        columns = [
            PyLegendTdsColumn("col1", "Integer"),
            PyLegendTdsColumn("col2", "String")
        ]
        frame: PyLegendTdsFrame = TableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.head(10)
        expected = """\
            select top 10
                "root".col1 as "col1",
                "root".col2 as "col2"
            from
                test_schema.test_table as "root" """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected).rstrip()
