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

from textwrap import dedent
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.legendql_api.frames.legendql_api_csv_input_frame import \
    LegendQLApiCsvNonExecutableInputTdsFrame


class TestLegendQLApiCsvInputFrame:
    test_csv_string = 'id,grp,name\n1,1,A\n3,1,B'

    def test_csv_non_executable_input_frame_creation(self) -> None:
        frame = LegendQLApiCsvNonExecutableInputTdsFrame.from_csv_string(
            csv_string=self.test_csv_string
        )
        frame = frame.extend(("col3", lambda r: r.get_integer('id') + 1))
        expected_sql = (
            'SELECT\n'
            '    "root"."id" AS "id",\n'
            '    "root"."grp" AS "grp",\n'
            '    "root"."name" AS "name"\n'
            'FROM\n'
            '    CSV(\n'
            '        \'id,grp,name\n'
            '1,1,A\n'
            '3,1,B\'\n'
            '    ) AS root'
        )
        expected_pure = (
            '#TDS\n'
            'id,grp,name\n'
            '1,1,A\n'
            '3,1,B#'
        )
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql
        assert frame.to_pure_query() == expected_pure

    def test_csv_non_executable_input_frame_extend(self) -> None:
        frame = LegendQLApiCsvNonExecutableInputTdsFrame.from_csv_string(
            csv_string=self.test_csv_string
        )
        frame = frame.extend(("col4", lambda r: r.get_integer('id') + 1))
        expected_sql = (
            'SELECT\n'
            '    "root"."id" AS "id",\n'
            '    "root"."grp" AS "grp",\n'
            '    "root"."name" AS "name",\n'
            '    ("root"."id" + 1) AS "col4"\n'
            'FROM\n'
            '    CSV(\n'
            '        \'id,grp,name\n'
            '1,1,A\n'
            '3,1,B\'\n'
            '    ) AS root'
        )
        expected_pure = (
            '#TDS\n'
            'id,grp,name\n'
            '1,1,A\n'
            '3,1,B#\n'
            '  ->extend(~col4:{r | toOne($r.id) + 1})'
        )
        assert frame.to_sql_query(FrameToSqlConfig()) == expected_sql
        assert frame.to_pure_query() == expected_pure

        frame= LegendQLApiCsvNonExecutableInputTdsFrame.from_csv_string(
            csv_string="id,grp,name\n1,1,A\n3,1,B\n3,1,C\n4,4,D\n3,1,E\n6,1,F\n7,4,G\n8,1,H\n9,5,I\n10,0,J")
        # .window_extend(
        #     frame.window(partition_by="grp",order_by=lambda r: [r.name.descending()]),
        #                             ("other", lambda p, w, r: p.ntile(r, 2))))

        frame= frame.window_extend(
            frame.window(partition_by="grp", order_by=lambda r: [r.id.descending()]),
                            ("other", lambda p, w, r: p.rank(w, r)))

        print(frame.to_pure_query())
        print("\n")
        print(frame.to_sql_query(FrameToSqlConfig()))