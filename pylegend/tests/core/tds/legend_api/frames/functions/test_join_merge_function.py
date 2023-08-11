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

import json
import pytest
from textwrap import dedent

from pylegend.core.tds.legend_api.frames.legend_api_tds_frame import LegendApiTdsFrame
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.extensions.tds.legend_api.frames.legend_api_table_spec_input_frame import LegendApiTableSpecInputFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame


class TestJoinByAppliedFunction:

    def test_join_on_single_col_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame1 = frame.drop(2)
        frame2 = frame.take(5)
        joined_frame = frame1.join(other=frame2.set_index("col2"), on="col2")

        expected = '''\
            SELECT
                "root1"."col1" AS "col1",
                "root1"."col2" AS "col2",
                "root2"."col1" AS "col1",
                "root2"."col2" AS "col2"
            FROM
                SELECT
                    "root1"."col1" AS "col1",
                    "root1"."col2" AS "col2"
                FROM
                    (
                        SELECT
                            "root".col1 AS "col1",
                            "root".col2 AS "col2"
                        FROM
                            test_schema.test_table AS "root"
                        OFFSET 2
                    ) AS "root1"
                LEFT OUTER JOIN
                    SELECT
                    "root2"."col1" AS "col1",
                    "root2"."col2" AS "col2"
                FROM
                    (
                        SELECT
                            "root".col1 AS "col1",
                            "root".col2 AS "col2"
                        FROM
                            test_schema.test_table AS "root"
                        LIMIT 5
                    ) AS "root2"
                    ON (col2)'''
        assert joined_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_merge_on_different_cols_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame1 = frame.drop(2)
        frame2 = frame.take(5)
        joined_frame = frame1.merge(right=frame2.set_index("col2"), left_on="col2", right_on="col2")

        expected = '''\
            SELECT
                "root1"."col1" AS "col1",
                "root1"."col2" AS "col2",
                "root2"."col1" AS "col1",
                "root2"."col2" AS "col2"
            FROM
                SELECT
                    "root1"."col1" AS "col1",
                    "root1"."col2" AS "col2"
                FROM
                    (
                        SELECT
                            "root".col1 AS "col1",
                            "root".col2 AS "col2"
                        FROM
                            test_schema.test_table AS "root"
                        OFFSET 2
                    ) AS "root1"
                INNER JOIN
                    SELECT
                    "root2"."col1" AS "col1",
                    "root2"."col2" AS "col2"
                FROM
                    (
                        SELECT
                            "root".col1 AS "col1",
                            "root".col2 AS "col2"
                        FROM
                            test_schema.test_table AS "root"
                        LIMIT 5
                    ) AS "root2"
                    ON (col2)'''
        assert joined_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)