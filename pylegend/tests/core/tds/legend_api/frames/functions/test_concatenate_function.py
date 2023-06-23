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
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig
from pylegend.core.tds.legend_api.frames.legend_api_tds_frame import LegendApiTdsFrame
from pylegend.extensions.tds.legend_api.frames.legend_api_table_spec_input_frame import LegendApiTableSpecInputFrame
from pylegend.tests.test_helpers.legend_service_frame import simple_person_service_frame
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)


class TestConcatenateAppliedFunction:

    def test_concatenate_error_on_different_size_frames(self) -> None:
        columns1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns1)

        columns2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.string_column("col3")
        ]
        frame2: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns2)

        with pytest.raises(ValueError) as v:
            frame1.concatenate(frame2)

        expected = (
            'Cannot concatenate two Tds Frames with different column counts. \n'
            'Frame 1 cols - (Count: 2) - [TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String)] \n'
            'Frame 2 cols - (Count: 3) - [TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), '
            'TdsColumn(Name: col3, Type: String)] \n'
        )
        assert v.value.args[0] == expected

    def test_concatenate_error_on_column_name_mismatch(self) -> None:
        columns1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns1)

        columns2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col3")
        ]
        frame2: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns2)

        with pytest.raises(ValueError) as v:
            frame1.concatenate(frame2)

        expected = (
            'Column name/type mismatch when concatenating Tds Frames at index 1. '
            'Frame 1 column - TdsColumn(Name: col2, Type: String), Frame 2 column - TdsColumn(Name: col3, Type: String)'
        )
        assert v.value.args[0] == expected

    def test_concatenate_error_on_column_type_mismatch(self) -> None:
        columns1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns1)

        columns2 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.float_column("col2")
        ]
        frame2: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns2)

        with pytest.raises(ValueError) as v:
            frame1.concatenate(frame2)

        expected = (
            'Column name/type mismatch when concatenating Tds Frames at index 1. '
            'Frame 1 column - TdsColumn(Name: col2, Type: String), Frame 2 column - TdsColumn(Name: col2, Type: Float)'
        )
        assert v.value.args[0] == expected

    def test_sql_gen_concatenate_function(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame1 = frame1.take(2)
        frame2: LegendApiTdsFrame = LegendApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2 = frame2.drop(2)
        frame2 = frame2.take(2)

        concatenate_frame = frame1.concatenate(frame2)
        expected = '''\
            SELECT
                "root"."col1" AS "col1",
                "root"."col2" AS "col2"
            FROM
                (
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 2
                    UNION ALL
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 2
                    OFFSET 2
                ) AS "root"'''
        assert concatenate_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)

    def test_e2e_concatenate_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])
        frame = frame.concatenate(frame).restrict(["First Name", "Firm/Legal Name"])
        expected = {'columns': ['First Name', 'Firm/Legal Name'],
                    'rows': [{'values': ['Peter', 'Firm X']},
                             {'values': ['John', 'Firm X']},
                             {'values': ['John', 'Firm X']},
                             {'values': ['Anthony', 'Firm X']},
                             {'values': ['Fabrice', 'Firm A']},
                             {'values': ['Oliver', 'Firm B']},
                             {'values': ['David', 'Firm C']},
                             {'values': ['Peter', 'Firm X']},
                             {'values': ['John', 'Firm X']},
                             {'values': ['John', 'Firm X']},
                             {'values': ['Anthony', 'Firm X']},
                             {'values': ['Fabrice', 'Firm A']},
                             {'values': ['Oliver', 'Firm B']},
                             {'values': ['David', 'Firm C']}]}
        res = frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    @pytest.mark.skip(reason="Legend engine doesn't parse the SQL")  # TODO: Should get this fixed in SQL parser
    def test_e2e_concatenate_function_complex(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) \
            -> None:
        frame: LegendApiTdsFrame = simple_person_service_frame(legend_test_server["engine_port"])

        frame1 = frame.restrict(["First Name", "Firm/Legal Name", "Age"])
        frame1 = frame1.take(3)

        frame2 = frame.restrict(["First Name", "Firm/Legal Name", "Age"])
        frame2 = frame2.drop(3)
        frame2 = frame2.take(2)

        result_frame = frame1.concatenate(frame2).restrict(["First Name", "Firm/Legal Name"])
        expected = {'columns': ['First Name', 'Firm/Legal Name'],
                    'rows': [{'values': ['Peter', 'Firm X']},
                             {'values': ['John', 'Firm X']},
                             {'values': ['John', 'Firm X']},
                             {'values': ['Anthony', 'Firm X']},
                             {'values': ['Fabrice', 'Firm A']}]}
        res = result_frame.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
