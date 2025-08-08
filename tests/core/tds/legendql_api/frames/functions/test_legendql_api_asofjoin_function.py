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

import pytest

from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame import LegendQLApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig
from pylegend.extensions.tds.legendql_api.frames.legendql_api_table_spec_input_frame import \
    LegendQLApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile


class TestJoinAppliedFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_join_error_on_incompatible_match_lambda(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.date_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame1.as_of_join(frame2, lambda: True)  # type: ignore
        assert r.value.args[0] == ("AsOfJoin match function should be a lambda which takes two arguments "
                                   "(TDSRow, TDSRow)")
        with pytest.raises(TypeError) as r:
            frame1.as_of_join(frame2, lambda x: True)  # type: ignore
        assert r.value.args[0] == ("AsOfJoin match function should be a lambda which takes two arguments "
                                   "(TDSRow, TDSRow)")

    def test_join_error_on_incompatible_join_lambda(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.date_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(TypeError) as r:
            frame1.as_of_join(frame2, lambda x, y: True, lambda: True)  # type: ignore
        assert r.value.args[0] == ("AsOfJoin join function should be a lambda which takes two arguments "
                                   "(TDSRow, TDSRow)")
        with pytest.raises(TypeError) as r:
            frame1.as_of_join(frame2, lambda x, y: True, lambda x: True)  # type: ignore
        assert r.value.args[0] == ("AsOfJoin join function should be a lambda which takes two arguments "
                                   "(TDSRow, TDSRow)")

    def test_join_error_on_duplicated_columns(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as r:
            frame1.as_of_join(frame2, lambda x, y: True)
        assert r.value.args[0] == (
            "Found duplicate columns in joined frames. Use rename function to ensure there are no duplicate columns in "
            "joined frames. Columns - Left Frame: ['col1', 'col2'], Right Frame: ['col1', 'col2']")

    def test_join_error_on_non_boolean_match_lambda(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame1.as_of_join(frame2, lambda x, y: 1)  # type: ignore
        assert r.value.args[0] == "AsOfJoin match function incompatible. Returns non boolean - <class 'int'>"

    def test_join_error_on_non_boolean_join_lambda(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame1.as_of_join(frame2, lambda x, y: True, lambda x, y: 1)  # type: ignore
        assert r.value.args[0] == "AsOfJoin join function incompatible. Returns non boolean - <class 'int'>"

    def test_join_error_on_failing_match_lambda(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame1.as_of_join(frame2, lambda x, y: x['col3'] > y['col1'])  # type: ignore
        assert r.value.args[0] == ("AsOfJoin match function incompatible. Error occurred while evaluating. "
                                   "Message: Column - 'col3' doesn't exist in the current frame. "
                                   "Current frame columns: ['col1', 'col2']")

    def test_join_error_on_failing_join_lambda(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(RuntimeError) as r:
            frame1.as_of_join(frame2, lambda x, y: x['col2'] > y['col1'], lambda x, y: x['col3'] == y['col1'])  # type: ignore
        assert r.value.args[0] == ("AsOfJoin join function incompatible. Error occurred while evaluating. "
                                   "Message: Column - 'col3' doesn't exist in the current frame. "
                                   "Current frame columns: ['col1', 'col2']")

    def test_query_gen_join_as_of_join(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col3"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)
        frame = frame1.as_of_join(frame2, lambda x, y: x['col1'] > y['col3'])  # type: ignore

        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), "
            "TdsColumn(Name: col3, Type: Integer), TdsColumn(Name: col4, Type: String)]"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->asOfJoin(
                #Table(test_schema.test_table2)#,
                {l, r | $l.col1 > $r.col3}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->asOfJoin(#Table(test_schema.test_table2)#, '
                '{l, r | $l.col1 > $r.col3})')

    def test_query_gen_join_as_of_join_with_join_function(self) -> None:
        cols1 = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table1'], cols1)
        cols2 = [
            PrimitiveTdsColumn.integer_column("col3"),
            PrimitiveTdsColumn.string_column("col4")
        ]
        frame2: LegendQLApiTdsFrame = LegendQLApiTableSpecInputFrame(['test_schema', 'test_table2'], cols2)
        frame = frame1.as_of_join(
            frame2, lambda x, y: x['col1'] > y['col3'], lambda x, y: x['col2'] == y['col4']  # type: ignore
        )

        assert "[" + ", ".join([str(c) for c in frame.columns()]) + "]" == (
            "[TdsColumn(Name: col1, Type: Integer), TdsColumn(Name: col2, Type: String), "
            "TdsColumn(Name: col3, Type: Integer), TdsColumn(Name: col4, Type: String)]"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table1)#
              ->asOfJoin(
                #Table(test_schema.test_table2)#,
                {l, r | $l.col1 > $r.col3},
                {l, r | $l.col2 == $r.col4}
              )'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == \
               ('#Table(test_schema.test_table1)#->asOfJoin(#Table(test_schema.test_table2)#, '
                '{l, r | $l.col1 > $r.col3}, {l, r | $l.col2 == $r.col4})')
