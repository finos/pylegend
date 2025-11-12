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
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
import pytest
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from types import SimpleNamespace
from unittest.mock import patch
from pylegend.core.tds.pandas_api.frames.functions.truncate_function import TruncateFunction


class TestTruncateFunction:
    
    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_truncate_invalid_axis(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.truncate(axis=1)
        assert v.value.args[0] == "The 'axis' parameter of the truncate function must be 0 or 'index', but got: 1"
    

    def test_truncate_invalid_copy(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.truncate(before=0, after=1, axis=0, copy=False)
        assert v.value.args[0] == "The 'copy' parameter of the truncate function must be True, but got: False"


    def test_truncate_before_not_int(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.truncate(before='a', after=1, axis=0, copy=True)
        assert v.value.args[0] == "The 'before' parameter of the truncate function must be an integer, but got: a (type: str)"


    def test_truncate_after_not_int(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.truncate(before=0, after='b', axis=0, copy=True)
        assert v.value.args[0] == "The 'after' parameter of the truncate function must be an integer, but got: b (type: str)"


    def test_truncate_before_greater_than_after(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        with pytest.raises(ValueError) as v:
            frame.truncate(before=5, after=3, axis=0, copy=True)
        assert v.value.args[0] == (
            "The 'before' parameter of the truncate function must be less than or equal to the 'after' parameter, "
            "but got: before=5, after=3"
        )

    def test_truncate_simple_query_generation(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.truncate(before=0, after=3)
        expected = '''\
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 4
                    OFFSET 0'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->slice(0, 4)'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False),
                                               self.legend_client) == ("#Table(test_schema.test_table)#->slice(0, 4)")
        
    def test_to_sql_uses_create_sub_query_when_base_has_offset_and_sets_offset_only(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.truncate(before=2, after=None)
        expected = '''\
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    OFFSET 2'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->drop(2)'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False),
                                               self.legend_client) == ("#Table(test_schema.test_table)#->drop(2)")

    def test_to_sql_uses_copy_query_and_sets_limit_when_after_provided(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame = frame.truncate(before=1, after=3)
        expected = '''\
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 3
                    OFFSET 1'''
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->slice(1, 4)'''
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False),
                                               self.legend_client) == ("#Table(test_schema.test_table)#->slice(1, 4)")

    def test_validate_resets_negative_before_and_after_and_accepts_axis_string(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2")
        ]
        frame1: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame1 = frame1.truncate(before=-3, after=None)
        expected1 = '''\
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    OFFSET 0'''
        assert frame1.to_sql_query(FrameToSqlConfig()) == dedent(expected1)
        assert generate_pure_query_and_compile(frame1, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->drop(0)'''
        )
        assert generate_pure_query_and_compile(frame1, FrameToPureConfig(pretty=False),
                                               self.legend_client) == ("#Table(test_schema.test_table)#->drop(0)")

        frame2: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)
        frame2 = frame2.truncate(before=0, after=-2)
        expected2 = '''\
                    SELECT
                        "root".col1 AS "col1",
                        "root".col2 AS "col2"
                    FROM
                        test_schema.test_table AS "root"
                    LIMIT 1
                    OFFSET 0'''
        assert frame2.to_sql_query(FrameToSqlConfig()) == dedent(expected2)
        
        assert generate_pure_query_and_compile(frame2, FrameToPureConfig(), self.legend_client) == dedent(
            '''\
            #Table(test_schema.test_table)#
              ->slice(0, 1)'''
        )
        assert generate_pure_query_and_compile(frame2, FrameToPureConfig(pretty=False),
                                               self.legend_client) == ("#Table(test_schema.test_table)#->slice(0, 1)")
