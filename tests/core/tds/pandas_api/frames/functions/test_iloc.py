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

import json
from textwrap import dedent

import pytest

from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile


class TestIlocFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_iloc_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # too many indexers
        with pytest.raises(IndexError) as i:
            frame.iloc[1, 2, 3]
        assert i.value.args[0] == "Too many indexers"

        # out of bounds
        with pytest.raises(IndexError) as i:
            frame.iloc[5, 2]
        assert i.value.args[0] == "single positional indexer is out-of-bounds"

        # not supported
        with pytest.raises(NotImplementedError) as n:
            frame.iloc[::2, :]
        assert n.value.args[0] == "iloc with slice step other than 1 is not supported yet in Pandas Api"

        with pytest.raises(NotImplementedError) as n:
            frame.iloc[2, 1:4:2]
        assert n.value.args[0] == "iloc with slice step other than 1 is not supported yet in Pandas Api"

        with pytest.raises(NotImplementedError) as n:
            frame.iloc[:, [True, False]]  # type: ignore
        assert n.value.args[0] == "iloc supports integer, slice, or tuple of these, but got indexer of type: <class 'list'>"

        with pytest.raises(NotImplementedError) as n:
            frame.iloc[lambda x: x % 2 == 0, :]  # type: ignore
        assert n.value.args[0] == (
            "iloc supports integer, slice, or tuple of these, "
            "but got indexer of type: <class 'function'>"
        )

    def test_iloc(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # basic
        newframe = frame.iloc[0, 0]
        expected_sql = '''\
            SELECT
                "root".col1 AS "col1"
            FROM
                test_schema.test_table AS "root"
            LIMIT 1
            OFFSET 0'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->slice(0, 1)
              ->select(~[col1])'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        # slice
        newframe = frame.iloc[1:3, 0:2]
        expected_sql = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            LIMIT 2
            OFFSET 1'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->slice(1, 3)
              ->select(~[col1, col2])'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        # slice with open ended
        newframe = frame.iloc[2:, 1:]
        expected_sql = '''\
            SELECT
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            OFFSET 2'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->drop(2)
              ->select(~[col2])'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        # comb
        newframe = frame.iloc[3, :]
        expected_sql = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            LIMIT 1
            OFFSET 3'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->slice(3, 4)
              ->select(~[col1, col2])'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

    def test_e2e_iloc_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # basic
        newframe = frame.iloc[0, 3]
        expected = {'columns': ['Firm/Legal Name'],
                    'rows': [{'values': ['Firm X']},
                             ]}
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # slice
        newframe = frame.iloc[1:4, 0:2]
        expected = {'columns': ['First Name', 'Last Name'],
                    'rows': [{'values': ['John', 'Johnson']},
                             {'values': ['John', 'Hill']},
                             {'values': ['Anthony', 'Allen']},
                             ]}
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # comb
        newframe = frame.iloc[2, :]
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': [{'values': ['John', 'Hill', 12, 'Firm X']},
                             ]}
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # out of bounds
        newframe = frame.iloc[10, :]
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': []}
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
