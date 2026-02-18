# Copyright 2026 Goldman Sachs
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


class TestLocFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_loc_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # too many indexers
        with pytest.raises(IndexError) as i:
            frame.loc[1, 2, 3]  # type: ignore
        assert i.value.args[0] == "Too many indexers"

        # label-based for rows
        with pytest.raises(TypeError) as t:
            frame.loc[5:, :]
        assert t.value.args[0] == "loc supports only ':' for row slicing. Label-based slicing for rows is not supported."

        # not supported
        with pytest.raises(TypeError) as t:
            frame.loc[[1, 2], :]  # type: ignore
        assert t.value.args[0] == "Unsupported key type for .loc row selection: <class 'list'>"

        # columns
        with pytest.raises(KeyError) as k:
            frame.loc[:, [True, 'col2']]  # type: ignore
        assert k.value.args[0] == "[True] not in index"

        with pytest.raises(IndexError) as i:
            frame.loc[:, [True]]
        assert i.value.args[0] == "Boolean index has wrong length: 1 instead of 2"

        with pytest.raises(TypeError) as t:
            frame.loc[:, {'col1'}]  # type: ignore
        assert t.value.args[0] == "Unsupported key type for .loc column selection: <class 'set'>"

    def test_loc_column(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # basic
        newframe = frame.loc[:, :]
        newframe1 = frame.loc[:]
        newframe2 = frame.loc[:,]  # type: ignore
        expected_sql = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert newframe1.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        assert newframe2.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

        expected_pure = '''\
            #Table(test_schema.test_table)#'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)
        assert generate_pure_query_and_compile(newframe1, FrameToPureConfig(), self.legend_client) == dedent(
            expected_pure)
        assert generate_pure_query_and_compile(newframe2, FrameToPureConfig(), self.legend_client) == dedent(
            expected_pure)

        # slice
        newframe = frame.loc[:, 'col1':'col2']  # type: ignore
        expected_sql = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->select(~[col1, col2])'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        # list
        newframe = frame.loc[:, [False, True]]
        expected_sql = '''\
            SELECT
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->select(~[col2])'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        newframe = frame.loc[:, ['col2']]
        expected_sql = '''\
            SELECT
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->select(~[col2])'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        # string
        newframe = frame.loc[:, 'col1']
        expected_sql = '''\
            SELECT
                "root".col1 AS "col1"
            FROM
                test_schema.test_table AS "root"'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->select(~[col1])'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

    def test_loc_row(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # condition
        newframe = frame.loc[(frame['col1'] > 2) & (frame['col2'] < 5), :]  # type: ignore
        expected_sql = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                (("root".col1 > 2) AND ("root".col2 < 5))'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->filter(c|(toOne($c.col1 > 2) && toOne($c.col2 < 5)))'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        # callable
        newframe = frame.loc[lambda x: (x['col1'] > 10) | (x['col2'] < 3), [True, False]]  # type: ignore
        expected_sql = '''\
            SELECT
                "root".col1 AS "col1"
            FROM
                test_schema.test_table AS "root"
            WHERE
                (("root".col1 > 10) OR ("root".col2 < 3))'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->filter(c|(toOne($c.col1 > 10) || toOne($c.col2 < 3)))
              ->select(~[col1])'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

    def test_e2e_loc_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # basic
        newframe = frame.loc[:, 'Firm/Legal Name']
        expected = {'columns': ['Firm/Legal Name'],
                    'rows': [{'values': ['Firm X']},
                             {'values': ['Firm X']},
                             {'values': ['Firm X']},
                             {'values': ['Firm X']},
                             {'values': ['Firm A']},
                             {'values': ['Firm B']},
                             {'values': ['Firm C']}
                             ]}
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # row condition
        newframe = frame.loc[frame['Age'] > 22, ['First Name', 'Last Name']]  # type: ignore
        expected = {'columns': ['First Name', 'Last Name'],
                    'rows': [{'values': ['Peter', 'Smith']},
                             {'values': ['Fabrice', 'Roberts']},
                             {'values': ['Oliver', 'Hill']},
                             {'values': ['David', 'Harris']}]}
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # row callable
        newframe = frame.loc[lambda x: x['First Name'].startswith('Jo'), [True, True, False, False]]  # type: ignore
        expected = {'columns': ['First Name', 'Last Name'],
                    'rows': [{'values': ['John', 'Johnson']},
                             {'values': ['John', 'Hill']}]}
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # empty frame
        newframe = frame.loc[:, 'Last Name':'First Name']  # type: ignore
        expected = {'columns': ['First Name', 'Last Name', 'Age', 'Firm/Legal Name'],
                    'rows': []}
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
