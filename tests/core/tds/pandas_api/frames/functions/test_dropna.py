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
from datetime import date, datetime
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


class TestDropnaFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_dropna_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # axis
        with pytest.raises(NotImplementedError) as n:
            frame.dropna(axis=1)
        assert n.value.args[0] == "axis=1 is not supported yet in Pandas API dropna"

        with pytest.raises(ValueError) as v:
            frame.dropna(axis=2)
        assert v.value.args[0] == "No axis named 2 for object type TdsFrame"

        # how
        with pytest.raises(ValueError) as v:
            frame.dropna(how="one")
        assert v.value.args[0] == "invalid how option: one"

        # thresh
        with pytest.raises(NotImplementedError) as n:
            frame.dropna(thresh=1)
        assert n.value.args[0] == "thresh parameter is not supported yet in Pandas API dropna"

        # subset
        with pytest.raises(TypeError) as t:
            frame.dropna(subset=5)  # type: ignore
        assert t.value.args[0] == "subset must be a list, tuple or set of column names. Got <class 'int'>"

        with pytest.raises(KeyError) as k:
            frame.dropna(subset=["col3"])
        assert k.value.args[0] == "['col3']"

        # inplace
        with pytest.raises(NotImplementedError) as n:
            frame.dropna(inplace=True)
        assert n.value.args[0] == "inplace=True is not supported yet in Pandas API dropna"

        # ignore_index
        with pytest.raises(NotImplementedError) as n:
            frame.dropna(ignore_index=True)
        assert n.value.args[0] == "ignore_index=True is not supported yet in Pandas API dropna"

    def test_drop(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # basic
        newframe = frame.dropna()
        expected_sql = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                (("root".col1 IS NOT NULL) AND ("root".col2 IS NOT NULL))'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

        expected_pure = '''\
                    #Table(test_schema.test_table)#
                      ->filter(c|($c.col1->isNotEmpty() && $c.col2->isNotEmpty()))'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(
            expected_pure)

        # how = all
        newframe = frame.dropna(how='all')
        expected_sql = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                (("root".col1 IS NOT NULL) OR ("root".col2 IS NOT NULL))'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->filter(c|($c.col1->isNotEmpty() || $c.col2->isNotEmpty()))'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        # subset
        newframe = frame.dropna(subset=['col1'])

        expected_sql = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                ("root".col1 IS NOT NULL)'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->filter(c|$c.col1->isNotEmpty())'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        # subset with how = all
        newframe = frame.dropna(subset=[], how='all')
        expected_sql = '''\
            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"
            WHERE
                false'''
        assert newframe.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure = '''\
            #Table(test_schema.test_table)#
              ->filter(c|1!=1)'''
        assert generate_pure_query_and_compile(newframe, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)


