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
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api, simple_trade_service_frame_pandas_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient
from tests.test_helpers import generate_pure_query_and_compile


class TestFillnaFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_fillna_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # value
        with pytest.raises(TypeError) as t:
            frame.fillna(value=[5])  # type: ignore
        assert t.value.args[0] == "'value' parameter must be a scalar or dict, but you passed a <class 'list'>"

        with pytest.raises(TypeError) as t:
            frame.fillna(value={'col1': [5]})  # type: ignore
        assert t.value.args[0] == "Non-scalar value of type <class 'list'> passed for column 'col1' in 'value' parameter"

        with pytest.raises(TypeError) as t:
            frame.fillna(value={8: 5})  # type: ignore
        assert t.value.args[0] == "All keys in 'value' dict must be strings representing column names, but found key of type <class 'int'>"

        with pytest.raises(ValueError) as v:
            frame.fillna()
        assert v.value.args[0] == "Must specify a fill 'value'"

        # axis
        with pytest.raises(NotImplementedError) as n:
            frame.fillna(value=5, axis=1)
        assert n.value.args[0] == "axis=1 is not supported yet in Pandas API fillna"

        with pytest.raises(ValueError) as v:
            frame.fillna(value=5, axis=2)
        assert v.value.args[0] == "No axis named 2 for object type TdsFrame"

        # inplace
        with pytest.raises(NotImplementedError) as n:
            frame.fillna(value=5, inplace=True)
        assert n.value.args[0] == "inplace=True is not supported yet in Pandas API fillna"

        # limit
        with pytest.raises(NotImplementedError) as n:
            frame.fillna(value=5, limit=5)
        assert n.value.args[0] == "limit parameter is not supported yet in Pandas API fillna"

    def test_fillna(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # basic
        new_frame = frame.fillna(value=100)
        expected_sql = '''\
            SELECT
                coalesce("root".col1, 100) AS "col1",
                coalesce("root".col2, 100) AS "col2"
            FROM
                test_schema.test_table AS "root"'''
        assert new_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

        expected_pure = '''\
        #Table(test_schema.test_table)#
          ->project(~['col1':c|coalesce($c.col1, 100), 'col2':c|coalesce($c.col2, 100)])'''
        assert generate_pure_query_and_compile(new_frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        # value as dict
        new_frame = frame.fillna(value={'col1': 100, 'col2': 200})
        expected_sql = '''\
            SELECT
                coalesce("root".col1, 100) AS "col1",
                coalesce("root".col2, 200) AS "col2"
            FROM
                test_schema.test_table AS "root"'''
        assert new_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

        expected_pure = '''\
        #Table(test_schema.test_table)#
          ->project(~['col1':c|coalesce($c.col1, 100), 'col2':c|coalesce($c.col2, 200)])'''
        assert generate_pure_query_and_compile(new_frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

        # unknown cols in dict
        new_frame = frame.fillna(value={'col1': 100, 'col3':200})
        expected_sql = '''\
            SELECT
                coalesce("root".col1, 100) AS "col1",
                "root".col2 AS "col2"
            FROM
                test_schema.test_table AS "root"'''
        assert new_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)

        expected_pure = '''\
        #Table(test_schema.test_table)#
          ->project(~['col1':c|coalesce($c.col1, 100), 'col2':c|$c.col2])'''
        assert generate_pure_query_and_compile(new_frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure)

    # flake8: noqa
    def test_e2e_fillna(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int, ]]) -> None:
        frame: PandasApiTdsFrame = simple_trade_service_frame_pandas_api(legend_test_server["engine_port"])

        # basic
        newframe = frame.fillna(value={
            "Settlement Date Time": datetime(1900, 1, 1, 0, 0, 0),
            "Product/Name": "temp",
            "Account/Name": "temp"
        })
        expected = {'columns': ['Id',
                                    'Date',
                                    'Quantity',
                                    'Settlement Date Time',
                                    'Product/Name',
                                    'Account/Name'],
                        'rows': [{'values': [1,
                                             '2014-12-01',
                                             25.0,
                                             '2014-12-02T21:00:00.000000000+0000',
                                             'Firm X',
                                             'Account 1']},
                                 {'values': [2,
                                             '2014-12-01',
                                             320.0,
                                             '2014-12-02T21:00:00.000000000+0000',
                                             'Firm X',
                                             'Account 2']},
                                 {'values': [3,
                                             '2014-12-01',
                                             11.0,
                                             '2014-12-02T21:00:00.000000000+0000',
                                             'Firm A',
                                             'Account 1']},
                                 {'values': [4,
                                             '2014-12-02',
                                             23.0,
                                             '2014-12-03T21:00:00.000000000+0000',
                                             'Firm A',
                                             'Account 2']},
                                 {'values': [5,
                                             '2014-12-02',
                                             32.0,
                                             '2014-12-03T21:00:00.000000000+0000',
                                             'Firm A',
                                             'Account 1']},
                                 {'values': [6,
                                             '2014-12-03',
                                             27.0,
                                             '2014-12-04T21:00:00.000000000+0000',
                                             'Firm C',
                                             'Account 1']},
                                 {'values': [7,
                                             '2014-12-03',
                                             44.0,
                                             '2014-12-04T15:22:23.123456789+0000',
                                             'Firm C',
                                             'Account 1']},
                                 {'values': [8,
                                             '2014-12-04',
                                             22.0,
                                             '2014-12-05T21:00:00.000000000+0000',
                                             'Firm C',
                                             'Account 2']},
                                 {'values': [9,
                                             '2014-12-04',
                                             45.0,
                                             '2014-12-05T21:00:00.000000000+0000',
                                             'Firm C',
                                             'Account 2']},
                                 {'values': [10, '2014-12-04', 38.0, '1900-01-01T00:00:00.000000000+0000', 'Firm C', 'Account 2']},
                                 {'values': [11, '2014-12-05', 5.0, '1900-01-01T00:00:00.000000000+0000', 'temp', 'temp']}]}

        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # unknown col in dict
        newframe = frame.fillna(value={
            "Product/Name": "temp",
            "Account/Name": "temp",
            "unknown": 100
        })
        expected = {'columns': ['Id',
                                'Date',
                                'Quantity',
                                'Settlement Date Time',
                                'Product/Name',
                                'Account/Name'],
                    'rows': [{'values': [1,
                                         '2014-12-01',
                                         25.0,
                                         '2014-12-02T21:00:00.000000000+0000',
                                         'Firm X',
                                         'Account 1']},
                             {'values': [2,
                                         '2014-12-01',
                                         320.0,
                                         '2014-12-02T21:00:00.000000000+0000',
                                         'Firm X',
                                         'Account 2']},
                             {'values': [3,
                                         '2014-12-01',
                                         11.0,
                                         '2014-12-02T21:00:00.000000000+0000',
                                         'Firm A',
                                         'Account 1']},
                             {'values': [4,
                                         '2014-12-02',
                                         23.0,
                                         '2014-12-03T21:00:00.000000000+0000',
                                         'Firm A',
                                         'Account 2']},
                             {'values': [5,
                                         '2014-12-02',
                                         32.0,
                                         '2014-12-03T21:00:00.000000000+0000',
                                         'Firm A',
                                         'Account 1']},
                             {'values': [6,
                                         '2014-12-03',
                                         27.0,
                                         '2014-12-04T21:00:00.000000000+0000',
                                         'Firm C',
                                         'Account 1']},
                             {'values': [7,
                                         '2014-12-03',
                                         44.0,
                                         '2014-12-04T15:22:23.123456789+0000',
                                         'Firm C',
                                         'Account 1']},
                             {'values': [8,
                                         '2014-12-04',
                                         22.0,
                                         '2014-12-05T21:00:00.000000000+0000',
                                         'Firm C',
                                         'Account 2']},
                             {'values': [9,
                                         '2014-12-04',
                                         45.0,
                                         '2014-12-05T21:00:00.000000000+0000',
                                         'Firm C',
                                         'Account 2']},
                             {'values': [10, '2014-12-04', 38.0, None, 'Firm C',
                                         'Account 2']},
                             {'values': [11, '2014-12-05', 5.0, None, 'temp', 'temp']}]}

        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
