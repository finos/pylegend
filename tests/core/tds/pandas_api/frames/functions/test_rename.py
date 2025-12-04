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

from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToSqlConfig, FrameToPureConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend.core.request.legend_client import LegendClient


class TestRenameFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_rename_type_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # axis
        with pytest.raises(ValueError) as v:
            frame.rename({'col1': '123'}, axis=2)
        assert v.value.args[0] == "Unsupported axis 2"

        with pytest.raises(ValueError) as v:
            frame.rename({'col1': '123'}, axis='b')
        assert v.value.args[0] == "Unsupported axis b"

        # errors
        with pytest.raises(ValueError) as v:
            frame.rename({'col1': '123'}, errors='raisee')
        assert v.value.args[0] == "errors must be 'ignore' or 'raise'. Got raisee"

        with pytest.raises(ValueError) as v:
            frame.rename({'col1': '123'}, errors=2)  # type: ignore
        assert v.value.args[0] == "errors must be 'ignore' or 'raise'. Got 2"

        # mapper
        with pytest.raises(TypeError) as t:
            frame.rename(['col1', 'col2'])  # type: ignore
        assert t.value.args[0] == "Rename mapping must be a dict or a callable, got <class 'list'>"

        # columns
        with pytest.raises(TypeError) as v1:
            frame.rename(columns=['col1', 'col2'])  # type: ignore
        assert v1.value.args[0] == "Rename mapping must be a dict or a callable, got <class 'list'>"

        # copy
        with pytest.raises(TypeError) as v1:
            frame.rename({'col1': '123'}, copy='yes')  # type: ignore
        assert v1.value.args[0] == "copy must be bool. Got <class 'str'>"

        # inplace
        with pytest.raises(TypeError) as v1:
            frame.rename({'col1': '123'}, inplace='yes')  # type: ignore
        assert v1.value.args[0] == "inplace must be bool. Got <class 'str'>"

    def test_rename_notimplemented_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # axis
        with pytest.raises(NotImplementedError) as v:
            frame.rename({'col1': '123'}, axis=0)
        assert v.value.args[0] == "Renaming index not supported yet in Pandas API"

        # index
        with pytest.raises(NotImplementedError) as v:
            frame.rename({'col1': '123'}, index={'col1': '123'})
        assert v.value.args[0] == "Index mapper not supported yet in Pandas API"

        # level
        with pytest.raises(NotImplementedError) as v:
            frame.rename({'col1': '123'}, level=0)
        assert v.value.args[0] == "level parameter not supported yet in Pandas API"

        # copy
        with pytest.raises(NotImplementedError) as v:
            frame.rename({'col1': '123'}, copy=False)
        assert v.value.args[0] == "copy=False not supported yet in Pandas API"

        # inplace
        with pytest.raises(NotImplementedError) as v:
            frame.rename({'col1': '123'}, inplace=True)
        assert v.value.args[0] == "inplace=True not supported yet in Pandas API"

    def test_rename_validation_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # both mapper and columns
        with pytest.raises(ValueError) as v:
            frame.rename({'col1': '123'}, columns={'col2': '456'})
        assert v.value.args[0] == "Cannot specify both 'axis' and any of 'index' or 'columns'"

        # missing column
        with pytest.raises(KeyError) as k:
            frame.rename({'col3': '123', 'col4': '345', 'col1': '111'}, errors='raise')
        assert k.value.args[0] == "['col3', 'col4'] not found in axis"

        # duplicate column
        with pytest.raises(ValueError) as v:
            frame.rename({'col2': 'col1'})
        assert v.value.args[0] == "Resulting columns contain duplicates after rename"

    def test_rename(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # mapper
        renamed_frame = frame.rename({'col2': 'renamed_col2'}, axis=1)
        expected_sql = '''\
                         SELECT
                             "root".col1 AS "col1",
                             "root".col2 AS "renamed_col2",
                             "root".col3 AS "col3"
                         FROM
                             test_schema.test_table AS "root"'''
        assert renamed_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure_pretty = '''\
        #Table(test_schema.test_table)#
          ->project(
            ~[col1:x|$x.col1, renamed_col2:x|$x.col2, col3:x|$x.col3]
          )'''
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure_pretty)  # noqa: E501
        expected_pure_compact = "#Table(test_schema.test_table)#->project(~[col1:x|$x.col1, renamed_col2:x|$x.col2, col3:x|$x.col3])"  # noqa: E501
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

        # columns
        renamed_frame = frame.rename(columns={'col3': 'renamed_col3', 'col1': 'renamed_col1'})
        expected_sql = '''\
                         SELECT
                             "root".col1 AS "renamed_col1",
                             "root".col2 AS "col2",
                             "root".col3 AS "renamed_col3"
                         FROM
                             test_schema.test_table AS "root"'''
        assert renamed_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure_pretty = '''\
        #Table(test_schema.test_table)#
          ->project(
            ~[renamed_col1:x|$x.col1, col2:x|$x.col2, renamed_col3:x|$x.col3]
          )'''
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure_pretty)  # noqa: E501
        expected_pure_compact = "#Table(test_schema.test_table)#->project(~[renamed_col1:x|$x.col1, col2:x|$x.col2, renamed_col3:x|$x.col3])"  # noqa: E501
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

        # empty mapper
        renamed_frame = frame.rename({})
        expected_sql = '''\
                         SELECT
                             "root".col1 AS "col1",
                             "root".col2 AS "col2",
                             "root".col3 AS "col3"
                         FROM
                             test_schema.test_table AS "root"'''
        assert renamed_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure_pretty = '''\
        #Table(test_schema.test_table)#
          ->project(
            ~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3]
          )'''
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure_pretty)  # noqa: E501
        expected_pure_compact = "#Table(test_schema.test_table)#->project(~[col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3])"  # noqa: E501
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

    def test_rename_chained(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.string_column("col2"),
            PrimitiveTdsColumn.float_column("col3")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # rename
        renamed_frame = frame.rename({'col2': 'renamed_col2'}, axis=1).rename(columns={'col3': 'renamed_col3'})
        expected_sql = '''\
                         SELECT
                             "root".col1 AS "col1",
                             "root".col2 AS "renamed_col2",
                             "root".col3 AS "renamed_col3"
                         FROM
                             test_schema.test_table AS "root"'''
        assert renamed_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure_pretty = '''\
        #Table(test_schema.test_table)#
          ->project(
            ~[col1:x|$x.col1, renamed_col2:x|$x.col2, col3:x|$x.col3]
          )
          ->project(
            ~[col1:x|$x.col1, renamed_col2:x|$x.renamed_col2, renamed_col3:x|$x.col3]
          )'''
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure_pretty)  # noqa: E501
        expected_pure_compact = (
            "#Table(test_schema.test_table)#"
            "->project(~[col1:x|$x.col1, renamed_col2:x|$x.col2, col3:x|$x.col3])"
            "->project(~[col1:x|$x.col1, renamed_col2:x|$x.renamed_col2, renamed_col3:x|$x.col3])"
        )
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

        # truncate
        newframe = frame.truncate(before=2, after=5)
        renamed_frame = newframe.rename(columns={'col3': 'renamed_col3'})
        expected_sql = '''\
                         SELECT
                             "root".col1 AS "col1",
                             "root".col2 AS "col2",
                             "root".col3 AS "renamed_col3"
                         FROM
                             test_schema.test_table AS "root"
                         LIMIT 4
                         OFFSET 2'''
        assert renamed_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure_pretty = '''\
        #Table(test_schema.test_table)#
          ->slice(2, 6)
          ->project(
            ~[col1:x|$x.col1, col2:x|$x.col2, renamed_col3:x|$x.col3]
          )'''
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure_pretty)  # noqa: E501
        expected_pure_compact = "#Table(test_schema.test_table)#->slice(2, 6)->project(~[col1:x|$x.col1, col2:x|$x.col2, renamed_col3:x|$x.col3])"  # noqa: E501
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

        # filter
        renamed_frame = frame.rename({'col1': 'renamed_col1'}).filter(items=['renamed_col1', 'col2'])
        expected_sql = '''\
                         SELECT
                             "root".col1 AS "renamed_col1",
                             "root".col2 AS "col2"
                         FROM
                             test_schema.test_table AS "root"'''
        assert renamed_frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)
        expected_pure_pretty = '''\
        #Table(test_schema.test_table)#
          ->project(
            ~[renamed_col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3]
          )
          ->select(~[renamed_col1, col2])'''
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(), self.legend_client) == dedent(expected_pure_pretty)  # noqa: E501
        expected_pure_compact = (
            "#Table(test_schema.test_table)#"
            "->project(~[renamed_col1:x|$x.col1, col2:x|$x.col2, col3:x|$x.col3])"
            "->select(~[renamed_col1, col2])"
        )
        assert generate_pure_query_and_compile(renamed_frame, FrameToPureConfig(pretty=False), self.legend_client) == expected_pure_compact  # noqa: E501

    def test_e2e_rename(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # mapper
        newframe = frame.rename({'First Name': 'FirstName', 'Last Name': 'LastName'})
        expected = {
            "columns": ["FirstName", "LastName", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # columns
        newframe = frame.rename(columns={'Age': 'PersonAge', 'Firm/Legal Name': 'FirmName'})
        expected = {
            "columns": ["First Name", "Last Name", "PersonAge", "FirmName"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # empty mapper
        newframe = frame.rename({})
        expected = {
            "columns": ["First Name", "Last Name", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

    def test_e2e_rename_chained(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # rename
        newframe = frame.rename({'First Name': 'FirstName'}).rename(columns={'Last Name': 'LastName'})
        expected = {
            "columns": ["FirstName", "LastName", "Age", "Firm/Legal Name"],
            "rows": [
                {"values": ["Peter", "Smith", 23, "Firm X"]},
                {"values": ["John", "Johnson", 22, "Firm X"]},
                {"values": ["John", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B"]},
                {"values": ["David", "Harris", 35, "Firm C"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # truncate
        newframe = frame.truncate(before=2, after=5).rename(columns={'Age': 'PersonAge'})
        # python
        expected = {
            "columns": ["First Name", "Last Name", "PersonAge", "Firm/Legal Name"],
            "rows": [
                {"values": ["John", "Hill", 12, "Firm X"]},
                {"values": ["Anthony", "Allen", 22, "Firm X"]},
                {"values": ["Fabrice", "Roberts", 34, "Firm A"]},
                {"values": ["Oliver", "Hill", 32, "Firm B"]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected

        # Filter
        newframe = frame.rename({'First Name': 'FirstName'}).filter(items=['FirstName', 'Age'])
        expected = {
            "columns": ["FirstName", "Age"],
            "rows": [
                {"values": ["Peter", 23]},
                {"values": ["John", 22]},
                {"values": ["John", 12]},
                {"values": ["Anthony", 22]},
                {"values": ["Fabrice", 34]},
                {"values": ["Oliver", 32]},
                {"values": ["David", 35]},
            ],
        }
        res = newframe.execute_frame_to_string()
        assert json.loads(res)["result"] == expected
